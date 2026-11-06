//! The C ABI as the JVM uses it: a module, VMs of it, calls, the callback.

use sluice_client_wasm::{
    FAILED, HOST_FAILED, OK, TABLE_ELEMENT, Vm, sw_error, sw_init, sw_module_describe,
    sw_module_new, sw_vm_call, sw_vm_deadline, sw_vm_free, sw_vm_memory, sw_vm_new, sw_vm_used,
};
use std::cell::RefCell;
use std::panic::{AssertUnwindSafe, catch_unwind, resume_unwind};

const PAGE: u64 = 64 * 1024;

/// A call's import, its values, and the memory as it is.
type Host = Box<dyn FnMut(u32, &mut [u64], *mut u8, u64) -> i32>;

thread_local! {
    static HOST: RefCell<Option<Host>> = const { RefCell::new(None) };
}

unsafe extern "C-unwind" fn callback(
    import: u32,
    args: *mut u64,
    count: u32,
    memory: *mut u8,
    size: u64,
) -> i32 {
    // SAFETY: the library's buffer holds `count` values, and one at least
    let args = unsafe { std::slice::from_raw_parts_mut(args, (count as usize).max(1)) };
    let mut host = HOST.with(|host| host.borrow_mut().take()).expect("a host");
    let status = catch_unwind(AssertUnwindSafe(|| host(import, args, memory, size)));
    HOST.with(|slot| *slot.borrow_mut() = Some(host));
    status.unwrap_or_else(|panic| resume_unwind(panic))
}

fn init() {
    // SAFETY: a plain function, for the process's life
    assert_eq!(unsafe { sw_init(callback, 512 * 1024, 10) }, OK);
}

fn error() -> String {
    let mut out = vec![0u8; 4096];
    // SAFETY: a buffer of that many bytes
    let length = unsafe { sw_error(out.as_mut_ptr(), out.len()) };
    out.truncate(length);
    String::from_utf8(out).unwrap()
}

fn instantiate(wat: &str, budget: u64) -> Result<*mut Vm, String> {
    init();
    let wasm = wat::parse_str(wat).unwrap();
    // SAFETY: the bytes and their length
    let module = unsafe { sw_module_new(wasm.as_ptr(), wasm.len()) };
    assert!(!module.is_null(), "{}", error());
    // SAFETY: a module just made, never freed here
    let vm = unsafe { sw_vm_new(module, budget, 100) };
    if vm.is_null() { Err(error()) } else { Ok(vm) }
}

fn call(vm: *mut Vm, export: u32, args: &[u64]) -> Result<u64, (i32, String)> {
    let mut values = [0u64; 16];
    values[..args.len()].copy_from_slice(args);
    // SAFETY: a VM of this thread, a buffer of 16 values
    let status = unsafe { sw_vm_call(vm, export, values.as_mut_ptr(), args.len() as u32) };
    if status == OK {
        Ok(values[0])
    } else {
        Err((
            status,
            if status == FAILED {
                error()
            } else {
                String::new()
            },
        ))
    }
}

/// Memories, the GC heap and tables come out of one budget, at instantiation too; what
/// it refuses fails the growth, or the call allocating.
#[test]
fn a_vm_holds_what_its_budget_lets_it() {
    let wat = r#"(module
        (type $bytes (array (mut i8)))
        (memory (export "memory") 1)
        (table $t 1 funcref)
        (func (export "grow_memory") (param i32) (result i32) (memory.grow (local.get 0)))
        (func (export "grow_table") (param i32) (result i32) (table.grow $t (ref.null func) (local.get 0)))
        (func (export "array") (param i32) (result i32) (array.len (array.new_default $bytes (local.get 0)))))"#;
    let vm = instantiate(wat, 8 * PAGE).unwrap();
    let mut peak = 0;

    assert_eq!(call(vm, 1, &[3]), Ok(1));
    // SAFETY: a VM of this thread
    assert_eq!(
        unsafe { sw_vm_used(vm, &mut peak) },
        4 * PAGE + TABLE_ELEMENT
    );
    assert_eq!(call(vm, 3, &[PAGE]), Ok(PAGE));
    let refused = call(vm, 3, &[4 * PAGE]).unwrap_err();
    assert_eq!(refused.0, FAILED);
    assert!(refused.1.contains("GC heap out of memory"), "{}", refused.1);
    // SAFETY: a VM of this thread
    let used = unsafe { sw_vm_used(vm, &mut peak) };
    assert!(
        used > 4 * PAGE + TABLE_ELEMENT && used <= 8 * PAGE && peak == used,
        "{used} {peak}"
    );
    let left = 8 * PAGE - used;
    assert_eq!(call(vm, 2, &[left / TABLE_ELEMENT + 1]), Ok(u64::MAX));
    assert_eq!(call(vm, 1, &[left / PAGE + 1]), Ok(u64::MAX));
    assert_eq!(call(vm, 2, &[left / TABLE_ELEMENT]), Ok(1));
    // SAFETY: a VM of this thread
    assert_eq!(
        unsafe { sw_vm_used(vm, &mut peak) },
        used + left / TABLE_ELEMENT * TABLE_ELEMENT
    );
    // SAFETY: not in a call, not used again
    unsafe { sw_vm_free(vm) };

    let past = instantiate(
        &wat.replace(
            "(memory (export \"memory\") 1)",
            "(memory (export \"memory\") 9)",
        ),
        8 * PAGE,
    );
    assert!(past.unwrap_err().contains("memory"));
}

/// Its memory, the module's exports and imports as the host checks them.
#[test]
fn a_module_is_described() {
    init();
    let wasm = wat::parse_str(
        r#"(module
            (import "env" "plain" (func (param i32 i64) (result i32)))
            (import "env" "float" (func (param f32)))
            (memory (export "memory") 2)
            (table 3 funcref)
            (func (export "f")))"#,
    )
    .unwrap();
    // SAFETY: the bytes and their length
    let module = unsafe { sw_module_new(wasm.as_ptr(), wasm.len()) };
    let mut out = vec![0u8; 256];
    // SAFETY: a module, a buffer of that many bytes
    let length = unsafe { sw_module_describe(module, out.as_mut_ptr(), out.len()) };
    out.truncate(length);

    let mut expected = Vec::new();
    expected.extend_from_slice(&1u32.to_le_bytes());
    expected.extend_from_slice(&1u32.to_le_bytes());
    expected.extend_from_slice(&(2 * PAGE + 3 * TABLE_ELEMENT).to_le_bytes());
    expected.extend_from_slice(&2u32.to_le_bytes());
    for (plain, name) in [(1u8, "plain"), (0, "float")] {
        expected.extend_from_slice(&[0, plain, 3, 0, 0, 0]);
        expected.extend_from_slice(b"env");
        expected.extend_from_slice(&(name.len() as u32).to_le_bytes());
        expected.extend_from_slice(name.as_bytes());
    }
    expected.extend_from_slice(&2u32.to_le_bytes());
    expected.extend_from_slice(&[2, 6, 0, 0, 0]);
    expected.extend_from_slice(b"memory");
    expected.extend_from_slice(&[0, 1, 0, 0, 0, b'f']);
    assert_eq!(out, expected);
    // SAFETY: the module, its budget
    assert!(unsafe { sw_vm_new(module, 16 * PAGE, 100) }.is_null());
    assert_eq!(error(), "an import this host does not serve");
    // SAFETY: the bytes and their length
    assert!(unsafe { sw_module_new([0u8; 4].as_ptr(), 4) }.is_null());
}

/// The callback serves the imports, calling into the VM meanwhile; its failure and its
/// panic trap, as a deadline or the stack running out do, and the VM goes on.
#[test]
fn a_call_ends_as_a_status_whatever_happens() {
    let wat = r#"(module
        (import "env" "host" (func $host (param i32 i64) (result i32)))
        (memory (export "memory") 1)
        (data (i32.const 16) "hello")
        (func (export "call") (param i32) (result i32) (call $host (local.get 0) (i64.const -1)))
        (func (export "allocate") (param i32) (result i32) (i32.add (local.get 0) (i32.const 1024)))
        (func (export "spin") (loop (br 0)))
        (func $recurse (export "recurse") (param i32) (result i32)
            (i32.add (call $recurse (local.get 0)) (i32.const 1))))"#;
    let vm = instantiate(wat, 16 * PAGE).unwrap();
    let address = vm as usize;
    HOST.with(|host| {
        *host.borrow_mut() = Some(Box::new(move |import, args, memory, size| {
            assert_eq!(import, 0);
            assert_eq!(size, PAGE);
            // SAFETY: the memory as it is during the callback
            let memory = unsafe { std::slice::from_raw_parts_mut(memory, size as usize) };
            assert_eq!(&memory[16..21], b"hello");
            assert_eq!(args[1], u64::MAX);
            match args[0] {
                0 => {
                    let mut nested = [7u64];
                    // SAFETY: the VM in its call, on its thread
                    let status =
                        unsafe { sw_vm_call(address as *const Vm, 2, nested.as_mut_ptr(), 1) };
                    assert_eq!(status, OK);
                    let mut length = 0;
                    // SAFETY: the VM in its call
                    let base = unsafe { sw_vm_memory(address as *const Vm, &mut length) };
                    assert_eq!((base, length), (memory.as_mut_ptr(), PAGE));
                    args[0] = nested[0];
                    OK
                }
                1 => 1,
                _ => panic!("in the host"),
            }
        }))
    });

    assert_eq!(call(vm, 1, &[0]), Ok(1031));
    assert_eq!(call(vm, 1, &[1]), Err((HOST_FAILED, String::new())));
    assert_eq!(
        call(vm, 1, &[2]),
        Err((FAILED, "panic: in the host".to_string()))
    );
    // SAFETY: a VM of this thread
    unsafe { sw_vm_deadline(vm, 2) };
    assert_eq!(
        call(vm, 3, &[]),
        Err((FAILED, "ran past its call-timeout".to_string()))
    );
    // SAFETY: a VM of this thread
    unsafe { sw_vm_deadline(vm, 100) };
    assert_eq!(
        call(vm, 4, &[0]),
        Err((FAILED, "wasm trap: call stack exhausted".to_string()))
    );
    assert_eq!(call(vm, 1, &[0]), Ok(1031));
    assert_eq!(call(vm, 0, &[]).unwrap_err().1, "export 0 is no function");
    // SAFETY: not in a call, not used again
    unsafe { sw_vm_free(vm) };
}
