//! sluice-client's proxy-wasm plugins on wasmtime, for the JVM through FFM.
//!
//! A narrow C ABI of plain scalars and pointers, so the Java side reads no struct of
//! wasmtime's: a module compiled once, VMs (a store and its instance) made from it, an
//! export called by its index, the linear memory as a base and a size. Every import is
//! the one callback the host registers, told which import and the call's values.
//!
//! - One budget bounds all a VM holds: its linear memories, its GC heap, its tables.
//! - A call runs to an epoch deadline, the epoch ticked by a thread of this library.
//! - Nothing unwinds out: a panic, as a trap or an error, ends as a status and a message
//!   ([`sw_error`]).
//!
//! A VM is used by one thread at a time; a call into it from the callback (the plugin's
//! allocator, say) runs on the callback's caller.

use std::cell::{Cell, RefCell, UnsafeCell};
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
use std::sync::OnceLock;
use std::time::Duration;

use wasmtime::wasmparser::{Parser, Payload};
use wasmtime::{
    AsContextMut, Caller, Collector, Config, Engine, Error, ExternType, Func, InstancePre, Linker,
    Memory, Module, ResourceLimiter, Store, StoreContextMut, Trap, Val, ValType,
};

/// The host's one entry, for every import a module has: `import` is its index among the
/// module's imports, `args` its `count` values, each as 64 bits (an i32 sign extended),
/// and the slot of its result (`args[0]`), `memory` and `memory_size` the linear memory as
/// it is. Not [`OK`] when the host failed, which knows why: the call traps.
pub type HostCallback = unsafe extern "C-unwind" fn(
    import: u32,
    args: *mut u64,
    count: u32,
    memory: *mut u8,
    memory_size: u64,
) -> i32;

/// It went as asked.
pub const OK: i32 = 0;

/// It failed, trapped, ran past its deadline or out of its budget: see [`sw_error`].
pub const FAILED: i32 = 1;

/// The host's callback failed, during the call.
pub const HOST_FAILED: i32 = 2;

/// What an element of a table is charged.
pub const TABLE_ELEMENT: u64 = 8;

/// The most values an export or an import of a plugin takes.
const MAX_VALUES: usize = 16;

/// The description's layout, for [`sw_module_describe`].
const DESCRIPTION_VERSION: u32 = 1;

struct Runtime {
    engine: Engine,
    callback: HostCallback,
}

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

thread_local! {
    static ERROR: RefCell<String> = const { RefCell::new(String::new()) };
}

fn set_error(message: String) {
    ERROR.with(|error| *error.borrow_mut() = message);
}

/// Runs `body`, its failure and its panic both kept as this thread's error.
fn guarded<R>(failed: R, body: impl FnOnce() -> Result<R, String>) -> R {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(Ok(value)) => value,
        Ok(Err(message)) => {
            set_error(message);
            failed
        }
        Err(panic) => {
            set_error(panicked(panic.as_ref()));
            failed
        }
    }
}

fn panicked(panic: &(dyn std::any::Any + Send)) -> String {
    let message = panic
        .downcast_ref::<&str>()
        .map(|text| text.to_string())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "a panic".to_string());
    format!("panic: {message}")
}

fn runtime() -> Result<&'static Runtime, String> {
    RUNTIME
        .get()
        .ok_or_else(|| "sw_init was not called".to_string())
}

/// Starts the runtime, once in a process: the engine, its epoch's ticker. A later call
/// changes nothing. `wasm_stack` is the most stack a call takes: the thread calling must
/// have more, or a deep recursion ends the process.
///
/// # Safety
///
/// `callback` must stay callable for the process's life.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_init(callback: HostCallback, wasm_stack: u64, tick_millis: u64) -> i32 {
    guarded(FAILED, || {
        if RUNTIME.get().is_some() {
            return Ok(OK);
        }
        let mut config = Config::new();
        config
            .epoch_interruption(true)
            .max_wasm_stack(usize::try_from(wasm_stack).map_err(|e| e.to_string())?)
            .wasm_gc(true)
            .wasm_function_references(true)
            .wasm_exceptions(true)
            .collector(Collector::Copying)
            // a trap's message is all the host tells, and a failing plugin may trap often
            .wasm_backtrace_max_frames(None)
            // signals as on Linux, where the JVM's handlers have been tried with these
            .macos_use_mach_ports(false);
        let engine = Engine::new(&config).map_err(|e| format!("{e:#}"))?;
        let ticked = engine.clone();
        let tick = Duration::from_millis(tick_millis.max(1));
        std::thread::Builder::new()
            .name("sluice-plugin-epoch".to_string())
            .spawn(move || {
                loop {
                    std::thread::sleep(tick);
                    ticked.increment_epoch();
                }
            })
            .map_err(|e| e.to_string())?;
        let _ = RUNTIME.set(Runtime { engine, callback });
        Ok(OK)
    })
}

/// Copies this thread's last error message (UTF-8) into `out`, as much as fits; its
/// whole length.
///
/// # Safety
///
/// `out` must be valid for `capacity` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_error(out: *mut u8, capacity: usize) -> usize {
    ERROR.with(|error| {
        let error = error.borrow();
        let length = error.len().min(capacity);
        if length > 0 {
            // SAFETY: the caller's buffer holds `capacity` bytes
            unsafe { ptr::copy_nonoverlapping(error.as_ptr(), out, length) };
        }
        error.len()
    })
}

/// A value's kind as a call carries it.
#[derive(Clone, Copy)]
enum Kind {
    I32,
    I64,
    F32,
    F64,
    /// What no plugin of this host passes: refused at the call.
    Other,
}

impl Kind {
    fn of(ty: &ValType) -> Kind {
        match ty {
            ValType::I32 => Kind::I32,
            ValType::I64 => Kind::I64,
            ValType::F32 => Kind::F32,
            ValType::F64 => Kind::F64,
            _ => Kind::Other,
        }
    }

    fn val(self, bits: u64) -> Result<Val, String> {
        Ok(match self {
            Kind::I32 => Val::I32(bits as i32),
            Kind::I64 => Val::I64(bits as i64),
            Kind::F32 => Val::F32(bits as u32),
            Kind::F64 => Val::F64(bits),
            Kind::Other => return Err("a value of a type this host does not pass".to_string()),
        })
    }

    fn bits(value: &Val) -> u64 {
        match value {
            Val::I32(v) => *v as i64 as u64,
            Val::I64(v) => *v as u64,
            Val::F32(v) => u64::from(*v),
            Val::F64(v) => *v,
            _ => 0,
        }
    }
}

/// A function's values: what a call converts.
struct Signature {
    params: Box<[Kind]>,
    results: Box<[Kind]>,
}

impl Signature {
    fn of(ty: &wasmtime::FuncType) -> Signature {
        Signature {
            params: ty.params().map(|p| Kind::of(&p)).collect(),
            results: ty.results().map(|r| Kind::of(&r)).collect(),
        }
    }

    /// Of i32 and i64 only, one result at most: what the host's callback can serve.
    fn plain(&self) -> bool {
        let integer = |kind: &Kind| matches!(kind, Kind::I32 | Kind::I64);
        self.params.len() <= MAX_VALUES
            && self.results.len() <= 1
            && self.params.iter().all(integer)
            && self.results.iter().all(integer)
    }
}

/// One budget for all a store's memories and tables, its GC heap among the memories: the
/// pages and elements they have, whether touched or not.
#[derive(Default)]
struct Budget {
    limit: usize,
    used: usize,
    peak: usize,
    /// What the growth in progress was charged, given back if it fails.
    pending: usize,
}

impl Budget {
    fn charge(&mut self, bytes: usize) -> bool {
        match self.used.checked_add(bytes) {
            Some(used) if used <= self.limit => {
                self.used = used;
                self.peak = self.peak.max(used);
                self.pending = bytes;
                true
            }
            _ => false,
        }
    }

    fn refund(&mut self) {
        self.used -= self.pending;
        self.pending = 0;
    }
}

impl ResourceLimiter for Budget {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        Ok(self.charge(desired.saturating_sub(current)))
    }

    fn memory_grow_failed(&mut self, _error: Error) -> wasmtime::Result<()> {
        self.refund();
        Ok(())
    }

    fn table_growing(
        &mut self,
        current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> wasmtime::Result<bool> {
        let elements = desired.saturating_sub(current);
        Ok(self.charge(elements.saturating_mul(TABLE_ELEMENT as usize)))
    }

    fn table_grow_failed(&mut self, _error: Error) -> wasmtime::Result<()> {
        self.refund();
        Ok(())
    }
}

/// A store's own data.
struct Host {
    budget: Budget,
    memory: Option<Memory>,
    /// The export of the memory, for a callback of the start function.
    memory_export: Option<String>,
    /// Its VM's slot of the caller of the callback in progress.
    nested: *const Cell<*mut c_void>,
}

// SAFETY: `nested` points into the VM owning the store, which moves between threads with
// it and is used by one at a time
unsafe impl Send for Host {}

/// The host's callback failed: it knows why.
#[derive(Debug)]
struct HostFailed;

impl std::fmt::Display for HostFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("the host failed")
    }
}

impl std::error::Error for HostFailed {}

/// A compiled module, the instantiation of its VMs prepared, and its description.
pub struct PluginModule {
    /// None when an import is none the callback can serve: the host refuses the module.
    pre: Option<InstancePre<Host>>,
    memory: Option<String>,
    exports: Vec<Option<Signature>>,
    description: Vec<u8>,
}

/// Compiles a module; null when it is none, the reason in [`sw_error`].
///
/// # Safety
///
/// `bytes` must be valid for `length` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_module_new(bytes: *const u8, length: usize) -> *mut PluginModule {
    guarded(ptr::null_mut(), || {
        let runtime = runtime()?;
        // SAFETY: the caller's buffer holds `length` bytes
        let wasm = unsafe { std::slice::from_raw_parts(bytes, length) };
        let module = Module::new(&runtime.engine, wasm).map_err(|e| format!("{e:#}"))?;
        let mut linker = Linker::<Host>::new(&runtime.engine);
        let mut servable = true;
        for (index, import) in module.imports().enumerate() {
            let ExternType::Func(ty) = import.ty() else {
                servable = false;
                continue;
            };
            let signature = Signature::of(&ty);
            if !signature.plain() {
                servable = false;
                continue;
            }
            let callback = runtime.callback;
            let index = index as u32;
            linker
                .func_new(
                    import.module(),
                    import.name(),
                    ty,
                    move |caller, params, results| {
                        host_call(caller, callback, index, &signature, params, results)
                    },
                )
                .map_err(|e| format!("{e:#}"))?;
        }
        let pre = if servable {
            Some(
                linker
                    .instantiate_pre(&module)
                    .map_err(|e| format!("{e:#}"))?,
            )
        } else {
            None
        };
        let memory = module
            .exports()
            .find(|export| matches!(export.ty(), ExternType::Memory(_)))
            .map(|export| export.name().to_string());
        let exports = module
            .exports()
            .map(|export| match export.ty() {
                ExternType::Func(ty) => Some(Signature::of(&ty)),
                _ => None,
            })
            .collect();
        let description = describe(&module, wasm)?;
        Ok(Box::into_raw(Box::new(PluginModule {
            pre,
            memory,
            exports,
            description,
        })))
    })
}

/// What the host checks of a module before running it, little endian:
/// `version u32, defined memories u32, initial bytes u64` (its defined memories and
/// tables, as the budget charges them), `imports u32` of `kind u8, plain u8, module, name`,
/// `exports u32` of `kind u8, name`, each name a `u32` length and UTF-8. A kind is 0 for a
/// function, 1 a table, 2 a memory, 3 a global, 4 a tag; plain, an import the callback
/// can serve.
fn describe(module: &Module, wasm: &[u8]) -> Result<Vec<u8>, String> {
    let mut memories = 0u32;
    let mut initial = 0u64;
    for payload in Parser::new(0).parse_all(wasm) {
        match payload.map_err(|e| e.to_string())? {
            Payload::MemorySection(reader) => {
                for memory in reader {
                    let memory = memory.map_err(|e| e.to_string())?;
                    memories += 1;
                    let page = 1u64 << memory.page_size_log2.unwrap_or(16);
                    initial = initial.saturating_add(memory.initial.saturating_mul(page));
                }
            }
            Payload::TableSection(reader) => {
                for table in reader {
                    let table = table.map_err(|e| e.to_string())?;
                    initial =
                        initial.saturating_add(table.ty.initial.saturating_mul(TABLE_ELEMENT));
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    out.extend_from_slice(&DESCRIPTION_VERSION.to_le_bytes());
    out.extend_from_slice(&memories.to_le_bytes());
    out.extend_from_slice(&initial.to_le_bytes());
    let name = |out: &mut Vec<u8>, text: &str| {
        out.extend_from_slice(&(text.len() as u32).to_le_bytes());
        out.extend_from_slice(text.as_bytes());
    };
    out.extend_from_slice(&(module.imports().len() as u32).to_le_bytes());
    for import in module.imports() {
        let ty = import.ty();
        let plain = matches!(&ty, ExternType::Func(f) if Signature::of(f).plain());
        out.push(kind(&ty));
        out.push(u8::from(plain));
        name(&mut out, import.module());
        name(&mut out, import.name());
    }
    out.extend_from_slice(&(module.exports().len() as u32).to_le_bytes());
    for export in module.exports() {
        out.push(kind(&export.ty()));
        name(&mut out, export.name());
    }
    Ok(out)
}

fn kind(ty: &ExternType) -> u8 {
    match ty {
        ExternType::Func(_) => 0,
        ExternType::Table(_) => 1,
        ExternType::Memory(_) => 2,
        ExternType::Global(_) => 3,
        ExternType::Tag(_) => 4,
    }
}

/// Copies a module's description ([`describe`]) into `out` if it fits; its length.
///
/// # Safety
///
/// `module` must come from [`sw_module_new`], `out` be valid for `capacity` bytes.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_module_describe(
    module: *const PluginModule,
    out: *mut u8,
    capacity: usize,
) -> usize {
    guarded(0, || {
        // SAFETY: the caller's module
        let module = unsafe { module.as_ref() }.ok_or("no module")?;
        let description = &module.description;
        if description.len() <= capacity {
            // SAFETY: the caller's buffer holds `capacity` bytes
            unsafe { ptr::copy_nonoverlapping(description.as_ptr(), out, description.len()) };
        }
        Ok(description.len())
    })
}

/// # Safety
///
/// `module` must come from [`sw_module_new`], with no VM of it left, and not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_module_free(module: *mut PluginModule) {
    guarded((), || {
        if !module.is_null() {
            // SAFETY: the caller gives it up
            drop(unsafe { Box::from_raw(module) });
        }
        Ok(())
    })
}

/// A store and its instance.
pub struct Vm {
    store: UnsafeCell<Store<Host>>,
    /// The caller of the callback in progress, the context of what the host does meanwhile.
    nested: Cell<*mut c_void>,
    /// The module's exports by index; a function's resolved.
    exports: Vec<Option<Func>>,
    module: *const PluginModule,
}

impl Vm {
    /// The context to use now: the callback's caller during a call, the store otherwise.
    ///
    /// # Safety
    ///
    /// Only on the thread using the VM, the context not kept past `f`.
    unsafe fn with_context<R>(&self, f: impl FnOnce(StoreContextMut<'_, Host>) -> R) -> R {
        let nested = self.nested.get();
        if nested.is_null() {
            // SAFETY: no call is in progress, so nothing else borrows the store
            f(unsafe { &mut *self.store.get() }.as_context_mut())
        } else {
            // SAFETY: the callback's frame holds the caller until the callback returns
            f(unsafe { &mut *(nested as *mut Caller<'static, Host>) }.as_context_mut())
        }
    }
}

/// Instantiates a module within a budget of `max_memory` bytes, its start function run
/// within `deadline_ticks`; null on failure.
///
/// # Safety
///
/// `module` must come from [`sw_module_new`] and outlive the VM.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_vm_new(
    module: *const PluginModule,
    max_memory: u64,
    deadline_ticks: u64,
) -> *mut Vm {
    guarded(ptr::null_mut(), || {
        let runtime = runtime()?;
        // SAFETY: the caller's module
        let plugin = unsafe { module.as_ref() }.ok_or("no module")?;
        let pre = plugin
            .pre
            .as_ref()
            .ok_or("an import this host does not serve")?;
        let budget = Budget {
            limit: usize::try_from(max_memory).unwrap_or(usize::MAX),
            ..Budget::default()
        };
        let host = Host {
            budget,
            memory: None,
            memory_export: plugin.memory.clone(),
            nested: ptr::null(),
        };
        let mut store = Store::new(&runtime.engine, host);
        store.limiter(|host| &mut host.budget);
        store.set_epoch_deadline(deadline_ticks);
        let vm = Box::new(Vm {
            store: UnsafeCell::new(store),
            nested: Cell::new(ptr::null_mut()),
            exports: Vec::new(),
            module,
        });
        let vm = Box::into_raw(vm);
        // SAFETY: just made, and boxed: `nested` stays where it is
        let made = unsafe { instantiate(&mut *vm, plugin, pre) };
        if let Err(message) = made {
            // SAFETY: just made, not handed out
            drop(unsafe { Box::from_raw(vm) });
            return Err(message);
        }
        Ok(vm)
    })
}

fn instantiate(vm: &mut Vm, plugin: &PluginModule, pre: &InstancePre<Host>) -> Result<(), String> {
    let store = vm.store.get_mut();
    store.data_mut().nested = &vm.nested;
    let instance = pre.instantiate(&mut *store).map_err(failure)?;
    let memory = match &plugin.memory {
        Some(name) => instance.get_memory(&mut *store, name),
        None => None,
    };
    store.data_mut().memory = memory;
    vm.exports = instance
        .exports(&mut *store)
        .map(|export| export.into_func())
        .collect();
    Ok(())
}

/// # Safety
///
/// `vm` must come from [`sw_vm_new`], not be in a call, and not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_vm_free(vm: *mut Vm) {
    guarded((), || {
        if !vm.is_null() {
            // SAFETY: the caller gives it up
            drop(unsafe { Box::from_raw(vm) });
        }
        Ok(())
    })
}

/// Sets the deadline of what the VM runs from now: `ticks` of the epoch.
///
/// # Safety
///
/// `vm` must come from [`sw_vm_new`], on the thread using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_vm_deadline(vm: *const Vm, ticks: u64) {
    guarded((), || {
        // SAFETY: the caller's VM
        let vm = unsafe { vm.as_ref() }.ok_or("no VM")?;
        // SAFETY: on the thread using it, the context kept for this call only
        unsafe { vm.with_context(|mut context| context.set_epoch_deadline(ticks)) };
        Ok(())
    })
}

/// Calls the function of export `export` (its index among the module's exports) with
/// `count` values of `args`, each as 64 bits; its result, if it has one, in `args[0]`.
/// [`OK`], [`FAILED`] (the reason in [`sw_error`]) or [`HOST_FAILED`].
///
/// # Safety
///
/// `vm` must come from [`sw_vm_new`], on the thread using it; `args` hold at least one
/// value and `count`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_vm_call(vm: *const Vm, export: u32, args: *mut u64, count: u32) -> i32 {
    guarded(FAILED, || {
        // SAFETY: the caller's VM
        let vm = unsafe { vm.as_ref() }.ok_or("no VM")?;
        // SAFETY: the module outlives its VMs
        let plugin = unsafe { &*vm.module };
        let index = export as usize;
        let (Some(Some(func)), Some(Some(signature))) =
            (vm.exports.get(index), plugin.exports.get(index))
        else {
            return Err(format!("export {export} is no function"));
        };
        let count = count as usize;
        if count != signature.params.len() || count > MAX_VALUES || signature.results.len() > 1 {
            return Err(format!(
                "export {export} takes {} values",
                signature.params.len()
            ));
        }
        // read before the call: a call from the callback meanwhile may reuse the buffer
        // SAFETY: the caller's buffer holds `count` values
        let values = unsafe { std::slice::from_raw_parts(args, count) };
        let mut params = [Val::I32(0); MAX_VALUES];
        for ((param, kind), bits) in params
            .iter_mut()
            .zip(signature.params.iter())
            .zip(values.iter())
        {
            *param = kind.val(*bits)?;
        }
        let mut results = [Val::I32(0)];
        let results = &mut results[..signature.results.len()];
        // SAFETY: on the thread using it, the context kept for this call only
        let called =
            unsafe { vm.with_context(|context| func.call(context, &params[..count], results)) };
        match called {
            Ok(()) => {
                if let Some(result) = results.first() {
                    // SAFETY: the caller's buffer holds one value at least
                    unsafe { args.write(Kind::bits(result)) };
                }
                Ok(OK)
            }
            Err(e) if e.chain().any(|cause| cause.is::<HostFailed>()) => Ok(HOST_FAILED),
            Err(e) => Err(failure(e)),
        }
    })
}

/// A failure's message, a trap's in the host's words.
fn failure(error: Error) -> String {
    match error.downcast_ref::<Trap>() {
        Some(Trap::Interrupt) => "ran past its call-timeout".to_string(),
        Some(trap) => trap.to_string(),
        None => error.root_cause().to_string(),
    }
}

/// The linear memory as it is now, its size in `size`; null with none.
///
/// # Safety
///
/// `vm` must come from [`sw_vm_new`], on the thread using it; what is returned is valid
/// until the next call into the VM.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_vm_memory(vm: *const Vm, size: *mut u64) -> *mut u8 {
    guarded(ptr::null_mut(), || {
        // SAFETY: the caller's VM
        let vm = unsafe { vm.as_ref() }.ok_or("no VM")?;
        // SAFETY: on the thread using it, the context kept for this call only
        let (base, length) = unsafe { vm.with_context(memory_of) };
        // SAFETY: the caller's slot
        unsafe { *size = length };
        Ok(base)
    })
}

fn memory_of(context: StoreContextMut<'_, Host>) -> (*mut u8, u64) {
    match context.data().memory {
        Some(memory) => (memory.data_ptr(&context), memory.data_size(&context) as u64),
        None => (ptr::null_mut(), 0),
    }
}

/// What the VM's budget is charged now, and at most so far in `peak`.
///
/// # Safety
///
/// `vm` must come from [`sw_vm_new`], on the thread using it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sw_vm_used(vm: *const Vm, peak: *mut u64) -> u64 {
    guarded(0, || {
        // SAFETY: the caller's VM
        let vm = unsafe { vm.as_ref() }.ok_or("no VM")?;
        // SAFETY: on the thread using it, the context kept for this call only
        let (used, most) = unsafe {
            vm.with_context(|context| {
                let budget = &context.data().budget;
                (budget.used as u64, budget.peak as u64)
            })
        };
        // SAFETY: the caller's slot
        unsafe { *peak = most };
        Ok(used)
    })
}

/// The caller of a callback in progress, in its VM's slot until the callback returns.
struct Nested<'a> {
    slot: &'a Cell<*mut c_void>,
    previous: *mut c_void,
}

impl Drop for Nested<'_> {
    fn drop(&mut self) {
        self.slot.set(self.previous);
    }
}

/// An import's call, through the host's callback.
fn host_call(
    mut caller: Caller<'_, Host>,
    callback: HostCallback,
    index: u32,
    signature: &Signature,
    params: &[Val],
    results: &mut [Val],
) -> wasmtime::Result<()> {
    let mut args = [0u64; MAX_VALUES];
    for (slot, param) in args.iter_mut().zip(params) {
        *slot = Kind::bits(param);
    }
    if caller.data().memory.is_none()
        && let Some(name) = caller.data().memory_export.clone()
    {
        // the start function runs before the instance is handed out
        let memory = caller
            .get_export(&name)
            .and_then(|export| export.into_memory());
        caller.data_mut().memory = memory;
    }
    let (base, size) = memory_of(caller.as_context_mut());
    let slot = caller.data().nested;
    // SAFETY: the slot is in the VM owning this store, which outlives the call
    let slot = unsafe { &*slot };
    let nested = Nested {
        slot,
        previous: slot.replace(&mut caller as *mut Caller<'_, Host> as *mut c_void),
    };
    // a panic of the host traps, rather than unwind through the plugin's frames
    let called = catch_unwind(AssertUnwindSafe(|| {
        // SAFETY: the callback sw_init was given, with `params.len()` values in `args`
        unsafe { callback(index, args.as_mut_ptr(), params.len() as u32, base, size) }
    }));
    drop(nested);
    let status = called.map_err(|panic| Error::msg(panicked(panic.as_ref())))?;
    if status != OK {
        return Err(Error::new(HostFailed));
    }
    if let (Some(result), Some(kind)) = (results.first_mut(), signature.results.first()) {
        *result = kind.val(args[0]).map_err(Error::msg)?;
    }
    Ok(())
}
