;; A plugin as a compiler for a language with objects emits one: its objects on the engine's
;; heap, linear memory only where the ABI wants pointers, a command rather than a reactor.
;; On a response it does what the first letter of the request's path says.
(module
  (rec
    (type $bytes (array (mut i8)))
    (type $node (struct (field $next (ref null $node)) (field $data (ref null $bytes)))))
  (type $count (struct (field $value (mut i32))))
  (type $thrown (struct (field $next (ref null $thrown)) (field $exception (ref null exn))))
  (tag $error)

  (import "env" "proxy_get_header_map_value" (func $get (param i32 i32 i32 i32 i32) (result i32)))
  (import "env" "proxy_add_header_map_value" (func $add (param i32 i32 i32 i32 i32) (result i32)))

  (memory (export "memory") 2)
  (table $table 0 funcref)

  (data (i32.const 1024) ":path")
  (data (i32.const 1032) "x-plugin")
  (data (i32.const 1040) "x-count")
  (data $value "built on the heap")

  (global $starts (mut i32) (i32.const 0))
  (global $action (mut i32) (i32.const 0))
  (global $free (mut i32) (i32.const 65536))
  (global $count (mut (ref null $count)) (ref.null $count))
  (global $held (mut (ref null $node)) (ref.null $node))
  (global $thrown (mut (ref null $thrown)) (ref.null $thrown))

  (func (export "_start")
    (global.set $starts (i32.add (global.get $starts) (i32.const 1)))
    (global.set $count (struct.new $count (i32.const 0))))

  (func (export "proxy_abi_version_0_2_1"))

  (func (export "proxy_on_memory_allocate") (param $size i32) (result i32)
    (local $at i32)
    (local.set $at (global.get $free))
    (global.set $free (i32.add (local.get $at) (local.get $size)))
    (local.get $at))

  (func (export "proxy_on_context_create") (param i32 i32))

  ;; Its start ran once, or it refuses.
  (func (export "proxy_on_vm_start") (param i32 i32) (result i32)
    (i32.eq (global.get $starts) (i32.const 1)))

  (func (export "proxy_on_configure") (param i32 i32) (result i32) (i32.const 1))

  (func (export "proxy_on_request_headers") (param i32 i32 i32) (result i32)
    (global.set $free (i32.const 65536))
    (drop (call $get (i32.const 0) (i32.const 1024) (i32.const 5) (i32.const 2000) (i32.const 2004)))
    (global.set $action (i32.load8_u (i32.add (i32.load (i32.const 2000)) (i32.const 1))))
    (i32.const 0))

  (func (export "proxy_on_request_body") (param i32 i32 i32) (result i32) (i32.const 0))

  (func (export "proxy_on_response_headers") (param i32 i32 i32) (result i32)
    (local $action i32)
    (local.set $action (global.get $action))
    (if (i32.eq (local.get $action) (i32.const 0x65 (; e ;))) (then (call $edit)))
    (if (i32.eq (local.get $action) (i32.const 0x67 (; g ;))) (then (call $garbage)))
    (if (i32.eq (local.get $action) (i32.const 0x61 (; a ;))) (then (call $hold_array)))
    (if (i32.eq (local.get $action) (i32.const 0x73 (; s ;))) (then (call $hold_structs)))
    (if (i32.eq (local.get $action) (i32.const 0x72 (; r ;))) (then (call $hold_exceptions)))
    (if (i32.eq (local.get $action) (i32.const 0x74 (; t ;))) (then (call $grow_table)))
    (if (i32.eq (local.get $action) (i32.const 0x6d (; m ;))) (then (call $grow_memory)))
    (if (i32.eq (local.get $action) (i32.const 0x68 (; h ;))) (then (call $huge)))
    (i32.const 0))

  (func (export "proxy_on_response_body") (param i32 i32 i32) (result i32) (i32.const 0))
  (func (export "proxy_on_done") (param i32) (result i32) (i32.const 1))
  (func (export "proxy_on_log") (param i32))
  (func (export "proxy_on_delete") (param i32))

  ;; From an array to linear memory byte by byte, and a count kept in a struct across calls.
  (func $edit
    (local $from (ref null $bytes))
    (local $i i32)
    (local $count i32)
    (local.set $from (array.new_data $bytes $value (i32.const 0) (i32.const 17)))
    (block $done
      (loop $copy
        (br_if $done (i32.ge_u (local.get $i) (array.len (local.get $from))))
        (i32.store8 (i32.add (i32.const 2048) (local.get $i))
          (array.get_u $bytes (local.get $from) (local.get $i)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $copy)))
    (drop (call $add (i32.const 2) (i32.const 1032) (i32.const 8) (i32.const 2048) (i32.const 17)))
    (local.set $count (i32.add (struct.get $count $value (global.get $count)) (i32.const 1)))
    (struct.set $count $value (global.get $count) (local.get $count))
    (i32.store8 (i32.const 2048) (i32.add (i32.const 0x30) (local.get $count)))
    (drop (call $add (i32.const 2) (i32.const 1040) (i32.const 7) (i32.const 2048) (i32.const 1))))

  (func $garbage
    (drop (array.new_default $bytes (i32.const 0x100000))))

  (func $hold_array
    (global.set $held
      (struct.new $node (global.get $held) (array.new_default $bytes (i32.const 0x100000)))))

  (func $hold_structs
    (local $i i32)
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (i32.const 0x10000)))
        (global.set $held (struct.new $node (global.get $held) (ref.null $bytes)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next))))

  (func $hold_exceptions
    (local $i i32)
    (block $done
      (loop $next
        (br_if $done (i32.ge_u (local.get $i) (i32.const 0x8000)))
        (global.set $thrown (struct.new $thrown (global.get $thrown) (call $caught)))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $next))))

  (func $caught (result (ref null exn))
    (block $caught (result (ref null exn))
      (try_table (catch_ref $error $caught)
        (throw $error))
      (ref.null exn)))

  (func $grow_table
    (drop (table.grow $table (ref.null func) (i32.const 0x100000))))

  (func $grow_memory
    (drop (memory.grow (i32.const 64))))

  (func $huge
    (drop (array.new_default $bytes (i32.const 0x1000000))))
)
