# Count a plugin's garbage inside a call

Difficulty: Medium

`PluginHeap` takes all a call allocates as held until the call has returned: the census needs the frames' references, and `StackFrame`'s `localRef` is package-private in Endive. So one call may allocate what is free of `max-memory`, garbage or not. `sluice-client/src/test/guest/rontolisp` shows what that costs: rontolisp's `search` over a 64KiB body allocates past the default 16MB in one call, all garbage (the numbers: rontolisp's `.todo/i60`).

It also rests on `ExecutionListener`, which Endive marks experimental and calls on every instruction. And Endive keeps an `i8` of a `WasmArray` in a `long`, so a string costs 8 times its bytes.

## What to do

Propose to Endive (`https://github.com/bytecodealliance/endive`), most useful first:

1. A hook on allocation, or a limit of the instance's GC heap: no cost on other instructions, and no listener to lose.
2. Reading a running machine's roots, for a census at the moment a call would go past the limit. A missed root only makes the limit looser, so it cannot break a plugin.
3. `WasmArray` keeping packed elements packed.

With 2, `PluginHeap` counts anew before failing a call. Without an answer, a class in `run.endive.runtime` on the same class loader could read the frames, which a version of Endive may break: not before there is a plugin that needs it.

## Done when

A plugin allocating more than its `max-memory` of garbage in one call runs, one holding that much still fails, and `proxy-wasm-plugins.md` and `.kb/client-filters.md` lose the limitation.
