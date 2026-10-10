Difficulty: Medium

# sluice-wasmlet: reuse guest instances via a persistent store-worker pool

Trigger: load testing shows instantiation in the profile. The warm path is
~200us today (`Loaded` doc in `wasm_host.rs`, the ignored
`instantiation_latency_reference`); at ~1k req/s that is ~0.2 core and the
point where a pool starts paying.

## Design (the only viable shape)

`Store::run_concurrent` consumes the store, so reuse cannot return instances
per request. Instead each pooled unit is a detached task that owns
`(Store, Service)` for its lifetime and runs `run_concurrent` **once**, with a
per-unit request channel loop inside:

    unit = tokio::spawn(async move {
        store.run_concurrent(async |accessor| {
            while let Some(job) = rx.recv().await {
                store.set_epoch_deadline(...);        // re-arm per request
                let result = service.handle(accessor, job.req).await;
                job.ack.send(result / stream handles);
            }
        })
    })

- pool per route (`Loaded`); idle units in a `Mutex<Vec<...>>`, bounded
- on exhaustion: fall back to the current instantiate-per-request path
  (hybrid keeps latency flat and the pool optional)
- response streaming keeps the current shape: the unit task relays body
  frames; the borrow of `accessor` must not span the whole body drain if
  that blocks the loop -- verify, and if it does, hand the body relay to a
  per-request task fed by a channel from inside the loop

## Correctness duties

- returned units must have an empty `ResourceTable` (check or drain; recycle
  the unit otherwise)
- guests keep module state across requests on the same unit: document that
  pooling assumes stateless / reentrant-safe guests; a misbehaving guest is
  a per-unit problem, not a process one
- hung guests block their unit: the existing epoch budget bounds the request,
  but a unit that wedges (broken channel state, stuck fiber) must be
  detected and replaced -- unit-level watchdog or recycle-on-error
- `Ctrl::Abort` / client disappearance must not poison the unit: cancel the
  in-flight handle, verify the store is idle, recycle or drop

## Acceptance

- warm path serves requests with no `instantiate_async` (assert via a counter
  or by construction); cold start and pool exhaustion still work
- a guest that panics mid-request leaves the unit usable or gets it recycled;
  subsequent requests on the route keep succeeding
- the busy-loop `/spin` and `/balloon` guests still degrade to clean
  responses with the pool enabled
- load comparison cold vs pooled recorded in the todo history on completion
