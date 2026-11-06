Difficulty: Medium

# Built-in filter: llm-guard

Limit what requests to the LLM APIs `llm-usage` knows may ask for: the models allowed and a ceiling on the output tokens.

## Why

`llm-usage` counts what a shared API key spends, and nothing stops a request from spending it. A route sharing a key through the tunnel needs the cheap limits on the request: no model outside a list, no `max_tokens` past a ceiling.

## Scope

- The same APIs and paths as `llm-usage` (OpenAI compatible, Anthropic), and the request body read with `JsonFields`.
- `models`: a request for another model is answered locally with `400`, in the API's own error format, before anything reaches the upstream.
- `max-output-tokens`: decide whether a larger `max_tokens` / `max_completion_tokens` / `max_output_tokens` is rejected or lowered. Lowering rewrites the body and holds it within the tunnel budget, as `include-usage` does.
- A body it cannot read (encoded, malformed, too large to hold) is refused, not passed: the filter is a limit, so it fails closed.
- Counters per route and outcome (`allowed`, `model`, `max_tokens`, `unreadable`).
- Not in scope: a token budget per period from `llm-usage`'s counters. File it once these limits are used.

## Done when

A request for a model outside the list and one past the ceiling never reach the upstream, streaming requests behave the same, `README.md` "Filters" lists it, and `.kb/client-filters.md` describes it beside `llm-usage`.
