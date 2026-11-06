Difficulty: Medium

# Built-in filter: redact

Replace patterns in text bodies as they stream, on requests, responses or both.

## Why

Masking secrets or personal data before it leaves the host (a token in an error page, an email in a log API) is the README's own example of a filter, and today it needs a plugin. A built-in streams without holding the body and without the copies into a plugin's memory.

## Scope

- Settings in `args`: the patterns and their replacements, the side, the content types (text and JSON by default).
- Literal patterns first. A regex only with a bounded match length (a setting, checked at the start), so a body is rewritten while it streams with a carry of that length between chunks. Decide whether an unbounded regex is refused or needs the whole body held within the budget.
- An encoded body is left as it is, counted as `outcome=encoded` as `llm-usage` does, unless `compression` (240) decoded it before.
- Counters per route: bodies seen, replacements made.

## Done when

A pattern split across chunk boundaries is replaced, a body without a match passes byte for byte, SSE stays live, `README.md` "Filters" lists it, and `.kb/client-filters.md` describes the limits.
