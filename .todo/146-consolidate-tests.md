Difficulty: Medium

# Reduce the test count: merge and parameterize redundant tests

Requested 2026-10-17: the test suite keeps growing (maintenance cost, CI test job ~6 min).
Cover behaviors by varying the conditions of existing tests rather than adding new ones,
and bring the total down.

## Work

- Inventory the tests per area (`sluice-it` E2E classes first: each class starts a full
  server + client stack, so classes cost the most), listing what each one proves.
- Merge tests that prove the same behavior or differ only in a parameter (JUnit
  `@ParameterizedTest`, one stack shared by several cases where isolation allows), and delete
  ones fully covered by others.
- Keep every behavior covered: for each removed test, name the test that still fails
  without that behavior. Do not weaken a test's preconditions pinning.
- Report the count of test classes / methods and the CI test job time before and after.
