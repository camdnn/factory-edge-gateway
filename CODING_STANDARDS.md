# Coding standards

The rules a reviewer applies to a diff. Each is a judgement call: formatting, vet and the test suite are enforced by `scripts/check.sh`.

## Tests

- Test behaviour through a module's public interface. Machine-status tests build observations and assert the resulting rows; store tests write then read through the store's own methods, never querying the database.
- Expected values are literals from a worked example (`time.Date(2026, 10, 21, 11, 59, 0, 0, time.UTC)`), not recomputed the way the code computes them.
- Pure functions take what they depend on (`now`, status history) as inputs instead of reading the clock or storage, so tests need no fakes.

## Go

- Composite literals of the repo's own struct types name their fields: `Event{At: g.DetectedAt, Kind: "gap"}`. Rows of an anonymous table-test struct are exempt.
- Inline comments give the reason in one line: `// compare timestamps, not sequences: sequences restart when the agent does`.
- Names and comments use the words in `GLOSSARY.md`, never its _Avoid_ terms.
