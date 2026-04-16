# Repository Guidelines

## Change Acceptance

- Any code change is accepted only if `prek -a` passes.
- Any code change is accepted only if the relevant unit tests pass.

## Rust Visibility

- Prefer controlling visibility at the type level.
- If a type is declared `pub(crate)`, its impl methods should generally use
  `pub` rather than repeating `pub(crate)` on each method.
- Only narrow method visibility below the type when there is a specific
  reason to do so.

## SNAFU Error Construction

- If you already have a `Result<T, E>` and only need to attach extra context,
  prefer `.context(...)` instead of hand-writing
  `map_err(|source| Error::... { source, ... })`.
- If a SNAFU selector needs to be used across module or crate boundaries, it
  is acceptable to adjust selector visibility with
  `#[snafu(visibility(...))]` so `.context(...)` remains available.
- If `map_err` is only doing a pure conversion such as `Type::from`, prefer
  using `?` directly to avoid unnecessary chaining.
- Only prefer `IntoError` when you no longer have a `Result`, but instead have
  a bare `source` error value and need to construct a SNAFU error that
  includes that `source`.
- If the error branch also performs extra side effects, such as persisting
  failure nodes, recording state, or branching on error kinds, keeping an
  explicit `match` is usually clearer than forcing a chained style.
- Avoid directly constructing `Error::Variant { ... }` as the default style;
  the usual preference order is `.context(...)`, then `IntoError`, and only
  then a hand-written variant.
- Do not use `String` as a cross-module or public-facing error channel; define
  a typed SNAFU error instead and convert to text only at the outer
  presentation boundary.
