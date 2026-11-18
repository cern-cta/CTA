# cta-model-macros

Procedural macros of [`cta-model`](../cta-model/README.md). Use them through `cta-model`, which re-exports them
and provides the traits the generated code implements.

- **`#[derive(Keyed)]`** implements `cta_model::Keyed` from the fields listed by `#[key(...)]`, borrowing the
  strings. See the documentation of `cta_model::Keyed` for the syntax.
