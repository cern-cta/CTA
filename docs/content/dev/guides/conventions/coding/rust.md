# Rust Conventions

Follow the [General Coding Conventions](general.md) and the
[Rust Style Guide](https://doc.rust-lang.org/stable/style-guide/index.html).

## Tooling

- Run `cargo fmt` for formatting. Don't hand-format, focus on writing good code;
- Run `cargo clippy` for static checks. Fix warnings rather than silencing them;
- Prefer `#[expect(...)]` over `#[allow(...)]` whenever possible;
- When using `#[expect(...)]` or `#[allow(...)]`, scope them as narrowly as possible
  and then explain why, using the `reason` attribute;
- Set up the [pre-commit hooks](../../tools-and-environment/pre-commit.md) so problems are
  caught before they reach the CI;
- Make sure your code runs fine with the version pinned in `rust-toolchain.toml`.

## Code structure

- Group related functions, types and traits into small, focused modules;
- Extract self-contained behaviour into its own module or crate when it could be reused;
- Keep the public API minimal: default to private, use `pub(crate)` where possible, and
  make something `pub` only when it is a deliberate part of the interface;
- Prefer borrowing (`&str`, `&[T]`) over owning (`String`, `Vec<T>`) in function arguments;
- Use newtypes and enums to make invalid states unrepresentable, instead of bare
  primitives or stringly-typed values;
- Prefer iterators and combinators over manual indexing, where this stays readable.

## Error handling

- Return `Result` and propagate errors with `?`. Don't swallow them;
- Use `thiserror` for typed errors in libraries, and `anyhow` for end-user binaries.
  Add context with `.context(...)` / `.with_context(...)` when using `anyhow`;
- Document the failure modes of public functions in an `# Errors` section;
- Avoid panicking in library code. Where a panic is acceptable (tests, `main`, or a
  violated invariant that is a genuine bug), use `.expect("...")` with a message
  stating *why* it can't fail. If the likelihood is practically zero, you can also use an
  `.unwrap()`, as long as you add a comment justifying it.
- Exception: `.unwrap()` is fine in tests.
- Document panics in a `# Panics` section on public functions.

## Unsafe code

- Avoid `unsafe` unless strictly required;
- Keep each `unsafe` block as small as possible;
- Every `unsafe` block must have a `// SAFETY:` comment explaining why it is sound;
  Every `unsafe fn` must have a `# Safety` doc section describing the caller's obligations;
- Consider `#![forbid(unsafe_code)]` in crates that don't need it.

## Dependencies

- Add dependencies deliberately: prefer well-maintained crates, and enable only the
  features you need;
- Commit `Cargo.lock` for binaries;
- Use `cargo machete` to find unused dependencies (automated in CI).
- Run `cargo deny` in the CI to catch vulnerabilities and license issues (automated in CI).

## Tests

- Add unit tests whenever you add or change functionality, and a regression test for
  every applicable bug fix. Run them with `cargo test`;
- Put unit tests in a `#[cfg(test)] mod tests` next to the code, and integration tests in `tests/`;
- Test error paths as well as the happy path;
- GitLab tracks coverage; don't let it drop for the code you touch.

## Documentation

- Document all public items with rustdoc (`///`), starting with a one-line summary;
- Add examples as doctests where useful, and keep them compiling;
- Update the rustdoc, including doctests, whenever behaviour changes;
- Use `//` comments to explain *why*, not *what*.
