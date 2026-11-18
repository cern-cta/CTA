# cta-client-macros

Procedural macros of [`cta-client`](../cta-client/README.md). Use them through `cta-client`, which re-exports them
and provides the types the generated code refers to.

- **`#[typed_admin_cmd]`** turns a plain struct into a typed admin command: a `bon` builder, the conversion into an
  `AdminCmd`, and the `TypedAdminCmd` / `UnaryAdminCmd` / `StreamingAdminCmd` implementations. See the documentation
  of `cta_client::typed_admin_cmd` for the syntax.
- **`#[admin_dao]`** turns a unit struct into a data access object of `cta-client`: a handle on the `AdminClient`,
  and the accessor of `AdminClient` returning it. Only usable within `cta-client`.
