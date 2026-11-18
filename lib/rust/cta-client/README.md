# cta-client

High-level client library for the [CTA](https://gitlab.cern.ch/cta/CTA)
frontend gRPC API. It sends typed admin commands over the gRPC bindings of
`cta-protobuf`.

```rust
let client = AdminClient::connect(&config).await?;

let cmd = storage_class::Ls::builder().name("my_class").build();
let mut classes = client.stream(cmd).await?;
```

## How it is organised

The `admin` module (`cta-admin-api`) has two layers, the second built on the first:

| Layer    | Module            | Crate        | Description |
|----------|-------------------|--------------|-------------|
| Commands | `admin::commands` | `cta-client` | One struct per subcommand, converted into the `AdminCmd` protobuf |
| Client   | `admin::client`           | `cta-client` | `AdminClient` sends a command and returns the protobuf reply or records |

### Commands

A command is a plain struct with the `#[typed_admin_cmd]` attribute (from
[`cta-client-macros`](../cta-client-macros/README.md)):

```rust
/// Adds a storage class (`storageclass add`).
#[typed_admin_cmd(Storageclass::Add)]
pub struct Add {
    /// Name of the storage class
    #[proto(StorageClass)]
    pub name: String,
    /// Number of tape copies of the files of the storage class
    #[proto(CopyNumber)]
    pub number_of_copies: u64,
    /// Virtual organisation of the storage class
    pub vo: String,
    /// Comment
    pub comment: String,
}

/// Lists the failed requests (`failedrequest ls`).
#[typed_admin_cmd(Failedrequest::Ls, response = FailedRequestLsItem)]
#[derive(Default)]
pub struct Ls {
    /// Only the archive requests
    #[proto(Justarchive, only_if_true)]
    pub just_archive: bool,
    /// Only the requests of this tape pool
    pub tape_pool: Option<String>,
}
```

- **`Command::SubCommand`** names the command and subcommand of the protobuf (`Tape::Ls` for
  `Cmd::Tape` and `SubCmd::SubcmdLs`), or the command alone for those without subcommand
  (`Version`).
- **`response = Record`** marks a streaming command (an `ls`), and names the record type it
  returns.
- **`with(Key = value, ...)`** gives options sent with a fixed value with every command
  (`#[typed_admin_cmd(Failedrequest::Ls, response = FailedRequestLsSummary, with(Summary = true))]`).
- Each field is an option, whose key is the field name in `PascalCase` (`tape_pool` →
  `TapePool`). **`#[proto(Key)]`** names another key, and **`#[proto(only_if_true)]`** sends a
  `bool` as a flag (only when `true`, defaulting to `false` in the builder).

Every command gets a builder (`storage_class::Add::builder().name("my_new_storage_class")...`)
and can be sent with `client.unary(cmd)` or `client.stream(cmd)`.

## Tests

Each group has a `tests.rs` next to its code. Run them with:

```bash
cargo test -p cta-client
```
