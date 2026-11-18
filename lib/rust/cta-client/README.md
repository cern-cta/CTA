# cta-client

High-level client library for the [CTA](https://gitlab.cern.ch/cta/CTA)
frontend gRPC API. It sends typed admin commands over the gRPC bindings of
`cta-protobuf`, and returns the business
objects of `cta-model` (`cta_model::tape::Tape`, `cta_model::tape::TapePool`, ...).

```rust
let client = AdminClient::connect(&config).await?;

let pool = client.tape_pools().get("my_pool").await?;
client.drives().down("my_drive").reason("maintenance").call().await?;
```

## How it is organised

The `admin` module (`cta-admin-api`) has three layers, each built on the previous one:

| Layer    | Module            | Crate        | Description |
|----------|-------------------|--------------|-------------|
| Commands | `admin::commands` | `cta-client` | One struct per subcommand, converted into the `AdminCmd` protobuf |
| Client   | `admin::client`           | `cta-client` | `AdminClient` sends a command and returns the protobuf reply or records |
| DAOs     | `admin::dao`      | `cta-client` | One data access object per resource (e.g. `client.tapes()`, ...), sending commands and returning domain types (`cta-model`) |

Most use cases will only require using the DAOs. The sections below follow one resource (storage classes) through each layer.

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

### Types

A domain type converts from a protobuf record with `TryFrom`. The conversion "hydrates"
the protobuf data into more intuitive objects, with timestamps, enums, and `None` types
and so on.

```rust
impl TryFrom<StorageClassLsItem> for StorageClass {
    type Error = ConversionError;

    fn try_from(item: StorageClassLsItem) -> Result<Self, Self::Error> {
        Ok(Self {
            name: item.name,
            virtual_organisation_name: item.vo,
            replica_count: item.nb_copies,
            metadata: Metadata::convert(
                "StorageClassLsItem",
                item.creation_log,
                item.last_modification_log,
                item.comment,
            )?,
        })
    }
}
```

`client.stream_as(cmd)` then returns a stream of these types.

### DAOs

A DAO is declared with `dao!`, which defines the struct and its accessor within
`AdminClient`. Its methods are plain `async` functions. The ones with optional
arguments use [`bon`](https://docs.rs/bon) builders, so they read like keyword
arguments.

```rust
dao! {
    /// Storage classes: see [`StorageClass`].
    pub struct StorageClasses;
    /// Accessor within `AdminClient`
    fn storage_classes;
}

#[bon]
impl StorageClasses {
    /// The storage class named `name`.
    pub async fn get(&self, name: &str) -> Result<StorageClass, AdminError> {
        let cmd = storage_class::Ls::builder().name(name).build();
        exactly_one(self.client.stream_as(cmd).await?, "storage class", name).await
    }

    /// Creates a storage class.
    #[builder]
    pub async fn create(
        &self,
        #[builder(start_fn, into)] name: String,
        #[builder(into)] virtual_organisation_name: String,
        #[builder(default = 1)] replica_count: u64,
        #[builder(into)] comment: String,
    ) -> Result<(), AdminError> {
        let cmd = storage_class::Add::builder()
            .name(name)
            .vo(virtual_organisation_name)
            .number_of_copies(replica_count)
            .comment(comment)
            .build();
        self.client.unary(cmd).await?;
        Ok(())
    }
}
```

which is used as:

```rust
client
    .storage_classes()
    .create("my_class")
    .virtual_organisation_name("ATLAS")
    .comment("for the tests")
    .call()
    .await?;
```

Two helpers are provided fir use in `get` methods:

- **`exactly_one`** is for an `ls` that filters on the server. It returns
  `AdminError::NotFound` when there is no record and `AdminError::MoreThanOne`
  when there are several.
- **`find_one`** is for an `ls` without a filter. It lists everything and keeps
  the record matching a closure.

The types (in `cta-model`), the commands and the DAOs are grouped by the same
domains (`admin`, `disk`, `file`, `library`, `routing`, `scheduler`, `tape`).

## Tests

Each group has a `tests.rs` next to its code.

- **Type tests** are in `cta-model`, next to the conversions they test. The JSONL fixtures shared by both crates come from the
  `test-support` feature of `cta-model`, enabled only as a dev-dependency of
  this crate;
- **DAO tests** run each method against a mock frontend, and compare the
  command it sent with the expected one.

Run them with:

```bash
cargo test -p cta-client
```
