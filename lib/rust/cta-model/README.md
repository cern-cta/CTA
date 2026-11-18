# cta-model

Business objects of [CTA](https://gitlab.cern.ch/cta/CTA): the resources of
the catalogue and the scheduler (tapes, tape pools, files, drives, ...),
and the workflow events a disk system notifies CTA of.

The resources are grouped by domain, and only reachable through their domain
module (`cta_model::tape::Tape`, `cta_model::tape::TapePool`, ...).

| Module      | Resources                                                            |
|-------------|----------------------------------------------------------------------|
| `admin`     | admin users, virtual organisations, versions                         |
| `tape`      | tapes, tape pools, media types                                       |
| `file`      | archived files, their tape and disk metadata, checksums, recycle bin |
| `library`   | drives, logical and physical libraries                               |
| `routing`   | storage classes, archive routes, mount policies, mount rules         |
| `disk`      | disk instances, disk instance spaces, disk systems                   |
| `scheduler` | user request queues, failed requests, repack requests                |

Each resource is built from its protobuf record with `TryFrom` (a `TapePool`
from a `TapePoolLsItem`, ...), failing with a `ConversionError`.

The workflow events (`events::Create`, `events::Archive`, `events::Retrieve`,
`events::CancelRetrieve`, `events::Delete`) describe the file (`FileInfo`,
with the `Owner` and `Checksum` of the `file` domain) and the user
(`Requester`).

## Tests

The conversion tests convert the JSONL samples of the Python `cta-sdk` tests,
and compare the result with the expected domain types.

```bash
cargo test -p cta-model
```
