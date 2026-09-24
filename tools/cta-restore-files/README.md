<!--
SPDX-FileCopyrightText: 2026 CERN
SPDX-License-Identifier: GPL-3.0-or-later
-->

# cta-restore-files

Command-line tool to restore deleted tape files in the
[CTA](https://gitlab.cern.ch/cta/CTA) catalogue and in the EOS namespace.

When a file is deleted from an EOS instance backed by CTA, the tape copy is not
erased: the corresponding entry is moved to the CTA catalogue's tape file
recycle bin. This tool lists those entries and can restore them, which involves
two steps per file:

1. recreate the entry in the EOS namespace if it is gone — containers,
   checksum, extended attributes (`sys.archive.file_id`, `eos.btime`) and a tape
   replica location (`eos::restore_deleted_file`);
2. restore the tape file copy in the CTA catalogue, pointing it at the
   (possibly new) disk file id (`cta::CtaEndpoint::restore_deleted_file_copy`).

## Usage

```bash
cta-restore-files --cta-frontend-endpoint https://cta-frontend.example.org:50051 \
                  --jwt-token-file /etc/cta/token.jwt \
                  --namespace-keytab-file namespace.keytab \
                  --vid V01001 \
                  list [--json]

cta-restore-files ... --archive-file-id 4294967296 restore
```

`list` prints the matching recycle-bin entries as a table, or as JSON Lines with
`--json`. `restore` restores every matching entry. The selection flags
(`--vid`, `--disk-instance`, `--archive-file-id`, `--copy-number`, `--file-id`)
are passed to the CTA frontend as filters of the `recycletapefile ls` admin
command; run `cta-restore-files --help` for the complete list.

Logging is configured through `RUST_LOG`, e.g. `RUST_LOG=debug`.

## Namespace keytab

The EOS endpoints are read from a namespace keytab file, one entry per line:

```text
<diskInstance> <endpoint> <token> [<alternativeHost>]
```

`#` starts a comment and blank lines are ignored. `<endpoint>` must use the
`http` or `https` scheme; the optional fourth field is the hostname to validate
the server's TLS certificate against. The file contains credentials - keep it
readable only by its owner and out of version control.
