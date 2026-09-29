# Copyright

- Files MUST have associated copyright and license metadata, following the repository's REUSE configuration.
- For new CERN-authored project files covered by the project license, use the SPDX header below with the creation year and the file format's comment syntax.
- Preserve existing copyright attribution and license notices when modifying or importing files. Do not replace third-party notices with the CERN example.
- Metadata may be supplied through an in-file header, a `.license` sidecar, or `REUSE.toml` annotations. Check existing annotations before adding redundant headers; Markdown and JSON files already have repository-wide coverage.
- Use sidecars or `REUSE.toml` where embedding a header is impractical, and keep annotations scoped to the files they describe.

## SPDX header

```text
SPDX-FileCopyrightText: <year-of-creation> CERN
SPDX-License-Identifier: GPL-3.0-or-later
```

## Validation

Use the REUSE checks described in [Pre-commit Hooks](../../tools-and-environment/pre-commit.md). CI also runs `reuse lint` across the repository.

REUSE checks licensing metadata and compliance with its specification. It does **not** determine whether licenses are compatible or whether imported material may be used; see the [REUSE FAQ](https://reuse.software/faq/).
