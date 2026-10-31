# SPDX-FileCopyrightText: 2023-2026 CERN
# SPDX-License-Identifier: GPL-3.0-or-later

"""Display layer for `ctaops-cli`.

Turns `cta-sdk` dataclasses into terminal output. The package is split into
three concerns:

* `encoders` — convert individual fields/values into a printable form (bytes
  → human-readable, enums → name, datetimes → timestamps, …).
* `formatters` — compose encoded fields into rows and columns for a given
  output format.
* `printers` — drive the output: pick a formatter based on the active display
  config and emit to stdout.

Per-resource customisation lives under
`ctaops.cli.display.dataclass_formatters` and plugs into the formatter layer.
"""
