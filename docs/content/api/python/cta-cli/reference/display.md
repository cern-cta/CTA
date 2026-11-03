# Display layer

The display layer turns `cta-sdk` dataclasses into terminal output. It is split into three concerns:

| Module | Concern |
| --- | --- |
| [encoders][cta.cli.display.encoders] | Convert individual fields/values into a printable form (bytes → human-readable, enums → name, datetimes → timestamps, …). |
| [formatters][cta.cli.display.formatters] | Compose encoded fields into rows and columns for a given output format. |
| [printers][cta.cli.display.printers] | Drive the output: pick a formatter based on the active display config and emit to stdout. |

Custom per-resource formatting (e.g. for `Drive`, `Tape`, `TapePool`, `Repack`) lives under `cta.cli.display.dataclass_formatters` and plugs into the formatter layer.


::: cta.cli.display.encoders

::: cta.cli.display.formatters

::: cta.cli.display.printers

::: cta.cli.display.dataclass_formatters
    options:
      show_submodules: true
