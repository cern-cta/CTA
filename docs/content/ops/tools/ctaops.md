# ctaops

`ctaops` is the command-line interface provided by `ctaops-cli` for managing CTA resources and local tape hardware.
It uses [`cta-sdk`](../../dev/guides/python/cta-sdk/index.md) to communicate with CTA and the optional [`ctaops-sdk`](../../dev/guides/python/ctaops-sdk/index.md) for tape-server operations.

Commands support table, JSON, and raw output, with configuration supplied through TOML files and command-line options.

## Installation

!!! abstract "Base install"
    ```bash
    python3 -m pip install ctaops-cli \
        --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
    ```

!!! abstract "Install with tape-server extra"
    ```bash
    python3 -m pip install 'ctaops-cli[tapeserver]' \
        --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
    ```

The `tapeserver` extra installs [`ctaops-sdk`](../../dev/guides/python/ctaops-sdk/index.md) and enables local hardware commands.
See the [system requirements](../../dev/guides/python/ctaops-sdk/index.md#system-requirements) for tape-server setup.

### Prerequisites

- Python 3.9 or later.
- A reachable CTA frontend, plus the `cta-admin` binary on the host running the CLI.
- An XRootD SSS keytab for authentication (typically `/etc/cta/tape-local.sss.keytab`).
- A CTA configuration file (typically `/etc/cta/cta-cli.conf`).

## Configuration

`ctaops-cli` reads configuration from the following sources, in order of decreasing precedence:

1. Command-line flags (e.g. `--format`, `--log-level`).
2. Environment variables (e.g. `CTAXrdSecSSSKT` to override the keytab path).
3. The TOML file pointed at by `--config-file` / `-C`, defaulting to `/etc/ctaops/ctaops-cli.toml`.
4. Built-in defaults (see [AppConfig](../../api/python/ctaops-cli/reference/config.md#ctaops.cli.config.AppConfig)).

### Reference configuration file

An example file is shipped with the package; you can dump it from the install directory:

!!! example "Print the bundled example config"
    ```bash
    python3 -c "import importlib.resources, ctaops.cli; \
        print(importlib.resources.files('ctaops.cli').joinpath('ctaops-cli.example.toml').read_text())"
    ```

```toml
--8<-- "tools/ctaops-cli/src/ctaops/cli/ctaops-cli.example.toml"
```

The bundled example currently uses `rotation_max_size` and `rotation_keep_files`; use `max_log_file_size` and `keep_file_count` for the current logging configuration.

### Environment overrides

| Variable | Effect |
| --- | --- |
| `CTAXrdSecSSSKT` | Overrides the SSS keytab path used by the `cta-admin` backend. Takes precedence over `[backends.CLI].sss_keytab_file` when set and readable. |

## Usage

See [Command Examples](ctaops-usage.md) for daily operator tasks.


## License

GPL-3.0-or-later
Copyright © 2026 CERN
