# cta

`cta` is the command-line interface provided by `cta-cli` for managing CTA resources and local tape hardware.
It uses [`cta-sdk`](../../dev/guides/python/cta-sdk/index.md) to communicate with CTA and the optional [`cta-tapeserver-sdk`](../../dev/guides/python/cta-tapeserver-sdk/index.md) for tape-server operations.

Commands support table, JSON, and raw output, with configuration supplied through TOML files and command-line options.

## Installation

!!! abstract "Base install"
    ```bash
    python3 -m pip install cta-cli \
        --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
    ```

!!! abstract "Install with tape-server extra"
    ```bash
    python3 -m pip install 'cta-cli[tapeserver]' \
        --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
    ```

The `tapeserver` extra installs [`cta-tapeserver-sdk`](../../dev/guides/python/cta-tapeserver-sdk/index.md) and enables local hardware commands.
See the [system requirements](../../dev/guides/python/cta-tapeserver-sdk/index.md#system-requirements) for tape-server setup.

### Prerequisites

- Python 3.9 or later.
- A reachable CTA frontend, plus the `cta-admin` binary on the host running the CLI.
- An XRootD SSS keytab for authentication (typically `/etc/cta/tape-local.sss.keytab`).
- A CTA configuration file (typically `/etc/cta/cta-cli.conf`).

## Configuration

`cta-cli` reads configuration from the following sources, in order of decreasing precedence:

1. Command-line flags (e.g. `--format`, `--log-level`).
2. Environment variables (e.g. `CTAXrdSecSSSKT` to override the keytab path).
3. The TOML file pointed at by `--config-file` / `-C`, defaulting to `/etc/cta/cta-cli.toml`.
4. Built-in defaults (see [AppConfig](../../api/python/cta-cli/reference/config.md#cta.cli.config.AppConfig)).

### Reference configuration file

An example file is shipped with the package; you can dump it from the install directory:

!!! example "Print the bundled example config"
    ```bash
    python3 -c "import importlib.resources, cta.cli; \
        print(importlib.resources.files('cta.cli').joinpath('cta-cli.example.toml').read_text())"
    ```

```toml
--8<-- "tools/cta-cli/src/cta/cli/cta-cli.example.toml"
```

The bundled example currently uses `rotation_max_size` and `rotation_keep_files`; use `max_log_file_size` and `keep_file_count` for the current logging configuration.

### Environment overrides

| Variable | Effect |
| --- | --- |
| `CTAXrdSecSSSKT` | Overrides the SSS keytab path used by the `cta-admin` backend. Takes precedence over `[backends.CTA_ADMIN].sss_keytab_file` when set and readable. |

## Usage

See [Command Examples](cta-usage.md) for daily operator tasks.


## License

GPL-3.0-or-later
Copyright © 2026 CERN
