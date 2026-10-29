# ctaops

`ctaops-cli` is the main command-line interface for the CERN Tape Archive (CTA) operations toolkit. It is the day-to-day tool a tape operator uses to manage, monitor, and troubleshoot a CTA tape archive system from a terminal.

The tool is a thin, opinionated [Click](https://click.palletsprojects.com/) front-end on top of [`cta-sdk`](../../dev/guides/python/cta-sdk/index.md) for talking to the CTA frontend, [`ctaops-utils`](../../dev/guides/python/ctaops-utils/index.md) for configuration / logging, and — optionally, on a tape server — [`ctaops-sdk`](../../dev/guides/python/ctaops-sdk/index.md) for local hardware operations.

**Key features:**

- 📋 **Resource command groups** — manage tapes, drives, storage classes, pools, policies, requests, and more.
- 📊 **Multiple output formats** — `table` (default), `json`, and `raw`, switchable globally or per command.
- ⚙️ **Layered configuration** — TOML files, environment variables, and command-line flags merged with predictable precedence.
- 🔍 **Rich filtering & querying** — first-class options on every `list` command.
- 📝 **Smart logging** — rotating, compressed log files driven by `ctaops-utils`.
- 🎯 **Aliases** — short forms for every command group (e.g. `ta` for `tape`, `dr` for `drive`, `tp` for `tape-pool`).
- 🔌 **Pluggable command architecture** — new command modules are auto-loaded from `ctaops.cli.commands`.
- 💾 **Tape server extra** — install with `[tapeserver]` to enable additional commands on a tape server.

> This is the tool to reach for when you want to *operate* CTA. For programmatic access, use [`cta-sdk`](../../dev/guides/python/cta-sdk/index.md) directly.

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

The `tapeserver` extra pulls in [`ctaops-sdk`](../../dev/guides/python/ctaops-sdk/index.md) and unlocks the local-hardware command groups. These commands only work when the local tape server is wired up correctly — see the [ctaops-sdk system requirements](../../dev/guides/python/ctaops-sdk/index.md#system-requirements).

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
4. Built-in defaults (see [AppConfig](../../../api/python/ctaops-cli/reference/config/#ctaops.cli.config.AppConfig)).

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

Please see the [Command Examples](ctaops-usage.md) page for command examples covering daily operator workflows.


## License

GPL-3.0-or-later
Copyright © 2026 CERN
