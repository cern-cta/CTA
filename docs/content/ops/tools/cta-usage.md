# cta Usage

This page presents practical examples for using `cta-cli` in day-to-day operations.

## First steps

```bash
# Show version
cta --version

# Top-level help
cta --help

# Help on any command group or subcommand
cta tape --help
cta tape list --help
```

## Global options

Global options go between `cta` and the command name:

```bash
# Switch the output format
cta --format json tape list --all
cta -f json tape list --all

# Crank the log level for one invocation
cta --log-level DEBUG tape list --all

# Use a non-default config file
cta --config-file /path/to/custom.toml tape list --all
cta -C /path/to/custom.toml tape list --all
```

| Flag | Effect |
| --- | --- |
| `--format` / `-f` | Override `[display].format`. One of `table`, `json`, `raw`. |
| `--log-level` | Override `[logging].level` for this invocation. |
| `--config-file` / `-C` | Use a different TOML config file. |
| `--version` | Print the `cta-cli` version and exit. |
| `--help` / `-h` | Show help (recursively available on every group / command). |

## Output formats

The display format is selected globally and respected by every command that returns structured output.

=== "Table (default)"
    ```bash
    cta tape list --all
    # ┌──────┬─────────┬────────────┬──────────────────┬──────────────┐
    # │ VID  │ State   │ Capacity   │ Data In Bytes    │ Files Count  │
    # ├──────┼─────────┼────────────┼──────────────────┼──────────────┤
    # │ L001 │ ACTIVE  │ 2.5 TB     │ 1.8 TB           │ 12,456       │
    # │ L002 │ DISABLED│ 2.5 TB     │ 850 GB           │ 5,231        │
    # └──────┴─────────┴────────────┴──────────────────┴──────────────┘
    ```
    Columns auto-size, numbers are right-aligned, byte values are humanised, and enums render as their name (`ACTIVE`, not `<State.ACTIVE: 1>`).

=== "JSON"
    ```bash
    cta --format json tape list --all
    # {"vid":"L001","state":"ACTIVE","capacity_in_bytes":2748779069440,...}
    # {"vid":"L002","state":"DISABLED","capacity_in_bytes":2748779069440,...}
    ```
    One JSON object per line — pipe straight into `jq`, `xargs`, or any line-oriented tool. Datetimes become Unix timestamps and enums become their string name.

=== "Raw"
    ```bash
    cta --format raw tape get L001
    # Tape(vid='L001', state=<State.ACTIVE: 1>, capacity_in_bytes=2748779069440, ...)
    ```
    The `repr()` of the underlying dataclass — handy for debugging or filing bug reports.

## Command aliases

Most groups have a short alias. Use whichever is more convenient:

| Full | Alias | Example |
| --- | --- | --- |
| `admin` | `ad` | `cta ad list` |
| `activity-mount-rule` | `amr` | `cta amr list` |
| `archive-route` | `ar` | `cta ar list` |
| `disk-instance` | `di` | `cta di list` |
| `disk-instance-space` | `dis` | `cta dis list` |
| `disk-system` | `ds` | `cta ds list` |
| `drive` | `dr` | `cta dr list` |
| `failed-request` | `fr` | `cta fr list` |
| `group-mount-rule` | `gmr` | `cta gmr list` |
| `logical-library` | `ll` | `cta ll list` |
| `media-type` | `mt` | `cta mt list` |
| `mount-policy` | `mp` | `cta mp list` |
| `physical-library` | `pl` | `cta pl list` |
| `recycle-bin-tape-file` | `rbtf` | `cta rbtf list` |
| `repack` | `re` | `cta re list` |
| `request` | `rq` | `cta rq list` |
| `storage-class` | `sc` | `cta sc list` |
| `tape` | `ta` | `cta ta list` |
| `tape-file` | `tf` | `cta tf list` |
| `tape-pool` | `tp` | `cta tp list` |
| `user-mount-rule` | `umr` | `cta umr list` |
| `virtual-organisation` | `vo` | `cta vo list` |

!!! tip
    Group aliases also work with their own subcommands and aliases — e.g. `cta ta ls --all` is `cta tape list --all`.

## Tape management

```bash
# List tapes (the --all flag is required when no filter is set)
cta tape list --all
cta tape list --tape-pool mypool --state ACTIVE
cta tape list --logical-library T0-LIB0

# Create a tape
cta tape create VID123 \
    --media-type LTO9 \
    --vendor Quantum \
    --logical-library T0-LIB0 \
    --tape-pool pool1 \
    --comment "New tape for archival"

# Inspect a tape
cta tape get VID123
cta --format json tape get VID123

# State transitions
cta tape change-state VID123 ACTIVE "Tape back in service"
cta tape update VID123 --state DISABLED "Failed verification"

# Reclaim a tape (irreversible — frees its data and marks it for reuse)
cta tape reclaim VID123 --copy-number 1

# Refresh statistics from the library
cta tape refresh-stats VID123
```

## Drive management

```bash
# List drives
cta drive list
cta drive list --logical-library T0-LIB0

# Inspect one drive
cta drive get T0-LIB0-DRV0

# Take a drive offline / bring it back
cta drive down T0-LIB0-DRV0 --reason "Scheduled maintenance"
cta drive up   T0-LIB0-DRV0 --reason "Maintenance complete"

# Update a drive comment
cta drive update T0-LIB0-DRV0 --comment "Drive recently replaced"
```

## Requests

```bash
# Active requests
cta request list --type ARCHIVE
cta request list --type RETRIEVE

# Group requests by tape pool or VO
cta request list --type ARCHIVE  --group-by tape_pool_name
cta request list --type RETRIEVE --group-by virtual_organisation_name

# Failed requests
cta failed-request list
cta failed-request list --type ARCHIVE
cta failed-request summary
cta failed-request delete FAILEDREQ123
```

## Tape pools, storage classes, mount policies

```bash
# Tape pools
cta tape-pool list
cta tape-pool create mypool \
    --virtual-organisation myvo \
    --partial-tape-number 2 \
    --comment "Primary tape pool"

# Storage classes
cta storage-class list
cta storage-class create sc1 \
    --number-of-copies 2 \
    --virtual-organisation myvo \
    --comment "Replicated storage"

# Mount policies
cta mount-policy list
cta mount-policy create policy1 \
    --archive-priority 5 \
    --archive-min-request-age 3600 \
    --retrieve-priority 10 \
    --retrieve-min-request-age 1800
cta mount-policy update policy1 --archive-priority 8

# Archive routes
cta archive-route create myvo sc1 tapepool1
```

## Admin and virtual organisation management

```bash
# Admins
cta admin list
cta admin create myuser --comment "New operator"

# Virtual organisations
cta virtual-organisation list
cta virtual-organisation create myvo --comment "Research group"
cta virtual-organisation update myvo --read-max-drives 10 --write-max-drives 5
```

## Backend version

```bash
# Show the connected CTA frontend / backend version
cta cta-version
cta v   # alias
```

## Troubleshooting

### Unknown command

Command groups use hyphens, not underscores:

```bash
cta tape-pool list      # ok
cta tape_pool list      # not recognised
cta tp list             # ok (alias for tape-pool)
```

### Configuration file not found

The CLI walks the precedence list above; if none of the locations exist it falls back to the built-in defaults and emits a warning. To verify which file was loaded, run with `--log-level DEBUG`.

### Authentication errors

```bash
# Verify the keytab exists and is readable
ls -l /etc/cta/tape-local.sss.keytab
test -r /etc/cta/tape-local.sss.keytab && echo OK || echo NOT-READABLE

# Override the keytab for a single shell
export CTAXrdSecSSSKT=/path/to/custom.keytab
cta tape list --all
```

### Command timeouts

Bump the backend timeout in your config file:

```toml
[backends.CLI]
timeout = 120  # seconds, default 60
```

### Debug logging on demand

```bash
# Drop a verbose trace into /tmp without touching your normal config
cta --log-level DEBUG --config-file <(cat <<'EOF'
[logging]
file_path = "/tmp/cta-debug.log"
level = "DEBUG"
EOF
) tape list --all

tail -f /tmp/cta-debug.log
```

## Tape server extra

When `cta-cli` is installed with the `[tapeserver]` extra on a host where [`ctaops-sdk`](../../dev/guides/python/ctaops-sdk/index.md) requirements are met (udev rules, `cta-smc`, `mt`, `sg_logs`), additional commands and command options light up automatically — typically for local mount/unmount, label, and media check operations.

!!! note
    Availability of these extra options is detected at start-up via [`tapeserver_option`](../../api/python/cta-cli/reference/entrypoint.md#cta.cli.context.tapeserver_option): when `ctaops.sdk` is importable, the corresponding Click options are registered; otherwise they are silently omitted.
