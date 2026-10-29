# ctaops Usage

This page presents practical examples for using `ctaops-cli` in day-to-day operations.

## First steps

```bash
# Show version
ctaops --version

# Top-level help
ctaops --help

# Help on any command group or subcommand
ctaops tape --help
ctaops tape list --help
```

## Global options

Global options go between `ctaops` and the command name:

```bash
# Switch the output format
ctaops --format json tape list --all
ctaops -f json tape list --all

# Crank the log level for one invocation
ctaops --log-level DEBUG tape list --all

# Use a non-default config file
ctaops --config-file /path/to/custom.toml tape list --all
ctaops -C /path/to/custom.toml tape list --all
```

| Flag | Effect |
| --- | --- |
| `--format` / `-f` | Override `[display].format`. One of `table`, `json`, `raw`. |
| `--log-level` | Override `[logging].level` for this invocation. |
| `--config-file` / `-C` | Use a different TOML config file. |
| `--version` | Print the `ctaops-cli` version and exit. |
| `--help` / `-h` | Show help (recursively available on every group / command). |

## Output formats

The display format is selected globally and respected by every command that returns structured output.

=== "Table (default)"
    ```bash
    ctaops tape list --all
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
    ctaops --format json tape list --all
    # {"vid":"L001","state":"ACTIVE","capacity_in_bytes":2748779069440,...}
    # {"vid":"L002","state":"DISABLED","capacity_in_bytes":2748779069440,...}
    ```
    One JSON object per line — pipe straight into `jq`, `xargs`, or any line-oriented tool. Datetimes become Unix timestamps and enums become their string name.

=== "Raw"
    ```bash
    ctaops --format raw tape get L001
    # Tape(vid='L001', state=<State.ACTIVE: 1>, capacity_in_bytes=2748779069440, ...)
    ```
    The `repr()` of the underlying dataclass — handy for debugging or filing bug reports.

## Command aliases

Most groups have a short alias. Use whichever is more convenient:

| Full | Alias | Example |
| --- | --- | --- |
| `admin` | `ad` | `ctaops ad list` |
| `activity-mount-rule` | `amr` | `ctaops amr list` |
| `archive-route` | `ar` | `ctaops ar list` |
| `disk-instance` | `di` | `ctaops di list` |
| `disk-instance-space` | `dis` | `ctaops dis list` |
| `disk-system` | `ds` | `ctaops ds list` |
| `drive` | `dr` | `ctaops dr list` |
| `failed-request` | `fr` | `ctaops fr list` |
| `group-mount-rule` | `gmr` | `ctaops gmr list` |
| `logical-library` | `ll` | `ctaops ll list` |
| `media-type` | `mt` | `ctaops mt list` |
| `mount-policy` | `mp` | `ctaops mp list` |
| `physical-library` | `pl` | `ctaops pl list` |
| `recycle-bin-tape-file` | `rbtf` | `ctaops rbtf list` |
| `repack` | `re` | `ctaops re list` |
| `request` | `rq` | `ctaops rq list` |
| `storage-class` | `sc` | `ctaops sc list` |
| `tape` | `ta` | `ctaops ta list` |
| `tape-file` | `tf` | `ctaops tf list` |
| `tape-pool` | `tp` | `ctaops tp list` |
| `user-mount-rule` | `umr` | `ctaops umr list` |
| `virtual-organisation` | `vo` | `ctaops vo list` |

!!! tip
    Group aliases also work with their own subcommands and aliases — e.g. `ctaops ta ls --all` is `ctaops tape list --all`.

## Tape management

```bash
# List tapes (the --all flag is required when no filter is set)
ctaops tape list --all
ctaops tape list --tape-pool mypool --state ACTIVE
ctaops tape list --logical-library T0-LIB0

# Create a tape
ctaops tape create VID123 \
    --media-type LTO9 \
    --vendor Quantum \
    --logical-library T0-LIB0 \
    --tape-pool pool1 \
    --comment "New tape for archival"

# Inspect a tape
ctaops tape get VID123
ctaops --format json tape get VID123

# State transitions
ctaops tape change-state VID123 ACTIVE "Tape back in service"
ctaops tape update VID123 --state DISABLED "Failed verification"

# Reclaim a tape (irreversible — frees its data and marks it for reuse)
ctaops tape reclaim VID123 --copy-number 1

# Refresh statistics from the library
ctaops tape refresh-stats VID123
```

## Drive management

```bash
# List drives
ctaops drive list
ctaops drive list --logical-library T0-LIB0

# Inspect one drive
ctaops drive get T0-LIB0-DRV0

# Take a drive offline / bring it back
ctaops drive down T0-LIB0-DRV0 --reason "Scheduled maintenance"
ctaops drive up   T0-LIB0-DRV0 --reason "Maintenance complete"

# Update a drive comment
ctaops drive update T0-LIB0-DRV0 --comment "Drive recently replaced"
```

## Requests

```bash
# Active requests
ctaops request list --type ARCHIVE
ctaops request list --type RETRIEVE

# Group requests by tape pool or VO
ctaops request list --type ARCHIVE  --group-by tape_pool_name
ctaops request list --type RETRIEVE --group-by virtual_organisation_name

# Failed requests
ctaops failed-request list
ctaops failed-request list --type ARCHIVE
ctaops failed-request summary
ctaops failed-request delete FAILEDREQ123
```

## Tape pools, storage classes, mount policies

```bash
# Tape pools
ctaops tape-pool list
ctaops tape-pool create mypool \
    --virtual-organisation myvo \
    --partial-tape-number 2 \
    --comment "Primary tape pool"

# Storage classes
ctaops storage-class list
ctaops storage-class create sc1 \
    --number-of-copies 2 \
    --virtual-organisation myvo \
    --comment "Replicated storage"

# Mount policies
ctaops mount-policy list
ctaops mount-policy create policy1 \
    --archive-priority 5 \
    --archive-min-request-age 3600 \
    --retrieve-priority 10 \
    --retrieve-min-request-age 1800
ctaops mount-policy update policy1 --archive-priority 8

# Archive routes
ctaops archive-route create myvo sc1 tapepool1
```

## Admin and virtual organisation management

```bash
# Admins
ctaops admin list
ctaops admin create myuser --comment "New operator"

# Virtual organisations
ctaops virtual-organisation list
ctaops virtual-organisation create myvo --comment "Research group"
ctaops virtual-organisation update myvo --read-max-drives 10 --write-max-drives 5
```

## Backend version

```bash
# Show the connected CTA frontend / backend version
ctaops cta-version
ctaops v   # alias
```

## Troubleshooting

### Unknown command

Command groups use hyphens, not underscores:

```bash
ctaops tape-pool list      # ok
ctaops tape_pool list      # not recognised
ctaops tp list             # ok (alias for tape-pool)
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
ctaops tape list --all
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
ctaops --log-level DEBUG --config-file <(cat <<'EOF'
[logging]
file_path = "/tmp/ctaops-debug.log"
level = "DEBUG"
EOF
) tape list --all

tail -f /tmp/ctaops-debug.log
```

## Tape server extra

When `ctaops-cli` is installed with the `[tapeserver]` extra on a host where [`ctaops-sdk`](../../dev/guides/python/ctaops-sdk/index.md) requirements are met (udev rules, `cta-smc`, `mt`, `sg_logs`), additional commands and command options light up automatically — typically for local mount/unmount, label, and media check operations.

!!! note
    Availability of these extra options is detected at start-up via [`tapeserver_option`](../../../api/python/ctaops-cli/reference/entrypoint/#ctaops.cli.context.tapeserver_option): when `ctaops.sdk` is importable, the corresponding Click options are registered; otherwise they are silently omitted.
