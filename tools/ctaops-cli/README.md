# ctaops-cli

See the CTA documentation for the [operator guide](../../docs/content/ops/tools/ctaops.md), [command examples](../../docs/content/ops/tools/ctaops-usage.md), and [extension guide](../../docs/content/dev/guides/python/ctaops-cli.md).

The main command-line interface for the CERN Tape Archive (CTA) operations toolkit. ctaops-cli provides a comprehensive set of commands for tape operators to manage, monitor, and troubleshoot CTA tape archive systems.

This tool combines the power of the **cta-sdk** (Python API client for CTA) with a user-friendly command-line interface, making it easy to perform CTA operations directly from the terminal.

**Key Features:**
- 📋 **23 Command Groups** - Manage tapes, drives, storage classes, pools, policies, and more
- 📊 **Multiple Output Formats** - Table (default), JSON, and raw text for different use cases
- ⚙️ **Flexible Configuration** - TOML-based configuration with command-line overrides
- 🔍 **Rich Filtering & Querying** - Powerful options to find exactly what you need
- 📝 **Smart Logging** - Rotating log files with compression and flexible log levels
- 🎯 **Quick Aliases** - Short aliases for frequently used commands (e.g., `ta` for `tape`)
- 🔌 **Extensible Design** - Custom formatters and pluggable command architecture
- 💾 **Tape Server Support** - Additional commands available with ctaops-sdk extra on tape servers

## Installation

### Prerequisites

- Python 3.9 or later
- CTA backend system with `cta-admin` command available
- Kerberos keytab for authentication (typically `/etc/cta/tape-local.keytab`)
- CTA configuration file (typically `/etc/cta/cta-cli.conf`)

### Install Package

```bash
python3 -m pip install ctaops-cli --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
```

### Optional Dependencies

For tape server-specific features (tape mounting, media check operations, and local drive management):

```bash
python3 -m pip install ctaops-cli[tapeserver] --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/stable/pypi/simple/
```

When installed on a tape server with the `ctaops-sdk` extra, ctaops-cli gains additional commands for managing local tape library hardware and operations. These commands are only available if the local tape server hardware is properly configured with `/dev/tape/by-name/` device symlinks (see [ctaops-sdk documentation](../../lib/python/ctaops-sdk/README.md) for setup).

## Quick Start

### Basic Setup

1. **Create configuration directory:**
   ```bash
   sudo mkdir -p /etc/ctaops
   sudo chmod 755 /etc/ctaops
   ```

2. **Copy example configuration:**
   ```bash
   python3 -c "import ctaops.cli; import importlib.resources; print(importlib.resources.files('ctaops.cli').joinpath('ctaops-cli.toml.example').read_text())" | sudo tee /etc/ctaops/ctaops-cli.toml
   ```

3. **Edit configuration** (adjust for your environment):
   ```bash
   sudo vim /etc/ctaops/ctaops-cli.toml
   ```

### First Commands

```bash
# Display version
ctaops --version

# Get help
ctaops --help
ctaops tape --help

# List all tapes (requires filter or --all flag)
ctaops tape list --all

# Get detailed info about a specific tape
ctaops tape get L12345

# List all drives
ctaops drive list

# Use short aliases for faster typing
ctaops ta list --all      # Same as: ctaops tape list --all
ctaops dr list            # Same as: ctaops drive list
ctaops tp ls              # Same as: ctaops tape-pool list
```

## Core Commands

ctaops-cli provides **23 command groups** with dozens of subcommands for daily tape operations. Here are the most commonly used:

**Note:** If you have the optional `ctaops-sdk` extra installed on a tape server, additional tape server-specific commands become available for local tape operations and hardware management.

### Tape Management

```bash
# List and filter tapes
ctaops tape list --all
ctaops tape list --tape-pool mypool --state ACTIVE
ctaops tape list --logical-library lib1

# Create a new tape
ctaops tape create VID123 \
  --media-type LTO9 \
  --vendor Quantum \
  --logical-library T0-LIB0 \
  --tape-pool pool1 \
  --comment "New tape for archival"

# Get tape details
ctaops tape get VID123
ctaops --format json tape get VID123

# Update tape information
ctaops tape update VID123 --state DISABLED "Tape failed verification"

# Tape state management
ctaops tape change-state VID123 ACTIVE "Tape back in service"

# Reclaim a tape (free its data, mark for reuse)
ctaops tape reclaim VID123 --copy-number 1

# Refresh tape statistics from library
ctaops tape refresh-stats VID123
```

### Drive Management

```bash
# List all drives
ctaops drive list

# List drives in a specific library
ctaops drive list --logical-library T0-LIB0

# Get drive details
ctaops drive get T0-LIB0-DRV0

# Bring drive online
ctaops drive up T0-LIB0-DRV0 --reason "Maintenance complete"

# Bring drive offline
ctaops drive down T0-LIB0-DRV0 --reason "Scheduled maintenance"

# Update drive comments
ctaops drive update T0-LIB0-DRV0 --comment "Drive recently replaced"
```

### UserRequest Management

```bash
# List archive requests
ctaops request list --type ARCHIVE

# List retrieve requests
ctaops request list --type RETRIEVE

# List requests grouped by tape pool
ctaops request list --type ARCHIVE --group-by tape_pool_name

# List requests grouped by virtual organisation
ctaops request list --type RETRIEVE --group-by virtual_organisation_name
```

### Failed UserRequest Handling

```bash
# List failed requests
ctaops failed-request list

# Filter by type
ctaops failed-request list --type ARCHIVE

# Get summary of failed requests
ctaops failed-request summary

# Delete a failed request (after fixing the underlying issue)
ctaops failed-request delete FAILEDREQ123
```

### Tape Pool Management

```bash
# List tape pools
ctaops tape-pool list

# Create a tape pool
ctaops tape-pool create mypool \
  --virtual-organisation myvo \
  --partial-tape-number 2 \
  --comment "Primary tape pool"

# Get pool details
ctaops tape-pool get mypool

# Update pool settings
ctaops tape-pool update mypool --comment "Updated pool description"
```

### Storage Class Management

```bash
# List storage classes
ctaops storage-class list

# Create a storage class
ctaops storage-class create sc1 \
  --number-of-copies 2 \
  --virtual-organisation myvo \
  --comment "Replicated storage"

# Link storage class to archive routes
ctaops archive-route create myvo sc1 tapepool1
```

### Mount Policy Management

```bash
# List mount policies
ctaops mount-policy list

# Create a mount policy
ctaops mount-policy create policy1 \
  --archive-priority 5 \
  --archive-min-request-age 3600 \
  --retrieve-priority 10 \
  --retrieve-min-request-age 1800

# Update policy priority
ctaops mount-policy update policy1 --archive-priority 8
```

### Administrator Management

```bash
# List admin users
ctaops admin list

# Create admin user
ctaops admin create myuser --comment "New operator"
```

### Virtual Organisation Management

```bash
# List virtual organisations
ctaops virtual-organisation list

# Create virtual organisation
ctaops virtual-organisation create myvo --comment "Research group"

# Update organisation settings
ctaops virtual-organisation update myvo --read-max-drives 10 --write-max-drives 5
```

## Command Aliases

Most commands have short aliases for convenience. Here are some common ones:

| Full Command | Alias | Usage |
|---|---|---|
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

## Output Formats

The Cli supports three output formats, configurable globally or per-command:

### Table Format (Default)

Human-readable ASCII tables with smart column formatting:

```bash
ctaops tape list --all
# ┌──────┬─────────┬────────────┬──────────────────┬──────────────┐
# │ VID  │ State   │ Capacity   │ Data In Bytes    │ Files Count  │
# ├──────┼─────────┼────────────┼──────────────────┼──────────────┤
# │ L001 │ ACTIVE  │ 2.5 TB     │ 1.8 TB           │ 12,456       │
# │ L002 │ DISABLED│ 2.5 TB     │ 850 GB           │ 5,231        │
# └──────┴─────────┴────────────┴──────────────────┴──────────────┘
```

**Features:**
- Automatically adjusts column width
- Right-aligned numbers for easy scanning
- Shows enums as human-readable names (e.g., "ACTIVE" not "enum.ACTIVE")
- Converts bytes to human-readable units (B, KB, MB, GB, TB, PB, EB)
- Handles nested dataclasses and lists

### JSON Format

Machine-readable JSON output, one object per line:

```bash
ctaops --format json tape list --all
# {"vid":"L001","state":"ACTIVE","capacity_in_bytes":2748779069440,"logical_library":"T0-LIB0","media_type":"LTO9",...}
# {"vid":"L002","state":"DISABLED",...}
```

**Benefits:**
- Perfect for automation and scripting
- Parse programmatically in scripts
- Datetime values converted to Unix timestamps
- Enums converted to string names

**Example:**
```bash
# Get list of active tapes
ctaops --format json tape list --all
```

### Raw Format

Plain text representation, useful for debugging:

```bash
ctaops --format raw tape get L001
# Tape(vid='L001', state=<State.ACTIVE: 1>, capacity_in_bytes=2748779069440, ...)
```

## Global Options

Options available for all commands (go after ctaops, before the command):

```bash
# Display version information
ctaops --version

# Set output format (table, json, raw)
ctaops --format json tape list --all
ctaops -f json tape list --all

# Use long format (more columns)
ctaops --long tape list --all

# Use short format (fewer columns, default)
ctaops --short tape list --all

# Set logging level
ctaops --log-level DEBUG tape list --all

# Use custom config file
ctaops --config-file /path/to/custom.toml tape list --all
ctaops -C /path/to/custom.toml tape list --all

# Help
ctaops --help                    # Overall help
ctaops tape --help               # Command group help
ctaops tape list --help          # Specific command help
```

## Configuration

### Configuration File Location

ctaops-cli reads configuration from (in order of precedence):
1. Path specified with `--config-file` / `-C` option
2. `$HOME/.ctaops/ctaops-cli.toml`
3. `/etc/ctaops/ctaops-cli.toml`
4. Built-in defaults

### Configuration File Structure

```toml
# Display settings
[display]
format = "table"          # Options: "table" (default), "json", "raw"
long = false              # Show all columns (when false, abbreviated)

# Logging configuration
[logging]
file_path = "/var/log/ctaops/ctaops-cli.log"
level = "WARNING"         # DEBUG, INFO, WARNING, ERROR, CRITICAL
max_log_file_size = 1000000000  # 1 GB before rotation
keep_file_count = 5       # Keep 5 rotated logs
date_format = "%Y-%m-%d %H:%M:%S"

# Backend selection and configuration
backend = "Cli"           # Currently only "Cli" is supported

# Cli backend configuration
[backends.Cli]
exec_path = "/usr/bin/cta-admin"
timeout = 60              # Command timeout in seconds
retry = 1                 # Retry count on failure
sss_keytab_file = "/etc/cta/tape-local.sss.keytab"
cta_conf_file = "/etc/cta/cta-cli.conf"

# CTA catalogue connection (optional)
cta_catalogue_conn_file = "/etc/cta/cta-catalogue.conf"
```

### Environment Variable Overrides

You can override the keytab file location:

```bash
# Use custom keytab
export CTAXrdSecSSSKT=/path/to/custom.keytab
ctaops tape list --all
```

### Command-Line Overrides

Configuration values can be overridden at the command line (global options go after ctaops, before the command):

```bash
# Override output format
ctaops --format json tape list --all

# Override logging level
ctaops --log-level DEBUG tape list --all

# Use custom config file
ctaops --config-file /path/to/myconfig.toml tape list --all
```

## Practical Examples

### Monitoring Operations

Daily operator tasks for system monitoring:

```bash
# Check for failed requests (common daily task)
ctaops failed-request summary --type ARCHIVE

# Monitor archive requests
ctaops request summary --type ARCHIVE

# Check drive status
ctaops drive list

# Find tapes in a specific pool with low usage_ratio
ctaops tape list --tape-pool mypool
```

### Tape Pool Management

Common tape pool operations:

```bash
# Create a new tape pool
ctaops tape-pool create archive_pool \
  --virtual-organisation production \
  --partial-tape-number 3 \
  --comment "Main archive pool"

# List tapes in pool
ctaops tape list --tape-pool archive_pool

# Get pool statistics
ctaops --format json tape-pool get archive_pool
```

### Drive Maintenance

Daily drive operations:

```bash
# Check all drives
ctaops drive list

# Get details of specific drive
ctaops drive get T0-LIB0-DRV0

# Take drive offline for maintenance
ctaops drive down T0-LIB0-DRV0 --reason "Scheduled preventive maintenance"

# Perform maintenance...

# Bring drive back online
ctaops drive up T0-LIB0-DRV0 --reason "Maintenance complete, drive tested"

# Verify drive is online
ctaops drive get T0-LIB0-DRV0
```

### Tape Lifecycle

Common tape management tasks:

```bash
# Create and prepare new tape
ctaops tape create NEWTAPE001 \
  --media-type LTO9 \
  --vendor "Quantum" \
  --logical-library T0-LIB0 \
  --tape-pool archive_pool \
  --comment "New tape for archival"

# Monitor tape usage
ctaops tape get NEWTAPE001

# When tape is full, disable it
ctaops tape update NEWTAPE001 --state DISABLED "Tape full"

# Reclaim space if needed (irreversible - deletes files)
ctaops tape reclaim NEWTAPE001 --copy-number 1
```

## Troubleshooting

### No such command error

Ensure you're using the correct command names:
```bash
# Correct (commands use hyphens)
ctaops tape-pool list
ctaops storage-class list
ctaops mount-policy list

# Incorrect (command names don't use underscores in the Cli)
ctaops tape_pool list        # Wrong
ctaops storage_class list    # Wrong

# Or use the alias (single word)
ctaops tp list               # Correct (alias for tape-pool)
ctaops sc list               # Correct (alias for storage-class)
```

### Configuration file not found

The tool looks for config files in this order:
1. Path specified with `--config-file` / `-C`
2. `$HOME/.ctaops/ctaops-cli.toml`
3. `/etc/ctaops/ctaops-cli.toml`

If none exist, you'll see a warning but the tool will continue with defaults.

```bash
# Verify which config file is being used
ctaops --log-level DEBUG --version 2>&1 | grep -i config
```

### Authentication errors

If you get Kerberos errors:

1. Verify keytab file exists:
   ```bash
   ls -l /etc/cta/tape-local.sss.keytab
   ```

2. Check keytab is readable:
   ```bash
   cat /etc/cta/tape-local.sss.keytab > /dev/null && echo "OK" || echo "NOT READABLE"
   ```

3. Override keytab path:
   ```bash
   export CTAXrdSecSSSKT=/path/to/custom.keytab
   ctaops tape list --all
   ```

### Command timeouts

If commands are timing out:

1. Check network connectivity to CTA backend
2. Increase timeout in config file:
   ```toml
   [backends.Cli]
   timeout = 120  # Increase from default 60 seconds
   ```

3. Or via command line (set in your shell):
   ```bash
   # To set globally in this.tape_session
   export CTA_ADMIN_TIMEOUT=120
   ```

### Logging for debugging

Enable debug logging to troubleshoot issues:

```bash
# Log to temporary file
ctaops --log-level DEBUG --config-file <(cat <<'EOF'
[logging]
file_path = "/tmp/ctaops-debug.log"
level = "DEBUG"
EOF
) tape list --all

# View log file
tail -f /tmp/ctaops-debug.log
```

### JSON parsing errors

When using JSON output format, each object is printed on a separate line, making it compatible with line-based tools.

```bash
# Get details about a specific tape
ctaops --format json tape get L12345

# List all active tapes
ctaops --format json tape list --all
```

## Architecture

### Component Integration

```
ctaops-cli
├── Click (Cli Framework)
├── cta-sdk (CTA Backend API)
├── ctaops-utils (Configuration, Logging, etc.)
├── Rich (Terminal Formatting)
└── [Optional] ctaops-sdk (Tape Server - additional commands on tape servers)
```

### Command Resolution

1. User enters: `ctaops tape list --all`
2. Click routes to tape command group
3. Command group resolves `list` subcommand
4. AppContext provides config and CTA client
5. Subcommand executes via CTA client
6. Results formatted based on display config
7. Output printed with Rich formatting

### Extensible Design

New commands can be added by:
1. Creating new file in `ctaops.cli.commands.`
2. Implementing Click command group decorator
3. Registering formatters for custom types (if needed)
4. Auto-loading happens on next Cli invocation

## Getting Help

```bash
# Overall help
ctaops --help

# Command group help
ctaops tape --help

# Specific command help
ctaops tape list --help
ctaops tape create --help

# Check version
ctaops --version
```

## License

GPL-3.0-or-later
Copyright © 2026 CERN
