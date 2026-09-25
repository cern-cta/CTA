# CTA Utils

The CTA Utils library contains various helpers that are used across the CTA operations utilities.

## Installation

### Configuration

```yaml
global:
  # ---------------------
  # Logging configuration
  # ---------------------
  logging:
    # Base directory for log files
    log_dir: "/var/log/cta-ops/"
    # Max size of log file before log rotation is triggered
    rotation_max_size: "1G"
    # Number of rotated files to keep
    rotation_keep_files: 5
    # Datetime format to use in log files
    date_format: "%Y-%m-%d %H:%M:%S"
    # Translate error messages from exteral tools when called. Leave empty to disable this feature.
    error_translation_file: "/etc/cta-ops/error-messages.yaml"
  # ---------------------------------------------
  # The system user for executing automated tasks
  # ---------------------------------------------
  default_user:
    name: "tape-local"
    group: "tape"
    sss_keytab_file: "/etc/cta/tape-local.keytab"
  # ---------------------------
  # Email notification settings
  # ---------------------------
  email:
    recipients:
      - changeme      # TO email addresses for notification emails
    sender: changeme  # FROM email address for notification emails
  # --------------------------------
  # Aesthetics, tabulation and color
  # --------------------------------
  # Python tabulate output formatting style
  table_format: "plain"
  table_max_col_width: 30
  # Colors for text highlighting
  colors:
    ansi_color_table: "\e[1;31m"    # "\033[1;31m"
    ansi_color_fail: "\e[1;31m"     # "\033[1;31m"
    ansi_color_success: "\e[1;32m"  # "\033[1;32m"
    ansi_end: "\e[0m"               # "\033[0m"
```

## Usage

### Features

#### Cmd Utils

Contains wrapper functions for command line execution and misc. utilities, such as unit conversions.

#### Config Holder

Provides objects for command line tool configuration management.

Also contains the means to automatically translate `cta-admin` error messages into more operator-friendly variants.

#### Dict Utils

General python dictionary interaction helpers.

#### Log Utils

Provides unified logging behavior, including log file creation, formatting, and built-in capacity-based log rotation.

It is intended to be used primarily by calling `init_logger(...)` and `log_and_exit(...)`, though some alternate legacy logging options are provided.

#### Mail Sender

Handles email notification templating and sending.

#### Oracle Utils

Used for oracle-specific DB operations. Deprecated.

#### Repack Utils

Common function for the tape repack workflow. 
The provided repack command line tools use these, but with this structure they may be easily be re-used in future tools.

#### SQL Utils

Common functions for executing SQL queries.

#### Table Utils

Common table formatting.

#### Tape Showqueues

Logic specific to showing CTA scheduling queues. 

#### Yaml Loader

Simple wrappers for handling yaml, such as for config files.
