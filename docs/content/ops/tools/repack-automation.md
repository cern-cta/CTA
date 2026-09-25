# ATRESYS - Automated Tape REpacking SYStem

ATRESYS is a tool for automating the tape repack process in a CTA system. The automation tools manage the repack on a 
level one step higher than the repack concept in CTA, such that the whole workflow both before and after the repack 
itself can be automated.

ATRESYS contains:

* A command line tool `cta-ops-repack-manager`, which allows an operator to interact with the system.
* A set of scripts `cta-ops-repack-<1-6>...` and `cta-ops-repack-monitor`, which are intended to be run by a job scheduler such as Rundeck or cron.

## Installation

Besides installing the package, a table like the following has to be set up in the target database:

```sql
                         Table "public.repack_status_prod"
     Column      |              Type              | Collation | Nullable | Default 
-----------------+--------------------------------+-----------+----------+---------
 tape            | character varying(20)          |           | not null | 
 entered         | timestamp(0) without time zone |           | not null | 
 started         | timestamp(0) without time zone |           |          | 
 repacked        | timestamp(0) without time zone |           |          | 
 quarantined     | timestamp(0) without time zone |           |          | 
 reclaimed       | timestamp(0) without time zone |           |          | 
 finished        | timestamp(0) without time zone |           |          | 
 files           | bigint                         |           | not null | 
 bytes           | bigint                         |           | not null | 
 usage           | smallint                       |           | not null | 
 last_write_date | timestamp(0) without time zone |           |          | 
 mode            | automation_modes               |           | not null | 
 priority        | smallint                       |           | not null | 
 mediatype       | character varying(20)          |           | not null | 
 tapepool        | character varying(20)          |           | not null | 
 responsible     | character varying(40)          |           | not null |
Indexes:
    "repack_status_prod_pkey" PRIMARY KEY, btree (tape)
```

### Requirements

* A DB with a schema as shown above. The tool is intended to be used with Postgresql.
* A working CTA environment. Use `cta-ops-repack-manager check-env` to verify that all needed pools have been created.
* An external scheduling tool, such as `cron` or `Rundeck`.

### Configuration

The behavior, database and priorities used by the tools can be configured using the following options:

``` yaml
tools:
  # ----------------------
  # ATRESYS - Automated Tape REpacking SYStem
  # ----------------------
  cta-ops-repack-automation:
    # Set debug-level logging by default
    debug: false
    # Set a new subdirectory specifically for repack
    logging:
      log_dir: "/var/log/cta-ops/repack/"
    # Credentials and details for the repack backend database
    database:
      username: "changeme"
      password: "changeme"
      name: "cta_repack"
      host: "changeme"
      port: "changeme"
      repack_table_name: "REPACK_STATUS"
    # Maximum number of concurrent repacks executed by these tools
    max_active_repacks: 10
    # The number of days a tape spends in the quarantine pool before being
    # reclaimed
    quarantine_days: 31
    # Settings specific to each script file
    cta-ops-repack-0-scan:
      # By default only tapes with occupancy > usage_threshold are selected
      tape_usage_threshold: 0.4
      # Default size of scan result
      number_of_tapes: 10
      # Exclude tapes in these pools from the scan
      ignore_pools:
        - "quarantine"
    cta-ops-repack-2-start:
      # Path for the text file containing the list of files found inside a given
      # tape, before the repack has started.
      existing_files_on_tape: "/var/log/cta-ops/repack/repack-tapefile-$DATE-$TAPE.txt"
      # Use this pattern to match the above created files for periodic cleanup.
      cleanup_pattern: "*repack-tapefile*"
      # Keep tapefile listings for this amount of days
      keep_tf_files_days: 14
    cta-ops-repack-3-end:
      # Log location for end-of-CTA-repack statistics
      repack-end-json-path: "/var/log/cta-ops/repack/cta-ops-repack-end-json.log"
    cta-ops-repack-monitor:
      # Fluentd-based monitoring
      ls_out: "/var/log/cta-ops/repack/cta-ops-repack-automation-json.log"
    # Mapping of priorities and mountpolicies
    default_priority: 10
    priorities:
      10:
        name: "low"
        mount_policy: "repack_low_priority"
        default_dst_pool_prefix: "tolabel"
      11:
        name: "low-erase"
        mount_policy: "repack_low_priority"
        default_dst_pool_prefix: "erase"
      49:
        name: "high-tolabel"
        mount_policy: "repack_high_priority"
        default_dst_pool_prefix: "tolabel"
      50:
        name: "high"
        mount_policy: "repack_high_priority"
        default_dst_pool_prefix: "erase"
```

## Usage

### States and operation modes

As a repacking tape moves through the system it will go through the following *states*:

1. **Entered:** The tape has been entered into the automation system.
2. **Started:** A repack object for the tape has been created in CTA.
3. **Repacked:** The repack of the tape has completed in CTA.
4. **Quarantined:** The tape is being held in quarantine before being reclaimed.
5. **Reclaimed:** The tape has been reclaimed using `cta-admin tape reclaim`.
6. **Finished:** The tape has been moved to its destination tolabel/erase pool.

Additionally, an automated repack may run in three different *modes*:

* **auto:** The tape is being processed by the repack automation system.
* **manual:** An operator is working with the tape, the repack automation system will not interact with it.
* **error:** An error has been detected and the repack automation system will not process the tape further until an operator has intervened.

### Command line tool

The repack automation system comes with a command line tool for operators to manage the automated repacks: `cta-ops-repack-manager`

This tool can be used similarly to cta-admin-ops, in order to view, change, remove, etc. jobs from the system.
Run `cta-ops-repack-manager --help` for a complete list of options

### Scheduled scripts

* **cta-ops-repack-0-scan:**
  * Scans through all known tapes and identifies tapes which should be repacked
  * Right now the script supports scanning for full tapes with a low level of utilization
* **cta-ops-repack-1-prepare:**
  * Prepares tapes for repack (adds them to the database)
    * Performs validity checks
* **cta-ops-repack-2-start:**
  * Launches the repack of the tapes which have not been repacked yet
  * Limits number of concurrent repacks in CTA
* **cta-ops-repack-3-end:**
  * Identifies tapes which have been successfully repacked
* **cta-ops-repack-4-quarantine:**
  * Moves repacked tapes to the quarantine pool and updates the DB accordingly
* **cta-ops-repack-5-reclaim:**
  * Reclaims tapes that have completed their quarantine
* **cta-ops-repack-6-finish:**
  * Identifies tapes that have been reclaimed and moves them to the appropriate "tolabel_" or "erase" pool
* **cta-ops-repack-monitor:**
  * Exports the current contents of the DB table to a JSON log file, which can be consumed by a monitoring tool

## Design decisions/philosophy

* `cta-ops-repack-manager` supports both short and extended output forms. The short output should be the default, the extended output can be used for troubleshooting.
* `cta-ops-repack-monitor` outputs JSON logs for easy parsing using, for instance, Fluentd.
