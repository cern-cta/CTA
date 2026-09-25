# Tool Installation and Shared Configuration

Operator tools are grouped by task in Tools & Reference, regardless of which package supplies them. Commands built in the CTA source tree are installed with the corresponding [CTA packages or images](../deployment/installation/index.md). Python tools such as `cta-ops-admin` and ATRESYS are distributed from [cta-operations-utilities](https://gitlab.cern.ch/cta/cta-operations-utilities).

The installation steps below apply to that Python distribution. Select a released tag compatible with the deployed CTA version and follow each tool's requirements. EOS-specific tools are documented under [EOS integration](../integrations/eos/tools.md).

## Installation

The tools may be installed using pip. We recommend to create a dedicated virtual environment, in order to avoid dependency conflicts with other pip packages.

For example:

``` bash
git clone https://gitlab.cern.ch/cta/cta-operations-utilities.git
# Review the cta-operations-utilities/requirements.txt file to select packages to install, or leave as-is to install everything.
python3 -m venv venv
source venv/bin/activate
python3 -m pip install --extra-index-url https://cta-public-repo.web.cern.ch/cta-operations/pip/simple/ --requirement cta-operations-utilities/requirements.txt
```

### Requirements

Requirements, such as other pip packages and back-end databases, are specified by each tool.
Use the Python and dependency versions required by the selected utilities release.

### Configuration

You will have to provide a configuration file for the tools to use, which by default is expected to be found at `/etc/cta-ops/cta-ops-config.yaml`.
A reference config file is provided at the root of the utilities repository: `cta-ops-config.yaml`
The `global` contains a set of general config options and default shared by all tools.
Each tool has it's own section under `tools` with tools-specific settings and overrides of the defaults.
You will only need the sections corresponding to the tools you install.

Additionally, some of the command line tools and library functionalities depend on knowing some basic information about the locally installed tape drives.
This information should be provided in a dedicated json file on the tape server, which is pointed to by the `global::tape::drive_facts` config option.
A command line tool to generate such a file from the information provided by the tape libraries themselves is included in the `ctaopsdrvconfgen` package.
The documentation for this package also specifies the intended format, in case you prefer to create this file manually.


## Shared logging and defaults

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

## Tape environment

```yaml
global:
  # ---------------------------------------
  # System binaries for the programs to run
  # ---------------------------------------
  binaries:
    sendmail: "/usr/sbin/sendmail"
    cta-admin: "/usr/bin/cta-admin"
    adler32: "/usr/bin/xrdadler32"
    mt: "/bin/mt"
    tape_label: "/usr/local/bin/tape-label"
    tape_mount: "/usr/local/bin/tape-mount"
    tape_unmount: "/usr/local/bin/tape-unmount"
    cta_smc: "/usr/bin/cta-smc"
    cta_rmcd: "/usr/bin/cta-rmcd"
    cta_label: "/usr/bin/cta-tape-label"
    lsscsi: "/usr/bin/lsscsi"
    sg_inq: "/usr/bin/sg_inq"
    sg_map: "/usr/bin/sg_map"
    sg_logs: "/usr/bin/sg_logs"
    grep: "/usr/bin/grep"
    cta_verify_file_cmd: "/usr/bin/cta-verify-file --id"
  # ----------------------------------------------
  # Settings specific to the tape environment
  # ----------------------------------------------
  tape:
    # Path to the output of `cta-ops-drive-config-generate` (file on disk)
    drive_facts: "/etc/facter/facts.d/drive_facts.json"
    # Path to the cta-admin command line utility config file to use.
    cta-admin_config: '/etc/cta-ops/cta-cli.conf'
    # Determine correct tape pool for a tape based on its logical library
    # and media type. The leaf text should be appended to 'tolabel' or 'erase',
    # based on the repack operation being performed (tolabel_IBM1L7, ...).
    lib_to_pool_map:
      IBM1L8:
        LTO7M: '_IBM1L7'
        LTO8: '_IBM1L8'
      IBM1L9:
        LTO9: '_IBM1L9'
      IBM360:
        3592JD15T: '_IBM355'
        3592JE20T: '_IBM3JE'
      IBM370:
        3592JE50T: '_IBM3JF'
      IBM460:
        3592JC7T: '_IBM4JC'
        3592JD15T: '_IBM455'
        3592JE20T: '_IBM3JE'
      IBM470':
        3592JE50T: '_IBM4JF'
      SPC1L9:
        LTO8: '_SPC1L8'
        LTO9: '_SPC1L9'
      SPC2L9:
        LTO8: '_SPC2L8'
        LTO9: '_SPC2L9'
    tape_drive_types:
      # LTO
      - name: '3588-F8C'
        model: 'LTO8'
      - name: 'IBM Ultrium-TD8 Fibre'
        model: 'LTO8'
      - name: '3588-F9C'
        model: 'LTO9'
      - name: 'IBM Ultrium-TD9 Full Height Fibre'
        model: 'LTO9'
      # Enterprise
      - name: '3592-55F'
        model: 'TS1155'
      - name: '3592-60F'
        model: 'TS1160'
      - name: '3592-70F'
        mode: 'TS1170'
    # Map drive prefixes to their corresponding logical libraries.
    # This is used to anser the question "Which logical library does this drive belong to?"
    # Key: Drive name root
    # Val: Logical library name
    drive_to_lib_map:
      separator: '-'              # Character to separate components of a drive name
      IBMLIB1-LTO8: 'IBM1L8'
      IBMLIB1-LTO9: 'IBM1L9'
      IBMLIB3-TS1155: 'IBM355'
      IBMLIB3-TS1160: 'IBM360'
      IBMLIB3-TS1170: 'IBM370'
      IBMLIB4-TS1155: 'IBM455'
      IBMLIB4-TS1160: 'IBM460'
      SPECTRALIB1-LTO9: 'SPC1L9'
      SPECTRALIB2-LTO9: 'SPC2L9'
      IBMLIB4-TS1170: 'IBM470'
    # As a precaution, allow media (re-)labeling only if tape belongs to one of these special pools:
    pool_prefixes_ok_for_labeling:
      - "tolabel_"
      - "erase_"
      - "test_"
    # A file path where one can find dynamic drive information
    # (the output of cta-ops-drive-config-generate),
    # to be used by other tools which need to know local drive information.
    drive_facts_file_path: 'changeme'
```

The examples above describe sections of the same configuration file; combine their `global` settings rather than defining duplicate YAML keys. Adapt paths, identities, and hardware mappings to the deployment.

## Developing operator tools

See the [upstream utilities repository](https://gitlab.cern.ch/cta/cta-operations-utilities) for library documentation and tool development.

## Troubleshooting

### Trouble installing psycopg2

Some CTA operator tools depend on a PostgreSQL database to function, however, psycopg2 can be a bit tricky to [install](https://www.psycopg.org/docs/install.html).
If the usual install and the binary install fail, you can try to build it yourself:

``` bash
python3 -m pip install wheel
tar xf psycopg2-*.tar.gz && pushd psycopg2-*/ && python3 -m pip wheel --wheel-dir ../ . && popd
```
Then install the package and re-run the install of the CTA operator tools as usual.

## Other useful resources

- [IBM tape-automation](https://github.com/IBM/tape-automation) - Open source tape tools by IBM, such as ITDT
