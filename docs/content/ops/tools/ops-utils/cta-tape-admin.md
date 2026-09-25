# TapeAdmin

*TapeAdmin* is a python library for interacting with tape infrastructure components, such as *tape media*, *tape drives* and *tape libraries*.
The aim is to have this library provide a consistent way of working with these devices to the higher-level tape operations tools.

This library is specific to tape infrastructure at CERN, so it expects a CTA deployment and supported libraries (IBM/Spectralogic) and drives (IBM) to be present.

## Installation

### Requirements

This library is tightly linked to the `ctautils` library and expects it to be present for purposes such as logging.

The CTA command line tools are needed for this library.

### Configuration

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
 
## Usage

The library may be used as a basis to develop additional tape infrastructure tools.

### Features

#### External binary configuration

TapeAdmin allows for the paths to required external tools and binaries to be specified in the configuration file.
These include fundamental tape commands such as `mt`, `lsscsi`, etc.
It will insert these paths into configured commands, and attempt to replace default locations specified in the code with their corresponding configured location.

#### CTA Admin configuration

Much of TapeAdmin's functionality relies on `cta-admin` commands.
These commands are automatically run with the configured keytab location and authentication methods.

