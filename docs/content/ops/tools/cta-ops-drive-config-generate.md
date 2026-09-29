# CTA Operations Drive Config Generate

This script generates tape drive configuration in JSON format for the CTA tape daemon.
It is intended to be used as a single source of truth for drive naming and related information on the host where a tape drive is connected.
The output may be fed into various templating and configuration tools.

Tape libraries supported:

* IBM TS4500 (configuration in JSON format)
* Spectra Logic TFinity (configuration in XML format)

For all tape drives connected to the given tape server, the script will use the tape drive serial number to extract additional information from the library configuration files.

The script outputs JSON data, which may be written to stdout or directly to a file.

## Installation

### Configuration

The tool can be configured in the standard config file like so:

``` yaml
tools:
  # -------------------------------
  # CTA Tape Drive Config Generator
  # -------------------------------
  cta-ops-drive-config-generate:
    # Timeout for called external commands, such as the scsi ones
    timeout: 10
    # Directory of your library config files in json or xml format
    library_config_dir: '/etc/cta-ops/library_drive_config'
```

### Legacy library name mapping

In a multi-drive CTA installation individual drives may be renamed without downtime on the system as a whole.
However, changing the name of a Logical Library impacts the system as a whole and likely requires downtime.

In the interest of facilitating transition periods where the old Logical Library names are kept until the next maintenance window, the tool supports a mapping where the computed Logical Library name ("DriveLogicalLibrary") can be translated back into an arbitrary existing one.
The tool will then take a computed name such as `IBMLIB1-LTO8` and translate it back to the preferred existing name, which in the example below is `IBM1L8`.

This may be configured in the following section of the config file, under the global `tape` configuration:

```yaml
    # Map drive prefixes to their corresponding logical libraries.
    # This is used to anser the question "Which logical library does this drive belong to?"
    # Key: Drive name root
    # Val: Logical library name
    drive_to_lib_map:
      separator: '-'              # Character to separate components of a drive name
      IBMLIB1-LTO8: 'IBM1L8'
      IBMLIB1-LTO9: 'IBM1L9'
      IBMLIB3-TS1155: 'IBM355'
      ...
```

Give the `--use-legacy-mapping` flag when running the tool in order to make use of this feature.

## Naming convention

...

The tool outputs names such as:

* `IBMLIB4-TS1160-F14C2R1`, which translates to:
    * Drive belonging to Logical Library `IBMLIB4`
    * Drive model `TS1160`
    * Drive position Frame 14, Column 2, Row 1
* `SPECTRALIB2-LTO9-F05B3S4`, which translates to:
    * Drive belonging to Logical Library `IBMLIB4`
    * Drive model `LTO9`
    * Drive position Frame 5, Drive Bay 3, Slot Position 4

## Example

``` bash
$ cta-ops-drive-config-generate
2024-02-19 17:37:05 [INFO] [get_scsi_media_changer] Detected first SCSI media changer device: /dev/sg1
2024-02-19 17:37:05 [INFO] [get_library_type] Detected tape library type and the serial number: IBM-03584L22-0000078AA8070401
2024-02-19 17:37:05 [INFO] [get_connected_drive_devices] Detected tape drive devices: ['/dev/sg0', '/dev/sg2']
2024-02-19 17:37:05 [INFO] [map_changer_device_to_nst] Tape drive /dev/sg0 with serial number 00000754423A is mapped to /dev/nst0
2024-02-19 17:37:05 [INFO] [map_changer_device_to_nst] Tape drive /dev/sg2 with serial number 0000075441DA is mapped to /dev/nst1
{
  "tape_drives": [
    {
      "DriveLogicalLibrary": "IBMLIB4-TS1160",
      "DriveName": "IBMLIB4-TS1160-F2C1R4",
      "DriveDevice": "/dev/nst0",
      "DriveControlPath": "smc3"
    },
    {
      "DriveLogicalLibrary": "IBMLIB4-TS1160",
      "DriveName": "IBMLIB4-TS1160-F2C1R3",
      "DriveDevice": "/dev/nst1",
      "DriveControlPath": "smc2"
    }
  ]
}
```

(logs are written to `stderr`, of course)

## Troubleshooting

### Error codes

If an error was encountered the tool will exit with one of the following exit codes:

* 1: Something unexpected has gone wrong and you must investigate
* 2: The drive could not be found in the library's drive list, resulting in an output of `{}`
* 3: The drive is likely busy, you will have to re-run the script once the drive is free.
