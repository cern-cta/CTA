# CTA Operations Drive Environmentals

Tape drives and media are sensitive to their ambient environmental conditions.
High humidity may cause condensation within the device, while low humidity may cause harm to the media when in use.
Furthermore, the temperature may affect how data is written or read from tape.

This tool extracts humidity and temperature information from tape drives which contain the respective sensors, and formats these data points into json for a monitoring system.

## Installation

### Requirements

This tool is intended to be run periodically using an external scheduler such as Cron or Rundeck.

### Configuration

The tool can be configured in the standard config file, under the `tools` section, like so:

``` yaml
tools:
  # -------------------------------
  # CTA Drive Environmentals
  # -------------------------------
  cta-ops-drive-environmentals:
    # Drive models which will not have their environmental statistics reported.
    # Useful for models which lack the sensors or are not supported by the script
    model_blacklist:
      - "ULT3580-TD8"
    # File to write results to
    output_file: "/var/log/cta-ops/drive-environmentals.json"
```

## Usage

``` sh
cta-ops-drive-environmentals --output-file /var/log/cta-ops/drive_environmentals.json
```

## Compatibility

This tool should work on recent IBM Enterprise and LTO drives from ULT3580-TD9 (LTO9) and later.


