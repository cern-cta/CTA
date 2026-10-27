---
date: 2026-05-12
section: 8ctaops
title: CTAOPS-UDEV
header: The CERN Tape Archive (CTA)
---

# NAME

ctaops-udev - Udev rule and helper that labels CTA tape hardware with stable
udev properties and creates `/dev/tape/by-name/` symlinks.

# DESCRIPTION

The **ctaops-udev** package installs a udev rule and a Python helper that
run when a tape medium changer (`sgN`, SCSI type 8) or a tape drive (`nstN`,
`scsi_tape`) appears on the system. For each such device the helper emits
the following properties on stdout, which udev imports onto the device:

- **CTAOPS_NAME** — logical name of the device. For a medium changer this
  is the logical library name (e.g. `T0-LIB0`); for a tape drive it is
  `<library>-<tech>-<location>` (e.g. `T0-LIB0-LTO9-F01C03R02`).
- **CTAOPS_LIBRARY_ADDRESS** — SCSI element address of the drive within
  its parent medium changer (drives only).
- **CTAOPS_LIBRARY_ORDINAL** — 0-based ordinal of the drive within its
  parent medium changer (drives only).

The udev rule additionally creates a persistent symlink at
`/dev/tape/by-name/${CTAOPS_NAME}` pointing at the corresponding kernel
device.

These properties are the contract consumed by **ctaops-sdk**'s local
repositories (e.g. `LocalNamedLibraryRepository`,
`LocalLocatedNamedDriveRepository`): devices that lack them are
considered unmanaged and silently skipped.

## Files

The RPM installs the following files:

- `/usr/lib/udev/rules.d/62-ctaops.rules` — udev rule matching tape devices
- `/usr/lib/udev/ctaops-udev` — Python helper invoked by the rule
  (`IMPORT{program}=`)
- `/usr/lib/udev/ctaops-udev.ini` — default configuration (sample)
- `/usr/share/man/man8/ctaops-udev.8ctaops` — this man page

# CAVEATS

The **ctaops-udev** RPM is provided **as-is** and is **not stable**. The
parsers, ordinal arithmetic, location regexes, and inventory layout are
tailored to the tape hardware deployed at CERN and will likely need to be
forked for any other site. See the online documentation for the full list
of assumptions.

# CONFIGURATION

The helper reads `/usr/lib/udev/ctaops-udev.ini` at every invocation. All
sections are optional and fall back to baked-in defaults:

```ini
[global]
library_config_dir = /etc/cta-ops/library_drive_config
exclude_vendor_logical_library = BACKUP, BACKUP2

[tech_model_drive_map]
LTO8   = 3588-F8C, IBM Ultrium-TD8 Fibre
LTO9   = 3588-F9C, IBM Ultrium-TD9 Full Height Fibre
LTO10  = 3588-FAC, IBM Ultrium-TDA Full Height Fibre
TS1155 = 3592-55F
TS1160 = 3592-60F
TS1170 = 3592-70F

[vendor_map_suffix]
IBM     = .json
SPECTRA = .xml
```

- **global.library_config_dir** — directory where the per-library
  inventory files live.
- **global.exclude_vendor_logical_library** — vendor logical-library
  names to ignore when picking `CTAOPS_NAME` for a changer (e.g.
  internal `BACKUP` partitions).
- **tech_model_drive_map.&lt;tech&gt;** — comma-separated SCSI model
  strings that should map to `<tech>` in the drive name.
- **vendor_map_suffix.&lt;vendor&gt;** — file extension expected for
  that vendor's inventory file.

# PRE-REQUISITES

For every tape library, place an inventory file under
`global.library_config_dir`. The filename convention is
`<VENDOR>-<MODEL>-<SERIAL>.<json|xml>`; the three values come from
`sg_inq` on the medium-changer device.

The file is produced by the vendor's own tool:

- **IBM** libraries — use *itdt*:

```bash
/opt/itdt/itdt -f /dev/smc RoS GET /v1/drives \
    > /etc/cta-ops/library_drive_config/IBM-03584L22-0000078AA6480401.json
```

- **Spectra Logic** libraries — use *slapi*:

```bash
/usr/bin/slapi --config /root/SPECTRA/slapi.conf \
    --server spectralib2 drivelist \
    > /etc/cta-ops/library_drive_config/SPECTRA-PYTHON-92400068FC.xml
```

# DIAGNOSTICS

Inspect what udev resolved for a given device:

```bash
udevadm info --query=all --name=/dev/nst0
```

Re-run the rules against a device to see the helper's output:

```bash
udevadm test /sys/class/scsi_tape/nst0
```

Check the udev daemon log for invocation failures:

```bash
journalctl -u systemd-udevd | grep ctaops-udev
```

# SEE ALSO

- udev(7)
- systemd-udevd(8)
- udevadm(8)
- The `ctaops-sdk` documentation — the library that consumes the
  `CTAOPS_NAME`, `CTAOPS_LIBRARY_ADDRESS`, and `CTAOPS_LIBRARY_ORDINAL`
  properties this tool produces.

# COPYRIGHT

Copyright © 2024-2026 CERN. License GPLv3+: GNU GPL version 3 or later
[http://gnu.org/licenses/gpl.html](http://gnu.org/licenses/gpl.html).
This is free software: you are free to change and redistribute it. There
is NO WARRANTY, to the extent permitted by law. In applying this licence,
CERN does not waive the privileges and immunities granted to it by virtue
of its status as an Intergovernmental Organization or submit itself to
any jurisdiction.
