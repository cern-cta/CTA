# CTA & Virtual Tape Libraries

In production CTA is used with physical Tape Libraries. If a physical Tape Library is not available - for example during development and testing - a Virtual Tape Library (VTL) can be used instead. A VTL is a data storage virtualization technology which presents a storage component (usually hard disk storage) as tape libraries or tape drives.

In CI, CTA does not use real tape hardware. Instead, we use a special kernel module that provides such a VTL: [mhvtl](https://github.com/markh794/mhvtl). This kernel module is installed automatically as part of the development setup, so you do not need to run these steps manually in that case. If, however, you want to install and configure mhvtl manually, then this page is for you.

## Installing mhvtl

To get the RPMs for mhvtl, either create them yourself from the [GitHub repo](https://github.com/markh794/mhvtl), or get them through the `cta-dependencies` repo:

- https://cta-public-repo.web.cern.ch/testing/cta-5/el9/cta-dependencies/x86_64/

Install the RPMs for the mhVTL software and kernel module:

```shell
dnf install -y kernel-devel-$(uname -r)
# Change the name of the rpm with wherever your rpms are located.
dnf install -y mhvtl-utils.rpm
```

Also install the command-line utilities to access the virtual tape drives. `mt` is used to control magnetic tape drives (rewinding,
ejecting, skipping files and blocks, etc.). `mtx` is used to control the robotic mechanism in tape libraries:

```shell
dnf install -y mt-st mtx lsscsi
```

## Configure mhvtl

Create the configuration directory `/etc/mhvtl`. The main configuration
file is `/etc/mhvtl/mhvtl.conf`:

```conf
# Home directory for configuration files
MHVTL_CONFIG_PATH=/etc/mhvtl

# Default media capacity in Mb
CAPACITY=1000

# Verbosity level [0|1|2|3]
VERBOSE=1

# Set kernel module debugging [0|1]
VTL_DEBUG=0
```

Configure tape libraries and drives as virtual SCSI devices in `/etc/mhvtl/device.conf`:

```conf
VERSION: 5

Library: 10 CHANNEL: 00 TARGET: 00 LUN: 00
  Vendor identification: STK
  Product identification: VLSTK
  Unit serial number: VLSTK
  NAA: 30:22:33:44:ab:00:08:00
  Compression: factor 1 enabled 1
  Compression type: lzo
  Home directory: /opt/mhvtl
  PERSIST: True
  Backoff: 400

Drive: 11 CHANNEL: 00 TARGET: 1 LUN: 00
  Library ID: 10 Slot: 1
  Vendor identification: STK
  Product identification: MHVTL
  Unit serial number: VDSTK1
  NAA: 30:22:33:44:ab:00:09:00
  Compression: factor 1 enabled 1
  Compression type: lzo
  Backoff: 400

Drive: 12 CHANNEL: 00 TARGET: 2 LUN: 00
  Library ID: 10 Slot: 2
  Vendor identification: STK
  Product identification: MHVTL
  Unit serial number: VDSTK2
  NAA: 30:22:33:44:ab:00:09:00
  Compression: factor 1 enabled 1
  Compression type: lzo
```

!!! warning

    Starting space is not for aesthetical reasons: MHVTL `device.conf` parser may remove some characters in `Product identification` if this space is missing in the next configuration line.

Next, create a configuration file per library to map the drives to the library and define the other elements of the library. For the example above, the configuration file will be `/etc/mhvtl/library_contents.10`. This library is defined with two drives, one robot picker and one Media Access Port (MAP). There are eight tape slots containing seven tapes:

```txt
Drive 1: VDSTK1
Drive 2: VDSTK2

Picker 1:

MAP 1:

Slot 1: V01001TA
Slot 2: V01002TA
Slot 3: V01003TA
Slot 4: V01004TA
Slot 5: V01005TA
Slot 6: V01006TA
Slot 7: V01007TA
Slot 8:
Slot 9:
Slot 10:
```

## Start mhVTL

Start the mhVTL service:

```shell
systemctl start mhvtl.target
```

The service requires the mhVTL kernel module `mhvtl.ko`, which should be loaded automatically by `systemd`.

After starting the mhVTL daemon, the files for the virtual tapes will be created automatically:

```shell
ls -lRF /opt/mhvtl/
```

```txt
/opt/mhvtl/:
total 0
drwxrwx---. 7 vtl vtl 86 May 15 09:58 10/

/opt/mhvtl/10:
total 0
drwxrwx---. 2 vtl vtl 42 May 15 09:58 V31001TA/
drwxrwx---. 2 vtl vtl 42 May 15 09:58 V31002TA/
drwxrwx---. 2 vtl vtl 42 May 15 09:58 V31003TA/
drwxrwx---. 2 vtl vtl 42 May 15 09:58 V31004TA/
drwxrwx---. 2 vtl vtl 42 May 15 09:58 V31005TA/

/opt/mhvtl/10/V31001TA:
total 4
-rw-rw----. 1 vtl vtl    0 May 15 09:58 data
-rw-rw----. 1 vtl vtl    0 May 15 09:58 indx
-rw-rw----. 1 vtl vtl 1536 May 15 09:58 meta
```

Validate that the Virtual Tape Library is working:

```shell
mtx -f /dev/smc inquiry
```

```txt
Product Type: Medium Changer
Vendor ID: 'STK'
Product ID: 'VLSTK'
Revision: '0105'
Attached Changer API: No
```

```shell
mtx -f /dev/smc status
```

```txt
Storage Changer /dev/smc:2 Drives, 9 Slots ( 1 Import/Export )
Data Transfer Element 0:Empty
Data Transfer Element 1:Empty
      Storage Element 1:Full :VolumeTag=V31001TA
      Storage Element 2:Full :VolumeTag=V31002TA
      Storage Element 3:Full :VolumeTag=V31003TA
      Storage Element 4:Full :VolumeTag=V31004TA
      Storage Element 5:Full :VolumeTag=V31005TA
      Storage Element 6:Empty
      Storage Element 7:Empty
      Storage Element 8:Empty
      Storage Element 9 IMPORT/EXPORT:Empty
```

## Virtual Tape Library Commands

Look for the medium changer (mediumx):

```shell
lsscsi -g | grep mediumx
```

The medium changer is `/dev/smc` thanks to the udev rule above

```shell
mtx -f /dev/smc status
```

Mount the tape in storage element 1 into data transfer element 0:

```shell
mtx -f  /dev/smc load 1 0
mtx -f  /dev/smc status
```

Check the status of the drive:

```shell
mt -f /dev/nst0 status
```

Rewind the tape:

```shell
mt -f /dev/nst0 rewind
```

Read the first block from the tape:

```shell
dd if=/dev/nst0 bs=262144 count=1
```

Successively repeating the above command will read out subsequent blocks
from the tape in the following format:

- VOL1 label (80 byte block)

- HDR1 label (80 byte block)

- HDR2 label (80 byte block)

- UHL1 label (80 byte block)

- End of tape file (0 records out)

- First block of user’s file (256 KiB)

- Second block of user’s file (256 KiB)

- …

- Last block of user’s file (<= 256 KiB)

- End of tape file (0 records out)

- EOF1 label (80 byte block)

- EOF2 label (80 byte block)

- UTL1 label (80 byte block)

- End of tape file (0 records out)

Rewind the tape:

```shell
mt -f /dev/nst0 rewind
```

Eject the tape:

```shell
mt -f /dev/nst0 eject
```

Unload the tape to storage element 1 from data transfer element 0:

```shell
mtx -f /dev/smc unload 1 0
```
