# CTA Ops Admin

The `ctaopsadmin` package provides a command line wrapper for CTA's `cta-admin` command, as well as additional operator tools for day-to-day tape-related tasks.

The goal of the `cta-ops-admin` command is to make life easier for operators by providing:

* Additional tools for hardware life-cycle management, such as the `drive test`, `tape label`, and `tape mediacheck` commands.
* Customization options for information output, such as tabulation options and unit formatting.
* More operator friendly error messages than what `cta-admin` provides by default.
* Mechanisms to separate operator actions from system activity, though options such as the choice of CTA frontend.

## Installation

### Configuration

The display options of `cta-ops-admin <item> ls`, as well as the behavior of the operations-specific tools can be extensively configured.

The following is a reference of available config options:

```yaml
tools:
  # -------------------------------
  # CTA OPS ADMIN
  # -------------------------------
  # Defaults for the operations-specific handlers
  cta-ops-admin:
    tape-drivetest:
      debug: false
      default_file_size: '10G'    # 10 GiB
      default_number_of_files: 100
      # default_block_size: 262144 # 256 * 1024
      default_file_path: '/tape-drivetest/'    # So it will be: /dev/shm/tape-drivetest/
    tape-export:
      debug: false
      number_of_export_attempts: 3
      sleep_time_between_export_attempts: 5  # in seconds
    tape-import:
      debug: false
    tape-label:
      debug: false
      not_free_timeout: 60  # This timeout is used for the delay between the end of tplabel and the end of the dismount
      lock_file: '/tmp/tape-label.lock'
    tape-location:
      debug: false
      cta_statistics_update_cmd: '/usr/bin/cta-statistics-update /etc/cta/cta-catalogue.conf'
    tape-mediacheck:
      debug: false
      random_file_size: 5368709120  # 5 * (1024 ** 3)  # Means 5 * 1024^3
    tape-mount:
      debug: false
    tape-unmount:
      debug: false
      cta_statistics_update_cmd: '/usr/bin/cta-statistics-update /etc/cta/cta-catalogue.conf'
    tape-summary:
      debug: false
      default_file_path: '/tape-summary/'
      cta_statistics_update_cmd: '/usr/bin/cta-statistics-update /etc/cta/cta-catalogue.conf'
    # Define commands and subcommands available to cta-ops-admin.
    # Any command without an explicit handler will be out-sourced to cta-admin instead.
    cta-ops-admin:
      debug: false
      # Substitute empty results in tables with this character
      empty_table_item_char: '-'
      # Define all supported commands
      commands:
        showqueues:
          alias: sq
          help: "Show CTA queue information"
          subcommands:
            retrieve:
              alias: r
              handler: showqueues-retrieve
            archive:
              alias: a
              handler: showqueues-archive
          table_headers:
            mountType: "Type"
            tapepool: "Tape Pool"
            vo: "VO"
            logicalLibrary: "Library"
            vid: "VID"
            queuedFiles: "Files Queued"
            queuedBytes: "Data Queued"
            oldestAge: "Oldest"
            minAge: "Youngest"
            priority: "Priority"
            readMaxDrives: "Read Maximum Drives"
            writeMaxDrives: "Write Maximum Drives"
            curBytes: "Current Bytes"
            curMounts: "Mounts CUR"
            curFiles: "Files CUR"
            data: "Data"
            bytesPerSecond: "MB/s"
            tapes: "Tapes"
            tapesCapacity: "Capacity"
            tapesFiles: "Files on Tapes"
            tapesBytes: "Data on Tapes"
            fullTapes: "Full Tapes"
            writableTapes: "Writable Tapes"
        tape:
          alias: ta
          help: "Manage tape media"
          subcommands:
            summary:
              handler: tape-summary
            location:
              handler: tape-location
            export:
              handler: tape-export
            import:
              handler: tape-import
            label:
              handler: tape-label
            mediacheck:
              handler: tape-mediacheck
            mount:
              handler: tape-mount
            unmount:
              handler: tape-unmount
            add:
            ch:
            rm:
            reclaim:
            ls:
          table_headers:
            vid: "VID"
            mediaType: "Media Type"
            vendor: "Vendor"
            logicalLibrary: "Library"
            purchaseOrder: "Order"
            tapepool: "Tapepool"
            vo: "VO"
            encryptionKeyName: "Encryption Key Name"
            capacity: "Capacity"
            occupancy: "Occupancy"
            lastFseq: "Last fseq"
            full: "Full"
            fromCastor: "From Castor"
            state: "State"
            stateReason: "State Reason"
            labelLog:
              drive: "Label Drive"
              time: "Label Time"
            lastWrittenLog:
              drive: "Last W.Drive"
              time: "Last W.Time"
            writeMountCount: "W.Mounts"
            lastReadLog:
              drive: "Last R.Drive"
              time: "Last R.Time"
            readMountCount: "R.Mounts"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        drive:
          alias: dr
          help: "Manage tape drives"
          subcommands:
            test:
              handler: tape-drivetest
            up:
              handler: 'custom:drive'
            down:
              handler: 'custom:drive'
            ls:
              handler: 'custom:drive'
            ch:
            rm:
          table_headers:
            logicalLibrary: "Logical Library"
            driveName: "Drive Name"
            host: "Host"
            desiredDriveState: "Desired State"
            mountType: "Mount Type"
            driveStatus: "Drive Status"
            driveStatusSince: "Drive Status Since"
            vid: "VID"
            tapepool: "Tapepool"
            filesTransferredInSession: "#Files"
            bytesTransferredInSession: "#Bytes"
            sessionId: "Session ID"
            timeSinceLastUpdate: "Time Since Last Update"
            currentPriority: "Current Priority"
            currentActivity: "Current Activity"
            ctaVersion: "CTA Version"
            devFileName: "Dev File Name"
            rawLibrarySlot: "Raw Library Slot"
        activitymountrule:
          help: "Manage mount policy to username mappings"
          alias: amr
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            diskInstance: "Instance"
            activityMountRule: "Rule Name"
            activityRegex: "Activity"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        admin:
          alias: ad
          help: "Manage CTA administrator accounts"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            user: "User"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        archiveroute:
          alias: ar
          help: "Manage Archive Routes"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            storageClass: "Storage Class"
            copyNumber: "Copy"
            tapepool: "Tapepool"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        diskinstance:
          alias: di
          help: "Manage Disk (EOS) Instances"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Name"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        diskinstancespace:
          alias: dis
          help: "Manage Disk Instance Spaces"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Name"
            diskInstance: "Instance"
            freeSpaceQueryUrl: "Free Space Query URL"
            refreshInterval: "Refresh Interval"
            freeSpace: "Free Space"
            lastRefreshTime: "Last Refresh Time"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        disksystem:
          alias: ds
          help: "Manage Disk Systems"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Name"
            diskInstance: "Instance"
            diskInstanceSpace: "Disk Space"
            fileRegexp: "Reg Exp"
            targetedFreeSpace: "Targeted Free Space"
            sleepTime: "Sleep"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        failedrequest:
          alias: fr
          help: "Manage Failed Requests"
          subcommands:
            rm:
            ls:
          table_headers:
            objectId: "Object ID"
            requestType: "Type"
            copyNb: "#Copy"
            tapepool: "Tapepool"
            requester:
              username: "User"
              groupname: "group"
        groupmountrule:
          alias: gmr
          help: "Manage Mount Policy to user group mappings"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            diskInstance: "Instance"
            groupMountRule: "Group"
            mountPolicy: "Policy"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        logicallibrary:
          alias: ll
          help: "Manage Logical Libraries"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Library"
            isDisabled: "Disabled"
            disabledReason: "Reason"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        mediatype:
          alias: mt
          help: "Manage Media Types"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Media Type"
            cartridge: "Cartridge"
            capacity: "Capacity"
            primaryDensityCode: "Primary Density Code"
            secondaryDensityCode: "Secondary Density Code"
            numberOfWraps: "Number of Wraps"
            minLpos: "Minimum LPos"
            maxLpos: "Maximum LPos"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
        mountpolicy:
          alias: mp
          help: "Manage Mount Policies"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Mount Policy"
            archivePriority: "Archive Priority"
            archiveMinRequestAge: "Archive Minumum Age"
            retrievePriority: "Retrieve Priority"
            retrieveMinRequestAge: "Retrieve Minimum Age"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        physicallibrary:
          alias: pl
          help: "Manage Physical Libraries"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Name"
            manufacturer: "Manufacturer"
            model: "Model"
            type: "Type"
            guiUrl: "GUI URL"
            webcamUrl: "WebCam URL"
            location: "Location"
            nbPhysicalCartridgeSlots: "Physical Cartridge Slots"
            nbAvailableCartridgeSlots: "Available Cartridge Slots"
            nbPhysicalDriveSlots: "Physical Drive Slots"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        recycletf:
          alias: rtf
          help: "Inspect files in the recycle bin"
          subcommands:
            ls:
          table_headers:
            archiveId: "Archive ID"
            copyNumber: "Copy No"
            vid: "VID"
            fsec: "fseq"
            blockId: "Block ID"
            instance: "Instance"
            diskFxid: "Disk fxid"
            size: "Size"
            checksumType: "Checksum Type"
            checksumValue: "Checksum Value"
            storageClass: "Storage Class"
            owner: "Owner"
            group: "Group"
            deletionTime: "Deletion Time"
            pathWhenDeleted: "Path When Deleted"
            reason: "Reason"
        repack:
          alias: re
          help: "Manage Repacks"
          subcommands:
            add:
            rm:
            ls:
            err:
          table_headers:
            creationLog.time: "C.Time"
            repackTime: "Repack Time"
            creationLog.username: "C.User"
            vid: "VID"
            tapepool: "Tape Pool"
            userProvidedFiles: "Provided Files"
            totalFilesOnTapeAtStart: "Files At Start"
            totalBytesOnTapeAtStart: "Bytes At Start"
            totalFilesToRetrieve: "Total Files"
            totalBytesToRetrieve: "Total Bytes"
            filesLeftToRetrieve: "Files to Retrieve"
            filesLeftToArchive: "Files to Archive"
            failed: "Failed"
            status: "Status"
        requestermountrule:
          alias: rmr
          help: "Manage Mount Policy to username mappings"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            diskInstance: "Instance"
            requesterMountRule: "Requester Mount Rule"
            mountPolicy: "Policy"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        storageclass:
          alias: sc
          help: "Manage Storage Classes"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Storage Class"
            nbCopies: "Number of Copies"
            vo: "VO"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        tapefile:
          alias: tf
          help: "Manage Tape Files"
          subcommands:
            ls:
            rm:
          table_headers:
            archiveId: "id"
            storageClass: "Storage Class"
            creationTime: "Creation Time"
        tapepool:
          alias: tp
          help: "Manage Tape Pools"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Name"
            vo: "VO"
            numTapes: "Number of Tapes"
            numPartialTapes: "Number of Partial"
            numPhysicalFiles: "Number of Physical Files"
            capacityBytes: "Capacity"
            dataBytes: "Data"
            encrypt: "Encrypt"
            supply: "Supply"
            created:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            modified:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
        version:
          alias: v
          help: "Show CTA version information"
        virtualorganization:
          alias: vo
          help: "Manage Virtual Organisations"
          subcommands:
            add:
            ch:
            rm:
            ls:
          table_headers:
            name: "Name"
            readMaxDrives: "Read Maximum Drives"
            writeMaxDrives: "Write Maximum Drives"
            maxFileSize: "Maximum File Size"
            diskinstance: "Disk Instace"
            creationLog:
              username: "C.User"
              host: "C.Host"
              time: "C.Time"
            lastModificationLog:
              username: "M.User"
              host: "M.Host"
              time: "M.Time"
            comment: "Comment"
```

## Usage

Use `cta-ops-admin` as you would use `cta-admin`:

``` bash
usage: cta-ops-admin [-h] [--verbose] [-C CONFIG_FILE] <cmd> [<subcmd>]
```

For example:

``` bash
usage: cta-ops-admin tape [-h]
                          {summary,location,export,import,label,mediacheck,mount,unmount,add,ch,rm,reclaim,ls}
                          ...

positional arguments:
  {summary,location,export,import,label,mediacheck,mount,unmount,add,ch,rm,reclaim,ls}
    summary
    location
    export
    import
    label
    mediacheck
    mount
    unmount
    add
    ch
    rm
    reclaim
    ls

optional arguments:
  -h, --help            show this help message and exit
```

``` bash
usage: cta-ops-admin drive [-h] {test,up,down,ls,ch,rm} ...

positional arguments:
  {test,up,down,ls,ch,rm}
    test
    up
    down
    ls
    ch
    rm

optional arguments:
  -h, --help            show this help message and exit
```

## Usage

Use it as you would use `cta-admin`, and try the additional operator tools:

### Drive tools

- `cta-ops-admin drive test`
    - Performs a read/write test on a given drive to check whether or not it functions as expected.

### Tape Tools

- `cta-ops-admin tape import|export`
    - A tools for bulk importing/exporting tapes through the library's I/O slot.
- `cta-ops-admin tape label`
    - Bulk label a selection of tapes, with additional safety checks to prevent data loss.
- `cta-ops-admin tape location`
    - Queries the SMC for the position of the given tape cartridge inside of its library.
- `cta-ops-admin tape mediacheck`
    - Similar to the `drive test` command, this tool performs read/write tests to test if a given cartridge is working as expected.
- `cta-ops-admin tape mount|unmount`
    - A tool for manually mounting/unmounting a tape in a drive in a user-friendly manner.
- `cta-ops-admin tape summary`
    - Provides a simple summary of a given tape for an operator.
