---
title: Repack workflow architecture
---

!!! warning "Deprecated"
    This page is deprecated and may contain information that is no longer up to date.

# Repack Workflows

Repacking a tape is very useful to an operator who wants to migrate data from one tape to another, to repair a tape, or to add missing copies of several files.

This chapter covers all the Repack functionalities that have been implemented for now.

## General repack workflow

The repack request can only be submitted while in the *REPACKING* or *REPACKING_DISABLED* states.
In addition, it requires setting up a default Virtual Organization (VO) for Repack.

Operator setup and commands are documented under [Repacking Tapes](../../../ops/administration/repack.md).

### Expansion of a Repack request

The expansion of the Repack request is the "transformation" of the
Repack request into multiple retrieve requests. In order to do that,
the expansion algorithm will ask the CTA Catalogue to give him all the files that
are located in the source tape (method `catalogue.getArchiveFilesForRepack Itor(sourceVID, fSeq)`).

For each files in the source tape, the scheduler will create a Retrieve
subrequest and queue each subrequest into the **RetrieveQueueToTransfer**.

### Repack Retrieve subrequest execution

According to the mount policies given to the Repack request during its submission, the Retrieve subrequests will be popped from the **RetrieveQueueToTransfer** and will be executed.
The successful Retrieve requests will be queued in the **RetrieveQueueToReportToRepackForSuccess**.

The failed Retrieve requests will be queued in the **RetrieveQueueToReportToRepackForFailure** (after 5 attempts of Retrieving).

### Reporting of the Repack Retrieve subrequest

The maintenance daemon will pop the Retrieve subrequest queued in the **RetrieveQueueToReportToRepackForSuccess** and in the **RetrieveQueueToReportToRepackForFailure**.
In the case of success or failure, the Repack Request will have its statistics updated (retrieved files, retrieved bytes, failed to retrieved files, failed to retrieve bytes).

The successful Retrieve subrequests will be transformed into **Archive subrequests** and queued into the **ArchiveQueueToTransferForRepack**.

### Repack Archive subrequest execution

The repack Archive subrequests will be popped from the **ArchiveQueueToTransferForUser** and will be executed. The successful ones will be queued into the **ArchiveQueueToReportToRepackForSuccess**, the failed ones will be queued in the **ArchiveQueueToReportToRepackForFailure**.

### Reporting of the Repack Archive subrequests

The maintenance daemon will pop the Archive subrequests queued in the **ArchiveQueueToReportToRepackForSuccess** and in the **ArchiveQueueToReportToRepackForFailure**.
It will then update the Repack Request statistics (Archived files, Archive bytes, failed to archive files, failed to archive bytes).

### End of the general Repack workflow

When all the Retrieve and Archive subrequests are reported as successful or failed, the Repack Request will have its status updated as **Complete** or **Failed**.

## Repack status during the execution of the Repack request

A Repack request can have all these status during the Repack workflow:

* Pending : the Repack request is in the **RepackQueuePending** waiting to be popped by the maintenance daemon.
* ToExpand : the Repack request is in the **RepackQueueToExpand** waiting to be popped by the maintenance daemon.
* Running : The first Retrieve or Archive subrequest has been reported as **successful** or **failed**
* Complete : All the Retrieve and Archive subrequest are completed have been reported as **successful** to the Repack Request.
* Failed : All the Retrieve and Archive subrequest are completed but at least one Retrieve or Archive subrequest has failed.

## Repack "just move"

The Repack "just move" workflow allows the user to **move** the files located in a source tape into another one (destination tape).

The files located in the source tape will be [moved to the recycle bin](recycle-bin.md). After a successful repack "just move", the source tape can be **reclaimed**.

In order to launch a Repack "just move" workflow, add the `--justmove` or `-m` option to the *repack add* command.

```sh
cta-admin repack add --vid V01001 --justmove --mountpolicy repack_mp
```

## Repack "just add copies"

The Repack "just add copies" workflow allow the user to create missing copies of the files that are on the source tape.
In order to do that, the operator will have to update the storage class of the files present on the source tape in order to increase the number of copies the files should have.

The expansion algorithm of the Repack request will create the Retrieve subrequest and indicate them that multiple files should be archived. According to this, one successful Retrieve subrequest will be transformed into multiple Archive subrequests.

In order to launch a Repack "just add copies", add the `--justaddcopies` or`-a` option to the *repack add* command.

```sh
cta-admin repack add --vid V01001 --justaddcopies --mountpolicy repack_mp
```

## Repack "Move and add copies"

This feature is the combination of the two previous ones. It will allow the user to move data from the source tape to another destination one and to create the missing copies of the files of the source tape.

In order to launch this workflow, no flags have to be added to the command :

```sh
cta-admin repack add --vid V01001 --mountpolicy repack_mp
```

## Repack "tape repair"

This workflow allow the operator to reinject files into CTA via Repack.

Imagine that a tape is broken and that 10 over 100 files could not be retrieved from the tape with a normal Retrieve request. The operator could try to recover the files with specific tools and copy these files directly into the Repack buffer. The name of each copied files should contain 9 characters and be named according to their fSeq in the source tape. Example : for the file located at the fSeq 10 on the source tape, the name of the file copied in the buffer has to be `000000010`.

The operator will then launch the Repack request.

During the expansion algorithm loop, `cta-taped` will detect that 10 files are already in the buffer. It will then create 10 Retrieve requests with the status **ToReportToRepackForSuccess** and queue them in the **RetrieveQueueToReportToRepackForSuccess**. These 10 Retrieve requests will then be transformed into Archive requests and the Repack process will continue.

## Other repack functionalities

The functionalities presented here are other Repack-related functionalities that are implemented.

### Repack a disabled tape

Currently, it is impossible for an operator to Retrieve files from a tape that is disabled. Launching a Repack request on a disabled tape without a specific option will **fail** the Repack request because it is not possible to Retrieve files from a disabled tape.

In order to override this behaviour, the operator can set a flag *--disabledtape* or *-d*. CTA will then know that the disabled tape could be mounted in order to Repack it.

```sh
cta-admin repack add --vid V01001 --disabledtape --mountpolicy repack_mp
```

### Repack cancellation

The operator can cancel a running Repack request by using this command :

```sh
cta-admin repack rm --vid V01001
```

The CTA frontend will then remove all the Repack subrequests and the Repack request itself from the objectstore.

### cta-admin repack ls indicates the destination tapes

By using the flag `---json`, an operator can see in which destination tape the archived files from a Repack request have been written to.

```sh
bash: cta-admin --json re ls | jq

[
    {
    "vid": "V01001",
    "repackBufferUrl": "root://disk-host//repack",
    "userProvidedFiles": "0",
    "totalFilesToRetrieve": "1153",
    "totalBytesToRetrieve": "17710080",
    "totalFilesToArchive": "1153",
    "totalBytesToArchive": "17710080",
    "retrievedFiles": "1153",
    "archivedFiles": "1153",
    "failedToRetrieveFiles": "0",
    "failedToRetrieveBytes": "0",
    "failedToArchiveFiles": "0",
    "failedToArchiveBytes": "0",
    "lastExpandedFseq": "1153",
    "status": "Complete",
    "destinationInfos": [
        {
        "vid": "V01003",
        "files": "1153",
        "bytes": "17710080"
        }
    ]
    }
]
```
