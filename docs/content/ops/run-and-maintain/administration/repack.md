---
title: Repacking Tapes
---

# Repacking Tapes

Repacking a tape is very useful for operators who want to migrate data from one tape to another, to repair a tape, or to add missing dual-copies to several files.

Repack requests can only be submitted to tapes in *REPACKING* or *REPACKING_DISABLED* states. \
In addition, the operator must set up a default Virtual Organization (VO) for Repack.

## Setting up a VO for Repack

Any Virtual Organization can be defined as the default VO for Repack at the moment of its creation or during a modification.
This can be set with the optional parameter `--isrepackvo <true|false>`.

Examples:

```bash
cta-admin virtualorganization ch --vo vo_repack -isrepackvo true
```

```bash
cta-admin virtualorganization add --vo vo_repack --readmaxdrives 1 --writemaxdrives 1 --diskinstance disk-instance --comment "vo_repack" --isrepackvo true
```

## Submitting a Repack Request

In order to submit a repack request, the user should follow these steps:

1. Change the tape state to *REPACKING* or *REPACKING_DISABLED*, and set it as `full`:

   Example:

    ```bash
    cta-admin tape ch --state REPACKING --full true --reason "Testing" --vid V01001
    ```

2. Wait for the tape state change to be completed:

   Example:

    ```bash
    cta-admin --json tape ls --vid V01001
    ```

3. Finally, submit the repack request:

   ```bash
   cta-admin repack <add|rm|ls|err> [<parameters>]
   ```

Some parameters should be noted (partial list):

- `--bufferURL`:
  - Overwrites the default buffer URL set in the CTA Frontend configuration file. It should respect the following format `root://disk-host//path/to/repack/buffer`.
- `--justmove`:
  - The files located on the tape to repack will be migrated on one or multiple tapes and removed from the original tape.
- `--justaddcopies`:
  - New (or missing) copies (as defined by the storage class) of the files located on the tape will be created and migrated. The original files on the tape won't be removed.
- `--mountpolicy`:
  - Allows to give a specific mount policy that will be applied to the repack subrequests (retrieve and archive requests). By default, a hardcoded mount policy is applied (every request priorities and minimum request ages = 1).

Once a repack request is submitted, it will be queued in the **RepackQueuePending**.
The Maintenance Daemon will then pop the repack request from the **RepackQueuePending** and start the repack request **expansion**.

**NOTE:** If the tape is on *REPACKING_DISABLED* the expansion will be performed and the requests enqueued, but the tape will not be mounted.
For the repacking to proceed, the state must be changed to *REPACKING*.

See [Repack Concepts](../../../concepts/data-management/repack.md) for the workflow.

## Repack status

A Repack request can have all these status during the Repack workflow:

* `Pending` : the Repack request is in the **RepackQueuePending** waiting to be popped by the maintenance daemon.
* `ToExpand` : the Repack request is in the **RepackQueueToExpand** waiting to be popped by the maintenance daemon.
* `Running` : The first Retrieve or Archive subrequest has been reported as **successful** or **failed**
* `Complete` : All the Retrieve and Archive subrequest are completed have been reported as **successful** to the Repack Request.
* `Failed` : All the Retrieve and Archive subrequest are completed but at least one Retrieve or Archive subrequest has failed.

## Repack modes

### Just Move

The Repack "just move" workflow allows the user to **move** the files located in a source tape into another one (destination tape).

The files located in the source tape will be [moved to the recycle bin](../../../concepts/data-management/recycle-bin.md). After a successful repack "just move", the source tape can be **reclaimed**.

In order to launch a Repack "just move" workflow, add the `--justmove` or `-m` option to the *repack add* command.

```sh
cta-admin repack add --vid V01001 --justmove --mountpolicy repack_mp
```

### Just Add Copies

The Repack "just add copies" workflow allow the user to create missing copies of the files that are on the source tape. \
In order to do that, the operator will have to update the storage class of the files present on the source tape in order to increase the number of copies the files should have.

The expansion algorithm of the repack request will create the retrieve sub-request and indicate them that multiple files should be archived.
According to this, one successful retrieve sub-request will be transformed into multiple archive sub-requests.

In order to launch a Repack "just add copies", add the `--justaddcopies` or`-a` option to the *repack add* command.

```sh
cta-admin repack add --vid V01001 --justaddcopies --mountpolicy repack_mp
```

### Move and Add Copies

This feature is the combination of the two previous ones.
It will allow the user to move data from the source tape to another destination one and to create any missing copies of the files of the source tape.

The files located in the source tape will be [moved to the recycle bin](../../../concepts/data-management/recycle-bin.md). After a successful repack "just move", the source tape can be **reclaimed**.

In order to launch this workflow, no flags have to be added to the command :

```sh
cta-admin repack add --vid V01001 --mountpolicy repack_mp
```

### Tape Repair

This workflow allow the operator to reinject file copies from the repack buffer into CTA, via repack.

Imagine that a tape is broken and that 10 over 100 files could not be retrieved from the tape with a normal retrieve request. \
In this scenario, the operator can try to recover the files with specific tools and copy them directly into the repack buffer.
The name of each copied files should contain 9 characters and be named according to their *fSeq* in the source tape.

Example:

- For the file located at the *fSeq* 10 on the source tape, the name of the file copied in the buffer has to be `000000010`.

The operator can then launch the repack request to reinject the file.

During the expansion algorithm loop, `cta-taped` will detect that 10 files are already in the buffer. \
It will then create 10 Retrieve requests with the status **ToReportToRepackForSuccess** and queue them in the **RetrieveQueueToReportToRepackForSuccess**.
These 10 Retrieve requests will then be transformed into archive sub-requests and the repack process will continue.

## Recovering from partial failures

A failed repack does not undo successful destination writes. Recovery must start from the current catalogue state, rather than assuming that all files are still active on the source tape. See [Completion and partial failures](../../../concepts/data-management/repack.md#completion-and-partial-failures).

### 1. Record the outcome and preserve recovery data

Before removing the repack request, save its status, error details, destination tape information, and submission settings: mode, buffer URL, mount policy, and any file-selection limits. Use `cta-admin repack ls` and `cta-admin repack err`; see the [command reference](../../tools/cta-admin.md) for filtering options.

Keep the source tape and repack-buffer files available while assessing recovery. Do not reclaim or relabel the source tape, or clear the buffer, merely because the request has reached a terminal state. If work is still running, establish whether it should finish or be cancelled before planning a replacement request.

### 2. Identify and correct the failure

Distinguish the stage that failed:

| Stage | Checks before retrying |
| --- | --- |
| Expansion | Storage classes and archive routes, repack VO configuration, buffer accessibility, and file-selection settings. |
| Retrieval | Source tape and drive errors, buffer capacity and write access, and whether affected files can be recovered separately. |
| Archival | Destination routes, writable tapes and drive availability, and whether the buffered source files remain readable. |
| Result processing | Maintenance Daemon logs and pending repack reports; a completed transfer may still be awaiting processing. |

Correct the underlying problem before submitting more work. For unreadable source files recovered by other means, use the [Tape Repair](#tape-repair) procedure.

### 3. Determine which files still need work

Compare the saved request results with active tape-copy records in the catalogue. Check both the original source and the reported destination tapes.

For a move, successfully replaced copies are now active on destination tapes; copies whose replacement failed normally remain active on the source. A new repack of that source selects its current active files, rather than replaying the original request.

For add-copies work, inspect the required copy numbers as well as locations. In particular, a combined move-and-add request can move the source copy successfully while failing to create an additional copy. That file is then absent from a fresh selection of the original source tape, so simply repeating the original request would miss the outstanding work. Plan add-copies requests using the tapes that now hold those files, with suitable selection limits.

### 4. Remove the old request and submit the remaining work

Once the diagnostics are saved and outstanding activity is accounted for, remove the old request using the [cancellation procedure](#repack-cancellation), then submit a new request following [Submitting a Repack Request](#submitting-a-repack-request). Removing a request does not roll back copies already recorded in the catalogue.

Choose the mode and source tapes from the assessment above. Recheck routes and storage classes: a new expansion uses the current configuration. If reusing the buffer, retain only files whose identity and integrity have been established; follow the tape-repair naming requirements for supplied files.

### 5. Verify before reclaiming or cleaning up

Check that the replacement requests have finished successfully and that the affected files have all required active copies. If the goal was to empty the original source tape, verify that no active copies remain there; a successful add-copies request alone does not establish this.

Only then proceed with buffer cleanup and, where appropriate, [tape reclamation](tapes-and-drives.md#verify-and-reclaim-tapes). Reclamation removes recycle-bin recovery metadata, and relabelling makes the old tape records inaccessible to normal reads.

## Other functionalities

Other repack-related functionalities are described here.

### Repack cancellation

The operator can cancel a running Repack request by using this command :

```sh
cta-admin repack rm --vid V01001
```

The CTA frontend will then remove all the Repack subrequests and the Repack request itself from the Scheduler DB.

### List destination tapes

By using the flag `--json`/`--jsonl`, an operator can see to which destination tapes the archived files from a repack request have been written to.

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

For automated repack management, see [cta-ops-repack (ATRESYS)](../../tools/repack-automation.md).
