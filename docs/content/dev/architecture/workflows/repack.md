!!! warning "Deprecated"
    This page is deprecated and may contain information that is no longer up to date.

# Repack Workflows

Repacking a tape is very useful to an operator who wants to migrate data from one tape to another, to repair a tape, or to add missing copies of several files.

This chapter covers all the Repack functionalities that have been implemented for now.

## General repack workflow

The repack request can only be submitted while in the *REPACKING* or *REPACKING_DISABLED* states.
In addition, it requires setting up a default Virtual Organization (VO) for Repack (since CTA `v4/5.10.7-1`).

### Setting up a VO for Repack

Any Virtual Organization can be defined as the default VO for Repack at the moment of its creation or during a modification.
This can be set with the optional parameter `--isrepackvo <true|false>`.

Examples:

```bash
cta-admin virtualorganization ch --vo vo_repack -isrepackvo true
```

```bash
cta-admin virtualorganization add --vo vo_repack --readmaxdrives 1 --writemaxdrives 1 --diskinstance ctaeos --comment "vo_repack" --isrepackvo true
```

### Submitting a Repack request

In order to submit a Repack request, the user should follow these steps:

1. Change the tape state to *REPACKING* or *REPACKING_DISABLED*:

    Example:

    ```bash
    cta-admin tape ch --state REPACKING --reason "Testing" --vid V01001
    ```

2. Wait for the tape state change to be completed:

    Example:

    ```bash
    cta-admin --json tape ls --vid V01001
    ```

3. Change the tape's "full" flag to true:

    Example:

    ```bash
    cta-admin tape ch -f true --vid V01001
    ```

4. Finally, submit the Repack request, using the **cta-admin repack** command tool :

    cta-admin re/repack add/rm/ls/err:

      This command allows to manage repack requests.

      Submit a repack request by using the "add" subcommand :
      * Specify the vid (--vid option) or all the vids to repack by giving a file path to the --vidfile option.
      * If the --bufferURL option is set, it will overwrite the default one. It should respect the following format : root://eosinstance//path/to/repack/buffer.
        The default bufferURL is set in the CTA frontend configuration file.
      * If the --justmove option is set, the files located on the tape to repack will be migrated on one or multiple tapes.
        If the --justaddcopies option is set, new (or missing) copies (as defined by the storage class) of the files located on the tape to repack will be created and migrated.
        By default, CTA will migrate AND add new (or missing) copies (as defined by the storage class) of the files located on the tape to repack.
      * The --mountpolicy option allows to give a specific mount policy that will be applied to the repack subrequests (retrieve and archive requests).
        By default, a hardcoded mount policy is applied (every request priorities and minimum request ages = 1).

        add [--vid/-v <vid>] [--vidfile/-f <filename>] [--bufferurl/-b <buffer URL>]
            [--justmove/-m] [--justaddcopies/-a]
            --mountpolicy/-u <mount_policy_name>
        rm  --vid/-v <vid>
        ls  [--vid/-v <vid>]
        err --vid/-v <vid>

After the submission of the Repack request, it will be queued in the **RepackQueuePending**.
The maintenance daemon will then pop the repack request from the **RepackQueuePending** and will start the repack request **expansion**.

**NOTE:** If the tape is on *REPACKING_DISABLED* the expansion will be performed and the requests enqueued, but the tape will not be mounted. For the repacking to proceed, the state must be changed to *REPACKING*.

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

The files located in the source tape will be [moved to the recycle bin](./recycle-bin.md). After a successful repack "just move", the source tape can be **reclaimed**.

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
    "repackBufferUrl": "root://ctaeos//eos/ctaeos/repack",
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
