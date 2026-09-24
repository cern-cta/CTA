!!! warning "Deprecated"
    This page is deprecated and may contain information that is no longer up to date.

# Deletion and Garbage Collection

## File deletion and recovering strategies

Here is the link of the draft documentation: [https://codimd.web.cern.ch/f9JQv3YzSmKJ_W_ezXN3fA?view](https://codimd.web.cern.ch/f9JQv3YzSmKJ_W_ezXN3fA?view)

## Delete File

* User-triggered disk copy removal (allowed or not, optional)
* Garbage collection of disk copies
* Complete deletion of files

## User-triggered disk copy removal

CASTOR has learned that it is not easy or even possible to implement the exact "garbage collection" policy
required by experiments when it comes to deleting disk copies of files safely stored on tape.  CASTOR has provided
the `stager_rm` command to end users to enable them to manually garbage collect files in their CASTOR disk
cache. We currently believe that an equivalent of the `stager_rm` command should be implemented in EOS.
Such a command could simply be a request to execute a `stager_rm` workflow action on a specific file.

## Garbage collection of disk copies

A double-criteria garbage collection will probably be necessary to keep free space in disk pools (file age
(LRU/FIFO/etc. ...) + pinning).

## Complete deletion of files

There is no interest in reporting failure to delete the file in CTA while the deletion proceeds in EOS,
so synchronous and asynchronous implementations are equivalent. The complete deletion of files from EOS raises
several race conditions (delete while archiving, delete while retrieving), but all will probably be resolved by
failure of data or metadata operations initiated from CTA to EOS, plus slow reconciliation.  The deletion of the
file can be represented by a notification message (as any file operations can).

When a user wants to definitely delete a file, he will use the command eos rm path/of/file. Here is a description of the usage.

We just archived a file using CTA:

```bash
eos ls -y
d0::t1   -rw-r--r--   1 user1    eosusers        15360 May 13 14:03 test00000000
```

CTA knows about the file:

```bash
cta-admin tapefile ls --vid V01001 --lookupnamespace
archive id copy no    vid  fseq block id instance disk id  size checksum type checksum value   storage class owner group    creation time sc vid sc fseq path
4294977302       1 V01001 10005   100041   ctaeos   10013 15.4K          NONE     0x3f891eec ctaStorageClass 11001  1100 2020-05-13 14:03      -       0 /eos/ctaeos/cta/25360c77-9bbd-46a4-a182-b6e5dde99727/0/test00000000
```

Let's delete the file via EOS:

```bash
eos rm test00000000
```

The file is not there anymore in the EOS namespace nor in the CTA namespace

```bash
eos ls -y
```

```bash
cta-admin tapefile ls --vid V01001 --lookupnamespace
archive id copy no    vid  fseq block id instance disk id  size checksum type checksum value   storage class owner group    creation time sc vid sc fseq path
```

!!!important
    The deleted file is neither present in the EOS namespace nor in the CTA Catalogue.

!!!important
    The file entry in the ARCHIVE_FILE and TAPE_FILE tables of the CTA Catalogue is removed, but the tape counters in the TAPE table where the file was are not updated !

## Why `eos rm` must ignore CTA failing to delete tape file(s) and why reconciliation is the answer

In response to an end user’s `eos rm` command the EOS MGM must first remove the tape location of a file from the EOS namespace before asking  CTA to delete the actual tape file(s).  See [EOS-4251](https://its.cern.ch/jira/browse/EOS-4251).  The EOS MGM namespace must also ignore any failure reported by CTA.  The EOS MGM should of course  log such a failure.

If these two steps were reversed and if removing the tape location of a file from the EOS namespace failed after a successful deletion of the CTA tape file(s) then the end user would have a false sense of security that their tape file(s) still existed. This would be considered data loss.

The proper solution is to allow EOS to delete a file from its namespace even if CTA fails to delete the actual tape files. This will only result in temporary dark tape data which is not a critical problem. CTA can asynchronously reconcile its tape file catalogue with the EOS namespace at a later point in time.

## Files recovering

### Use cases

There are two main use cases for files recovering. Each use case has sub-use-cases.
* Recover files that have been deleted by a user or by EOS
  * By ArchiveFileId, by DiskFileId, by file path
* Recover files that have been repacked
  * Rollback a repack on a repacked tape
* For later: recover bad copies of a file
  * We would like to be able to recover a copy of a file from another one. Example : copyNb 1 of a file is broken we would like to restore it by using the copyNb 2.

For each use-case, we would like to be able to list what are the candidates to be recovered.

## CTA workflow when deleting a file

The deletion of a file from CTA is done by the frontend when the **delete** workflow is triggered. The method `processDELETE()` is called.

The following steps are executed:

* Delete the request from the objectstore (if it exists)
* Move the file to delete into a recycle-bin

### Deletion of the objectstore's ArchiveRequest

If a **delete** is called on a file that is not yet archived to tape, the corresponding ongoing ArchiveRequest will be deleted from the objectstore. EOS gives us the ArchiveRequest's objectstore id if the Archiving is ongoing.

### Moving of the file to delete into a recycle-bin

If a **delete** is called after the file is successfully archived to tape, the ArchiveFile (`ARCHIVE_FILE`) and the TapeFiles (`TAPE_FILE`) entries will be moved to the [recycle-bin](./recycle-bin.md)

## Definitely remove a file from CTA

The only way to definitely remove a file from CTA is to **reclaim** the tape where the file is located.
