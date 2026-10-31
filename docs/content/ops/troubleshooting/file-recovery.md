!!! info "Documentation incomplete"

    This page contains TODOs for unfinished documentation. Address the marked items before removing this notice.

# Recycle Bin and File Recovery

See [Recycle Bin Concepts](../../concepts/data-management/recycle-bin.md) for the recovery boundary. CTA metadata recovery and disk-system namespace recovery must be treated separately.

!!! warning "Needs content review"
    The examples below were moved from a deprecated developer page and need a correctness review before use.

## How files reach the recycle bin

Tape files enter the recycle bin in two ways: the disk system deletes a file, or an operator repacks a tape. In both cases the active `ARCHIVE_FILE`/`TAPE_FILE` entries are removed and the recovery metadata is kept.

### User deletion (`eos rm`)

When a user submits an `eos rm` command on a file, the file is inserted in the recycle bin. For example, the file `test00000000` is located in the tape vid `V01001`:

```none
$ cta-admin tapefile ls --vid V01001 -l
archive id copy no    vid fseq block id instance disk fxid  size checksum type checksum value   storage class owner group    creation time path
4294967301       1 V01001    2       11   ctaeos         c 15.4K       ADLER32       7177e5d6 ctaStorageClass 11001  1100 2021-02-10 16:45 /eos/ctaeos/preprod/ba6347ae-96ea-426b-9e91-e3a138bf1e0b/0/test00000000
```

Let's delete it on EOS:

```none
$ eos rm /eos/ctaeos/preprod/ba6347ae-96ea-426b-9e91-e3a138bf1e0b/0/test00000000
```

The file is now inserted in the recycle-bin

```none
$ cta-admin recycletf ls --fxid c
archive id copy no    vid fseq block id instance disk fxid  size checksum type checksum value   storage class owner group    deletion time                                                       path when deleted reason
4294967301       1 V01001    2       11   ctaeos         c 15.4K       ADLER32       7177e5d6 ctaStorageClass 11001  1100 2021-02-10 16:47 /eos/ctaeos/preprod/ba6347ae-96ea-426b-9e91-e3a138bf1e0b/0/test00000000 File deleted by root from the ctaeos instance
```

The file is not in the tape file entries anymore:

```none
$ cta-admin tapefile ls --fxid c --instance ctaeos
archive id copy no vid fseq block id instance disk fxid size checksum type checksum value storage class owner group creation time path
```

In summary, when a user deletes a file with the `eos rm` command, the associated tape files are moved to the recycle-bin. The associated ARCHIVE_FILE and TAPE_FILE entries are deleted.

### Tape repack

When an operator repacks a tape, the tape files located on the source tape will be deleted from the TAPE_FILE table and will be put into the recycle-bin.

Example:

The tape V01001 that contain 1 file has been repacked.

```none
$ cta-admin re ls
          c.time repackTime    c.user    vid providedFiles totalFiles totalBytes filesToRetrieve filesToArchive failed   status
2021-02-11 13:43        16s ctaadmin2 V01001             0          1      15.4K               0              0      0 Complete
```

No files are on this tape anymore:

```none
$ cta-admin tapefile ls --vid V01001
archive id copy no vid fseq block id instance disk fxid size checksum type checksum value storage class owner group creation time path
```

The repacked files are on the recycle-bin with the reason **REPACK**:

```none
$ cta-admin recycletf ls --vid V01001
archive id copy no    vid fseq block id instance disk fxid  size checksum type checksum value   storage class owner group    deletion time path when deleted reason
4294967298       1 V01001    1        0   ctaeos         9 15.4K       ADLER32       0bc0e709 ctaStorageClass 11001  1100 2021-02-11 13:43                 - REPACK
```

## Listing recycle-bin entries

Entries can be listed in two ways:

- By EOS fxid (hexadecimal form of the diskFileId)
- By tape VID

### By EOS file ID (fxid)

```none
$ cta-admin recycletf ls --fxid 9
archive id copy no    vid fseq block id instance disk fxid  size checksum type checksum value   storage class owner group    deletion time path when deleted reason
4294967298       1 V01001    1        0   ctaeos         9 15.4K       ADLER32       0bc0e709 ctaStorageClass 11001  1100 2021-02-11 13:43                 - REPACK
```

### By VID

```none
$ cta-admin recycletf ls --vid V01001
archive id copy no    vid fseq block id instance disk fxid  size checksum type checksum value   storage class owner group    deletion time path when deleted reason
4294967298       1 V01001    1        0   ctaeos         9 15.4K       ADLER32       0bc0e709 ctaStorageClass 11001  1100 2021-02-11 13:43                 - REPACK
```

## Recovering files

### Procedure (EOS only)

For EOS-backed instances, use `cta-eos-restore-files`. It lists the recycle-bin entries and restores each file in both places that need it: the EOS namespace and the CTA catalogue. It replaces restoring the two sides by hand.

1. **Select the candidates.** List the matching recycle-bin entries with `cta-eos-restore-files ... list`. Equivalent information is
available from `cta-admin recycletf ls --vid <VID>` or `--fxid <FXID>`;
2. **Check tape availability.** Recovery needs readable tape data. Make sure the tape holding the selected copies has not been reclaimed (reclaiming a tape permanently removes its entries from the recycle-bin) or relabelled;
3. **Restore.** Run `cta-eos-restore-files ... restore` with the same selection flags. For each file, the tool:
    1. recreates the entry in the EOS namespace if it no longer exists (containers, checksum, the `sys.archive.file_id` and `sys.eos.btime` extended attributes and a tape replica location);
    2. restores the tape file copy in the CTA catalogue, pointing it at the (most likely new) EOS file ID.
4. **Verify.** Check that `cta-admin tapefile ls --vid <VID>` shows the files again, that `cta-admin recycletf ls` no longer lists them, and that the files are visible in EOS (`eos fileinfo fxid:<FXID>`).

Consult the `README.md` of `cta-eos-restore-files` (or its `--help`) for more information on its parameters and configuration.

## Removing files from the recycle bin

The only way to remove files from the recycle bin is to **reclaim** the tape where they were located.

!!! warning
    Reclaiming is irreversible: once a tape is reclaimed, its entries can no longer be restored. Make sure nothing on the tape still needs recovering.

```none
$ cta-admin recycletf ls --vid V01001
archive id copy no    vid fseq block id instance disk fxid  size checksum type checksum value   storage class owner group    deletion time path when deleted reason
4294967298       1 V01001    1        0   ctaeos         9 15.4K       ADLER32       0bc0e709 ctaStorageClass 11001  1100 2021-02-11 13:43                 - REPACK
```

```none
$ cta-admin tape reclaim --vid V01001
$ cta-admin recycletf ls --vid V01001
archive id copy no    vid fseq block id instance disk fxid  size checksum type checksum value   storage class owner group    deletion time path when deleted reason
```
