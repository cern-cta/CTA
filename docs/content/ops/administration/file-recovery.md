# Recycle Bin and File Recovery

See [Recycle Bin Concepts](../../concepts/data-management/recycle-bin.md) for the recovery boundary. CTA metadata recovery and disk-system namespace recovery must be treated separately.

!!! warning "Needs content review"
    The examples below were moved from a deprecated developer page and need a correctness review before use.

## Recovery procedure

!!! info "Documentation outline"
    Document selecting recovery candidates, checking tape availability, restoring CTA metadata, restoring disk-system metadata, and verifying the result here.

## EOS

The [EOS metadata consistency and recovery guide](../integrations/eos/metadata-recovery.md) reserves the recovery scenarios for missing namespace entries and retained entries with unchanged or changed file IDs.

### When a user deletes a file with `eos rm`

When a user submits an `eos rm` command on a file, the file is inserted in the recycle-bin. Example:

The file `test00000000` is located in the tape vid `V01001`:

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

To conclude, when a user deletes a file with the `eos rm` command, the associated tape files are moved to the recycle-bin. The associated ARCHIVE_FILE and TAPE_FILE entries are deleted.

## When an operator repacks a tape

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

## List the files located in the recycle-bin

There are two ways of listing the files located in the recycle-bin:

- By eos fxid (hexadecimal form of the diskFileId)
- By tape VID

### EOS: by hexadecimal disk file ID

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

## Remove the files from the recycle-bin

The only way to remove the files from the recycle-bin is to **reclaim** the tape where these files were located.

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
