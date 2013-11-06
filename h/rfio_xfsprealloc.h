/* RFIO XFS preallocation support */
/* 2006/02/15 KELEMEN Peter <Peter.Kelemen@cern.ch> CERN IT/FIO/LA */

#ifndef RFIO_XFSPREALLOC_H
#define RFIO_XFSPREALLOC_H

#include <sys/types.h>
#include <sys/stat.h>

/* NOTE(fuji): Since the filesize is not part of the protocol, we cannot
 * now apriori how much the client is going to write, so we reserve the
 * amount requested, and try to unreserve the remaining space later.
 * If the file grows above this, XFS will take care of allocating the rest
 * and we have nothing to do. */

/* ex:
 * RFIOD XFSPREALLOC 1024
 * ...would make RFIO preallocate 1 GiB for each file. */

void rfio_xfs_resvsp64(int fd, unsigned long mbytes);
void rfio_xfs_unresvsp64(int fd, unsigned long mbytes, off64_t written);

#endif /* RFIO_XFSPREALLOC_H */

/* eof */
