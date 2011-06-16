/*
 * Copyright (C) 1996-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	send_scsi_cmd - Send a SCSI command to a device */
/*	return	-5	if not supported on this platform (serrno = SEOPNOTSUP)
 *		-4	if SCSI error (serrno = EIO)
 *		-3	if CAM error (serrno = EIO)
 *		-2	if ioctl fails with errno (serrno = errno)
 *		-1	if open/stat fails with errno (message fully formatted)
 *		 0	if successful with no data transfer
 *		>0	number of bytes transferred
 */

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <fcntl.h>
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/stat.h>
#include "serrno.h"

void find_sgpath(char *sgpath, int maj, int min) {
}


int send_scsi_cmd (int tapefd,
                   char *path,
                   int do_not_open,
                   unsigned char *cdb,
                   int cdblen,
                   unsigned char *buffer,
                   int buflen,
                   char *sense,
                   int senselen,
                   int timeout,   /* in milliseconds */
                   int flags,
                   int *nb_sense_ret,
                   char **msgaddr)
{
  return 0;
}


void get_ss_msg(int scsi_status,
                char **msgaddr)
{
}
