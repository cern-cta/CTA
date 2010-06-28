/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <string.h>

/*      inquiry - get information about the type of a drive */
#if defined(linux)
#include <sys/types.h>
#include <fcntl.h>
#include "scsictl.h"
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
#endif
#include "sendscsicmd.h"

int inquiry(fd, path, inq_data)
int fd;
char *path;
unsigned char *inq_data;
{
	char func[16];
	unsigned char buf[36];
	unsigned char cdb[6];
	char *msgaddr;
	int nb_sense_ret;
  char sense[MAXSENSE];
	int tapefd = -1;

  (void)fd;
	ENTRY (inquiry);
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x12;		/* inquiry */
	cdb[4] = 36;
	if (send_scsi_cmd (tapefd, path, 0, cdb, 6, buf, 36,
                     sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}
	memcpy (inq_data, buf+8, 28);
	inq_data[28] = '\0';
        RETURN (0);
}

/*      inquiry80 - get drive serial number */
int inquiry80(fd, path, inq_data)
int fd;
char *path;
unsigned char *inq_data;
{
	unsigned char buf[16];
	char func[16];
	unsigned char cdb[6];
	char *msgaddr;
	int nb_sense_ret;
  char sense[MAXSENSE];
	int rc;
	int tapefd = -1;

  (void)fd;
	ENTRY (inquiry80);
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x12;		/* inquiry */
	cdb[1] = 0x01;
	cdb[2] = 0x80;
	cdb[4] = 16;
	if ((rc = send_scsi_cmd (tapefd, path, 0, cdb, 6, buf, 16,
                           sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr)) < 0) {
#ifndef NOTRACE
		if (rc != -4 || nb_sense_ret < 14 ||
		    (sense[2] & 0xF) != 5 || sense[12] != 0x24 || sense[13] != 0)
#endif
			usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}
	memcpy (inq_data, buf+4, buf[3]);
	inq_data[buf[3]] = '\0';
        RETURN (0);
}
