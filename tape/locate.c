/*
 * Copyright (C) 1997-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: locate.c,v $ $Revision: 1.2 $ $Date: 2000/03/31 15:07:55 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include "Ctape.h"
#include "scsictl.h"
locate(tapefd, path, blockid)
int tapefd;
char *path;
unsigned char *blockid;
{
	unsigned char cdb[10];
	char func[16];
	char *msgaddr;
	int nb_sense_ret;
	int rc;
	char sense[MAXSENSE];

	ENTRY (locate);
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x2B;		/* locate */
	memcpy (&cdb[3], blockid, 4);
	rc = send_scsi_cmd (tapefd, path, 0, cdb, 10, NULL, 0,
		sense, 38, 180000, SCSI_NONE, &nb_sense_ret, &msgaddr);
	if (rc < 0) {
		usrmsg (func, "%s", msgaddr);
		if (rc == -1 || rc == -2) {
			RETURN (-errno);
		} else {
			RETURN (-EIO);	/* error */
		}
	}
	RETURN (0);
}

read_pos(tapefd, path, blockid)
int tapefd;
char *path;
unsigned char *blockid;
{
	unsigned char cdb[10];
	unsigned char data[20];
	char func[16];
	char *msgaddr;
	int nb_sense_ret;
	int rc;
	char sense[MAXSENSE];

	ENTRY (read_pos);
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x34;		/* read position */
	rc = send_scsi_cmd (tapefd, path, 0, cdb, 10, data, 20,
		sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr);
	if (rc < 0) {
		usrmsg (func, "%s", msgaddr);
		if (rc == -1 || rc == -2) {
			RETURN (-errno);
		} else {
			RETURN (-EIO);	/* error */
		}
	}
	if ((data[0] & 0x4) == 0)	/* block position is valid */
		memcpy (blockid, &data[4], 4);
	RETURN (0);
}
