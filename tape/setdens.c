/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: setdens.c,v $ $Revision: 1.7 $ $Date: 2007/03/08 09:42:06 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */
 
/*	setdens - set density and compression flag */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "scsictl.h"
#include "serrno.h"

extern int send_scsi_cmd( int, char *, int, char *, int, char *, int, char *, int, int, int, int *, char **);

int setdens(tapefd, path, devtype, den)
int tapefd;
char *path;
char *devtype;
int den;
{
	unsigned char cdb[6];
	unsigned char comppage;
	unsigned char dencode;
	struct devinfo *devinfo;
	char func[16];
	int j;
	unsigned char mscmd[28];
	char *msgaddr;
	int nb_sense_ret;
	char sense[MAXSENSE];

	ENTRY (setdens);

	/* find density code and code page for compression */

	devinfo = Ctape_devinfo (devtype);
	if (*devinfo->devtype == 0) RETURN (0);	/* unknown device */
	comppage = devinfo->comppage;
	for (j = 0; j < CA_MAXDENFIELDS; j++)
		if (den == devinfo->dencodes[j].den) break;
	if (j < CA_MAXDENFIELDS)
		dencode = devinfo->dencodes[j].code;
	else
		dencode = 0;

	/* do a mode sense first */

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x1A;
	cdb[2] = comppage;
	cdb[4] = sizeof (mscmd);
	memset (mscmd, 0, sizeof(mscmd));
	if (send_scsi_cmd (tapefd, path, 0, cdb, 6, mscmd, cdb[4],
	    sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}

	/* change cdb to issue a mode select */

	cdb[0] = 0x15;
	cdb[1] = comppage ? 0x10 : 0;	/* PF bit */
	cdb[2] = 0;
	cdb[3] = 0;
	if (comppage == 0) cdb[4] = 0x0C;
	mscmd[0] = 0;
	if (dencode) {
		mscmd[4] = dencode;
	}
	if (den & IDRC) {	/* enable data compression */
		if (comppage == 0x0F) {
			mscmd[14] = 0x80;
			mscmd[15] = 0x80;
		} else if (comppage == 0x10) {
			mscmd[26] = 1;
		}
	} else {		/* disable data compression */
		if (comppage == 0x0F) {
			mscmd[14] = 0;
			mscmd[15] = 0;
		} else if (comppage == 0x10) {
			mscmd[26] = 0;
		}
	}
	if (send_scsi_cmd (tapefd, path, 0, cdb, 6, mscmd, cdb[4],
	    sense, 38, 30000, SCSI_OUT, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}
	RETURN (0);
}
