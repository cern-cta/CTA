/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
 
/*	setdens - set density and compression flag */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "scsictl.h"
#include "serrno.h"
#include "sendscsicmd.h"

int setCompression(int tapefd,
            char *path,
            int den)
{
	unsigned char cdb[6];
	char func[16];
	unsigned char mscmd[28];
	char *msgaddr;
	int nb_sense_ret;
	char sense[MAXSENSE];

	ENTRY (setCompression);

	/* do a mode sense first */

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x1A;
	cdb[2] = 0x10; /* device configuration mode page */
	cdb[4] = sizeof (mscmd);
	memset (mscmd, 0, sizeof(mscmd));
	if (send_scsi_cmd (tapefd, path, 0, cdb, 6, mscmd, cdb[4],
	    sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}

	/* change cdb to issue a mode select */

	cdb[0] = 0x15;
	cdb[1] = 0x10; /* PF bit means nothing for IBM, LTO, T10000 */
	cdb[2] = 0;
	cdb[3] = 0;
	mscmd[0] = 0;  /* header.modeDataLength 
                          must be 0 for IBM, LTO ignored by T10000 */
	if (den & IDRC) {	/* enable data compression */
          mscmd[26] = 1; /* modePage.selectDataComprAlgorithm */
	} else {		/* disable data compression */
          mscmd[26] = 0; /* modePage.selectDataComprAlgorithm */
	}
	if (send_scsi_cmd (tapefd, path, 0, cdb, 6, mscmd, cdb[4],
	    sense, 38, 30000, SCSI_OUT, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}
	RETURN (0);
}
