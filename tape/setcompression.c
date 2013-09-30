/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
 
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "scsictl.h"
#include "serrno.h"
#include "sendscsicmd.h"

/**
 * This function switch on or swith off comression on the drive denends on 
 * the density code for the drive in TPCONFIG. 'GC' - means compression will be
 * switched on  and 'G' means to switch it off.
 *
 * @param  tapefd       A tapefd to pass to the send_scsi_cmd.
 * @param  path         A path to pass to the send_scsi_cmd
 * @param  den          A density for the tape from TPCONFIG.
 * 
 * @return 0 if there was not any error inside send_scsi_cmd else -1.
 * In case of error serrno is set by send_scis_cmd.                    
 */

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
