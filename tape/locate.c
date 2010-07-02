/*
 * Copyright (C) 1997-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: locate.c,v $ $Revision: 1.7 $ $Date: 2007/11/20 15:41:28 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "scsictl.h"
#include "serrno.h"
#include <stdlib.h>
#include "sendscsicmd.h"

int locate(int tapefd,
           char *path,
           unsigned char *blockid)
{
	unsigned char cdb[10];
	char func[16];
	char *msgaddr;
	int nb_sense_ret;
	char sense[MAXSENSE];
        int locate_timeout = LOCATE_MIN_TIMEOUT; 
        char* getconfent(); 
        char * p = NULL;

	ENTRY (locate);
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x2B;		/* locate */
	memcpy (&cdb[3], blockid, 4);
        
        p = getconfent ("TAPE", "LOCATE_TIMEOUT", 0);
        if (NULL != p) {                
                locate_timeout = (int)atoi(p);
                /* 
                   atoi()'s behaviour is undefined if
                   the value is not convertible. For 
                   this reason (and in order to prevent
                   to give up too early), reset the 
                   timeout to the lower threshold if 
                   it is too small.
                */   
                if (locate_timeout < LOCATE_MIN_TIMEOUT) {
                        locate_timeout = LOCATE_MIN_TIMEOUT;
                }
        }
                
	if (send_scsi_cmd (tapefd, path, 0, cdb, 10, NULL, 0,
	    sense, 38, locate_timeout, SCSI_NONE, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}
	RETURN (0);
}

int read_pos(int tapefd,
             char *path,
             unsigned char *blockid)
{
	unsigned char cdb[10];
	unsigned char data[20];
	char func[16];
	char *msgaddr;
	int nb_sense_ret;
	char sense[MAXSENSE];

	ENTRY (read_pos);
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x34;		/* read position */
	if (send_scsi_cmd (tapefd, path, 0, cdb, 10, data, 20,
	    sense, 38, 30000, SCSI_IN, &nb_sense_ret, &msgaddr) < 0) {
		usrmsg (func, "%s", msgaddr);
		RETURN (-1);
	}
	if ((data[0] & 0x4) == 0)	/* block position is valid */
		memcpy (blockid, &data[4], 4);
	RETURN (0);
}
