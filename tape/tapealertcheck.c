/*
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "scsictl.h"
#include "Ctape_api.h"
#include "serrno.h" 


static char *func = "tapealertcheck";

/* Functions that checks 9840/9940/T10000/LTO/3592 tape flags */
int
get_tape_alerts(tapefd, path, devtype)
int tapefd;
char *path;
char *devtype;
{
#if defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	unsigned char *endpage;
	unsigned char *p;
	unsigned short pagelen;
	unsigned short parmcode;
    int tapealerts;
	unsigned char buffer[256];	/* LOG pages are returned in this buffer */
	unsigned char cdb[10];
	char *msgaddr;
	int nb_sense_ret;
	unsigned char sense[256];	/* Sense bytes are returned in this buffer */
    int rc;
    
    tapealerts = 0;
    
  	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x4D;	/* LOG SENSE */
	cdb[7] = (sizeof(buffer) & 0xFF00) >> 8;
	cdb[8] = sizeof(buffer) & 0x00FF;
    if (strcmp (devtype, "9840") == 0 ||
        strcmp (devtype, "9940") == 0 ||
        strcmp (devtype, "994B") == 0 ||
        strcmp (devtype, "T10000") == 0 ||
        strcmp (devtype, "LTO") == 0 ||
        strcmp (devtype, "3592") == 0) {
        cdb[2] = 0x40 | 0x2E;   /* PC = 1, Tape Alerts Page */
	} else {
		serrno = SEOPNOTSUP;
		return (-1);
	}
 
	if ((rc = send_scsi_cmd (tapefd, path, 0, cdb, 10,
                             buffer, sizeof(buffer),
                             sense, 38, 10000, SCSI_IN,
                             &nb_sense_ret, &msgaddr)) < 0) {
        tplogit (func, "%s", msgaddr);
        return -1; 
    }
    
	p = buffer;

	pagelen = *(p+2) << 8 | *(p+3);
	endpage = p + 4 + pagelen;
	p += 4;

    if (strcmp (devtype, "9840") == 0 ||
        strcmp (devtype, "9940") == 0 ||
        strcmp (devtype, "994B") == 0 ||
        strcmp (devtype, "T10000") == 0 ||
        strcmp (devtype, "LTO") == 0 ||
        strcmp (devtype, "3592") == 0) {
        
        while (p < endpage) {
            parmcode = *p << 8 | *(p+1);
            switch (parmcode) {
            case 0x03: /*Tape Alert Hard Error*/
                tapealerts = tapealerts | *(p+4);
                break;
            case 0x04: /*Tape Alert Media*/ 
                tapealerts = tapealerts | (*(p+4)<<2);
				break;
             case 0x05:/*Tape Read Failure*/
                tapealerts = tapealerts | (*(p+4)<<4);
                break;
            case 0x06: /*Tape Alert Write Failure*/ 
                tapealerts = tapealerts | (*(p+4)<<8);
                break;
            }
            /* Moving to next parameter */
            p += *(p+3) + 4;
        }
    } else {
		return 0;
    }
    
    return tapealerts;
    
#endif
}

