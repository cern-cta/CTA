/*
 * Copyright (C) 1996-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "%W% %G% CERN IT-PDP/DM Fabien Collin/Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#if defined(ADSTAR)
#include <sys/Atape.h>
#else
#include "scsictl.h"
#endif
#include "Ctape_api.h" 

get_compression_stats(tapefd, path, devtype, comp_stats)
int tapefd;
char *path;
char *devtype;
COMPRESSION_STATS *comp_stats;
{
#if defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	unsigned long kbytes_from_host;
	unsigned long kbytes_to_tape;
	unsigned long kbytes_from_tape;
	unsigned long kbytes_to_host;
	int c;
	unsigned char *endpage;
	unsigned char *p;
	unsigned short pagelen;
	unsigned short parmcode;
#if defined(ADSTAR)
	struct log_sense log_sense;
#else
	unsigned char buffer[256];	/* LOG pages are returned in this buffer */
	unsigned char cdb[10];
	char *msgaddr;
	int nb_sense_ret;
	unsigned char sense[256];	/* Sense bytes are returned in this buffer */
#endif
#if defined(ADSTAR)
	if (strcmp (devtype, "3590") == 0) {
		if ((c = ioctl (tapefd, STIOC_LOG_SENSE, &log_sense) < 0)
			return (c);
		p = log_sense.data;
	}
#else
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x4D;	/* LOG SENSE */
	cdb[7] = (sizeof(buffer) & 0xFF00) >> 8;
	cdb[8] = sizeof(buffer) & 0x00FF;
	if (strncmp (devtype, "DAT", 3) == 0)
		cdb[2] = 0x40 | 0x39;	/* PC = 1, compression page  */
	else if (strncmp (devtype, "DLT", 3) == 0)
		cdb[2] = 0x40 | 0x32;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "3590") == 0)
		cdb[2] = 0x40 | 0x38;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "SD3") == 0)
		cdb[2] = 0x40 | 0x30;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "9840") == 0)
	        cdb[2] = 0x40 | 0x0C;   /* PC = 1, sequential access device page */
 
	if ((c = send_scsi_cmd (tapefd, path, 0, cdb, 10, buffer, sizeof(buffer),
	    sense, 38, 10000, SCSI_IN, &nb_sense_ret, &msgaddr)) < 0)
		return (c);

	p = buffer;
#endif
	pagelen = *(p+2) << 8 | *(p+3);
	endpage = p + 4 + pagelen;
	p += 4;

	if (strncmp (devtype, "DAT", 3) == 0) {		/* values in kB */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x5:
				kbytes_from_host =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			case 0x6:
				kbytes_to_host =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			case 0x7:
				kbytes_to_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			case 0x8:
				kbytes_from_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			}
			p += *(p+3) + 4;
		}
	} else if (strncmp (devtype, "DLT", 3) == 0) {	/* values in bytes */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x2:
				kbytes_to_host =
				    (*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
			case 0x3:
				kbytes_to_host +=
				    *(p+5) << 6 | *(p+6) >> 2;
			case 0x4:
				kbytes_from_tape =
				    (*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
			case 0x5:
				kbytes_from_tape +=
				    *(p+5) << 6 | *(p+6) >> 2;
			case 0x6:
				kbytes_from_host =
				    (*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
			case 0x7:
				kbytes_from_host +=
				    *(p+5) << 6 | *(p+6) >> 2;
			case 0x8:
				kbytes_to_tape =
				    (*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
			case 0x9:
				kbytes_to_tape +=
				    *(p+5) << 16 | *(p+6) >> 2;
			}
			p += *(p+3) + 4;
		}
	} else if (strcmp (devtype, "3590") == 0) {	/* values in kB */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x1:
				kbytes_from_host =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			case 0x3:
				kbytes_to_host =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			case 0x5:
				kbytes_to_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			case 0x7:
				kbytes_from_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
			}
			p += *(p+3) + 4;
		}
	} else if (strcmp (devtype, "SD3") == 0) {	/* values in bytes */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0xF:
				kbytes_to_host =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			case 0x10:
				kbytes_from_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			case 0x11:
				kbytes_from_host =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			case 0x12:
				kbytes_to_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			}
			p += *(p+3) + 4;
		}
	} else if (strcmp (devtype, "9840") == 0) {      /* values in bytes */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x0:
				kbytes_from_host =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			case 0x1:
				kbytes_to_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			case 0x2:
				kbytes_from_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			case 0x3:
				kbytes_to_host =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
			}
			p += *(p+3) + 4;
		}
	}
	comp_stats->from_host = kbytes_from_host;
	comp_stats->to_tape = kbytes_to_tape;
	comp_stats->from_tape = kbytes_from_tape;
	comp_stats->to_host = kbytes_to_host;  
	return (0);
#endif
}

clear_compression_stats(tapefd, path, devtype)
int tapefd;
char *path;
char *devtype;
{
#if defined(ADSTAR) || defined(SOLARIS25) || defined(sgi) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	int c;
	unsigned char *endpage;
	unsigned char *p;
#if defined(ADSTAR)
#error Not implemented ???
#else
	unsigned char cdb[10];
	char *msgaddr;
	int nb_sense_ret;
	unsigned char sense[256];	/* Sense bytes are returned in this buffer */

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x4C;	/* LOG SELECT */ /* Done for STK 9840, SD3 and IBM 3590 */
	cdb[1] = 0x02; /* PCR set */ /* Check for other devices */
	cdb[2] = 0xC0; /* PC = 3 */

	if ((c = send_scsi_cmd (tapefd, path, 0, cdb, 10, NULL, 0,
	    sense, 38, 10000, SCSI_IN, &nb_sense_ret, &msgaddr)) < 0)
		return (c);

	return (0);
#endif
#endif
}
