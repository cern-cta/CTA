/*
 * Copyright (C) 1996-2003 by CERN/IT/ADC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getcompstat.c,v $ $Revision: 1.18 $ $Date: 2005/03/01 13:52:06 $ CERN Fabien Collin/Jean-Philippe Baud/Benjamin Couturier";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#if defined(ADSTAR)
#include <sys/Atape.h>
#include <sys/scsi.h>
#else
#include "scsictl.h"
#endif
#include "Ctape_api.h" 
#include "serrno.h" 

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
	unsigned char *endpage;
	unsigned char *p;
	unsigned short pagelen;
	unsigned short parmcode;
#if defined(ADSTAR)
	struct log_sense_page log_sense_page;
#else
	unsigned char buffer[256];	/* LOG pages are returned in this buffer */
	unsigned char cdb[10];
	char *msgaddr;
	int nb_sense_ret;
	unsigned char sense[256];	/* Sense bytes are returned in this buffer */
#endif
#if defined(ADSTAR)
	if (strcmp (devtype, "3590") == 0) {
		memset (&log_sense_page, 0, sizeof(log_sense_page));
		log_sense_page.page_code = 0x38;
		if (ioctl (tapefd, SIOC_LOG_SENSE_PAGE, &log_sense_page) < 0) {
			serrno = errno;
			return (-1);
		}
		p = log_sense_page.data;
	}
#else
	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x4D;	/* LOG SENSE */
	cdb[7] = (sizeof(buffer) & 0xFF00) >> 8;
	cdb[8] = sizeof(buffer) & 0x00FF;
	if (strncmp (devtype, "DAT", 3) == 0)
		cdb[2] = 0x40 | 0x39;	/* PC = 1, compression page  */
	else if (strncmp (devtype, "DLT", 3) == 0 ||
		 strcmp (devtype, "SDLT") == 0 ||
		 strcmp (devtype, "LTO") == 0)
		cdb[2] = 0x40 | 0x32;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "3490") == 0 ||
		 strcmp (devtype, "3590") == 0 ||
		 strcmp (devtype, "3592") == 0)
		cdb[2] = 0x40 | 0x38;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "SD3") == 0)
		cdb[2] = 0x40 | 0x30;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "9840") == 0 ||
		 strcmp (devtype, "9940") == 0)
	        cdb[2] = 0x40 | 0x0C;   /* PC = 1, sequential access device page */
	else {
		serrno = SEOPNOTSUP;
		return (-1);
	}
 
	if (send_scsi_cmd (tapefd, path, 0, cdb, 10, buffer, sizeof(buffer),
	    sense, 38, 10000, SCSI_IN, &nb_sense_ret, &msgaddr) < 0)
		return (-1);

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
				break;
			case 0x6:
				kbytes_to_host =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
				break;
			case 0x7:
				kbytes_to_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
				break;
			case 0x8:
				kbytes_from_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
				break;
			}
			p += *(p+3) + 4;
		}
	} else if (strncmp (devtype, "DLT", 3) == 0 ||
		   strcmp (devtype, "SDLT") == 0 ||
		   strcmp (devtype, "LTO") == 0) {	/* values in bytes */

/* Fixed by BC 2003/04/22
   On IBM/HP LTO drives, the number of bytes field can be
   a negative offset.
*/

	while (p < endpage) {
		parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x2:
				kbytes_to_host =
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
				break;
			case 0x3:
				kbytes_to_host +=
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8) >> 10;
				break;
			case 0x4:
				kbytes_from_tape =
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
				break;
			case 0x5:
				kbytes_from_tape +=
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8) >> 10;
				break;
			case 0x6:
				kbytes_from_host =
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
				break;
			case 0x7:
				kbytes_from_host +=
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8) >> 10;
				break;
			case 0x8:
				kbytes_to_tape =
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7)) << 10;
				break;
			case 0x9:
				kbytes_to_tape +=
					(*(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8) >> 10;
				break;
			}
			p += *(p+3) + 4;
		}
	} else if (strcmp (devtype, "3490") == 0 ||
		   strcmp (devtype, "3590") == 0 ||
		   strcmp (devtype, "3592") == 0) {	/* values in kB */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x1:
				kbytes_from_host =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
				break;
			case 0x3:
				kbytes_to_host =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
				break;
			case 0x5:
				kbytes_to_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
				break;
			case 0x7:
				kbytes_from_tape =
				    *(p+4) << 24 | *(p+5) << 16 | *(p+6) << 8 | *(p+7);
				break;
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
				break;
			case 0x10:
				kbytes_from_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
				break;
			case 0x11:
				kbytes_from_host =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
				break;
			case 0x12:
				kbytes_to_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
				break;
			}
			p += *(p+3) + 4;
		}
	} else if (strcmp (devtype, "9840") == 0 ||
		   strcmp (devtype, "9940") == 0) {      /* values in bytes */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x0:
				kbytes_from_host =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
				break;
			case 0x1:
				kbytes_to_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
				break;
			case 0x2:
				kbytes_from_tape =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
				break;
			case 0x3:
				kbytes_to_host =
				    *(p+6) << 30 | *(p+7) << 22 | *(p+8) << 14 | *(p+9) << 6 | *(p+10) >> 2;
				break;
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
	unsigned char *endpage;
	unsigned char *p;
#if defined(ADSTAR)
	struct sc_iocmd sc_iocmd;

	memset (&sc_iocmd, 0, sizeof(sc_iocmd));
	sc_iocmd.scsi_cdb[0] = 0x4C;
	sc_iocmd.scsi_cdb[1] = 0x02;	/* PCR set */
	sc_iocmd.timeout_value = 10;	/* seconds */
	sc_iocmd.command_length = 10;

	if (ioctl (tapefd, STIOCMD, &sc_iocmd) < 0) {
		serrno = errno;
		return (-1);
	}
#else
	unsigned char cdb[10];
	char *msgaddr;
	int nb_sense_ret;
	unsigned char sense[256];	/* Sense bytes are returned in this buffer */

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x4C;	/* LOG SELECT */
	cdb[1] = 0x02; /* PCR set */
	cdb[2] = 0xC0; /* PC = 3 */

	if (send_scsi_cmd (tapefd, path, 0, cdb, 10, NULL, 0,
	    sense, 38, 10000, SCSI_NONE, &nb_sense_ret, &msgaddr) < 0)
		return (-1);

	return (0);
#endif
#endif
}
