/*
 * Copyright (C) 1996-2003 by CERN/IT/ADC
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include "scsictl.h"
#include "Ctape_api.h" 
#include "serrno.h" 
#include "sendscsicmd.h" 

int get_compression_stats(int tapefd,
                          char *path,
                          char *devtype,
                          COMPRESSION_STATS *comp_stats)
{
	unsigned long kbytes_from_host = 0;
	unsigned long kbytes_to_tape = 0;
	unsigned long kbytes_from_tape = 0;
	unsigned long kbytes_to_host = 0;
	unsigned char *endpage;
	unsigned char *p;
	unsigned short pagelen;
	unsigned short parmcode;
	unsigned char buffer[256];	/* LOG pages are returned in this buffer */
	unsigned char cdb[10];
	char *msgaddr;
	int nb_sense_ret;
	char sense[256];	/* Sense bytes are returned in this buffer */

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x4D;	/* LOG SENSE */
	cdb[7] = (sizeof(buffer) & 0xFF00) >> 8;
	cdb[8] = sizeof(buffer) & 0x00FF;
	if (strcmp (devtype, "LTO") == 0)
		cdb[2] = 0x40 | 0x32;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "3592") == 0)
		cdb[2] = 0x40 | 0x38;	/* PC = 1, compression page  */
	else if (strcmp (devtype, "T10000") == 0)
	        cdb[2] = 0x40 | 0x0C;   /* PC = 1, sequential access device page */
	else {
		serrno = SEOPNOTSUP;
		return (-1);
	}
 
	if (send_scsi_cmd (tapefd, path, 0, cdb, 10, buffer, sizeof(buffer),
                     sense, 38, SCSI_IN, &nb_sense_ret, &msgaddr) < 0)
		return (-1);

	p = buffer;
	pagelen = *(p+2) << 8 | *(p+3);
	endpage = p + 4 + pagelen;
	p += 4;

	if (strcmp (devtype, "LTO") == 0) {	/* values in bytes */

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
	} else if (strcmp (devtype, "3592") == 0) {	/* values in kB */
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
	} else if (strcmp (devtype, "T10000") == 0) {      /* values in bytes */
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
	} else {
		kbytes_from_host = 0;
		kbytes_to_tape = 0;
		kbytes_from_tape = 0;
		kbytes_to_host = 0;
	}
	comp_stats->from_host = kbytes_from_host;
	comp_stats->to_tape = kbytes_to_tape;
	comp_stats->from_tape = kbytes_from_tape;
	comp_stats->to_host = kbytes_to_host;  
	return (0);
}

int clear_compression_stats(int tapefd,
                            char *path,
                            char *devtype)
{
#if defined(linux)
	unsigned char cdb[10];
	char *msgaddr;
	int nb_sense_ret;
	char sense[256];	/* Sense bytes are returned in this buffer */

	memset (cdb, 0, sizeof(cdb));
	cdb[0] = 0x4C;	/* LOG SELECT */
	cdb[1] = 0x02; /* PCR set */
	cdb[2] = 0xC0; /* PC = 3 */

	if (send_scsi_cmd (tapefd, path, 0, cdb, 10, NULL, 0,
	    sense, 38, SCSI_NONE, &nb_sense_ret, &msgaddr) < 0)
		return (-1);

#endif
	(void)tapefd;
	(void)path;
	(void)devtype;
	return (0);
}
