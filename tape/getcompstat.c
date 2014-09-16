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
#include "endian.h"
#include "stdint.h"

uint64_t getU64ValueFromLogSenseParameter(
  const unsigned char *const logSenseParameter)  {
  union {
    unsigned char tmp[8];
    uint64_t val64;
  } u;
  const unsigned char lenOffset=3; 
  const unsigned char valOffset=4;
  u.tmp[0]=*(logSenseParameter+lenOffset)>0?*(logSenseParameter+valOffset):0;
  u.tmp[1]=*(logSenseParameter+lenOffset)>1?*(logSenseParameter+valOffset+1):0;
  u.tmp[2]=*(logSenseParameter+lenOffset)>2?*(logSenseParameter+valOffset+2):0;
  u.tmp[3]=*(logSenseParameter+lenOffset)>3?*(logSenseParameter+valOffset+3):0;
  u.tmp[4]=*(logSenseParameter+lenOffset)>4?*(logSenseParameter+valOffset+4):0;
  u.tmp[5]=*(logSenseParameter+lenOffset)>5?*(logSenseParameter+valOffset+5):0;
  u.tmp[6]=*(logSenseParameter+lenOffset)>6?*(logSenseParameter+valOffset+6):0;
  u.tmp[7]=*(logSenseParameter+lenOffset)>7?*(logSenseParameter+valOffset+7):0;

  u.val64 = be64toh(u.val64);
     
  return u.val64>>(64-(*(logSenseParameter+lenOffset)<<3));     
}

uint64_t getS64ValueFromLogSenseParameter(
  const unsigned char *const logSenseParameter)  {
  union {
    unsigned char tmp[8];
    uint64_t val64U;
    int64_t  val64S;
  } u;
  const unsigned char lenOffset=3;      
  const unsigned char valOffset=4;
  u.tmp[0]=*(logSenseParameter+lenOffset)>0?*(logSenseParameter+valOffset):0;
  u.tmp[1]=*(logSenseParameter+lenOffset)>1?*(logSenseParameter+valOffset+1):0;
  u.tmp[2]=*(logSenseParameter+lenOffset)>2?*(logSenseParameter+valOffset+2):0;
  u.tmp[3]=*(logSenseParameter+lenOffset)>3?*(logSenseParameter+valOffset+3):0;
  u.tmp[4]=*(logSenseParameter+lenOffset)>4?*(logSenseParameter+valOffset+4):0;
  u.tmp[5]=*(logSenseParameter+lenOffset)>5?*(logSenseParameter+valOffset+5):0;
  u.tmp[6]=*(logSenseParameter+lenOffset)>6?*(logSenseParameter+valOffset+6):0;
  u.tmp[7]=*(logSenseParameter+lenOffset)>7?*(logSenseParameter+valOffset+7):0;

  u.val64U = be64toh(u.val64U);

  return  (u.val64S < 0?-(-u.val64S>> (64-(*(logSenseParameter+lenOffset)<<3))):
          (u.val64S>>(64-(*(logSenseParameter+lenOffset)<<3))));
}

int get_compression_stats(int tapefd,
                          char *path,
                          char *devtype,
                          COMPRESSION_STATS *comp_stats)
{
        uint64_t bytesToHost;
        uint64_t bytesFromTape;
        uint64_t bytesFromHost;
        uint64_t bytesToTape;
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
        bytesToHost=bytesFromTape=bytesFromHost=bytesToTape=0;
        
	if (strcmp (devtype, "LTO") == 0) {	/* values in bytes */
        const uint64_t mb      = 1000000;
	while (p < endpage) {
		parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x2:
				bytesToHost =
				    getU64ValueFromLogSenseParameter(p) * mb;
				break;
			case 0x3:
				bytesToHost +=
				    getS64ValueFromLogSenseParameter(p);
				break;
			case 0x4:
				bytesFromTape =
				    getU64ValueFromLogSenseParameter(p) * mb;
				break;
			case 0x5:
				bytesFromTape +=
		                    getS64ValueFromLogSenseParameter(p);
				break;
			case 0x6:
				bytesFromHost =
				    getU64ValueFromLogSenseParameter(p) * mb;
				break;
			case 0x7:
				bytesFromHost +=
				    getS64ValueFromLogSenseParameter(p);
				break;
			case 0x8:
				bytesToTape =
				    getU64ValueFromLogSenseParameter(p) * mb;
				break;
			case 0x9:
				bytesToTape +=
			            getS64ValueFromLogSenseParameter(p);
				break;
			}
			p += *(p+3) + 4;
		}
	} else if (strcmp (devtype, "3592") == 0) {	/* values in kB */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x1:
				bytesFromHost =
				    getU64ValueFromLogSenseParameter(p) << 10;
				break;
			case 0x3:
				bytesToHost =
				    getU64ValueFromLogSenseParameter(p) << 10;
				break;
			case 0x5:
				bytesToTape =
				    getU64ValueFromLogSenseParameter(p) << 10;
				break;
			case 0x7:
				bytesFromTape =
				    getU64ValueFromLogSenseParameter(p) << 10;
				break;
			}
			p += *(p+3) + 4;
		}
	} else if (strcmp (devtype, "T10000") == 0) {      /* values in bytes */
		while (p < endpage) {
			parmcode = *p << 8 | *(p+1);
			switch (parmcode) {
			case 0x0:
				bytesFromHost =
				    getU64ValueFromLogSenseParameter(p);
				break;
			case 0x1:
				bytesToTape =
				    getU64ValueFromLogSenseParameter(p);
				break;
			case 0x2:
				bytesFromTape =
				    getU64ValueFromLogSenseParameter(p);
				break;
			case 0x3:
				bytesToHost =
				    getU64ValueFromLogSenseParameter(p);
				break;
			}
			p += *(p+3) + 4;
		}
	} 
	
	comp_stats->from_host = bytesFromHost >> 10;
	comp_stats->to_tape = bytesToTape >> 10;
	comp_stats->from_tape = bytesFromTape >> 10;
	comp_stats->to_host = bytesToHost >> 10;  
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
