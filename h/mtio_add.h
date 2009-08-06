/*
** $Id: mtio_add.h,v 1.1 2009/08/06 15:28:04 wiebalck Exp $
*/

/*
** This file defines the MTIOCSENSE ioctl, the interface to get at the SCSI sense
** bytes. This definition is not in the official kernel yet, but will hopefully
** make it there at some point. For the time being, this header needs to be included
** instead. Something along the lines of 
**
** #inlude <linux/mtio.h>
** #ifndef MTIOCSENSE
** #include "mtio_add.h"
** #endif
**
** should make sure that the code is forward compatible.
**
** In order to successfully use this ioctl, loading the st-kmod driver is required.
*/ 

#ifndef _MTIO_ADD_H
#define _MTIO_ADD_H

/* structure for MTIOCSENSE - mag tape get sense data */
#define MTIOCSENSE_SENSE_LENGTH 252
#define MTIOCSENSE_CDB_LENGTH   16
struct mtsense {
	__u16 length;	        	    /* the length of the returned sense data */
 	__u8 cdb[MTIOCSENSE_CDB_LENGTH];    /* the command that returned the sense data */
 	__u8 latest;		            /* is this sense data from the latest SCSI command to device? */
 	__s32 residual;		            /* the residual returned from HBA */
 	__u8 data[MTIOCSENSE_SENSE_LENGTH]; /* space for all data in REQUEST SENSE response */
 };
 
#define MTIOCSENSE      _IOR('m', 4, struct mtsense)    /* get latest sense data */
#define MT_ST_SILI	0x4000

#endif
