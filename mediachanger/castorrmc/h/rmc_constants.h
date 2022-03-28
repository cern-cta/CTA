/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2001-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once
#include "Castor_limits.h"

#define RMC_CHECKI  5   /* max interval to check for work to be done */
#define RMC_PRTBUFSZ 180
#define RMC_REPBUFSZ 524288 /* must be >= max media changer server reply size */
#define RMC_REQBUFSZ 256    /* must be >= max media changer server request size */
#define RMC_MAGIC	0x120D0301
#define RMC_TIMEOUT	5	/* netread timeout while receiving a request */
#define	RMC_RETRYI	60
#define	RMC_LOGBUFSZ 1024

#define RMC_PORT 5014

#define RMC_MAXRQSTATTEMPTS 10 /* Maximum number of attempts a retriable RMC request should be issued */

			/* SCSI media changer utilities exit codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	CONFERR	  4	/* configuration error */

			/* Request types */

#define	RMC_GETGEOM        1 /* Get robot geometry */
#define	RMC_FINDCART       2 /* Find cartridge(s) */
#define RMC_READELEM       3 /* Read element status */
#define	RMC_MOUNT          4 /* Mount request */
#define	RMC_UNMOUNT        5 /* Unmount request */
#define	RMC_EXPORT         6 /* Export tape request */
#define	RMC_IMPORT         7 /* Import tape request */
#define	RMC_GENERICMOUNT   8 /* Generic (SCSI or ACS) mount request */
#define	RMC_GENERICUNMOUNT 9 /* Generic (SCSI or ACS) mount request */

			/* SCSI media changer server reply types */

#define	MSG_ERR		1
#define	MSG_DATA	2
#define	RMC_RC		3

                        /* SCSI media changer server messages */

#define	RMC00	"RMC00 - SCSI media changer server not available on %s\n"
#define	RMC01	"RMC01 - robot parameter is mandatory\n"
#define	RMC02	"RMC02 - %s error : %s\n"
#define	RMC03	"RMC03 - illegal function %d\n"
#define	RMC04	"RMC04 - error getting request, netread = %d\n"
#define	RMC05	"RMC05 - cannot allocate enough memory\n"
#define	RMC06	"RMC06 - invalid value for %s\n"
#define	RMC09	"RMC09 - fatal configuration error: %s %s\n"
#define	RMC46	"RMC46 - request too large (max. %d)\n"
#define	RMC92	"RMC92 - %s request by %d,%d from %s\n"
#define	RMC98	"RMC98 - %s\n"
