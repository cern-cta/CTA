/*
 * $Id: rmc.h,v 1.2 2007/04/19 15:18:19 sponcec3 Exp $
 */

/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: rmc.h,v $ $Revision: 1.2 $ $Date: 2007/04/19 15:18:19 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _RMC_H
#define _RMC_H

			/* SCSI media changer server constants */

#include "osdep.h"
#include "rmc_constants.h"
#include "smc.h"
#define	CHECKI  5	/* max interval to check for work to be done */
#define	MAXRETRY 5
#define	RETRYI	60
#define	LOGBUFSZ 1024
#define	PRTBUFSZ 180
#define	REPBUFSZ 524288	/* must be >= max media changer server reply size */
#define	REQBUFSZ 256	/* must be >= max media changer server request size */
#define RMC_MAGIC	0x120D0301
#define RMC_TIMEOUT	5	/* netread timeout while receiving a request */

int rmclogit(char*, char*, ...);

#define RETURN(x) \
	{ \
	rmclogit (func, "returns %d\n", (x)); \
	return ((x)); \
	}

			/* Request types */

#define	RMC_GETGEOM	1	/* Get robot geometry */
#define	RMC_FINDCART	2	/* Find cartridge(s) */
#define RMC_READELEM	3	/* Read element status */
#define	RMC_MOUNT	4	/* Mount request */
#define	RMC_UNMOUNT	5	/* Unmount request */
#define	RMC_EXPORT	6	/* Export tape request */
#define	RMC_IMPORT	7	/* Import tape request */

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

                        /* SCSI media changer server structures */

struct extended_robot_info {
	int	smc_fd;
	char	smc_ldr[CA_MAXRBTNAMELEN+1];
	int	smc_support_voltag;
	struct robot_info robot_info;
};
#endif
