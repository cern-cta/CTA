/*
 * $Id: rmc.h,v 1.2 2007/04/19 15:18:19 sponcec3 Exp $
 */

/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 */

#ifndef _RMC_H
#define _RMC_H

			/* SCSI media changer server constants */

#include "osdep.h"
#include "rmc_constants.h"
#define	MAXRETRY 5
#define	RETRYI	60
#define	LOGBUFSZ 1024
#define RMC_MAGIC	0x120D0301
#define RMC_TIMEOUT	5	/* netread timeout while receiving a request */

int rmclogit(const char *const func, const char *const msg, ...);

#define RETURN(x) \
	{ \
	rmclogit (func, "returns %d\n", (x)); \
	return ((x)); \
	}

#endif
