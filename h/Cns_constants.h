/*
 * Cns_constants.h,v 1.10 2001/04/29 16:00:59 baud Exp
 */

/*
 * Copyright (C) 1999-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)Cns_constants.h,v 1.10 2001/04/29 16:00:59 CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _CNS_CONSTANTS_H
#define _CNS_CONSTANTS_H
#include "Castor_limits.h"

#define DIRXSIZE (HYPERSIZE+WORDSIZE+LONGSIZE+LONGSIZE+LONGSIZE+HYPERSIZE+TIME_TSIZE+TIME_TSIZE+TIME_TSIZE+WORDSIZE+BYTESIZE)

#ifdef CSEC
#define CNS_PORT 5510
#else
#define CNS_PORT 5010
#endif

		/* constants used for setting retention period on disk */

#define	AS_LONG_AS_POSSIBLE	0x7FFFFFFF
#define	INFINITE_LIFETIME	0x7FFFFFFE

			/* long options for Cns utilities */

#define	OPT_CLASS_GID	0
#define	OPT_CLASS_GROUP	1
#define	OPT_CLASS_ID	2
#define	OPT_CLASS_NAME	3
#define	OPT_CLASS_UID	4
#define	OPT_CLASS_USER	5
#define	OPT_FLAGS	6
#define	OPT_MAX_DRV	7
#define	OPT_MAX_FSZ	8
#define	OPT_MAX_SSZ	9
#define	OPT_MIGR_INTV	10
#define	OPT_MIN_FSZ	11
#define	OPT_MIN_TIME	12
#define	OPT_NBCOPIES	13
#define	OPT_NEW_C_NAME	14
#define	OPT_RETENP_DISK	15
#define	OPT_TPPOOLS	16

			/* name server utilities exit codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	CONFERR	  4	/* configuration error */
#endif
