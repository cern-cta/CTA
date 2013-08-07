/*
 * $Id: Cns_constants.h,v 1.6 2008/11/03 10:34:07 waldron Exp $
 */

/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef _CNS_CONSTANTS_H
#define _CNS_CONSTANTS_H
#include "Castor_limits.h"

#define DIRXSIZE (HYPERSIZE+WORDSIZE+LONGSIZE+LONGSIZE+LONGSIZE+HYPERSIZE+TIME_TSIZE+TIME_TSIZE+TIME_TSIZE+WORDSIZE+BYTESIZE)

#define CNS_ROOT "/castor"
#define CNS_SEC_PORT 5510
#define CNS_PORT 5010

                /* constants used for checking mode bits of files */

#define S_ONLYPERM	(S_IXOTH|S_IWOTH|S_IROTH|S_IXGRP|S_IWGRP|S_IRGRP|S_IXUSR|S_IWUSR|S_IRUSR)
#define S_PERMDIR	(S_ONLYPERM|S_IFDIR)
#define S_PERMFILE	(S_ONLYPERM|S_IFREG)
#define S_PERM		(S_ONLYPERM|S_IFREG|S_IFDIR)

		/* constants used for setting retention period on disk */

#define	AS_LONG_AS_POSSIBLE	0x7FFFFFFF
#define	INFINITE_LIFETIME	0x7FFFFFFE

			/* ACL types */

#define CNS_ACL_USER_OBJ	1
#define CNS_ACL_USER		2
#define CNS_ACL_GROUP_OBJ	3
#define CNS_ACL_GROUP		4
#define CNS_ACL_MASK		5
#define CNS_ACL_OTHER		6
#define CNS_ACL_DEFAULT		0x20

			/* path parsing options */

#define CNS_MUST_EXIST	1
#define CNS_NOFOLLOW	2

			/* long options for Cns utilities */

#define	OPT_CLASS_GID	1
#define	OPT_CLASS_GROUP	2
#define	OPT_CLASS_ID	3
#define	OPT_CLASS_NAME	4
#define	OPT_CLASS_UID	5
#define	OPT_CLASS_USER	6
#define	OPT_FLAGS	7
#define	OPT_MAX_DRV	8
#define	OPT_MAX_FSZ	9
#define	OPT_MAX_SSZ	10
#define	OPT_MIGR_INTV	11
#define	OPT_MIN_FSZ	12
#define	OPT_MIN_TIME	13
#define	OPT_NBCOPIES	14
#define	OPT_NEW_C_NAME	15
#define	OPT_RETENP_DISK	16
#define	OPT_TPPOOLS	17
#define	OPT_IDMAP_GID	18
#define	OPT_IDMAP_GROUP	19
#define	OPT_IDMAP_UID	20
#define	OPT_IDMAP_USER	21

			/* name server utilities exit codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	CONFERR	  4	/* configuration error */

			/* timeout constants */

#define DEFAULT_RETRYCNT     10      /* Default retry count => 100 secs (cf. RETRYI) */
#define DEFAULT_CONNTIMEOUT  (2*60)  /* Default connect timeout limit = 2mins. */

#endif
