/*
 * $Id: Cupv_constants.h,v 1.1 2004/10/21 13:47:45 jdurand Exp $
 */

/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cupv_constants.h,v $ $Revision: 1.1 $ $Date: 2004/10/21 13:47:45 $ CERN IT-PDP/DM Ben Couturier
 */

#ifndef _CUPV_CONSTANTS_H
#define _CUPV_CONSTANTS_H
#include "Castor_limits.h"

#ifdef CSEC
#define SCUPV_PORT 5520
#endif
#define CUPV_PORT 56013


/* options for Cupv utilities */

/*  #define MAXGRPNAMELEN CA_MAXHOSTNAMELEN */
#define MAXGRPNAMELEN 63

#define MAXDOMAINLEN  40

#define OPT_UID 0
#define OPT_GID 1
#define OPT_SRC 2
#define OPT_TGT 3
#define OPT_PRV 4
#define OPT_USR 5
#define OPT_GRP 6
#define OPT_NEWSRC 7
#define OPT_NEWTGT 8

/* UPV utilities exit codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	CONFERR	  4	/* configuration error */


/*  Roles for Castor users */

#define P_NONE            0x0
#define P_OPERATOR        0x1
#define P_TAPE_OPERATOR   0x2
#define P_GRP_ADMIN      0x4
#define P_ADMIN           0x8
#define P_UPV_ADMIN       0x10
#define P_TAPE_SYSTEM     0x20
#define P_STAGE_SYSTEM    0x40

#define STR_NONE            "NONE"
#define STR_OPERATOR        "OPER"
#define STR_TAPE_OPERATOR   "TP_OPER"
#define STR_GRP_ADMIN      "GRP_ADMIN"
#define STR_ADMIN           "ADMIN"
#define STR_UPV_ADMIN       "UPV_ADMIN"
#define STR_TAPE_SYSTEM     "TP_SYSTEM"
#define STR_STAGE_SYSTEM    "ST_SYSTEM"
#define STR_SEP             "|"

#define STR_PRIV_LIST " OPER, TP_OPER, GRP_ADMIN, ADMIN, UPV_ADMIN, TP_SYSTEM, ST_SYSTEM"

#define REGEXP_START_CHAR   '^'
#define REGEXP_END_CHAR     '$'
#define REGEXP_END_STR      "$"

#define MAXPRIVSTRLEN      30
#define MAXSQLSTMENTLEN    500

#endif











