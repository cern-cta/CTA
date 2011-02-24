/*
 * $Id: vmgr_constants.h,v 1.5 2008/11/06 16:45:42 murrayc3 Exp $
 */

/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: vmgr_constants.h,v $ $Revision: 1.5 $ $Date: 2008/11/06 16:45:42 $ CERN IT-PDP/DM Jean-Philippe Baud
 */

#ifndef _VMGR_CONSTANTS_H
#define _VMGR_CONSTANTS_H
#include "Castor_limits.h"

#define MARSHALLED_TAPE_ENTRYSZ (CA_MAXVIDLEN+1+CA_MAXVSNLEN+1+CA_MAXTAPELIBLEN+1+CA_MAXDENLEN+1+CA_MAXLBLTYPLEN+1+CA_MAXMODELLEN+1+CA_MAXMLLEN+1+CA_MAXMANUFLEN+1+CA_MAXSNLEN+1+WORDSIZE+TIME_TSIZE+WORDSIZE+CA_MAXPOOLNAMELEN+1+LONGSIZE+LONGSIZE+LONGSIZE+LONGSIZE+CA_MAXSHORTHOSTLEN+1+CA_MAXSHORTHOSTLEN+1+LONGSIZE+LONGSIZE+TIME_TSIZE+TIME_TSIZE+LONGSIZE)

#ifdef VMGRCSEC
#define SVMGR_PORT 5513
#endif
#define VMGR_PORT 5013
			/* status flags */

#define	DISABLED  1
#define	EXPORTED  2
#define	TAPE_BUSY 4
#define	TAPE_FULL 8
#define	TAPE_RDONLY 16
#define	ARCHIVED 32

#define LIBRARY_ONLINE  (0)
#define LIBRARY_OFFLINE (1)

			/* options for vmgr utilities */

#define	OPT_MANUFACTURER	0
#define	OPT_MEDIA_COST		1
#define	OPT_MEDIA_LETTER	2
#define	OPT_MODEL		3
#define	OPT_NATIVE_CAPACITY	4
#define	OPT_POOL_GID		5
#define	OPT_POOL_GROUP		6
#define	OPT_POOL_UID		7
#define	OPT_POOL_USER		8
#define	OPT_SN			9
#define	OPT_STATUS		10
#define	OPT_CAPACITY		11
#define	OPT_LIBRARY_NAME	12
#define	OPT_NBSIDES		13
#define	OPT_SIDE		14
#define	OPT_TAG			15
#define OPT_DGN			16
#define OPT_WEIGHT		17
#define OPT_DELTA		18

#define OPT_VALID_WEIGHT        (0x1)
#define OPT_VALID_DELTA		(0x2)


			/* volume manager utilities exit codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	CONFERR	  4	/* configuration error */

#define MARSHALLED_TAPE_BYTE_U64_ENTRYSZ    \
(                                           \
  /* vid          */ CA_MAXVIDLEN+1       + \
  /* vsn          */ CA_MAXVSNLEN+1       + \
  /* library      */ CA_MAXTAPELIBLEN+1   + \
  /* density      */ CA_MAXDENLEN+1       + \
  /* lbltype      */ CA_MAXLBLTYPLEN+1    + \
  /* model        */ CA_MAXMODELLEN+1     + \
  /* media_letter */ CA_MAXMLLEN+1        + \
  /* manufacturer */ CA_MAXMANUFLEN+1     + \
  /* sn           */ CA_MAXSNLEN+1        + \
  /* nbsides      */ WORDSIZE             + \
  /* etime        */ TIME_TSIZE           + \
  /* side         */ WORDSIZE             + \
  /* poolname     */ CA_MAXPOOLNAMELEN+1  + \
  /* free_space   */ HYPERSIZE            + \
  /* nbfiles      */ LONGSIZE             + \
  /* rcount       */ LONGSIZE             + \
  /* wcount       */ LONGSIZE             + \
  /* rhost        */ CA_MAXSHORTHOSTLEN+1 + \
  /* whost        */ CA_MAXSHORTHOSTLEN+1 + \
  /* rjid         */ LONGSIZE             + \
  /* wjid         */ LONGSIZE             + \
  /* rtime        */ TIME_TSIZE           + \
  /* wtime        */ TIME_TSIZE           + \
  /* status       */ LONGSIZE               \
)

#endif
