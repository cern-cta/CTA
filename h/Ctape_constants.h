/*
 * $Id: Ctape_constants.h,v 1.2 2006/11/09 15:12:09 itglp Exp $
 */

/*
 * Copyright (C) 1994-2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Ctape_constants.h,v $ $Revision: 1.2 $ $Date: 2006/11/09 15:12:09 $ CERN IT-PDP/DM Jean-Philippe Baud - Benjamin Couturier
 */

#ifndef _CTAPE_CONSTANTS_H
#define _CTAPE_CONSTANTS_H
#include "Castor_limits.h"

#ifdef CSEC
#define STAPE_PORT 5511
#endif
#define TAPE_PORT 5011

/* From Castor_limits.h */
#define CA_MAXDENNUM   28    /* Maximum number of known densities for tapes */

			/* tape daemon request options */

#define	DEFDGN	"CART"	/* default device group name */

			/* config status */

#define	CONF_DOWN	0
#define	CONF_UP		1

			/* byte interpretation in dumptape */

#define	DMP_ASC		1	/* ASCII interpretation */
#define	DMP_EBC		3	/* EBCDIC interpretation */

			/* special processing flags */

#define	NOPOS		2	/* do not reposition tape */
#define	NOTRLCHK	4	/* do not check trailer labels */
#define	DOUBLETM	4	/* write 2 tapemarks at EOI */
#define	IGNOREEOI	8	/* do not take 2 consecutive TMs as EOI */
#define	LOCATE_DONE	16	/* used internally */

			/* filstat values */

#define	CHECK_FILE	0	/* check file existence and fid in HDR1 */
#define	NEW_FILE	1	/* write a new file w/out checking file existence or fid */
#define	APPEND		2	/* append data to an existing tape file */
#define	NOFILECHECK	3	/* do not check file existence */

			/* mount mode */

#define	WRITE_DISABLE	0
#define	WRITE_ENABLE	1

			/* position method */

#define TPPOSIT_FSEQ	0	/* position by file sequence number */
#define TPPOSIT_FID	1	/* position by fid (dataset name) */
#define TPPOSIT_EOI	2	/* position at end of information to add a file */
#define TPPOSIT_BLKID	3	/* position by block id (locate) */

			/* release flags */

#define TPRLS_ALL	1	/* release all resources */
#define TPRLS_PATH	2	/* release path */
#define TPRLS_KEEP_RSV	4	/* keep reservation */
#define TPRLS_NOUNLOAD	8	/* release resource but do not unload drive */
#define TPRLS_UNLOAD   16	/* release resource and always unload drive */
#define TPRLS_NOWAIT   32	/* do not wait for the release to complete */

			/* options for tape utilities */

#define	TPOPT_NBSIDES	1
#define	TPOPT_SIDE	2

			/* tape utilities exit codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	CONFERR	  4	/* configuration error */

			/* retry intervals */

#define VOLBSYRI 60	/* retry interval if volume busy */

/* acs loader type to prevent CASTOR from using the read-only option */
#define ACS_NO_READ_ONLY "noro"

#endif
