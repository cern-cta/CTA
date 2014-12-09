/*
 * $Id: Ctape_constants.h,v 1.6 2009/01/12 08:32:34 wiebalck Exp $
 */

/*
 * Copyright (C) 1994-2004 by CERN/IT/ADC/CA
 * All rights reserved
 */

/*
 */

#pragma once
#include "h/Castor_limits.h"

			/* config status */

#define	CONF_DOWN	0
#define	CONF_UP		1

			/* mount mode */

#define	WRITE_DISABLE	0
#define	WRITE_ENABLE	1

			/* position method */

/* Note that the next 4 lines should always be identical to the declaration
 * In the future, the two enums should be merged
 */
enum PositionCommandCode {
  TPPOSIT_FSEQ = 0,     /* position by file sequence number */
  TPPOSIT_FID = 1,      /* position by fid (dataset name) */
  TPPOSIT_EOI = 2,      /* position at end of information to add a file */
  TPPOSIT_BLKID = 3     /* position by block id (locate) */
};
			/* tape utilities exit codes */

#define	USERR	  1	/* user error */
#define	SYERR 	  2	/* system error */
#define	CONFERR	  4	/* configuration error */


