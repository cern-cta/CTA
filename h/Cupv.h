/*
 * $Id: Cupv.h,v 1.2 2002/06/07 16:04:20 bcouturi Exp $
 */

/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * @(#)$RCSfile: Cupv.h,v $ $Revision: 1.2 $ $Date: 2002/06/07 16:04:20 $ CERN IT-PDP/DM Ben Couturier
 */

#ifndef _CUPV_H
#define _CUPV_H

			/* UPV constants */

#include "Cupv_constants.h"
#include "osdep.h"
#define CUPV_MAGIC	0x7770777
#define CUPV_LISTTIMEOUT 300	/* timeout while waiting for the next list sub-req */
#define CUPV_TIMEOUT	5	/* netread timeout while receiving a request */
#define	MAXRETRY 5
#define	RETRYI	60
#define LISTBUFSZ 3960
#define LOGBUFSZ 1024
#define PRTBUFSZ  180
#define REPBUFSZ 3964	/* must be >= max UPV reply size */
#define REQBUFSZ  820	/* must be >= max UPV request size */

			/* UPV request types */

#define CUPV_ADD	 0
#define CUPV_DELETE	 1
#define CUPV_LIST	 2
#define CUPV_MODIFY	 3
#define CUPV_CHECK	 4
#define CUPV_SHUTDOWN	 5
#define CUPV_ENDLIST	 6

			/* UPV reply types */

#define	MSG_ERR		1
#define	MSG_DATA	2
#define	CUPV_RC		3
#define	CUPV_IRC	4

			/* UPV messages */

#define CUP00	"CUP00 - User Privilege Validator not available on %s\n" 
#define	CUP02	"CUP02 - %s error : %s\n" 
#define CUP03   "CUP03 - illegal function %d\n" 
#define CUP04   "CUP04 - error getting request, netread = %d\n" 
#define	CUP09	"CUP09 - fatal configuration error: %s %s\n" 
#define	CUP23	"CUP23 - %s is not accessible\n" 
#define CUP46	"CUP46 - request too large (max. %d)\n" 
#if defined(_WIN32) 
#define	CUP52	"CUP52 - WSAStartup unsuccessful\n" 
#define	CUP53	"CUP53 - you are not registered in the unix group/passwd mapping file\n" 
#endif 

/*  #define CUP64	"CUP64 - parameter inconsistency with CUPV for vid %s: %s on request <-> %s in CUPV\n" */
#define	CUP92	"CUP92 - %s request by %d,%d from %s\n" 
#define	CUP98	"CUP98 - %s\n" 

#endif

