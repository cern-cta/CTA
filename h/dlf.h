/*
 *
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: dlf.h,v $ $Revision: 1.3 $ $Date: 2004/07/08 15:43:18 $ CERN IT-ADC Vitaly Motyakov
 */

#ifndef _DLF_H
#define _DLF_H

			/* dlf constants */

#include "dlf_constants.h" 
#include "osdep.h"
#define DLF_MAGIC	0x68767001
#define DLF_LISTTIMEOUT 300	/* timeout while waiting for the next request */
#define DLF_READRQTIMEOUT 300	/* timeout while waiting for the next request */
#define DLF_TIMEOUT	5	/* netread timeout while receiving a request */
#define	MAXRETRY 5
#define	RETRYI	60
#define LISTBUFSZ 3960
#define LOGBUFSZ 1024
#define DLF_PRTBUFSZ  180
#define DLF_REPBUFSZ 16380	/* must be >= max DLF reply size */
#define DLF_REQBUFSZ 10240	/* must be >= max DLF request size */

#define DLF_LAST_BUFFER 1

#define DLF_MAXPARNAMELEN 16
#define DLF_MAXSTRVALLEN 254
#define DLF_MAXFACNAMELEN 16
#define DLF_TIMESTRLEN    14
#define DLF_SENDBUFSIZE 16384

			/* logging facility request types */

#define DLF_SHUTDOWN	 6
#define DLF_STORE        7
#define DLF_GETMSGTEXTS  8
#define DLF_ENTERFAC     9
#define DLF_GETFAC      10
#define DLF_ENTERTXT    11
#define	DLF_MODFAC      12
#define	DLF_DELFAC      13
#define DLF_MODTXT      14
#define DLF_DELTXT      15

			/* logging facility reply types */

#define	MSG_ERR		1
#define	MSG_DATA	2
#define	DLF_RC		3
#define	DLF_IRC 	4

			/* logging facility messages */

#define DLF00	"DLF00 - DLF server not available on %s\n"
#define	DLF02	"DLF02 - %s error : %s\n"
#define DLF03   "DLF03 - illegal function %d\n"
#define DLF04   "DLF04 - error getting request, netread = %d\n"
#define DLF05   "DLF05 - %s error : %s. Facility = %s\n"
#define	DLF09	"DLF09 - fatal configuration error: %s %s\n"
#define	DLF23	"DLF23 - %s is not accessible\n"
#define DLF46	"DLF46 - request too large (max. %d)\n"
#if defined(_WIN32)
#define	DLF52	"DLF52 - WSAStartup unsuccessful\n"
#define	DLF53	"DLF53 - you are not registered in the unix group/passwd mapping file\n"
#endif
#define	DLF92	"DLF92 - %s request by %d,%d from %s\n"
#define	DLF98	"DLF98 - %s\n"
#endif
