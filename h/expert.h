/*
 *
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: expert.h,v $ $Revision: 1.4 $ $Date: 2005/01/07 09:18:01 $ CERN IT-ADC Vitaly Motyakov
 */

#ifndef _EXPERT_H
#define _EXPERT_H

			/* expert constants */

#include "expert_constants.h" 
#include "osdep.h"

struct exp_api_thread_info
{
  char*           errbufp;
  int             errbuflen;
  int             initialized;
  int             vm_errno;
};

#define EXP_ERRSTRING "EXPERT SYSTEM ERROR"

#define EXP_RP_STATUS   1
#define EXP_RP_ERRSTRING 2

#define EXP_ST_ACCEPTED 1
#define EXP_ST_ERROR    2

#define EXP_MAGIC	0x68777001
#define EXP_LISTTIMEOUT 300	/* timeout while waiting for the next request */
#define EXP_READRQTIMEOUT 300	/* timeout while waiting for the next request */
#define EXP_TIMEOUT	5	/* netread timeout while receiving a request */
#define	MAXRETRY 5
#define	RETRYI	60
#define LISTBUFSZ 3960
#define LOGBUFSZ 1024
#define EXP_PRTBUFSZ  180
#define EXP_REPBUFSZ 16380	/* must be >= max EXPERT reply size */
#define EXP_REQBUFSZ 10240	/* must be >= max EXPERT request size */

#define EXP_LAST_BUFFER 1

#define EXP_MAXPARNAMELEN 16
#define EXP_MAXSTRVALLEN 254
#define EXP_MAXFACNAMELEN 16
#define EXP_TIMESTRLEN    14
#define EXP_SENDBUFSIZE 16384

			/* expert facility request types */

#define EXP_SHUTDOWN	     6
#define EXP_EXECUTE          7
#define EXP_RQ_FILESYSTEM    8
#define EXP_RQ_MIGRATOR      9
#define EXP_RQ_RECALLER     10
#define EXP_RQ_GC           11
#define EXP_RQ_REPLICATION  12

			/* expert facility messages */

#define EXP00	"EXP00 - expert server not available on %s\n"
#define	EXP02	"EXP02 - %s error : %s\n"
#define EXP03   "EXP03 - illegal function %d\n"
#define EXP04   "EXP04 - error getting request, netread = %d\n"
#define	EXP09	"EXP09 - fatal configuration error: %s %s\n"
#define	EXP23	"EXP23 - %s is not accessible\n"
#define EXP46	"EXP46 - request too large (max. %d)\n"
#if defined(_WIN32)
#define	EXP52	"EXP52 - WSAStartup unsuccessful\n"
#define	EXP53	"EXP53 - you are not registered in the unix group/passwd mapping file\n"
#endif
#define	EXP92	"EXP92 - %s request by %d,%d from %s\n"
#define	EXP98	"EXP98 - %s\n"
#define EXP99   "EXP99 - %d\n"

#endif
