/*
 *
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: expert_daemon.h,v $ $Revision: 1.2 $ $Date: 2008/07/28 16:55:05 $ CERN IT-ADC Vitaly Motyakov
 */
 
#ifndef _EXPERT_DAEMON_H
#define _EXPERT_DAEMON_H


#define CHECKI	5	/* max interval to check for work to be done */

#define RETURN(x) \
	{ \
	return ((x)); \
	}



/* expert server function prototypes */

EXTERN_C void DLL_DECL exp_logreq _PROTO((char*, char*));
EXTERN_C int DLL_DECL explogit _PROTO((char *, char *, ...));
EXTERN_C int DLL_DECL sendrep _PROTO((int, int, ...));
EXTERN_C int DLL_DECL exp_srv_execute _PROTO((int, char*, char*, int));

#endif
