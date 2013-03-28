/*
 *
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 *
 */
 
#ifndef _EXPERT_DAEMON_H
#define _EXPERT_DAEMON_H


#define CHECKI	5	/* max interval to check for work to be done */

#define RETURN(x) \
	{ \
	return ((x)); \
	}



/* expert server function prototypes */

EXTERN_C void exp_logreq (char*, char*);
EXTERN_C int explogit (char *, char *, ...);
EXTERN_C int sendrep (int, int, ...);
EXTERN_C int exp_srv_execute (int, char*, char*, int);

#endif
