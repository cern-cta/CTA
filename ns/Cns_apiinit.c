/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*	Cns_apiinit - allocate thread specific or global structures */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include "Cglobals.h"
#include "Cns_api.h"
#include "serrno.h"
static int Cns_api_key = -1;

int DLL_DECL
Cns_apiinit(thip)
struct Cns_api_thread_info **thip;
{
	Cglobals_get (&Cns_api_key,
	    (void **) thip, sizeof(struct Cns_api_thread_info));
	if (*thip == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	if(! (*thip)->initialized) {
		umask ((*thip)->mask = umask (0));
		(*thip)->initialized = 1;
		(*thip)->fd = -1;
	}
	return (0);
}

int DLL_DECL *
C__Cns_errno()
{
struct Cns_api_thread_info *thip;
	Cglobals_get (&Cns_api_key,
	    (void **) &thip, sizeof(struct Cns_api_thread_info));
	return (&thip->ns_errno);
}
