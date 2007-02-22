/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*	vmgr_apiinit - allocate thread specific or global structures */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Cglobals.h"
#include "vmgr_api.h"
#include "serrno.h"
static int vmgr_api_key = -1;

int vmgr_apiinit(thip)
struct vmgr_api_thread_info **thip;
{
	Cglobals_get (&vmgr_api_key,
	    (void **) thip, sizeof(struct vmgr_api_thread_info));
	if (*thip == NULL) {
		serrno = ENOMEM;
		return (-1);
	}
	if(! (*thip)->initialized) {
		(*thip)->initialized = 1;
	}
	return (0);
}

int *
C__vmgr_errno()
{
struct vmgr_api_thread_info *thip;
	Cglobals_get (&vmgr_api_key,
	    (void **) &thip, sizeof(struct vmgr_api_thread_info));
	return (&thip->vm_errno);
}
