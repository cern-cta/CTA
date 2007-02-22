/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
/*	Cupv_apiinit - allocate thread specific or global structures */

#include <errno.h>    
#include <stdlib.h>
#include <sys/types.h>
#include "Cglobals.h"
#include "Cupv_api.h"
#include "serrno.h"
static int Cupv_api_key = -1;

int Cupv_apiinit(struct Cupv_api_thread_info **thip)
{
	Cglobals_get (&Cupv_api_key,
	    (void **) thip, sizeof(struct Cupv_api_thread_info));
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
C__Cupv_errno()
{
struct Cupv_api_thread_info *thip;
	Cglobals_get (&Cupv_api_key,
	    (void **) &thip, sizeof(struct Cupv_api_thread_info));
	return (&thip->vm_errno);
}
