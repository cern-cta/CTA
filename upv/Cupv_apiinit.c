/*
 * Copyright (C) 1999-2002 by CERN/IT/DS/HSM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cupv_apiinit.c,v $ $Revision: 1.1 $ $Date: 2002/05/28 09:37:57 $ CERN IT-DS/HSM Ben Couturier";
#endif /* not lint */

/*	Cupv_apiinit - allocate thread specific or global structures */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Cglobals.h"
#include "Cupv_api.h"
#include "serrno.h"
static int Cupv_api_key = -1;

Cupv_apiinit(thip)
struct Cupv_api_thread_info **thip;
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
