/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: dlf_apiinit.c,v $ $Revision: 1.1 $ $Date: 2003/08/20 12:56:35 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

/*	dlf_apiinit - allocate thread specific or global structures */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Cglobals.h"
#include "dlf_api.h"
#include "serrno.h"
static int dlf_api_key = -1;

dlf_apiinit(thip)
struct dlf_api_thread_info **thip;
{
	Cglobals_get (&dlf_api_key,
	    (void **) thip, sizeof(struct dlf_api_thread_info));
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
C__dlf_errno()
{
struct dlf_api_thread_info *thip;
	Cglobals_get (&dlf_api_key,
	    (void **) &thip, sizeof(struct dlf_api_thread_info));
	return (&thip->vm_errno);
}
