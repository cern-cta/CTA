/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: exp_apiinit.c,v $ $Revision: 1.1 $ $Date: 2004/06/30 16:18:35 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

/*	exp_apiinit - allocate thread specific or global structures */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include "Cglobals.h"
#include "expert.h"
#include "serrno.h"
static int exp_api_key = -1;

exp_apiinit(thip)
struct exp_api_thread_info **thip;
{
	Cglobals_get (&exp_api_key,
	    (void **) thip, sizeof(struct exp_api_thread_info));
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
C__exp_errno()
{
struct exp_api_thread_info *thip;
	Cglobals_get (&exp_api_key,
	    (void **) &thip, sizeof(struct exp_api_thread_info));
	return (&thip->vm_errno);
}
