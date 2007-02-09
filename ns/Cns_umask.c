/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Cns_umask - get and set CASTOR file creation mask */

#include <sys/types.h>
#include "Cns_api.h"

mode_t DLL_DECL
Cns_umask(mode_t cmask)
{
	mode_t oldmask;
	struct Cns_api_thread_info *thip;

	if (Cns_apiinit (&thip))
		return (-1);
	oldmask = thip->mask;
	thip->mask = cmask & 0777;
	return (oldmask);
}
