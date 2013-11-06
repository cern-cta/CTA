/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* compat.c     Compatibility library                                   */
#include <osdep.h>
#include <net.h>

int dorecv(int x,
           char *y,
	   int z)
{
	return(netread(x,y,z));
}

int dosend(int x,
           char *y,
           int z)
{
	return(netwrite(x,y,z));
}
