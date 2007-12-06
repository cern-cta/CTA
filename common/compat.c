/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* compat.c     Compatibility library                                   */
#include <osdep.h>
#include <net.h>

int DLL_DECL dorecv(x, y, z)
int     x, z;
char    *y;
{
	return(netread(x,y,z));
}

int DLL_DECL dosend(x, y, z)
int     x, z;
char    *y;
{
	return(netwrite(x,y,z));
}
