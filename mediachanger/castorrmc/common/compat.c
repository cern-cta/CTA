/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: compat.c,v $ $Revision: 1.3 $ $Date: 2000/05/31 10:33:53 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

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
