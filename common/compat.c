/*
 * Copyright (C) 1990,1991 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)compat.c	1.1 03/15/91 CERN CN-SW/DC Frederic Hemmer";
#endif /* not lint */

/* compat.c     Compatibility library                                   */

extern int (*recvfunc)();
extern int (*sendfunc)();

#define netread    (*recvfunc)             /* temporary super-read         */
#define netwrite   (*sendfunc)             /* temporary super-write        */

int     dorecv(x, y, z)
int     x, z;
char    *y;
{
	return(netread(x,y,z));
}

int     dosend(x, y, z)
int     x, z;
char    *y;
{
	return(netwrite(x,y,z));
}
