/*
 * $Id: compat.c,v 1.2 1999/12/09 13:39:32 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: compat.c,v $ $Revision: 1.2 $ $Date: 1999/12/09 13:39:32 $ CERN/IT/PDP/DM Frederic Hemmer";
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
