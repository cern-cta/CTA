/*
 * $Id: misc.c,v 1.4 2000/05/29 16:42:02 obarring Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: misc.c,v $ $Revision: 1.4 $ $Date: 2000/05/29 16:42:02 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* misc.c       Remote File I/O - Miscellaneous utility functions       */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */
#include <string.h>
#include <osdep.h>

void DLL_DECL striptb(s)                 /* Strip trailing blanks       */
char    *s;
{
	register int    i;

	for (i=strlen(s)-1;s[i]==' ';i--);
	s[i+1]='\0';
}
