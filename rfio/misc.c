/*
 * $Id: misc.c,v 1.3 1999/12/09 13:46:52 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: misc.c,v $ $Revision: 1.3 $ $Date: 1999/12/09 13:46:52 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* misc.c       Remote File I/O - Miscellaneous utility functions       */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */
#include <string.h>

void striptb(s)                 /* Strip trailing blanks                */
char    *s;
{
	register int    i;

	for (i=strlen(s)-1;s[i]==' ';i--);
	s[i+1]='\0';
}
