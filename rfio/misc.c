/*
 * Copyright (C) 1990-1997 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)misc.c	3.2 08/08/97 CERN CN-SW/DC Frederic Hemmer";
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
