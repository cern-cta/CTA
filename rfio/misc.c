/*
 * $Id: misc.c,v 1.2 1999/07/20 12:48:02 jdurand Exp $
 *
 * $Log: misc.c,v $
 * Revision 1.2  1999/07/20 12:48:02  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

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
