/*
 * $Id: misc.c,v 1.5 2007/09/28 15:04:32 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

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
