/*
 * $Id: misc.c,v 1.6 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* misc.c       Remote File I/O - Miscellaneous utility functions       */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */
#include <string.h>
#include <osdep.h>

/* Strip trailing blanks       */
void striptb(char    *s)
{
  register int    i;

  for (i=strlen(s)-1;s[i]==' ';i--);
  s[i+1]='\0';
}
