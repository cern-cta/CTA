/*
 * Copyright (C) 1990,1991 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)ferror.c	3.3 05/06/93 CERN CN-SW/DC Antoine Trannoy";
#endif /* not lint */

/* ferror.c      Remote File I/O - tell if an error happened            */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

int rfio_ferror(fp)
	FILE * fp ; 
{
	return (ferror(fp)) ;
}

