/*
 * $Id: ferror.c,v 1.3 1999/12/09 13:46:42 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: ferror.c,v $ $Revision: 1.3 $ $Date: 1999/12/09 13:46:42 $ CERN/IT/PDP/DM Antoine Trannoy";
#endif /* not lint */

/* ferror.c      Remote File I/O - tell if an error happened            */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

int rfio_ferror(fp)
	FILE * fp ; 
{
	return (ferror(fp)) ;
}

