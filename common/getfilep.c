/*
 * $Id: getfilep.c,v 1.2 1999/12/09 13:39:38 jdurand Exp $
 */

/*
 * Copyright (C) 1991-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: getfilep.c,v $ $Revision: 1.2 $ $Date: 1999/12/09 13:39:38 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* getfilep.c   Fortran I/O to C mapper                                 */

/*
 * DISCLAIMER : This software is provided as is, without any warranty
 *              of reliability. It has been written by looking at
 *              IRIX internal structures and is therefore subject to
 *              modifications of IRIX system.
 */

/* changed by   date               description                          */
/*+-----------+-----------+--------------------------------------------+*/
/* F. Hemmer    13 Dec 90       Initial writing for IRIX 3.1            */
/* F. Hemmer    23 Jan 91       Added MIPS Ultrix support               */

#if defined(sgi) || (defined(mips) && defined(ultrix))
#include <stdio.h>              /* Standard Input/Output                */
#include <cmplrs/fio.h>

#define DEBUG   0

extern unit *f77units;
unit *uinc;

FILE *getfilep_(lun)            /* Get file pointer associated to lun.  */
int     *lun;
{
	register int    i;      /* F77 unit array index                 */
	struct UNIT    *units;  /* F77 unit array pointer               */
	register char   *p;     /* For pointer calculation              */

	p = (char *)f77units;
/*
 * IRIX apparently only uses 0 < unit < mxunit-1
 */
	for (i=0;i<mxunit-1;i++)  {
		p += sizeof(unit);
#if DEBUG
		fprintf(stdout,"f77units[%d]: unit=%d, fp=%x\n",i,
			((struct UNIT *)p)->luno,
			((struct UNIT *)p)->ufd);
#endif /* DEBUG */
		if (((struct UNIT *)p)->luno == *lun)     {
			return(((struct UNIT *)p)->ufd);
		}
	}
	return((FILE *)-1);
}

#endif /* (sgi) || ((mips) && (ultrix)) */

#if defined (_AIX)
#include <stdio.h>
#include <sys/limits.h>
#include <sys/stat.h>

FILE           *
#if defined(_IBMESA)
getfilep_(filename)
#else
getfilep(filename)
#endif
        char           *filename;
{
        struct stat     statbuf;
        int             i, j;
        ino_t           inodef;
        static FILE    *filep = 0;

	/* go through the process of getting file pointer only */
	/* the first time that getfilep is called.later use the*/
	/* same pointer.Hence filep is defined static.   */

if (filep == 0) {
        /*
         * first put null character at end of file name since fortran leaves
         * all trailing blank
         */

        i = 255;
        while (filename[i] == ' ')
                i--;
        filename[++i] = '\0';
        /* now get the inode number of the file */

        if ((j = stat(filename, &statbuf)) == -1) {
                return ((FILE *) - 1);
        }
        inodef = statbuf.st_ino;

        /* now look for a file descriptor with same inode number */
        /* and associate a stream with it and return the stream */

        for (i = 0; i <= getdtablesize(); i++) {
                if ((j = fstat(i, &statbuf)) == -1)
                        continue;
                if (inodef == statbuf.st_ino) {
                        filep = fdopen(i, "r");
                        return (filep);
                }
        }
        return ((FILE *) - 1);
} else
        return (filep);
}
#endif                          /* _AIX */

