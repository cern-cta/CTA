/*
 * $Id: lun2fn.c,v 1.5 1999/12/10 19:44:21 baran Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: lun2fn.c,v $ $Revision: 1.5 $ $Date: 1999/12/10 19:44:21 $ CERN/IT/PDP/DM Frederic Hemmer";
#endif /* not lint */

/* lun2fn.c     Remote File I/O - translate FORTRAN LUN to file name    */

/*
 * C bindings :
 *
 * assign syntax :
 *
 * fort.LUN:<filepath>[,<filepath>[,<filepath>...]]
 *
 * char *lun2fn(int lun)
 *
 */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include "rfio.h"               /* remote file I/O definitions          */
#include <pwd.h>                /* password entry structure             */
#include <Cpwd.h>

extern char     *getenv();

char *
lun2fn(lun)                     /* find file name corresponding to lun  */
int     lun;
{
   char    *afile;         /* assign file name                     */
   FILE    *fp;            /* a file pointer                       */
   char    *p, *p1;        /* character pointers                   */
   int     clun;           /* current lun entry                    */
   static  char    buf[1024];  	   /* general purpose buffer     	        */
/*
 * Open the assign file, get the corresponding entry
 */
   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "lun2fn: looking environment for %s", "RFASSIGN");
   if ((p = getenv("RFASSIGN")) != NULL)   {
      afile = p;
   }
   else {  /* No RFASSIGN env var, so get the passwd entry         */
      TRACE(1, "rfio", "lun2fn: getting home directory name");
      if ((p = Cgetpwuid(getuid())->pw_dir) == NULL)   {
	 END_TRACE();
	 return(NULL);
      }
      sprintf(buf,"%s/%s",p,DEFASNFNAM);
      afile = buf;
   }

   TRACE(1, "rfio", "lun2fn: opening %s", afile);
   if ((fp = fopen(afile, "r")) == NULL)   {
      if (errno == ENOENT)    {
#if hpux
	 sprintf (buf, "ftn%02d", lun);
#else
	 sprintf (buf, "fort.%d", lun);
#endif
	 TRACE(1, "rfio", "lun2fn: assigning unit %d to %s", lun, buf);
	 END_TRACE();
	 return(buf);
      }
      else    {
	 END_TRACE();
	 return(NULL);
      }
   }

   for (;(p=fgets(buf, BUFSIZ, fp)) != NULL;)   {
      p = strchr(p,'.');
      p1 = strchr(p+1, ':');
      *(p1++)='\0';
      clun = atoi(p+1);
      TRACE(1, "rfio", "lun2fn: processing entry %d", clun);
      if (clun == lun) {      /* matching entry       */
	 p = p1;
	 /* The string is terminated by \n, \0 or \, */
	 if ((p1 = strpbrk(p,"\n\0,")) != NULL) {
	    *p1 = '\0';
	 }
	 break;
      }
      else    {
	 p = NULL;
      }
   }
   (void) fclose(fp);

   if (p == NULL)  {               /* no matching entry    */
#if hpux
      sprintf (buf, "ftn%d", lun);
#else
      sprintf (buf, "fort.%d", lun);
#endif

      TRACE(1, "rfio", "lun2fn: assigning unit %d to %s", lun, buf);
      END_TRACE();
      return(buf);
   }
   else    {
      TRACE(1, "rfio", "lun2fn: assigning unit %d to %s", lun, p);
      END_TRACE();
      return(p);
   }
}
