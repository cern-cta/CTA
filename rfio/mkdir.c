/*
 * $Id: mkdir.c,v 1.10 2004/01/23 10:27:45 jdurand Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: mkdir.c,v $ $Revision: 1.10 $ $Date: 2004/01/23 10:27:45 $ CERN/IT/PDP/DM Antony Simmins";
#endif /* not lint */

/* mkdir.c       Remote File I/O - make a directory file                */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */

#include "rfio.h"               /* Remote File I/O general definitions  */

int DLL_DECL rfio_mkdir(dirpath, mode)     /* Remote mkdir              */
char		*dirpath;          /* remote directory path             */
int		mode;              /* remote directory mode             */
{
   char     buf[256];       /* General input/output buffer          */
   register int    s;              /* socket descriptor            */
   int             status;         /* remote mkdir() status        */
   int     	len;
   char    	*host;
   char    	*filename;
   char    	*p=buf;
   int 		rt ;
   int 		rcode, parserc ;
   mode_t	curmask;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_mkdir(%s, %o)", dirpath, mode);

   if (!(parserc = rfio_parseln(dirpath,&host,&filename,NORDLINKS))) {
      /* if not a remote file, must be local or HSM  */
      if ( host != NULL ) {
          /*
           * HSM file
           */
          TRACE(1,"rfio","rfio_mkdir: %s is an HSM path",
                filename);
          END_TRACE();
          rfio_errno = 0;
          return(rfio_HsmIf_mkdir(filename,mode));
      }
      TRACE(1, "rfio", "rfio_mkdir: using local mkdir(%s, %o)",
	    filename, mode);

      END_TRACE();
      rfio_errno = 0;
      status = mkdir(filename,mode);
      if ( status < 0 ) serrno = 0;
      return(status);
   }
   if (parserc < 0) {
	   END_TRACE();
	   return(-1);
   }

   /* Applies the umask to the mode                     */
   curmask = umask(0);          /* get the mode         */
   umask(curmask);              /* restore it           */
   mode &= ~curmask;            /* and apply it to mode */
  
   s = rfio_connect(host,&rt);
   if (s < 0)      {
      END_TRACE();
      return(-1);
   }

   len = strlen(filename)+ LONGSIZE + 1;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_MKDIR);
   marshall_WORD(p, geteuid());
   marshall_WORD(p, getegid());
   marshall_LONG(p, len);
   p= buf + RQSTSIZE;
   marshall_STRING(p, filename);
   marshall_LONG(p, mode);
   TRACE(1,"rfio","rfio_mkdir: mode %o",mode);
   TRACE(2,"rfio","rfio_mkdir: sending %d bytes",RQSTSIZE+len) ;
   if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      TRACE(2, "rfio", "rfio_mkdir: write(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   p = buf;
   TRACE(2, "rfio", "rfio_mkdir: reading %d bytes", LONGSIZE);
   if (netread_timeout(s, buf, 2* LONGSIZE, RFIO_CTRL_TIMEOUT) != (2 * LONGSIZE))  {
      TRACE(2, "rfio", "rfio_mkdir: read(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   unmarshall_LONG(p, status);
   unmarshall_LONG(p, rcode);
   TRACE(1, "rfio", "rfio_mkdir: return %d",status);
   rfio_errno = rcode;
   (void) close(s);
   if (status)     {
      END_TRACE();
      return(-1);
   }
   END_TRACE();
   return (0);
}
