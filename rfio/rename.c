/*
 * $Id: rename.c,v 1.11 2004/03/03 11:15:59 obarring Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rename.c,v $ $Revision: 1.11 $ $Date: 2004/03/03 11:15:59 $ CERN/IT/PDP/DM Antony Simmins";
#endif /* not lint */

/* rename.c       Remote File I/O - change the name of a file           */

#define RFIO_KERNEL     1       /* KERNEL part of the routines          */
#if defined(_WIN32)
#define MAXHOSTNAMELEN (CA_MAXHOSTNAMELEN+1)
#else
#include <sys/param.h>
#endif
#include "rfio.h"               /* Remote File I/O general definitions  */

/*
** NB This does not implement a rename across hosts
*/

int  DLL_DECL rfio_rename(fileo, filen)  /* Remote rename               */
char		*fileo,		/* remote old path  			*/
   *filen;		/* remote new path            		*/
{
   char     buf[BUFSIZ];       /* General input/output buffer          */
   register int    s;              /* socket descriptor            */
   int             status;         /* remote rename() status       */
   int     	len;
   char    	hostnameo[MAXHOSTNAMELEN],
      hostnamen[MAXHOSTNAMELEN],
      filenameo[MAXFILENAMSIZE],
      filenamen[MAXFILENAMSIZE];
   char		*host,
      *path;
   char    	*p=buf;
   int 		rt, parserc ;
   int 		rcode ;
   int		rpo, rpn;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_rename(%s, %s)", fileo, filen);

   *hostnameo = *hostnamen = '\0';
   rpo = rfio_parseln(fileo,&host,&path,NORDLINKS);

   if (host != NULL)
      strcpy(hostnameo, host);

   strcpy(filenameo, path);

   rpn = parserc = rfio_parse(filen,&host, &path);

   if (parserc < 0) {
	   END_TRACE();
	   return(-1);
   }

   if (host != NULL)
      strcpy(hostnamen, host);

   strcpy(filenamen, path);

   /*
   ** We do not allow a rename across hosts as this implies
   ** a copy (cf move(1)). This may change in the future.
   */
   if ((!rpo && rpn) || (rpo && !rpn)) {
      serrno = SEXHOST;
      END_TRACE();
      return(-1);
   }

   if (rpo && rpn)
   { if (strcmp(hostnameo, hostnamen) != 0) {
      serrno = SEXHOST;
      END_TRACE();
      return(-1);
   }
   }

   if ((!rpo) && (!rpn)) {
      /* if not a remote file, must be local or HSM  */
      if ( *hostnameo != '\0' && *hostnamen != '\0' ) {
          /*
           * HSM file
           */
          TRACE(1,"rfio","rfio_rename: %s and %s are HSM paths",
                filenameo,filenamen);
          END_TRACE();
          rfio_errno = 0;
          return(rfio_HsmIf_rename(filenameo,filenamen));
      }
      /* if not remote files, must be local  */
      TRACE(1, "rfio", "rfio_rename: using local rename(%s, %s)",
	    filenameo, filenamen);

      END_TRACE();
      rfio_errno = 0;
      status = rename(filenameo,filenamen);
      if ( status < 0 ) serrno = 0;
      return(status);
   }

   s = rfio_connect(hostnameo,&rt);
   if (s < 0)      {
      END_TRACE();
      return(-1);
   }

   len = strlen(filenameo) + strlen(filenamen) + 2;
   if ( RQSTSIZE+len > BUFSIZ ) {
     TRACE(2,"rfio","rfio_rename: request too long %d (max %d)",
           RQSTSIZE+len,BUFSIZ);
     END_TRACE();
     (void) netclose(s);
     serrno = E2BIG;
     return(-1);
   }
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_RENAME);
   marshall_WORD(p, geteuid());
   marshall_WORD(p, getegid());
   marshall_LONG(p, len);
   p= buf + RQSTSIZE;
   marshall_STRING(p, filenameo);
   marshall_STRING(p, filenamen);
   TRACE(1,"rfio","rfio_rename: filenameo %s, filenamen %s",
	 filenameo, filenamen);
   TRACE(2,"rfio","rfio_rename: sending %d bytes",RQSTSIZE+len) ;
   if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      TRACE(2, "rfio", "rfio_rename: write(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   p = buf;
   TRACE(2, "rfio", "rfio_rename: reading %d bytes", LONGSIZE);
   if (netread_timeout(s, buf, 2 * LONGSIZE, RFIO_CTRL_TIMEOUT) != (2 * LONGSIZE))  {
      TRACE(2, "rfio", "rfio_rename: read(): ERROR occured (errno=%d)", errno);
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   unmarshall_LONG(p, status);
   unmarshall_LONG(p, rcode);
   TRACE(1, "rfio", "rfio_rename: return %d",status);
   rfio_errno = rcode;
   (void) close(s);
   if (status)     {
      END_TRACE();
      return(-1);
   }
   END_TRACE();
   return (0);
}
