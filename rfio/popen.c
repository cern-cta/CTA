/*
 * $Id: popen.c,v 1.4 1999/12/09 13:47:00 jdurand Exp $
 */

/*
 * Copyright (C) 1994-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: popen.c,v $ $Revision: 1.4 $ $Date: 1999/12/09 13:47:00 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* popen.c       Remote pipe I/O - open file a file                      */

/*
 * System remote file I/O
 */
#define RFIO_KERNEL     1
#include <fcntl.h>
#if defined(_WIN32)
#define MAXHOSTNAMELEN 64
#else
#include <sys/param.h>          /* For MAXHOSTNAMELEN definition  */
#endif
#include <stdlib.h>
#include "rfio.h"
extern RFILE *rfilefdt[MAXRFD] ;
#ifndef linux
extern char *sys_errlist[];     /* system error list                    */
#endif
#if (defined(_AIX) && defined(_IBMESA)) || (defined(__osf__) && defined(__alpha)) || defined(_WIN32)
extern char *cuserid();
#endif

RFILE *rfio_popen( rcom , type )
char * rcom 	; 
char *type 	;
{

   char *host 	;
   RFILE *rfp 	;
   char *p , *cp, *cp2    ;
   char command[MAXCOMSIZ]; /* command with remote syntax */
   char *uname 	;
   char *pcom 	;	 
   int rt 		;	/* daemon is in the site or remote ? */
   int rcode, status = 0 ;
   int len 	;
   FILE *file, *popen() 	;
   char localhost[MAXHOSTNAMELEN];
   char buf[BUFSIZ] ;

   INIT_TRACE("RFIO_TRACE");

   if ( (int)strlen(rcom) > MAXCOMSIZ -5 ) {
      serrno = SEUMSG2LONG ;
      return NULL ;
   }

   /* 
    * get the stderr stream if any
    */
   strcpy(command, rcom) ;
   strcat (command, " 2>&1");
   /*
    * Allocate and initialize a remote file descriptor.
    */
   if ((rfp = (RFILE *)malloc(sizeof(RFILE))) == NULL)        {
      TRACE(2, "rfio", "rfio_popen: malloc(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return NULL ;
   }
   rfio_setup(rfp) ;
   TRACE(3,"rfio","Allocated buffer at %x",rfp);
   cp = strchr(command,':') ;
   cp2 = strchr(command,' ') ;

   /* Bug fix when having a : in the command line but the command is local */
   /* If the first space is before the ':', the command is really local */
   if (cp2 < cp)
      cp = NULL;

   if (cp != NULL) {
      *cp = '\0' ;
      host = command ;
      pcom =  cp + 1 ;
   }
   if ( gethostname(localhost, MAXHOSTNAMELEN) < 0) {
      TRACE(1,"rfio","gethostname() failed");
      return NULL;
   }

   /* 
    * file is local       
    */
   if ( (cp == NULL) || !strcmp( host, localhost)  ) {
      TRACE(3,"rfio","popen(%s,%s): local mode",command,type) ;
      if (cp == NULL) 
#if defined(_WIN32)
	 file = _popen(command, type);
#else
         file = popen(command,type); 
#endif
      else
#if defined(_WIN32)
	 file = _popen(pcom, type);
#else      
	 file = popen(pcom, type) ;
#endif
      rfio_errno = 0;
      if ( file == NULL ) {
	 TRACE(1,"rfio","popen() failed ,error %d", errno) ;
	 return (NULL) ;
      }
      rfp->fp_save = file;
      memcpy( &(rfp->fp), file, sizeof(FILE))  ;
      return ( rfp ) ;
   }
   /* 
    * Parsing The command
    */

   TRACE(2,"rfio", "RFIO descriptor allocated");
   TRACE( 3, "rfio","rfio_popen(): host <%s>, command <%s>",host, pcom);
   if ( (rfp->s = rfio_connect(host , &rt)) < 0) {
      TRACE(2,"rfio","freeing RFIO descriptor at 0X%X", rfp);
      (void) free((char *)rfp);
      END_TRACE();
      return NULL ;
   }

   /*
    * Remote file table is not large enough.
    */
   if ( rfp->s >= MAXRFD ) {
      TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rfp);
      (void) free((char *)rfp);
      END_TRACE();
      errno= EMFILE ;
      return NULL ;
   }
   rfilefdt[rfp->s]=rfp;

		
   p= buf ;
   if ( (uname=cuserid(NULL)) == NULL) {
      TRACE(2, "rfio" ,"rfio_popen: cuserid error %s",sys_errlist[errno]);
      (void) free((char *)rfp);
      END_TRACE();
      return NULL ;
   }
	

   len = 2*WORDSIZE+strlen(type)+strlen(pcom)+strlen(uname)+3 ;
   marshall_WORD(p,B_RFIO_MAGIC) 	;
   marshall_WORD(p,RQST_POPEN) 	;
   marshall_LONG(p,len) 		;
   if (netwrite_timeout(rfp->s,buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE ) {
      TRACE(2,"rfio","rfio_popen: write(): ERROR occured (errno=%d)",errno);
      free((char *)rfp) ;
      END_TRACE() ;
      return NULL ;
   }
   p = buf ;
   marshall_WORD(p,rfp->uid) 	;
   marshall_WORD(p,rfp->gid) 	;
   marshall_STRING(p,type) 	;
   marshall_STRING(p,pcom) 	;
   marshall_STRING(p,uname) 	;
   if (netwrite_timeout(rfp->s,buf, len, RFIO_CTRL_TIMEOUT) != len ) {
      TRACE(2,"rfio","rfio_popen: write(): ERROR occured (errno=%d)",errno);
      free((char *)rfp) ;
      END_TRACE() ;
      return NULL ;
   }
		
   /*
    * Getting status and current offset.
    */
   if (netread_timeout(rfp->s,buf, WORDSIZE+LONGSIZE, RFIO_CTRL_TIMEOUT) != (WORDSIZE+LONGSIZE)) {
      TRACE(2, "rfio","rfio_popen: read(): ERROR occured (errno=%d)", errno);
      free((char *)rfp);
      END_TRACE();
      return NULL ;
   }
   p = buf ;
   unmarshall_LONG(p, status) ;
   unmarshall_WORD(p, rcode) ;
   TRACE(1,"rfio","rfio_popen: return status(%d), rcode(%d) for fd(%d)",status,rcode, rfp->s) ;

   if (status < 0) {
      rfio_errno= rcode ;
      free((char *)rfp) ;
      END_TRACE() ;
      return NULL ;
   }
   else
      return rfilefdt[rfp->s] ;
		
}
