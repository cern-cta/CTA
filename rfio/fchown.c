/*
 * $Id: fchown.c,v 1.1 2002/06/16 06:49:11 baud Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fchown.c,v $ $Revision: 1.1 $ $Date: 2002/06/16 06:49:11 $ CERN/IT/DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/* fchown.c      Remote File I/O - change ownership of a file            */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1   
#include "rfio.h"          
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Remote file chown
 */
int DLL_DECL rfio_fchown(s, owner, group)  
int      s;
int owner ;            /* Owner's uid */
int group ;            /* Owner's gid */
{
   int status ;
   char   * p ;
   char * trp ;
   int temp=0 ;
   char     rfio_buf[BUFSIZ];
   int s_index = -1;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_fchown(%d, %d, %d)", s, owner, group);
   /* 
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_fchown: using local fchown(%d, %d, %d)", s, owner, group);
      status = fchown(s, owner, group);
      rfio_errno = 0;
      END_TRACE();
      return(status);
   }
   /*
    * Checking magic number.
    */
   if (rfilefdt[s_index]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ; 
      rfio_rfilefdt_freeentry(s_index);
      (void) close(s) ;
      END_TRACE();
      return(-1);
   }
   /*
    * Sending request.
    */
   p= rfio_buf ;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_FCHOWN);
   marshall_WORD(p, owner);
   marshall_WORD(p, group);
   TRACE(2,"rfio","rfio_fchown: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_fchown: write(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return(-1);
   }
   /*
    * Getting data from the network.
    */
   for(;;) {
      WORD    req ; 
      LONG  rcode ;
      LONG msgsiz ;

      TRACE(2, "rfio", "rfio_fchown: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ; 
      if (netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_DATA_TIMEOUT) != rfilefdt[s_index]->_iobuf.hsize) {
	 TRACE(2, "rfio", "rfio_fchown: read(): ERROR occured (errno=%d)", errno);
	 if ( temp ) (void) free(trp) ; 
	 END_TRACE() ;
	 return(-1); 
      }
      p = rfio_buf ;
      unmarshall_WORD(p,req) ;
      unmarshall_LONG(p,status) ; 
      unmarshall_LONG(p, rcode) ; 
      unmarshall_LONG(p, msgsiz) ;
      switch(req) {
       case RQST_FCHOWN:
	  rfio_errno=  rcode ;
	  if ( temp ) (void) free(trp) ; 
	  TRACE(1,"rfio","rfio_fchown: return status %d, rcode %d",status,rcode) ;
	  END_TRACE() ;
	  return(status) ;
       case RQST_READAHEAD:
       case RQST_LASTSEEK:
       case RQST_PRESEEK:
	  /* 
	   * At this point a temporary buffer may need to be created
	   * to receive data which is going to be thrown away.
	   */
	  if ( temp == 0 ) {
	     if ( rfilefdt[s_index]->_iobuf.base==NULL || rfilefdt[s_index]->_iobuf.dsize<msgsiz ) {
		temp= 1 ; 
		TRACE(3,"rfio","rfio_fchown: allocating momentary buffer of size %d",msgsiz) ; 
		if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
		   TRACE(3,"rfio","rfio_fchown: malloc(): ERROR occured (errno=%d)",errno) ; 
		   END_TRACE() ; 
		   return(-1) ;
		}
	     }
	     else
		trp= iodata(rfilefdt[s_index]) ;
	  }
	  if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	     TRACE(2,"rfio","rfio_fchown: read(): ERROR occured (errno=%d)",errno) ;
	     if ( temp ) (void) free(trp) ; 
	     END_TRACE() ; 
	     return(-1) ;
	  }
	  break ; 
       default:
	  TRACE(1,"rfio","rfio_fchown(): Bad control word received\n") ; 
	  rfio_errno= SEINTERNAL ;
	  if ( temp ) (void) free(trp) ; 
	  END_TRACE() ; 
	  return(-1) ;
      }
   }	
}
