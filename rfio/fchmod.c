/*
 * $Id: fchmod.c,v 1.3 2002/09/20 06:59:34 baud Exp $
 */

/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fchmod.c,v $ $Revision: 1.3 $ $Date: 2002/09/20 06:59:34 $ CERN/IT/DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/* fchmod.c      Remote File I/O - change file mode of a file            */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1   
#include "rfio.h"          
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Remote file fchmod
 */
int DLL_DECL rfio_fchmod(s, mode)  
int    s;
int mode;                      /* remote directory mode */
{
   int status ;
   char   * p ;
   char * trp ;
   int temp=0 ;
   char     rfio_buf[BUFSIZ];
   int s_index = -1;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_fchmod(%d, %o)", s, mode);
   /* 
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_fchmod: using local fchmod(%d, %o)", s, mode);
      status = fchmod(s, mode);
      if ( status < 0 ) serrno = 0;
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
   marshall_WORD(p, RQST_FCHMOD);
   marshall_LONG(p, mode);
   TRACE(2,"rfio","rfio_fchmod: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_fchmod: write(): ERROR occured (errno=%d)", errno);
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

      TRACE(2, "rfio", "rfio_fchmod: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ; 
      if (netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_DATA_TIMEOUT) != rfilefdt[s_index]->_iobuf.hsize) {
	 TRACE(2, "rfio", "rfio_fchmod: read(): ERROR occured (errno=%d)", errno);
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
       case RQST_FCHMOD:
	  rfio_errno=  rcode ;
	  if ( temp ) (void) free(trp) ; 
	  TRACE(1,"rfio","rfio_fchmod: return status %d, rcode %d",status,rcode) ;
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
		TRACE(3,"rfio","rfio_fchmod: allocating momentary buffer of size %d",msgsiz) ; 
		if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
		   TRACE(3,"rfio","rfio_fchmod: malloc(): ERROR occured (errno=%d)",errno) ; 
		   END_TRACE() ; 
		   return(-1) ;
		}
	     }
	     else
		trp= iodata(rfilefdt[s_index]) ;
	  }
	  if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	     TRACE(2,"rfio","rfio_fchmod: read(): ERROR occured (errno=%d)",errno) ;
	     if ( temp ) (void) free(trp) ; 
	     END_TRACE() ; 
	     return(-1) ;
	  }
	  break ; 
       default:
	  TRACE(1,"rfio","rfio_fchmod(): Bad control word received\n") ; 
	  serrno= SEINTERNAL ;
	  if ( temp ) (void) free(trp) ; 
	  END_TRACE() ; 
	  return(-1) ;
      }
   }	
}
