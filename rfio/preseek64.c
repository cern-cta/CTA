/*
 * $Id: preseek64.c,v 1.1 2002/11/19 10:51:23 baud Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: preseek64.c,v $ $Revision: 1.1 $ $Date: 2002/11/19 10:51:23 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* preseek64.c      Remote File I/O - preseeking.		*/

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1  
#ifdef linux
#include <sys/uio.h>
#endif
#include "rfio.h"     
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

#ifndef min
#define min(a,b)	(((a)<(b)) ? (a):(b))
#endif

/*
 * Remote file seek
 */
int DLL_DECL rfio_preseek64(s,iov,iovnb)   
int      s ;
int  iovnb ;
struct iovec64 *iov ;
{
  int s_index;
   char   * p ;		/* Pointer to buffer		*/
   int	 i ; 		/* Loop index			*/
   int    temp = 0 ; 	/* Temporary buffer exists ?	*/
   char *trp= NULL ; 	/* Pointer to temporary buffer	*/
   char     rfio_buf[BUFSIZ];
	
   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1,"rfio","rfio_preseek64(%d, %x, %d)",s,iov,iovnb) ;
   /*
    * The file is local.
    * Nothing has to be done.
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1 ) {
      END_TRACE() ;
      return 0 ;
   }
   /*
    * Checking magic number
    */
   if ( rfilefdt[s_index]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ;
      rfio_rfilefdt_freeentry(s_index);
      (void) close(s);
      END_TRACE();
      return -1 ;
   }
   /*
    * Nothing specified.
    */
   if ( iovnb == 0 ) {
      END_TRACE() ;
      return 0 ; 
   }
   /*
    * Repositionning file mark if needed.
    */
   if ( (rfilefdt[s_index]->readissued || rfilefdt[s_index]->preseek) && rfilefdt[s_index]->lseekhow == -1 ) {
      rfilefdt[s_index]->lseekhow= SEEK_SET ;
      rfilefdt[s_index]->lseekoff64= rfilefdt[s_index]->offset64 ;
   }
   /*
    * Resetting flags.
    * preseek() is forbidden when RFIO aren't buffered.
    */
   rfilefdt[s_index]->eof= 0 ; 
   rfilefdt[s_index]->preseek= 0 ;
   rfilefdt[s_index]->nbrecord= 0 ;
   rfilefdt[s_index]->readissued= 0 ; 
   if ( rfilefdt[s_index]->_iobuf.base ) {
      rfilefdt[s_index]->_iobuf.count= 0 ; 
      rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ; 
   }
   else 	{
      errno= EINVAL ; 
      END_TRACE() ;
      return -1 ;
   }
   /*
    * Is rfio_buf large enough to contain the request ? 
    * If not a temporary buffer is allocated.
    * THe request is then sent.
    */
   if ( (BUFSIZ-RQSTSIZE)/(HYPERSIZE+LONGSIZE) < iovnb ) {
      temp= 1 ;
      if ((trp= (char *)malloc(RQSTSIZE+(HYPERSIZE+LONGSIZE)*iovnb)) == NULL) return -1 ;
   }
   else	{
      trp= rfio_buf ;
   }
   p= trp ;
   marshall_WORD(p,RFIO_MAGIC) ;
   marshall_WORD(p,RQST_PRESEEK64) ;
   marshall_LONG(p,rfilefdt[s_index]->_iobuf.dsize) ;
   marshall_LONG(p,iovnb) ;
   p= trp + RQSTSIZE ;
   for(i= 0; i<iovnb; i ++) {
      marshall_HYPER(p,iov[i].iov_base) ;
      marshall_LONG(p,iov[i].iov_len) ;
   }
   TRACE(2,"rfio","rfio_preseek64: sending %d bytes",RQSTSIZE+(HYPERSIZE+LONGSIZE)*iovnb) ;
   if (netwrite_timeout(s,trp,RQSTSIZE+(HYPERSIZE+LONGSIZE)*iovnb,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+(HYPERSIZE+LONGSIZE)*iovnb)) {
      TRACE(2,"rfio","rfio_preseek64: write(): ERROR occured (errno=%d)",errno) ;
      if ( temp ) (void) free(trp) ;
      END_TRACE() ;
      return -1 ;
   }
   /*
    * Freeing the temporary buffer if 
    * it has been created.
    */
   if ( temp ) (void) free(trp) ;
   /*
    * Getting data from the network.
    */
   for(;;) {
      WORD 	req ; 
      int  status ;
      int   rcode ;
      int  msgsiz ;

      msgsiz= rfilefdt[s_index]->_iobuf.hsize + rfilefdt[s_index]->_iobuf.dsize ;
      TRACE(2, "rfio", "rfio_preseek64: reading %d bytes",msgsiz) ; 
      if (netread_timeout(s,rfilefdt[s_index]->_iobuf.base,msgsiz,RFIO_CTRL_TIMEOUT) != msgsiz) {
	 TRACE(2,"rfio","rfio_preseek64: read(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ; 
      }
      p= rfilefdt[s_index]->_iobuf.base ;
      unmarshall_WORD(p,req) ;
      unmarshall_LONG(p,status) ;
      unmarshall_LONG(p,rcode)  ; 
      unmarshall_LONG(p,msgsiz) ; 
      switch(req) {
       case RQST_FIRSTSEEK:
	  rfio_errno = rcode ;
	  TRACE(1,"rfio","rfio_preseek64: status %d, rcode %d",status,rcode) ;
	  if ( status == -1 ) {
	     END_TRACE() ; 
	     return -1 ;
	  }
	  rfilefdt[s_index]->preseek= ( status == iovnb ) ? 2 : 1 ; 
	  rfilefdt[s_index]->nbrecord= status ; 
	  rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ; 
	  rfilefdt[s_index]->_iobuf.count= 0 ; 
	  END_TRACE() ; 
	  return 0 ;
       case RQST_PRESEEK64:
       case RQST_LASTSEEK:
       case RQST_READAHD64:
	  break ; 
       default:
	  TRACE(1,"rfio","rfio_preseek64(): Bad control word received") ; 
	  serrno= SEINTERNAL ;
	  END_TRACE() ; 
	  return -1 ;
      }
   }	
}
