/*
 * $Id: fstat.c,v 1.15 2004/10/27 09:52:50 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fstat.c,v $ $Revision: 1.15 $ $Date: 2004/10/27 09:52:50 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fstat.c      Remote File I/O - get file status                       */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1   
#include "rfio.h"          
#include "rfio_rfilefdt.h"

#include <stdlib.h>            /* malloc prototype */

/*
 * Remote file stat
 */
int DLL_DECL rfio_fstat(s, statbuf)  
int     s;
struct stat *statbuf;
{
#if (defined(__alpha) && defined(__osf__))
   return (rfio_fstat64(s,statbuf));
#else
   int status ;
#if defined(IRIX64) || defined(__ia64__) || defined(__x86_64)
   struct stat64 statb64;

   if ((status = rfio_fstat64(s,&statb64)) == 0)
       (void) stat64tostat(&statb64, statbuf);
   return (status);
#else
   char   * p ;
   char * trp ;
   int temp=0 ;
   char     rfio_buf[BUFSIZ];
   int s_index = -1;

   // Avoiding Valgrind error messages about uninitialized data
   memset(rfio_buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_fstat(%d, %x)", s, statbuf);
   /* 
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_fstat: using local fstat(%d, %x)", s, statbuf);
      status = fstat(s, statbuf);
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
    * File mark has to be repositionned.
    */
   if ( (rfilefdt[s_index]->readissued || rfilefdt[s_index]->preseek) && rfilefdt[s_index]->lseekhow == -1 ) {
      rfilefdt[s_index]->lseekhow= SEEK_SET ;
      rfilefdt[s_index]->lseekoff= rfilefdt[s_index]->offset ; 
   }
   /*
    * Flags on current status are reset.
    */
   rfilefdt[s_index]->eof= 0 ; 
   rfilefdt[s_index]->preseek= 0 ;
   rfilefdt[s_index]->nbrecord= 0 ;
   rfilefdt[s_index]->readissued= 0 ; 
   if ( rfilefdt[s_index]->_iobuf.base ) {
      rfilefdt[s_index]->_iobuf.count= 0 ; 
      rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ;
   }	
   /*
    * Sending request.
    */
   p= rfio_buf ;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_FSTAT);
/*
  marshall_LONG(p, rfilefdt[s_index]->lseekhow) ; 
*/
   marshall_LONG(p, rfilefdt[s_index]->lseekoff) ; 
   marshall_LONG(p, rfilefdt[s_index]->lseekhow) ; 
   TRACE(2,"rfio","rfio_fstat: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_fstat: write(): ERROR occured (errno=%d)", errno);
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

      TRACE(2, "rfio", "rfio_fstat: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ; 
      if (netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_DATA_TIMEOUT) != rfilefdt[s_index]->_iobuf.hsize) {
	 TRACE(2, "rfio", "rfio_fstat: read(): ERROR occured (errno=%d)", errno);
	 if ( temp ) (void) free(trp) ; 
	 END_TRACE() ;
	 return -1 ; 
      }
      p = rfio_buf ;
      unmarshall_WORD(p,req) ;
      unmarshall_LONG(p,status) ; 
      unmarshall_LONG(p, rcode) ; 
      unmarshall_LONG(p,msgsiz) ; 
      switch(req) {
       case RQST_FSTAT:
	  TRACE(2, "rfio", "rfio_fstat: reading %d bytes",msgsiz);
	  if (netread_timeout(s,rfio_buf,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	     TRACE(2,"rfio","rfio_fstat: read(): ERROR occured (errno=%d)",errno) ;
	     if ( temp ) (void) free(trp) ; 
	     END_TRACE() ;
	     return -1 ; 
	  }
	  p = rfio_buf ;
	  unmarshall_WORD(p,statbuf->st_dev);
	  unmarshall_LONG(p,statbuf->st_ino);
	  unmarshall_WORD(p,statbuf->st_mode);
	  unmarshall_WORD(p,statbuf->st_nlink);
	  unmarshall_WORD(p,statbuf->st_uid);
	  unmarshall_WORD(p,statbuf->st_gid);
	  unmarshall_LONG(p,statbuf->st_size);
	  unmarshall_LONG(p,statbuf->st_atime);
	  unmarshall_LONG(p,statbuf->st_mtime);
	  unmarshall_LONG(p,statbuf->st_ctime);
	  rfio_errno=  rcode ;
	  if ( temp ) (void) free(trp) ; 
	  TRACE(1,"rfio","rfio_fstat: return status %d, rcode %d",status,rcode) ;
	  END_TRACE() ;
	  return status ;
       case RQST_READAHEAD:
       case RQST_READAHD64:
       case RQST_LASTSEEK:
       case RQST_PRESEEK:
       case RQST_PRESEEK64:
	  /* 
	   * At this point a temporary buffer may need to be created
	   * to receive data which is going to be thrown away.
	   */
	  if ( temp == 0 ) {
	     if ( rfilefdt[s_index]->_iobuf.base==NULL || rfilefdt[s_index]->_iobuf.dsize<msgsiz ) {
		temp= 1 ; 
		TRACE(3,"rfio","rfio_fstat: allocating momentary buffer of size %d",msgsiz) ; 
		if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
		   TRACE(3,"rfio","rfio_fstat: malloc(): ERROR occured (errno=%d)",errno) ; 
		   END_TRACE() ; 
		   return -1 ;
		}
	     }
	     else
		trp= iodata(rfilefdt[s_index]) ;
	  }
	  if ( netread_timeout(s,trp,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
	     TRACE(2,"rfio","rfio_fstat: read(): ERROR occured (errno=%d)",errno) ;
	     if ( temp ) (void) free(trp) ; 
	     END_TRACE() ; 
	     return -1 ;
	  }
	  break ; 
       default:
	  TRACE(1,"rfio","rfio_fstat(): Bad control word received\n") ; 
	  serrno= SEINTERNAL ;
	  if ( temp ) (void) free(trp) ; 
	  END_TRACE() ; 
	  return -1 ;
      }
   }	
#endif
#endif
}
