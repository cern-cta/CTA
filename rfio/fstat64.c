/*
 * $Id: fstat64.c,v 1.3 2003/01/22 10:18:18 baud Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: fstat64.c,v $ $Revision: 1.3 $ $Date: 2003/01/22 10:18:18 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, P. Gaillardon";
#endif /* not lint */

/* fstat64.c      Remote File I/O - get file status                       */

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
int DLL_DECL rfio_fstat64(s, statbuf)  
int            s;
struct stat64  *statbuf;
{
   int status ;
   char   * p ;
   char * trp ;
   int temp=0 ;
   char     rfio_buf[BUFSIZ];
   int s_index = -1;
   WORD    req ; 
   LONG  rcode ;
   LONG msgsiz ;
   int       n ;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_fstat64(%d, %x)", s, statbuf);
   /* 
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_fstat64: using local fstat(%d, %x)", s, statbuf);
      status = fstat64(s, statbuf);
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
   marshall_WORD(p, RQST_FSTAT64);
   TRACE(2,"rfio","rfio_fstat64: sending %d bytes", RQSTSIZE, rfilefdt[s_index]->lseekhow);
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_fstat64: write(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return(-1);
   }
   /*
    * Getting data from the network.
    */

   TRACE(2, "rfio", "rfio_fstat64: reading %d bytes",rfilefdt[s_index]->_iobuf.hsize) ;
   n = netread_timeout(s,rfio_buf,rfilefdt[s_index]->_iobuf.hsize,RFIO_DATA_TIMEOUT); 
   if (n != rfilefdt[s_index]->_iobuf.hsize) {
      TRACE(2, "rfio", "rfio_fstat64: read(): ERROR occured (errno=%d)", errno);
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
      case RQST_FSTAT64:
         TRACE(2, "rfio", "rfio_fstat64: reading %d bytes",msgsiz);
         if (netread_timeout(s,rfio_buf,msgsiz,RFIO_DATA_TIMEOUT) != msgsiz ) {
            TRACE(2,"rfio","rfio_fstat64: read(): ERROR occured (errno=%d)",errno) ;
            if ( temp ) (void) free(trp) ;
            END_TRACE() ;
            return -1 ; 
         }
         p = rfio_buf ;
         unmarshall_WORD(p,statbuf->st_dev);
         unmarshall_HYPER(p,statbuf->st_ino);
         unmarshall_WORD(p,statbuf->st_mode);
         unmarshall_WORD(p,statbuf->st_nlink);
         unmarshall_WORD(p,statbuf->st_uid);
         unmarshall_WORD(p,statbuf->st_gid);
         unmarshall_HYPER(p,statbuf->st_size);
         unmarshall_LONG(p,statbuf->st_atime);
         unmarshall_LONG(p,statbuf->st_mtime);
         unmarshall_LONG(p,statbuf->st_ctime);
#if !defined(_WIN32)
         if ( msgsiz > (5*WORDSIZE+3*LONGSIZE+2*HYPERSIZE) ) {
            unmarshall_LONG(p, statbuf->st_blksize);
            unmarshall_HYPER(p, statbuf->st_blocks);
         } else {
            statbuf->st_blksize = 0;
            statbuf->st_blocks  = 0;
         }
#endif
         rfio_errno= rcode ;
         if ( temp ) (void) free(trp) ;
         TRACE(1,"rfio","rfio_fstat64: return status %d, rcode %d", status, rcode) ;
         END_TRACE() ;
         return status ;
      case RQST_READAHD64:
      case RQST_LASTSEEK:
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
         TRACE(1,"rfio","rfio_fstat64(): Bad control word received\n") ; 
         serrno= SEINTERNAL ;
         if ( temp ) (void) free(trp) ;
         END_TRACE() ;
         return -1 ;
   }
   
}
