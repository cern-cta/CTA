/*
 * $Id: read.c,v 1.22 2004/10/27 09:52:51 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: read.c,v $ $Revision: 1.22 $ $Date: 2004/10/27 09:52:51 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, F. Hassine";
#endif /* not lint */

/* read.c       Remote File I/O - read  a file                          */


#include <syslog.h>             /* system logger 			*/
#include <errno.h>
#include <string.h>

/*
 * System remote file I/O definitions
 */

#define RFIO_KERNEL     1 
#include "rfio.h"  
#include "rfio_rfilefdt.h"

/* Forward reference */

static int rfio_preread();

/*
 * Remote file read
 */
int DLL_DECL rfio_read(s, ptr, size)
void    *ptr;
int     s, size;
{
  int s_index;

   /* Remote file ? */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) != -1)
   {
      if (rfilefdt[s_index]->version3 == 1)
      {
	 /* New V3 stream protocol for sequential transfers */
	 return(rfio_read_v3(s,(char *)ptr,size));
      }
      else
	 return(rfio_read_v2(s,(char *)ptr,size));
   }
   else
      return(rfio_read_v2(s,(char *)ptr,size));
}

int rfio_read_v2(s, ptr, size)     
char    *ptr;
int     s, size;
{
   int status ;		/* Status and return code from remote   */
   int HsmType, save_errno;
   int nbytes ; 		/* Bytes still to read			*/
   int s_index;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_read(%d, %x, %d)", s, ptr, size);

   nbytes = size;
   if (nbytes == 0)     {
      END_TRACE();
      return(0);
   }

#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logrd(s,size);
#endif

   /*
    * Check HSM type. The CASTOR HSM uses normal RFIO (local or remote)
    * to perform the I/O. Thus we don't call rfio_HsmIf_read().
    */
   HsmType = rfio_HsmIf_GetHsmType(s,NULL);
   if ( HsmType > 0 ) {
       if ( HsmType != RFIO_HSM_CNS ) {
           status = rfio_HsmIf_read(s,ptr,size);
           if ( status == -1 ) {
               save_errno = errno;
               rfio_HsmIf_IOError(s,errno);
               errno = save_errno;
           }
           END_TRACE();
           return(status);
       }
   }

   /* 
    * The file is local.
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2,"rfio","rfio_read: using local read(%d, %x, %d)", s, ptr, nbytes);
      status = read(s, ptr, nbytes);
      if ( status < 0 ) serrno = 0;
      if ( HsmType == RFIO_HSM_CNS ) {
          save_errno = errno;
          rfio_HsmIf_IOError(s,errno);
          errno = save_errno;
      }
      END_TRACE();

      rfio_errno = 0;
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
    * Checking mode 64.
    */
   if (rfilefdt[s_index]->mode64) {
      status = rfio_read64_v2(s, ptr, size);
      END_TRACE();
      return(status);
   }

   if ( ! rfilefdt[s_index]->socset ) {
      char * ifce, *p ;
      int bufsize ;
      extern char * getconfent() ;
      extern char * getifnam() ;
		
      rfilefdt[s_index]->socset ++ ;
      ifce = ( char * ) getifnam(s) ;
      bufsize= DEFIOBUFSIZE ;
      if ( (p = getconfent("RFIORCVBUF", ifce , 0)) != NULL ) {
	 if ((bufsize = atoi(p)) <= 0)     {
	    bufsize = DEFIOBUFSIZE;
	 }
      }
      else
	 /* reset error code */
	 serrno=0;
	
      /*
       * Set socket buffer size.
       */
	
      TRACE(2, "rfio", "rfio_read: setsockopt(SOL_SOCKET, SO_RCVBUF): for %s : %d", ifce, bufsize);
      if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char *)&(bufsize), sizeof(bufsize)) == -1) {
	 TRACE(2, "rfio" ,"rfio_read: setsockopt(SO_RCVBUF)");
	 syslog(LOG_ALERT, "rfio: setsockopt(SO_RCVBUF): %s", strerror(errno));
      }
   }
   /*
    * A preseek has been issued. If rfio_preread() does not succeed 
    * in its search for the correct data, usual operations are done.
    */
   if ( rfilefdt[s_index]->preseek ) {
      int 	offset ;	/* Saving current offset	*/

      offset= rfilefdt[s_index]->offset ;
      status= rfio_preread(s, ptr, size) ;
      if ( status != -2 ) {
	 END_TRACE() ;
	 return status ;
      }
      rfilefdt[s_index]->offset= offset ;
      rfilefdt[s_index]->lseekhow= SEEK_SET ;
      rfilefdt[s_index]->lseekoff= offset ;	
   }
   /*
    * The file mark has to be repositionned.
    * Local flags are reset.
    */
   if ( rfilefdt[s_index]->lseekhow != -1 ) {
      rfilefdt[s_index]->eof= 0 ; 
      rfilefdt[s_index]->readissued= 0 ; 
      if ( rfilefdt[s_index]->_iobuf.base ) {
	 rfilefdt[s_index]->_iobuf.count= 0 ; 
	 rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ; 
      }
   }
   /*
    * I/O are unbuffered.
    */
   if ( rfilefdt[s_index]->_iobuf.base == NULL ) {
      /*
       * If the end of file has been reached, there is no
       * no need for sending a request across.
       */
      if ( rfilefdt[s_index]->eof == 1 ) {
	 END_TRACE() ; 
	 return 0 ; 
      }
      /*
       * For unbuffered read ahead I/O, the request
       * size has to be always the same one.
       */
      if ( rfilefdt[s_index]->ahead && rfilefdt[s_index]->_iobuf.dsize && rfilefdt[s_index]->_iobuf.dsize != size ) {
	 TRACE(2,"rfio","rfio_read: request size %d is imcompatible with the previous one %d",
	       size,rfilefdt[s_index]->_iobuf.dsize) ;
	 errno= EINVAL ;
	 END_TRACE() ; 
	 return 0 ; 
      }
      rfilefdt[s_index]->_iobuf.dsize= size ;
      /*
       * Sending a request to fill the
       * user buffer.
       */
      TRACE(2,"rfio","rfio_read: call rfio_filbuf(%d,%x,%d) at line %d",
		s,ptr,size,__LINE__);
      if ( (status= rfio_filbuf(s,ptr,size)) < 0 ) { 
	 TRACE(2,"rfio","rfio_read: rfio_filbuf returned %d",status);
	 rfilefdt[s_index]->readissued= 0 ; 
         if ( HsmType == RFIO_HSM_CNS ) 
              rfio_HsmIf_IOError(s,(rfio_errno > 0 ? rfio_errno : serrno));
	 END_TRACE() ; 
	 return status ;
      }
      TRACE(2,"rfio","rfio_read: rfio_filbuf returned %d",status);
      rfilefdt[s_index]->offset += status ; 
      if ( status != size ) {
	 TRACE(2,"rfio","rfio_read: status=%d != size=%d, set eof",status,size);
	 rfilefdt[s_index]->eof= 1 ; 
	 rfilefdt[s_index]->readissued= 0 ; 
      }
      END_TRACE() ;
      return status ; 
   }
   /*
    * I/O are buffered.
    */
   for(;;)	{
      int count;
      /*
       * There is still some valid data in cache.
       */
      if ( rfilefdt[s_index]->_iobuf.count ) {

	 count= (nbytes>rfilefdt[s_index]->_iobuf.count) ? rfilefdt[s_index]->_iobuf.count : nbytes ;	
	 TRACE(2, "rfio", "rfio_read: copy %d cached bytes from 0X%X to 0X%X",count,
	       rfilefdt[s_index]->_iobuf.ptr, ptr);
	 (void) memcpy(ptr,rfilefdt[s_index]->_iobuf.ptr,count) ;
	 ptr+= count ; 
	 nbytes -= count ; 
	 rfilefdt[s_index]->_iobuf.count -= count ;
	 rfilefdt[s_index]->_iobuf.ptr += count ;
      }
      /*
       * User request has been satisfied.
       */
      if ( nbytes == 0 ) {
	 rfilefdt[s_index]->offset += size ;
	 TRACE(2,"rfio", "rfio_read: User request has been satisfied, size=%d, offset=%d, count=%d, s=%d, eof=%d",size,rfilefdt[s_index]->offset,rfilefdt[s_index]->_iobuf.count,s,rfilefdt[s_index]->eof);
	 END_TRACE() ;
	 return size ;
      }
      /*
       * End of file has been reached. 
       * The user is returned what's left.
       */
      if (rfilefdt[s_index]->eof == 1) {
	 TRACE(2,"rfio", "rfio_read: End of file (s=%d, eof=%d) has been reached. size=%d, nbytes=%d, offset=%d",s,rfilefdt[s_index]->eof,size,nbytes,rfilefdt[s_index]->offset);
	 rfilefdt[s_index]->offset += size - nbytes ;
	 END_TRACE() ; 
	 return ( size - nbytes ) ; 
      }
      /*
       * Buffer is going to be fill up.
       */
      rfilefdt[s_index]->_iobuf.count = 0;
      rfilefdt[s_index]->_iobuf.ptr = iodata(rfilefdt[s_index]);
      /*
       * If file offset is going to be moved we have to remember what the 
       * offset should be within the new file buffer.
       * Note: file offset and buffer offset may be different in case 
       * several consecutive lseek() calls has been issued between two reads.
       */
      if ( rfilefdt[s_index]->lseekhow != -1 ) {
	 count =  rfilefdt[s_index]->offset - rfilefdt[s_index]->lseekoff;
      } else {
	 count = 0;
      }

      TRACE(2,"rfio","rfio_read: call rfio_filbuf(%d,%d,%d) at line %d",s,rfilefdt[s_index]->_iobuf.base,rfilefdt[s_index]->_iobuf.dsize,__LINE__);
      status= rfio_filbuf(s,rfilefdt[s_index]->_iobuf.base,rfilefdt[s_index]->_iobuf.dsize) ; 
      TRACE(2,"rfio","rfio_read: rfio_filbuf returned %d",status);
      if ( status < 0 ) {
	 rfilefdt[s_index]->readissued= 0 ; 
         if ( HsmType == RFIO_HSM_CNS ) 
              rfio_HsmIf_IOError(s,(rfio_errno > 0 ? rfio_errno : serrno));
	 END_TRACE() ; 
	 return -1 ;
      }
      if ( status != rfilefdt[s_index]->_iobuf.dsize ) {
	 TRACE(2,"rfio","rfio_read: dsize=%d, set eof",rfilefdt[s_index]->_iobuf.dsize);
	 rfilefdt[s_index]->eof= 1 ; 
	 rfilefdt[s_index]->readissued= 0 ; 
      }
      rfilefdt[s_index]->_iobuf.count= status ;
      /*
       * Make sure that file offset is correctly set within the buffer.
       * Note: file offset and buffer offset may be different in case
       * several consecutive lseek() calls has been issued between two reads.
       */
      rfilefdt[s_index]->_iobuf.count -= count; 
      rfilefdt[s_index]->_iobuf.ptr += count;
   }
}

/*
 * Called when working in preseek() mode.
 * Return code of -2 tells that rfio_preread()
 * has not been able to satisfy the request.
 */
static int rfio_preread(s,buffer,size)
int	    s ; 
char * buffer ;		/* Pointer to user buffer.		*/
int 	 size ;		/* How many bytes do we want to read ?	*/
{
   int	ncount ;
   int	  ngot ;
   int s_index;

   TRACE(1,"rfio","rfio_preread(%d,%x,%d)",s,buffer,size) ;
   ngot= 0 ;
   ncount= 0 ; 
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
     serrno = SEINTERNAL;
     END_TRACE() ; 
     return -1 ; 	
   }
   do 	{
      char 	   * p ;	/* Pointer to data buffer.		*/
      int	status ;
      int	 rcode ;
      int	offset ;
      int	   len ; 

      p= rfilefdt[s_index]->_iobuf.ptr ;
      unmarshall_LONG(p,offset) ; 
      unmarshall_LONG(p,len) ; 
      unmarshall_LONG(p,status) ; 
      unmarshall_LONG(p,rcode) ; 
      /*
       * Data we are looking for is in the current record.
       */
      TRACE(2,"rfio","rfio_preread: record offset is %d and its length is %d",offset,len) ;
      TRACE(2,"rfio","rfio_preread: We want to go at offset %d",rfilefdt[s_index]->offset) ; 
      if ( (offset <= rfilefdt[s_index]->offset) && (rfilefdt[s_index]->offset < (offset+len)) ) {
	 /*
	  * lseek() or read() returned an error.
	  */
	 if ( status == -1 ) {
	    rfio_errno= rcode ;
	    END_TRACE() ; 
	    return -1 ; 	
	 }
	 /*
	  * Copying data into user buffer.
	  */
	 p+= rfilefdt[s_index]->offset - offset ;
	 ncount= min(size-ngot,status-(rfilefdt[s_index]->offset-offset)) ;
	 TRACE(2, "rfio", "rfio_preread: copy %d cached bytes from 0X%X to 0X%X",ncount,p,buffer+ngot);
	 (void) memcpy(buffer+ngot,p,ncount) ;
	 rfilefdt[s_index]->offset += ncount ;
	 ngot += ncount ;
	 /*
	  * The current record is reaching the end of file.
	  */
	 if ( len != status ) {
	    TRACE(2, "rfio", "rfio_preread: len=%d != status=%d, set eof",len,status);
	    rfilefdt[s_index]->eof= 1 ;
	    END_TRACE() ;
	    return ngot ;
	 }
	 /*
	  * The user request is satisfied.
	  */
	 if ( ngot == size ) {
	    END_TRACE() ;
	    return ngot ;
	 }
      }
      /*
       * Pointing to the next record.
       */
      if ( status == -1 ) {
	 rfilefdt[s_index]->_iobuf.ptr += 4*LONGSIZE ;
      }
      else	{
	 rfilefdt[s_index]->_iobuf.ptr += 4*LONGSIZE + status ;
      }
      rfilefdt[s_index]->nbrecord -- ;
      /*
       * No more data in the buffer.
       */
      if (  rfilefdt[s_index]->nbrecord == 0 ) {
	 WORD	req ;
	 int  msgsiz ;

	 /*
	  * It was the last message.
	  * No more data will be sent.
	  */
	 if ( rfilefdt[s_index]->preseek == 2 ) 
	    break  ;
	 /*
	  * Filling the buffer.
	  */
	 msgsiz= rfilefdt[s_index]->_iobuf.hsize + rfilefdt[s_index]->_iobuf.dsize ;	
	 TRACE(2,"rfio","rfio_preread: reading %d bytes",msgsiz) ; 
	 if ( netread_timeout(s,rfilefdt[s_index]->_iobuf.base,msgsiz,RFIO_CTRL_TIMEOUT) != msgsiz ) {
	    TRACE(2,"rfio","rfio_preread: read(): ERROR occured (errno=%d)",errno) ;
	    END_TRACE() ;
	    return -1 ;
	 }
	 p= rfilefdt[s_index]->_iobuf.base ;
	 unmarshall_WORD(p,req) ; 
	 unmarshall_LONG(p,status) ; 
	 unmarshall_LONG(p,rcode) ; 
	 unmarshall_LONG(p,msgsiz) ; 
	 if ( status == -1 )  break ; 
	 rfilefdt[s_index]->nbrecord= status ;
	 rfilefdt[s_index]->_iobuf.ptr= iodata(rfilefdt[s_index]) ;
	 rfilefdt[s_index]->preseek= ( req == RQST_LASTSEEK ) ? 2 : 1 ;

      }
   } while( rfilefdt[s_index]->preseek ) ;
   /*
    * Preseek data did not satisfied the read() request.
    * rfio_read() will process the request.
    */
   rfilefdt[s_index]->nbrecord= 0 ;
   rfilefdt[s_index]->preseek= 0 ; 
   END_TRACE() ;
   return -2 ;
}

/*
 * Filling RFIO buffer. 
 */
int rfio_filbuf(s,buffer,size)         
int	    s ; 
char * buffer ;		/* Pointer to the buffer.		*/
int 	 size ;		/* How many bytes do we want to read ?	*/
{
   int status ;
   int  rcode ;
   int msgsiz ;
   WORD   req ; 
   char   * p ; 		/* Pointer to buffer			*/
   int nbytes ;		/* Number of bytes to read		*/
   int  hsize ; 		/* Message header size			*/
   int firstread= 0 ; 	/* The request has just been issued.	*/	
   char     rfio_buf[BUFSIZ];
   int s_index;

   // Avoiding Valgrind error messages about uninitialized data
   memset(rfio_buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1,"rfio","rfio_filbuf(0X%X,%d) entered",buffer,size) ;
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) < 0) {
	 TRACE(2,"rfio","rfio_filbuf: rfio_rfilefdt_findentry(): ERROR occured (serrno=%d)", serrno) ;
	 END_TRACE() ;
	 return -1 ; 
   }
   nbytes = size;
   hsize= rfilefdt[s_index]->_iobuf.hsize ;
   /*
    * If necessary a read request is sent.
    */
   if ( ! rfilefdt[s_index]->readissued ) {
      firstread= 1 ;
      p= rfio_buf ; 
      marshall_WORD(p, RFIO_MAGIC);
      marshall_WORD(p,(rfilefdt[s_index]->ahead)?RQST_READAHEAD:RQST_READ) ; 
      marshall_LONG(p,size) ;
      marshall_LONG(p,rfilefdt[s_index]->lseekhow) ;
      marshall_LONG(p,rfilefdt[s_index]->lseekoff) ; 
      rfilefdt[s_index]->lseekhow= -1 ;
      TRACE(2,"rfio","rfio_filbuf: s=%d, s_index=%d, writing %d bytes, lseekoff=%d",s,s_index,RQSTSIZE,
            rfilefdt[s_index]->lseekoff) ;
      if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE)  {
	 TRACE(2,"rfio","rfio_filbuf: write(): ERROR occured (errno=%d)", errno) ;
	 END_TRACE() ;
	 return -1 ; 
      }
      if ( rfilefdt[s_index]->ahead ) rfilefdt[s_index]->readissued= 1 ; 
   }
   /*
    * Reading data from network.
    */
   do 	{
      /*
       * The buffer is the user buffer. 
       * Only data can be written in it.
       */
      if ( rfilefdt[s_index]->_iobuf.base == NULL ) {
	 TRACE(2, "rfio", "rfio_filbuf: reading %d bytes",hsize) ; 
	 if ( netread_timeout(s,rfio_buf,hsize,RFIO_CTRL_TIMEOUT) != hsize ) {
	    TRACE(2,"rfio","rfio_filbuf: read(): ERROR occured (errno=%d)", errno);
	    END_TRACE();
	    return -1 ; 
	 }
	 p= rfio_buf ;
	 unmarshall_WORD(p,req) ;	/* RQST_READ, RQST_READAHEAD or RQST_FIRSTREAD	*/
	 unmarshall_LONG(p,status) ;
	 unmarshall_LONG(p, rcode) ;
	 unmarshall_LONG(p,msgsiz) ;
	 if ( status < 0 ) {
	    rfio_errno= rcode ;
	    if ( rcode == 0 ) 
	       serrno = SENORCODE ;
	    END_TRACE() ; 
	    return -1 ;
	 }
	 nbytes= msgsiz ;	/* Nb of bytes still to read	*/
	 p= buffer ; 		/* Pointer to buffer		*/
      }
      else	{
	 int nread ;
	 int nwant ; 
	 int  ngot ; 

	 for(nread= 0,nwant= size+hsize; nread<hsize; nread += ngot, nwant -= ngot) {
	    TRACE(2, "rfio", "rfio_filbuf: receiving %d bytes",nwant) ; 
	    if ((ngot= recv(s,buffer+nread,nwant, 0)) <= 0)   {
	       if (ngot == 0)  {
		  serrno = SECONNDROP;
		  TRACE(2, "rfio", "rfio_filbuf: read(): ERROR occured (serrno=%d)", serrno) ;
	       }
	       else    {
		  TRACE(2, "rfio", "rfio_filbuf: read(): ERROR occured (errno=%d)", errno) ;
	       }
	       END_TRACE() ;
	       return -1 ;
	    }
	    TRACE(2,"rfio","rfio_filbuf: %d bytes received",ngot) ; 
	 }
	 p= buffer ;
	 unmarshall_WORD(p,req) ;	
	 unmarshall_LONG(p,status) ;
	 unmarshall_LONG(p, rcode) ;
	 unmarshall_LONG(p,msgsiz) ;
	 if ( status < 0 ) {
	    rfio_errno= rcode ;
	    if ( rcode == 0 )
	       serrno = SENORCODE ;
	    END_TRACE() ; 
	    return -1 ;
	 }
	 nbytes= msgsiz + hsize - nread ;	/* Nb of bytes still to read	*/
	 p= buffer + nread ;			/* Pointer to buffer		*/
      }
      /*
       * Receiving data remaining in the current message.
       */
      if ( nbytes ) {
	 TRACE(2,"rfio","rfio_filbuf: reading last %d bytes",nbytes) ; 
	 if ( netread_timeout(s,p,nbytes,RFIO_DATA_TIMEOUT) != nbytes ) {
	    TRACE(2, "rfio", "rfio_filbuf: read(): ERROR occured (errno=%d)", errno) ;
	    END_TRACE() ;
	    return -1 ;
	 }
      }
   } while ( firstread && (req==RQST_READAHEAD || req==RQST_PRESEEK || req==RQST_LASTSEEK)) ;
   /*
    * Data we were waiting for has been received.
    */
   TRACE(1, "rfio", "rfio_filbuf: status %d, rcode %d", status, rcode) ;
   END_TRACE() ;
   return status ;
}
