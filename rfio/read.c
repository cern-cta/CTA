/*
 * Copyright (C) 1990-1997 by CERN/IT/PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)read.c	3.15 05/06/98  F. Hemmer, A. Trannoy, F. Hassine";
#endif /* not lint */

/* read.c       Remote File I/O - read  a file                          */


#include <syslog.h>             /* system logger 			*/
#ifndef linux
extern char *sys_errlist[];     /* system error list                    */
#endif

/*
 * System remote file I/O definitions
 */

#define RFIO_KERNEL     1 
#include "rfio.h"  

/* Forward reference */

static int rfio_preread();
static int socset = 0 ;

/*
 * Remote file read
 */
int rfio_read(s, ptr, size)
     char    *ptr;
     int     s, size;
{
  /* Remote file ? */
  if ((s >= 0) && (s < MAXRFD) && (rfilefdt[s] != NULL))
    {
      if (rfilefdt[s]->version3 == 1)
	{
	  /* New V3 stream protocol for sequential transfers */
	  return(rfio_read_v3(s,ptr,size));
	}
      else
	return(rfio_read_v2(s,ptr,size));
    }
  else
    return(rfio_read_v2(s,ptr,size));
}

int rfio_read_v2(s, ptr, size)     
char    *ptr;
int     s, size;
{
	int status ;		/* Status and return code from remote   */
	int nbytes ; 		/* Bytes still to read			*/

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
	 * The file is local.
	 */
	if ( s<0 || s >= MAXRFD || rfilefdt[s] == NULL) {
		TRACE(2,"rfio","rfio_read: using local read(%d, %x, %d)", s, ptr, nbytes);
		status = read(s, ptr, nbytes);
		END_TRACE();
		rfio_errno = 0;
		return(status);
	}

	/*
	 * Checking magic number.
	 */
	if (rfilefdt[s]->magic != RFIO_MAGIC) {
		serrno = SEBADVERSION ;
		(void) close(s) ;
		free((char *)rfilefdt[s]);
		rfilefdt[s] = NULL;
		END_TRACE();
		return(-1);
	}

	if ( !socset ) {
		char * ifce, *p ;
		int bufsize ;
		
		socset ++ ;
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
	                syslog(LOG_ALERT, "rfio: setsockopt(SO_RCVBUF): %s", sys_errlist[errno]);
	        }
	}
	/*
	 * A preseek has been issued. If rfio_preread() does not succeed 
	 * in its search for the correct data, usual operations are done.
	 */
	if ( rfilefdt[s]->preseek ) {
		int 	offset ;	/* Saving current offset	*/

		offset= rfilefdt[s]->offset ;
		status= rfio_preread(s, ptr, size) ;
		if ( status != -2 ) {
			END_TRACE() ;
			return status ;
		}
		rfilefdt[s]->offset= offset ;
		rfilefdt[s]->lseekhow= SEEK_SET ;
		rfilefdt[s]->lseekoff= offset ;	
	}
	/*
	 * The file mark has to be repositionned.
	 * Local flags are reset.
	 */
	if ( rfilefdt[s]->lseekhow != -1 ) {
		rfilefdt[s]->eof= 0 ; 
		rfilefdt[s]->readissued= 0 ; 
		if ( rfilefdt[s]->_iobuf.base ) {
			rfilefdt[s]->_iobuf.count= 0 ; 
			rfilefdt[s]->_iobuf.ptr= iodata(rfilefdt[s]) ; 
		}
	}
	/*
	 * I/O are unbuffered.
	 */
	if ( rfilefdt[s]->_iobuf.base == NULL ) {
		/*
		 * If the end of file has been reached, there is no
		 * no need for sending a request across.
		 */
		if ( rfilefdt[s]->eof ) {
			END_TRACE() ; 
			return 0 ; 
		}
		/*
		 * For unbuffered read ahead I/O, the request
		 * size has to be always the same one.
		 */
		if ( rfilefdt[s]->ahead && rfilefdt[s]->_iobuf.dsize && rfilefdt[s]->_iobuf.dsize != size ) {
			TRACE(2,"rfio","rfio_read: request size %d is imcompatible with the previous one %d",
				size,rfilefdt[s]->_iobuf.dsize) ;
			errno= EINVAL ;
			END_TRACE() ; 
			return 0 ; 
		}
		rfilefdt[s]->_iobuf.dsize= size ;
		/*
		 * Sending a request to fill the
	 	 * user buffer.
		 */
		if ( (status= rfio_filbuf(s,ptr,size)) < 0 ) { 
			rfilefdt[s]->readissued= 0 ; 
			END_TRACE() ; 
			return status ;
		}	
		rfilefdt[s]->offset += status ; 
		if ( status != size ) {
			rfilefdt[s]->eof= 1 ; 
			rfilefdt[s]->readissued= 0 ; 
		}
		END_TRACE() ;
		return status ; 
	}
	/*
	 * I/O are buffered.
	 */
	for(;;)	{
		/*
		 * There is still some valid data in cache.
		 */
		if ( rfilefdt[s]->_iobuf.count ) {
			int count ; 

			count= (nbytes>rfilefdt[s]->_iobuf.count) ? rfilefdt[s]->_iobuf.count : nbytes ;	
			TRACE(2, "rfio", "rfio_read: copy %d cached bytes from 0X%X to 0X%X",count,
				 rfilefdt[s]->_iobuf.ptr, ptr);
			(void) memcpy(ptr,rfilefdt[s]->_iobuf.ptr,count) ;
			ptr+= count ; 
			nbytes -= count ; 
			rfilefdt[s]->_iobuf.count -= count ;
			rfilefdt[s]->_iobuf.ptr += count ;
		}
		/*
		 * User request has been satisfied.
		 */
		if ( nbytes == 0 ) {
			rfilefdt[s]->offset += size ;
			END_TRACE() ;
			return size ;
		}
		/*
		 * End of file has been reached. 
		 * The user is returned what's left.
		 */
		if (rfilefdt[s]->eof) {
			rfilefdt[s]->offset += size - nbytes ;
			END_TRACE() ; 
			return ( size - nbytes ) ; 
		}
		/*
		 * Buffer is going to be fill up.
		 */
		rfilefdt[s]->_iobuf.count = 0;
		rfilefdt[s]->_iobuf.ptr = iodata(rfilefdt[s]);
		status= rfio_filbuf(s,rfilefdt[s]->_iobuf.base,rfilefdt[s]->_iobuf.dsize) ; 
		if ( status < 0 ) {
			rfilefdt[s]->readissued= 0 ; 
			END_TRACE() ; 
			return -1 ;
		}
		if ( status != rfilefdt[s]->_iobuf.dsize ) {
			rfilefdt[s]->eof= 1 ; 
			rfilefdt[s]->readissued= 0 ; 
		}
		rfilefdt[s]->_iobuf.count= status ;
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

	TRACE(1,"rfio","rfio_preread(%d,%x,%d)",s,buffer,size) ;
	ngot= 0 ;
	ncount= 0 ; 
	do 	{
		char 	   * p ;	/* Pointer to data buffer.		*/
		int	status ;
		int	 rcode ;
		int	offset ;
		int	   len ; 

		p= rfilefdt[s]->_iobuf.ptr ;
		unmarshall_LONG(p,offset) ; 
		unmarshall_LONG(p,len) ; 
		unmarshall_LONG(p,status) ; 
		unmarshall_LONG(p,rcode) ; 
		/*
		 * Data we are looking for is in the current record.
		 */
		TRACE(2,"rfio","rfio_preread: record offset is %d and its length is %d",offset,len) ;
		TRACE(2,"rfio","rfio_preread: We want to go at offset %d",rfilefdt[s]->offset) ; 
		if ( (offset <= rfilefdt[s]->offset) && (rfilefdt[s]->offset < (offset+len)) ) {
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
			p+= rfilefdt[s]->offset - offset ;
			ncount= min(size-ngot,status-(rfilefdt[s]->offset-offset)) ;
			TRACE(2, "rfio", "rfio_preread: copy %d cached bytes from 0X%X to 0X%X",ncount,p,buffer+ngot);
			(void) memcpy(buffer+ngot,p,ncount) ;
			rfilefdt[s]->offset += ncount ;
			ngot += ncount ;
			/*
			 * The current record is reaching the end of file.
			 */
			if ( len != status ) {
				rfilefdt[s]->eof= 1 ;
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
			rfilefdt[s]->_iobuf.ptr += 4*LONGSIZE ;
		}
		else	{
			rfilefdt[s]->_iobuf.ptr += 4*LONGSIZE + status ;
		}
		rfilefdt[s]->nbrecord -- ;
		/*
		 * No more data in the buffer.
		 */
		if (  rfilefdt[s]->nbrecord == 0 ) {
			WORD	req ;
			int  msgsiz ;

			/*
			 * It was the last message.
			 * No more data will be sent.
			 */
			if ( rfilefdt[s]->preseek == 2 ) 
				break  ;
			/*
			 * Filling the buffer.
			 */
			msgsiz= rfilefdt[s]->_iobuf.hsize + rfilefdt[s]->_iobuf.dsize ;	
			TRACE(2,"rfio","rfio_preread: reading %d bytes",msgsiz) ; 
			if ( netread(s,rfilefdt[s]->_iobuf.base,msgsiz) != msgsiz ) {
				TRACE(2,"rfio","rfio_preread: read(): ERROR occured (errno=%d)",errno) ;
				END_TRACE() ;
				return -1 ;
			}
			p= rfilefdt[s]->_iobuf.base ;
			unmarshall_WORD(p,req) ; 
			unmarshall_LONG(p,status) ; 
			unmarshall_LONG(p,rcode) ; 
			unmarshall_LONG(p,msgsiz) ; 
			if ( status == -1 )  break ; 
                        rfilefdt[s]->nbrecord= status ;
                        rfilefdt[s]->_iobuf.ptr= iodata(rfilefdt[s]) ;
			rfilefdt[s]->preseek= ( req == RQST_LASTSEEK ) ? 2 : 1 ;

		}
	} while( rfilefdt[s]->preseek ) ;
	/*
	 * Preseek data did not satisfied the read() request.
	 * rfio_read() will process the request.
	 */
	rfilefdt[s]->nbrecord= 0 ;
	rfilefdt[s]->preseek= 0 ; 
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

	TRACE(1,"rfio","rfio_filbuf(0X%X,%d) entered",buffer,size) ;
	hsize= rfilefdt[s]->_iobuf.hsize ;
	/*
	 * If necessary a read request is sent.
	 */
	if ( ! rfilefdt[s]->readissued ) {
		firstread= 1 ;
		p= rfio_buf ; 
		marshall_WORD(p, RFIO_MAGIC);
		marshall_WORD(p,(rfilefdt[s]->ahead)?RQST_READAHEAD:RQST_READ) ; 
		marshall_LONG(p,size) ;
		marshall_LONG(p,rfilefdt[s]->lseekhow) ;
		marshall_LONG(p,rfilefdt[s]->lseekoff) ; 
		rfilefdt[s]->lseekhow= -1 ;
		TRACE(2,"rfio","rfio_filbuf: writing %d bytes",RQSTSIZE) ;
		if (netwrite(s,rfio_buf,RQSTSIZE) != RQSTSIZE)  {
			TRACE(2,"rfio","rfio_filbuf: write(): ERROR occured (errno=%d)", errno) ;
			END_TRACE() ;
			return -1 ; 
		}
		if ( rfilefdt[s]->ahead ) rfilefdt[s]->readissued= 1 ; 
	}
	/*
	 * Reading data from network.
	 */
	do 	{
		/*
		 * The buffer is the user buffer. 
		 * Only data can be written in it.
		 */
		if ( rfilefdt[s]->_iobuf.base == NULL ) {
			TRACE(2, "rfio", "rfio_filbuf: reading %d bytes",hsize) ; 
			if ( netread(s,rfio_buf,hsize) != hsize ) {
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
			if ( netread(s,p,nbytes) != nbytes ) {
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
	return status ;
}
