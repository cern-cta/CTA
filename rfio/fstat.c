/*
 * $Id: fstat.c,v 1.2 1999/07/20 12:47:59 jdurand Exp $
 *
 * $Log: fstat.c,v $
 * Revision 1.2  1999/07/20 12:47:59  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990,1991 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)fstat.c	3.3 5/6/98 CERN CN-SW/DC F. Hemmer, A. Trannoy";
#endif /* not lint */

/* fstat.c      Remote File I/O - get file status                       */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1   
#include "rfio.h"          

/*
 * Remote file stat
 */
int  rfio_fstat(s, statbuf)  
	int     s;
	struct stat *statbuf;
{
	int status ;
	char   * p ;
	char * trp ;
	int temp=0 ;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_fstat(%d, %x)", s, statbuf);
	/* 
	 * The file is local
	 */
	if ( s>= MAXRFD || rfilefdt[s] == NULL) {
		TRACE(2, "rfio", "rfio_fstat: using local fstat(%d, %x)", s, statbuf);
		status = fstat(s, statbuf);
		rfio_errno = 0;
		END_TRACE();
		return(status);
	}
	/*
	 * Checking magic number.
	 */
	if (rfilefdt[s]->magic != RFIO_MAGIC) {
		serrno = SEBADVERSION ; 
		(void) close(s) ;
		free((char *)rfilefdt[s]);
		rfilefdt[s] = NULL ;
		END_TRACE();
		return(-1);
	}
	/*
	 * File mark has to be repositionned.
	 */
	if ( (rfilefdt[s]->readissued || rfilefdt[s]->preseek) && rfilefdt[s]->lseekhow == -1 ) {
		rfilefdt[s]->lseekhow= SEEK_SET ;
		rfilefdt[s]->lseekoff= rfilefdt[s]->offset ; 
	}
	/*
	 * Flags on current status are reset.
	 */
	rfilefdt[s]->eof= 0 ; 
	rfilefdt[s]->preseek= 0 ;
	rfilefdt[s]->nbrecord= 0 ;
	rfilefdt[s]->readissued= 0 ; 
	if ( rfilefdt[s]->_iobuf.base ) {
		rfilefdt[s]->_iobuf.count= 0 ; 
		rfilefdt[s]->_iobuf.ptr= iodata(rfilefdt[s]) ;
	}	
	/*
	 * Sending request.
	 */
	p= rfio_buf ;
	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_FSTAT);
/*
	marshall_LONG(p, rfilefdt[s]->lseekhow) ; 
*/
	marshall_LONG(p, rfilefdt[s]->lseekoff) ; 
	marshall_LONG(p, rfilefdt[s]->lseekhow) ; 
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

		TRACE(2, "rfio", "rfio_fstat: reading %d bytes",rfilefdt[s]->_iobuf.hsize) ; 
		if (netread_timeout(s,rfio_buf,rfilefdt[s]->_iobuf.hsize,RFIO_DATA_TIMEOUT) != rfilefdt[s]->_iobuf.hsize) {
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
		   case RQST_LASTSEEK:
		   case RQST_PRESEEK:
			/* 
			 * At this point a temporary buffer may need to be created
			 * to receive data which is going to be thrown away.
			 */
			if ( temp == 0 ) {
			   if ( rfilefdt[s]->_iobuf.base==NULL || rfilefdt[s]->_iobuf.dsize<msgsiz ) {
				temp= 1 ; 
				TRACE(3,"rfio","rfio_fstat: allocating momentary buffer of size %d",msgsiz) ; 
				if ( (trp= ( char *) malloc(msgsiz)) == NULL ) {
					TRACE(3,"rfio","rfio_fstat: malloc(): ERROR occured (errno=%d)",errno) ; 
					END_TRACE() ; 
					return -1 ;
				}
			   }
			   else
				trp= iodata(rfilefdt[s]) ;
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
			rfio_errno= SEINTERNAL ;
			if ( temp ) (void) free(trp) ; 
			END_TRACE() ; 
			return -1 ;
		}
	}	
}
