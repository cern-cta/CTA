/*
 * $Id: lockf64.c,v 1.1 2002/11/19 10:51:22 baud Exp $
 */

/*
 * Copyright (C) 1994-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: lockf64.c,v $ $Revision: 1.1 $ $Date: 2002/11/19 10:51:22 $ CERN/IT/PDP/DM Antony Simmins, P. Gaillardon";
#endif /* not lint */

/* lockf64.c       Remote File I/O - record locking on files      */

#define RFIO_KERNEL     1     /* KERNEL part of the routines          */

#include "rfio.h"             /* Remote File I/O general definitions  */
#include "rfio_rfilefdt.h"
#include "u64subr.h"


int DLL_DECL rfio_lockf64(sd, op, siz)    /* Remote lockf             */
int      sd;                              /* file descriptor          */
int      op;                              /* lock operation           */
off64_t  siz;                             /* locked region            */
{
   static char    buf[256];       /* General input/output buffer      */
   int            status;         /* remote lockf() status            */
   int      len;
   int      replen;
   char     *p=buf;
   int      rt ;
   int      rcode ;
   int      s_index;
   char     tmpbuf[21];

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_lockf64(%d, %d, %s)", sd, op, i64tostr(siz,tmpbuf,0));

   /* 
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(sd,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(1, "rfio", "rfio_lockf64: using local lockf64(%d, %d, %s)",
         sd, op, i64tostr(siz,tmpbuf,0));
      END_TRACE();
      rfio_errno = 0;
      status = lockf64(sd,op,siz);
      if ( status < 0 ) serrno = 0;
      return(status);
   }
   
   /*
    * Reject call if bufferized or streamed mode
    */
   if ( rfilefdt[s_index]->_iobuf.base != NULL || rfilefdt[s_index]->version3 ) {
      TRACE(1, "rfio","rfio_lockf64: lockf rejected in bufferized or streamed mode");
      END_TRACE();
      errno = EBADF;
      return(-1);
   } 

   /*
    * Repositionning file mark if needed.
    */
   if (  (rfilefdt[s_index]->lseekhow == -1)
      && ( rfilefdt[s_index]->readissued || rfilefdt[s_index]->preseek ) ) {
      rfilefdt[s_index]->lseekhow= SEEK_SET ;
      rfilefdt[s_index]->lseekoff64= rfilefdt[s_index]->offset64 ;
   }
   /*
    * Resetting flags
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
   
   len = 1*LONGSIZE+ 2*HYPERSIZE;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_LOCKF64);
   marshall_LONG(p, rfilefdt[s_index]->lseekhow) ; 
   marshall_LONG(p, len);
   p= buf + RQSTSIZE;
   marshall_LONG(p, op);
   marshall_HYPER(p, siz);
   marshall_HYPER(p, rfilefdt[s_index]->lseekoff64) ; 
   TRACE(1,"rfio","rfio_lockf64: op %d, siz %s", op, i64tostr(siz,tmpbuf,0));
   TRACE(2,"rfio","rfio_lockf64: sending %d bytes",RQSTSIZE+len) ;
   if (netwrite_timeout(sd, buf, RQSTSIZE+len, RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      TRACE(2, "rfio", "rfio_lockf64: write(): ERROR occurred (errno=%d)", errno);
      (void) close(sd);
      END_TRACE();
      return(-1);
   }
   p = buf;
   replen = 2*LONGSIZE;
   TRACE(2, "rfio", "rfio_lockf64: reading %d bytes", replen);
   if (netread_timeout(sd, buf, replen, RFIO_DATA_TIMEOUT) != replen)  {
      TRACE(2, "rfio", "rfio_lockf64: read(): ERROR occurred (errno=%d)", errno);
      (void) close(sd);
      END_TRACE();
      return(-1);
   }
   unmarshall_LONG(p, status);
   unmarshall_LONG(p, rcode);
   TRACE(1, "rfio", "rfio_lockf64: return %d",status);
   rfio_errno = rcode;
   if (status)     {
      END_TRACE();
      return(-1);
   }
   END_TRACE();
   return (0);
}
