/*
 * $Id: open.c,v 1.7 2000/05/02 19:38:34 baud Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: open.c,v $ $Revision: 1.7 $ $Date: 2000/05/02 19:38:34 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, F. Hassine";
#endif /* not lint */

/* open.c       Remote File I/O - open file a file                      */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include <syslog.h>             /* system logger                        */
#include "rfio.h"               /* remote file I/O definitions          */
#include "rfcntl.h"             /* remote file control mapping macros   */
#if !defined(_WIN32)
#include <arpa/inet.h>          /* for inet_ntoa()                      */
#endif
#include <pwd.h>
#include <stdlib.h>
#include <Cpwd.h>

#ifndef linux
extern char *sys_errlist[];     /* system error list                    */
#endif

RFILE  *rfilefdt[MAXRFD];        /* File descriptors tables             */

static void rfio_setup_ext(iop,uid,gid,passwd)
RFILE   *iop;
int 	uid;
int 	gid;
int	passwd;
{
   extern char * getenv() ; 	/* External declaration		*/
   char * cp ; 			/* Character pointer		*/
   int v ;
   char     rfio_buf[BUFSIZ] ;

   if ( cp= getenv("RFIO_READOPT") ) {
      v = atoi(cp) ;
      rfiosetopt(RFIO_READOPT, &v , 4) ; 
   }
   /*
    * We don't allow READAHEAD without buffering 
    * until the Ultra bug ( or mine ) is fixed.
    if ( rfioreadopt(RFIO_READOPT) == 2 ) {
    v = 3 ;
    rfiosetopt( RFIO_READOPT, &v ,4) ; 
    }
   */

   iop->magic = RFIO_MAGIC;
   iop->s = -1;
   if (uid || gid)
      iop->mapping=0;
   else
      iop->mapping=1;
   iop->passwd=passwd; /* used only if mapping == 0 */
   iop->uid = (uid==0 ? geteuid(): uid);
   iop->gid = (gid==0 ? getegid(): gid);
   INIT_TRACE("RFIO_TRACE");
   TRACE ( 1,"rfio","rfio_setup_ext(%d,%d,%d)",iop,uid,gid);
   TRACE ( 2,"rfio","rfio_setup_ext: owner s uid is %d",iop->uid);
   TRACE ( 2,"rfio","rfio_setup_ext: owner s gid is %d",iop->gid);
   END_TRACE();
   (void) umask(iop->umask=umask(0));
   iop->ftype = FFTYPE_C;
#if !defined(VM) && !defined(MVS)
   iop->binary = 0;                 /* no translation needed        */
#endif /* VM */
   iop->eof = 0;
   iop->unit = 0;
   iop->access = 0;
   iop->format = 0;
   iop->recl = 0;
   iop->blank = 0;
   iop->opnopt = 0;
   iop->offset= 0 ; 
   iop->_iobuf.base = NULL;
   iop->_iobuf.ptr = NULL;
   iop->_iobuf.count = 0;
   iop->_iobuf.hsize = 0;
   iop->_iobuf.dsize = 0;
   iop->lseekhow= -1 ; 
   iop->ahead= rfioreadopt(RFIO_READOPT) & RFIO_READAHEAD ;
   iop->eof= 0 ; 
   iop->readissued= 0 ; 
   iop->preseek = 0 ; 
   iop->nbrecord= 0 ; 
   strcpy(iop->host,"????????");
}

int     rfio_cleanup(s)         /* cleanup rfio descriptor              */
int     s;
{
   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_cleanup(%d)", s);

   if (rfilefdt[s] != NULL) {
      if (rfilefdt[s]->magic != RFIO_MAGIC && rfilefdt[s]->magic != B_RFIO_MAGIC) {
	 serrno = SEBADVERSION ; 
	 END_TRACE();
	 return(-1);
      }
      if (rfilefdt[s]->_iobuf.base != NULL)    {
	 TRACE(2, "rfio", "freeing I/O buffer at 0X%X", rfilefdt[s]->_iobuf.base);
	 (void) free(rfilefdt[s]->_iobuf.base);
      }
      TRACE(2, "rfio", "closing %d",s) ;
      (void) close(s) ;
      TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rfilefdt[s]);
      (void) free((char *)rfilefdt[s]);
      rfilefdt[s] = NULL;
   }
   END_TRACE();
   return(0);
}


int rfio_open(filepath, flags, mode)
char    *filepath ;
int     flags,mode ;
{
   int n;
   int old;
   int fd;

   old = rfioreadopt(RFIO_READOPT);

   if ((old & RFIO_STREAM) == RFIO_STREAM)
   {
      /* New V3 stream protocol for sequential transfers is requested */
      
      if ((n = rfio_open_v3(filepath,flags,mode)) < 0)
      {
	 if (serrno == SEPROTONOTSUP) /* Server doesn't support V3 protocol, call previous open (V2)*/
	 {
	    int newopt = RFIO_READBUF;

	    rfiosetopt(RFIO_READOPT,&newopt,4); /* 4 = dummy len value */
	    fd = rfio_open_v2(filepath, flags, mode);
	    if ((fd >= 0) && (fd < MAXRFD) && (rfilefdt[fd] != NULL))
	       rfilefdt[fd]->version3 = 0;
	    return(fd);
	 }
	 else 
	    return(-1);
      }
      else
      {
	 /* To indicate to rfio_open_ext (called directly by rtcopy) that the negociation has already
	    been done */
	 int newopt = RFIO_STREAM | RFIO_STREAM_DONE;

	 rfiosetopt(RFIO_READOPT,&newopt,4); /* 4 = dummy len value */
	 if ((n >= 0) && (n < MAXRFD) && (rfilefdt[n] != NULL))
	    rfilefdt[n]->version3 = 1;
	 return(n);
      }
   }
   else 
   {
      /* Call previous version */
      fd = rfio_open_v2(filepath, flags, mode);
      if ((fd >= 0) && (fd < MAXRFD) && (rfilefdt[fd] != NULL))
	 rfilefdt[fd]->version3 = 0;
      return(fd);
   }
} 
	
int 	rfio_open_v2(filepath, flags, mode)
char    * filepath ;
int     flags,mode ;
{
   char rh[1] ;
   rh[0]='\0' ;
  
   return(rfio_open_ext(filepath, flags, mode,(uid_t)0,(gid_t)0,0,rh,rh));
} 

/*
 * Remote file open.
 */
int	rfio_open_ext(filepath, flags, mode,uid,gid,passwd,reqhost,vmstr) 
char    * filepath ;
int 	flags,mode ;
uid_t	uid;
gid_t	gid;
int 	passwd ;
char 	* reqhost; /* In case of a Non-mapped I/O with uid & gid 
		      sepcified, which host will be contacted
		      for key check ? */
char  	*vmstr ;
{
   /*
    * TODO: support special filemode for binary/nobinary. This only
    *       applies to non-ascii machines (e.g. IBM VM/MVS)
    */
   int     status ;	/* Return code 			*/
   int	 rcode ;	/* Remote errno			*/
   int        len ;
   char    * host ; 
   char * filename;
   char  * account;
   char       * p ;	/* Pointer to rfio buffer	*/
   RFILE    * rfp ;	/* Remote file pointer          */
   WORD	   req ;
   struct passwd *pw;
   int 	    rt ; 	/* daemon in site(0) or not (1) */	
   int    bufsize ; 	/* socket buffer size 		*/	
   struct sockaddr_in      to;
   int                     tolen;
   struct  hostent *hp;
   extern void rfio_setup_ext();
   extern char * getifnam() ;
   int old,n;
   char     rfio_buf[BUFSIZ];

   INIT_TRACE("RFIO_TRACE");
   TRACE(1,"rfio","rfio_open_ext(%s, %d, %d, %d, %d, %d, %s, %s)",filepath,flags,mode,uid,gid,passwd,reqhost,vmstr ) ;

	/* the rtcopy program calls directly rfio_open_ext, so we (again) do this test here, ugly */
   old = rfioreadopt(RFIO_READOPT);

   /* New V3 stream protocol for sequential transfers */
   /* If RFIO_STREAM_DONE is also set, the negociation has already been done */
   if (old == RFIO_STREAM)
   {
      if ((n = rfio_open_ext_v3(filepath,flags,mode,uid,gid,passwd,reqhost,vmstr)) < 0)
      {
	 /* Server doesn't support V3 protocol, call previous open*/
	 if (serrno == SEPROTONOTSUP) 
	 {
	    int newopt = RFIO_READBUF;
		    
	    rfiosetopt(RFIO_READOPT,&newopt,4); /* 4 = dummy len value */
	    /* In this case, the version3 field is set to 1 later in this routine */
	 }
	 else 
	    return(-1);
      }
      else
      {
	 if ((n >= 0) && (n < MAXRFD) && (rfilefdt[n] != NULL))
	    rfilefdt[n]->version3 = 1;
	 return(n);
      }
   }

   /* Version 2 behaviour starts here */	
#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logst();
#endif /* CLIENTLOG */

   /*
	 * The file is local.
	 */
   if ( ! rfio_parse(filepath,&host,&filename) ) {
      status= open(filename, flags, mode) ;
      rfio_errno = 0;
      END_TRACE() ; 
#if defined (CLIENTLOG)
      /* Client logging */
      rfio_logop(status,filename,host,flags);
#endif /* CLIENTLOG */
      return status ;
   }

   /*
	 * Allocate and initialize a remote file descriptor.
	 */
   if ((rfp = (RFILE *)malloc(sizeof(RFILE))) == NULL)        {
      TRACE(2, "rfio", "rfio_open: malloc(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return(-1);
   }
   rfio_setup_ext(rfp,(int)uid,(int)gid,passwd) ;
   TRACE(2, "rfio", "RFIO descriptor allocated at 0x%X", rfp);
   /*
	 * Connecting server.
	 */
   rfp->s = rfio_connect(host,&rt);
   if (rfp->s < 0)      {
      TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rfp);
      (void) free(rfp);
      END_TRACE();
      return(-1);
   }
   tolen=sizeof(to);
   if (getpeername(rfp->s,(struct sockaddr *)&to, &tolen)<0)        {
      syslog(LOG_ALERT, "rfio: open: getpeername: %s\n",sys_errlist[errno]);
   }
   if ((hp = gethostbyaddr((char *) (&to.sin_addr), sizeof(struct in_addr), to.sin_family)) == NULL){
      strncpy(rfp->host, (char *)inet_ntoa(to.sin_addr), RESHOSTNAMELEN );
   }
   else    {
      strncpy(rfp->host,hp->h_name, RESHOSTNAMELEN );
   }


   if ( !rt && !rfp->mapping ) {
      rfp->uid=geteuid() ;
      rfp->gid=getegid() ;
      TRACE(3,"rfio", "re-setting (uid,gid) to %d,%d",rfp->uid,rfp->gid) ;
      rfp->mapping = 1 ;
   }
		
   /*
	 * Remote file table is not large enough.
	 */
   if ( rfp->s >= MAXRFD ) {
      TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rfp);
      (void) free(rfp);
      END_TRACE();
      errno= EMFILE ;
      return -1 ;
   }
   rfilefdt[rfp->s]=rfp;

	/* Set version3 to false since we are running version 2 here */
   rfilefdt[rfp->s]->version3 = 0;

   bufsize= DEFIOBUFSIZE ;
   if ((p = getconfent("RFIO", "IOBUFSIZE", 0)) != NULL)        {
      if ((bufsize = atoi(p)) <= 0)     {
	 bufsize = DEFIOBUFSIZE;
      }
   }
   else 
      /* reset error code */
      serrno=0;

   TRACE(2, "rfio", "rfio_open: setsockopt(SOL_SOCKET, SO_KEEPALIVE)");
   rcode = 1 ;
   if (setsockopt(rfp->s, SOL_SOCKET, SO_KEEPALIVE,(char *)&rcode, sizeof (int) ) == -1) {
      TRACE(2, "rfio" ,"rfio_open: setsockopt(SO_KEEPALIVE) failed");
      syslog(LOG_ALERT, "rfio: open: setsockopt(SO_KEEPALIVE): %s", sys_errlist[errno]);
   }

   /*
	 * Allocate, if necessary, an I/O buffer.
	 */
   rfp->_iobuf.hsize= 3*LONGSIZE + WORDSIZE ;
   if ( rfioreadopt(RFIO_READOPT) & RFIO_READBUF ) {

      rfp->_iobuf.dsize = bufsize - rfp->_iobuf.hsize;
      if ((rfp->_iobuf.base = malloc(bufsize)) == NULL)  {
	 rfio_cleanup(rfp->s);
	 END_TRACE();
	 return -1 ; 
      }
      TRACE(2, "rfio", "I/O buffer allocated at 0x%X", rfp->_iobuf.base) ;
      rfp->_iobuf.count = 0;
      rfp->_iobuf.ptr = iodata(rfp) ;
   }

   if ( (pw = Cgetpwuid(geteuid()) ) == NULL ) {
      TRACE(2, "rfio" ,"rfio_open: Cgetpwuid() error %s",sys_errlist[errno]);
      rfio_cleanup(rfp->s);
      END_TRACE();
      return -1 ;
   }
        /*
	 * Building and sending request.
	 */
   if ((account = getacct()) == NULL) account = "";
   TRACE(2,"rfio","rfio_open: uid %d gid %d umask %o ftype %d, mode %d, flags %d",
	 rfp->uid,rfp->gid,rfp->umask,rfp->ftype,mode,flags) ;
   TRACE(2,"rfio","rfio_open: account: %s",account) ;
   TRACE(2,"rfio","rfio_open: filename: %s",filename) ;
   if (reqhost != NULL && strlen(reqhost) )
      TRACE(2,"rfio","rfio_open: requestor's host: %s",reqhost) ;
   p= rfio_buf ;
   len= 5*WORDSIZE + 3*LONGSIZE + strlen(account) + strlen(filename) +strlen(pw->pw_name) + strlen(reqhost) + strlen(vmstr) + 5 ;
   marshall_WORD(p,B_RFIO_MAGIC) ;
   marshall_WORD(p,RQST_OPEN) ;
   marshall_LONG(p,len) ;
   p= rfio_buf + RQSTSIZE ;
   marshall_WORD(p,rfp->uid) ;
   marshall_WORD(p,rfp->gid) ;
   marshall_WORD(p,rfp->umask) ;
   marshall_WORD(p,FFTYPE_C) ;
   marshall_LONG(p,htonopnflg(flags)) ;
   marshall_LONG(p,mode) ;
   marshall_STRING(p,account) ;
   marshall_STRING(p,filename) ;
   marshall_STRING(p,pw->pw_name) ;
   marshall_STRING(p,reqhost) ;
   marshall_LONG(p,rfp->passwd);
   marshall_WORD(p,rfp->mapping);
   marshall_STRING(p, vmstr) ;
   TRACE(2,"rfio","rfio_open: sending %d bytes",RQSTSIZE+len) ;
   if (netwrite_timeout(rfp->s,rfio_buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      TRACE(2,"rfio","rfio_open: write(): ERROR occured (errno=%d)", errno) ;
      syslog(LOG_ALERT, "rfio: open: %s (error %d with %s) [uid=%d,gid=%d,pid=%d] in netwrite(%d,0X%X,%d)",
	     sys_errlist[errno], errno, rfp->host, rfp->uid, rfp->gid, getpid(), rfp->s, rfio_buf, RQSTSIZE+len);
      rfio_cleanup(rfp->s) ;
      END_TRACE() ;
      return -1 ;
   }
   /*
	 * Getting status and current offset.
	 */
   TRACE(1, "rfio", "rfio_open: reading %d bytes",rfp->_iobuf.hsize) ;
   if (netread_timeout(rfp->s,rfio_buf,rfp->_iobuf.hsize, RFIO_CTRL_TIMEOUT) != rfp->_iobuf.hsize ) {
      TRACE(2, "rfio", "rfio_open: read(): ERROR occured (errno=%d)", errno);
      syslog(LOG_ALERT, "rfio: open: %s (error %d with %s) [uid=%d,gid=%d,pid=%d] in netread(%d,0X%X,%d)",
	     sys_errlist[errno], errno, rfp->host, rfp->uid, rfp->gid, getpid(), rfp->s, rfio_buf, RQSTSIZE+len);
      rfio_cleanup(rfp->s);
      END_TRACE();
      return(-1);
   }
   p= rfio_buf ;
   unmarshall_WORD(p,req) ; 
   unmarshall_LONG(p,status) ;
   unmarshall_LONG(p, rcode) ; 
   TRACE(1,"rfio","rfio_open: return status(%d), rcode(%d), fd: %d",status,rcode,rfp->s) ;
   if ( status < 0 ) {
      if ( rcode >= SEBASEOFF)
	 serrno = rcode ;
      else
	 rfio_errno= rcode ;
      /* Operation failed but no error message was sent */
      if ( rcode == 0 )
	 serrno = SENORCODE ;
      rfio_cleanup(rfp->s) ;
      END_TRACE() ;
      return -1 ;
   }
   else {
      rfp->offset= status ;
   }

#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logop(rfp->s,filename,host,flags);
#endif
   /*
	 * The file is open, update rfp->fp
	 */
#if defined(hpux)
   rfp->fp.__fileL = rfp->s;
#else
#if defined(linux)
   rfp->fp._fileno = rfp->s;
#else
#if defined(__Lynx__)
   rfp->fp._fd = rfp->s;
#else
   rfp->fp._file = rfp->s;
#endif  /* __Lynx__ */
#endif  /* linux */
#endif  /* hpux */
#if defined(CRAY)
   rfp->fp._file50= rfp->s ;
#endif	/* CRAY	*/
   END_TRACE() ;
   return (rfp->s) ;
}

void rfio_setup(iop)
RFILE   *iop;
{
   (void)rfio_setup_ext(iop,0,0,0);
}

