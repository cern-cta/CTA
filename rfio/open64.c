/*
 * $Id: open64.c,v 1.7 2004/10/27 09:52:51 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: open64.c,v $ $Revision: 1.7 $ $Date: 2004/10/27 09:52:51 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, F. Hassine, P. Gaillardon";
#endif /* not lint */

/* open64.c       Remote File I/O - open file a file                      */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include <syslog.h>             /* system logger                        */
#include <errno.h>
#include <string.h>
#include "rfio.h"               /* remote file I/O definitions          */
#include "rfio_rfilefdt.h"
#include "rfcntl.h"             /* remote file control mapping macros   */
#if !defined(_WIN32)
#include <arpa/inet.h>          /* for inet_ntoa()                      */
#include <netinet/in.h>
#include <netinet/tcp.h>
#endif
#include <pwd.h>
#include <stdlib.h>
#include <Cpwd.h>

extern RFILE *rfilefdt[MAXRFD];        /* File descriptors tables       */

static void rfio_setup64_ext(iop,uid,gid,passwd)
RFILE   *iop;
int 	uid;
int 	gid;
int	passwd;
{
   extern char * getenv() ; 	/* External declaration		*/
   char     *cp ;               /* Character pointer		*/
   int      v ;
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

   iop->magic  = RFIO_MAGIC;
   iop->mode64 = 1;
   iop->s = -1;
   if (uid || gid)
      iop->mapping=0;
   else
      iop->mapping=1;
   iop->passwd=passwd; /* used only if mapping == 0 */
   iop->uid = (uid==0 ? geteuid(): uid);
   iop->gid = (gid==0 ? getegid(): gid);
   INIT_TRACE("RFIO_TRACE");
   TRACE ( 1,"rfio","rfio_setup64_ext(%d,%d,%d)",iop,uid,gid);
   TRACE ( 2,"rfio","rfio_setup64_ext: owner s uid is %d",iop->uid);
   TRACE ( 2,"rfio","rfio_setup64_ext: owner s gid is %d",iop->gid);
   END_TRACE();
   (void) umask(iop->umask=umask(0));
   iop->bufsize = 0;
   iop->ftype = FFTYPE_C;
   iop->binary = 0;                 /* no translation needed        */
   iop->eof = 0;
   iop->unit = 0;
   iop->access = 0;
   iop->format = 0;
   iop->recl = 0;
   iop->blank = 0;
   iop->opnopt = 0;
   iop->offset = 0 ; 
   iop->socset = 0 ; 
   iop->_iobuf.base = NULL;
   iop->_iobuf.ptr = NULL;
   iop->_iobuf.count = 0;
   iop->_iobuf.hsize = 0;
   iop->_iobuf.dsize = 0;
   iop->lseekhow= -1 ; 
   iop->ahead= rfioreadopt(RFIO_READOPT) & RFIO_READAHEAD ;
   iop->eof= 0 ; 
   iop->readissued = 0; 
   iop->preseek    = 0; 
   iop->nbrecord   = 0; 
   iop->version3   = 0; 
   iop->offset64   = 0;
   iop->lseekoff64 = 0;
   strcpy(iop->host,"????????");
}

int DLL_DECL rfio_open64(filepath, flags, mode)
char    *filepath ;
int     flags,mode ;
{
   int n;
   int n_index;
   int old;
   int fd;
   int fd_index;

   old = rfioreadopt(RFIO_READOPT);

   /* The decay of protocol 64 into 32 is assumed innerly */
   if ((old & RFIO_STREAM) == RFIO_STREAM)
      n = rfio_open64_v3(filepath,flags,mode);
   else 
      n = rfio_open64_v2(filepath,flags,mode);
   return(n);
} 
	
int 	rfio_open64_v2(filepath, flags, mode)
char    * filepath ;
int     flags,mode ;
{
   char rh[1] ;
   rh[0]='\0' ;
  
   return(rfio_open64_ext(filepath, flags, mode,(uid_t)0,(gid_t)0,0,rh));
} 

/*
 * Remote file open.
 */
int	rfio_open64_ext(filepath, flags, mode,uid,gid,passwd,reqhost) 
char    * filepath ;
int 	flags,mode ;
uid_t	uid;
gid_t	gid;
int 	passwd ;
char 	* reqhost; /* In case of a Non-mapped I/O with uid & gid 
		      sepcified, which host will be contacted
		      for key check ? */

{
   int     status ;	/* Return code 			*/
   int     rcode ;	/* Remote errno			*/
   int     len ;        /* Request length               */
   int     replen;      /* Reply   length               */
   char    * host ; 
   char    * filename;
   char    * account;
   char    * p ;	/* Pointer to rfio buffer	*/
   RFILE   * rfp ;	/* Remote file pointer          */
   int     rfp_index;
   WORD	   req ;        /* Request code                 */
   struct  passwd *pw;
   int     rt ; 	/* daemon in site(0) or not (1) */	
   int     bufsize ; 	/* socket buffer size 		*/	
   struct  sockaddr_in to;
#if defined(_AIX)
   socklen_t tolen;
#else
   int     tolen;
#endif
   struct  hostent *hp;
   extern  void rfio_setup_ext();
   extern char * getacct() ;
   extern char * getconfent() ;
   extern  char * getifnam() ;
   int     old,n,parserc;
   int     n_index;
   off64_t offset;      /* Open offset length           */
   char    rfio_buf[BUFSIZ];

   // Avoiding Valgrind error messages about uninitialized data
   memset(rfio_buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1,"rfio","rfio_open64_ext(%s, 0%o, 0%o, %d, %d, %d, %s)",
     filepath,flags,mode,uid,gid,passwd,reqhost ) ;

   /* the rtcopy program calls directly rfio_open_ext, so we (again) do this test here, ugly */
   old = rfioreadopt(RFIO_READOPT);

   /* V3 stream protocol for sequential transfers                  */
   /* The decay of protocol 64 into 32 is assumed innerly          */
   if (old == RFIO_STREAM) {
      n = rfio_open64_ext_v3(filepath,flags,mode,uid,gid,passwd,reqhost);
      return(n);
   }

   /* Version 2 behaviour starts here */	
#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logst();
#endif /* CLIENTLOG */

   /*
    * The file is local.
    */
   if ( ! (parserc = rfio_parse(filepath,&host,&filename)) ) {
      /* if not a remote file, must be local or HSM  */
      if ( host != NULL ) {
          /*
           * HSM file
           */
          TRACE(1,"rfio","rfio_open64_ext: %s is an HSM path",
                filename);
          END_TRACE();
          rfio_errno = 0;
          return(rfio_HsmIf_open(filename,flags,mode,1));
      }
      TRACE(1,"rfio","rfio_open64_ext: %s is a local path",
            filename);
      END_TRACE();
      status = open64(filename, flags, mode) ;
      if ( status < 0 ) serrno = 0;
      rfio_errno = 0;
      END_TRACE() ; 
#if defined (CLIENTLOG)
      /* Client logging */
      rfio_logop(status,filename,host,flags);
#endif /* CLIENTLOG */
      return status ;
   }
   if (parserc < 0) {
	   END_TRACE();
	   return(-1);
   }

   /*
    * Allocate and initialize a remote file descriptor.
    */
   if ((rfp = (RFILE *)malloc(sizeof(RFILE))) == NULL)        {
      TRACE(2, "rfio", "rfio_open64_ext: malloc(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return(-1);
   }
   rfio_setup64_ext(rfp,(int)uid,(int)gid,passwd) ;
   TRACE(2, "rfio", "rfio_open64_ext: RFIO descriptor allocated at 0x%X", rfp);
   /*
    * Connecting server.
    */
   rfp->s = rfio_connect(host,&rt);
   if (rfp->s < 0)      {
      TRACE(2, "rfio", "rfio_open64_ext: freeing RFIO descriptor at 0X%X", rfp);
      (void) free(rfp);
      END_TRACE();
      return(-1);
   }
   tolen=sizeof(to);
   if (getpeername(rfp->s,(struct sockaddr *)&to, &tolen)<0)        {
      syslog(LOG_ALERT, "rfio: open64: getpeername: %s\n",strerror(errno));
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
      TRACE(3,"rfio", "rfio_open64_ext: re-setting (uid,gid) to %d,%d",rfp->uid,rfp->gid) ;
      rfp->mapping = 1 ;
   }
		
   /*
    * Remote file table is not large enough.
    */
   if ((rfp_index = rfio_rfilefdt_allocentry(rfp->s)) == -1) {
      TRACE(2, "rfio", "rfio_open64_ext: freeing RFIO descriptor at 0X%X", rfp);
      (void) free(rfp);
      END_TRACE();
      errno= EMFILE ;
      return -1 ;
   }
   rfilefdt[rfp_index]=rfp;

   /* Set version3 to false since we are running version 2 here */
   rfilefdt[rfp_index]->version3 = 0;

   bufsize= DEFIOBUFSIZE ;
   if ((p = getconfent("RFIO", "IOBUFSIZE", 0)) != NULL)        {
      if ((bufsize = atoi(p)) <= 0)     {
	 bufsize = DEFIOBUFSIZE;
      }
   }
   else 
      /* reset error code */
      serrno=0;

   TRACE(2, "rfio", "rfio_open64_ext: setsockopt(SOL_SOCKET, SO_KEEPALIVE)");
   rcode = 1 ;
   if (setsockopt(rfp->s, SOL_SOCKET, SO_KEEPALIVE,(char *)&rcode, sizeof (int) ) == -1) {
      TRACE(2, "rfio" ,"rfio_open64_ext: setsockopt(SO_KEEPALIVE) failed");
      syslog(LOG_ALERT, "rfio: open64: setsockopt(SO_KEEPALIVE): %s", strerror(errno));
   }

   if ( (p = getenv("RFIO_TCP_NODELAY")) != NULL ) {
	   rcode = 1;
	   TRACE(2,"rfio","rfio_open: setsockopt(IPPROTO_TCP,TCP_NODELAY)");
	   if ( setsockopt(rfp->s,IPPROTO_TCP,TCP_NODELAY,(char *)&rcode,sizeof(rcode)) == -1 ) {
		   TRACE(2,"rfio","rfio_open: setsockopt(IPPROTO_TCP,TCP_NODELAY) failed");
	   }
   }

   /*
    * Allocate, if necessary, an I/O buffer.
    */
   rfp->_iobuf.hsize= 3*LONGSIZE + WORDSIZE;
   if ( rfioreadopt(RFIO_READOPT) & RFIO_READBUF ) {

      rfp->_iobuf.dsize = bufsize - rfp->_iobuf.hsize;
      if ((rfp->_iobuf.base = malloc(bufsize)) == NULL)  {
         rfio_cleanup(rfp->s);
         END_TRACE();
         return -1 ; 
      }
      TRACE(2, "rfio", "rfio_open64_ext: I/O buffer allocated at 0x%X", rfp->_iobuf.base) ;
      rfp->_iobuf.count = 0;
      rfp->_iobuf.ptr = iodata(rfp) ;
   }

   if ( (pw = Cgetpwuid(geteuid()) ) == NULL ) {
      TRACE(2, "rfio" ,"rfio_open64_ext: Cgetpwuid() error %s",strerror(errno));
      rfio_cleanup(rfp->s);
      END_TRACE();
      return -1 ;
   }
   /*
    * Building and sending request.
    */
   /* if ((account = getacct()) == NULL) */ account = "";
   TRACE(2,"rfio","rfio_open64_ext: uid %d gid %d umask %o ftype %d, mode 0%o, flags 0%o",
      rfp->uid,rfp->gid,rfp->umask,rfp->ftype,mode,flags) ;
   TRACE(2,"rfio","rfio_open64_ext: account: %s",account) ;
   TRACE(2,"rfio","rfio_open64_ext: filename: %s",filename) ;
   if (reqhost != NULL && strlen(reqhost) )
      TRACE(2,"rfio","rfio_open64_ext: requestor's host: %s",reqhost) ;
   p= rfio_buf ;
   len= 5*WORDSIZE + 3*LONGSIZE + strlen(account) + strlen(filename) +strlen(pw->pw_name) + strlen(reqhost) + 4 ;
   marshall_WORD(p,B_RFIO_MAGIC) ;
   marshall_WORD(p,RQST_OPEN64) ;
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
   errno = 0;
   TRACE(2,"rfio","rfio_open64_ext: sending %d bytes",RQSTSIZE+len) ;
   if (netwrite_timeout(rfp->s,rfio_buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      TRACE(2,"rfio","rfio_open64_ext: write(): ERROR occured (errno=%d)", errno) ;
      syslog(LOG_ALERT, "rfio: open64: %s (error %d with %s) [uid=%d,gid=%d,pid=%d] in netwrite(%d,0X%X,%d)",
	     strerror(errno), errno, rfp->host, rfp->uid, rfp->gid, getpid(), rfp->s, rfio_buf, RQSTSIZE+len);
      rfio_cleanup(rfp->s) ;
      END_TRACE() ;
      return -1 ;
   }
   /*
    * Getting status and current offset.
    */
   replen = rfp->_iobuf.hsize + HYPERSIZE;
   TRACE(1, "rfio", "rfio_open64_ext: reading %d bytes",rfp->_iobuf.hsize) ;
   if (netread_timeout(rfp->s, rfio_buf, replen, RFIO_CTRL_TIMEOUT) != replen ) {
      TRACE(2, "rfio", "rfio_open64_ext: read(): ERROR %d occured (errno=%d, serrno=%d)",
        replen, errno, serrno);
#if !defined(_WIN32)
      if ( serrno == SECONNDROP || errno == ECONNRESET ) {
#else
      if ( serrno == SECONNDROP || serrno == SETIMEDOUT ) {
#endif
         rfio_cleanup(rfp->s);
         /* Server doesn't support 64 mode, call 32 one*/
         n = rfio_open_ext(filepath,flags,mode,uid,gid,passwd,reqhost,"");
	 return(n);
      }
      else {
         syslog(LOG_ALERT,
            "rfio: open64: %s (error %d with %s) [uid=%d,gid=%d,pid=%d] in netread(%d,0X%X,%d)",
            strerror(errno), errno, rfp->host, rfp->uid, rfp->gid, getpid(), rfp->s, rfio_buf, replen);
         rfio_cleanup(rfp->s);
         return(-1);
      }
   }
   p= rfio_buf ;
   unmarshall_WORD(p,req) ; 
   unmarshall_LONG(p,status) ;
   unmarshall_LONG(p, rcode) ;
   p += LONGSIZE;
   unmarshall_HYPER(p, offset) ; 
   TRACE(1,"rfio","rfio_open64_ext: return status(%d), rcode(%d), fd: %d",status,rcode,rfp->s) ;
   if ( status < 0 ) {
      if ( rcode >= SEBASEOFF)
	 serrno = rcode ;
      else
	 rfio_errno= rcode ;
      /* Operation failed but no error message was sent */
      if ( rcode == 0 )
	 serrno = SENORCODE ;
      rfio_cleanup(rfp->s) ;
      if (serrno == SEPROTONOTSUP) {
         /* Server doesn't support 64 mode, call 32 one*/
         n = rfio_open_ext(filepath,flags,mode,uid,gid,passwd,reqhost,"");
	 return(n);
      }
      return -1 ;
   }
   else {
      rfp->offset64= offset ;
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
   END_TRACE() ;
   return (rfp->s) ;
}

void rfio_setup64(iop)
RFILE   *iop;
{
   (void)rfio_setup64_ext(iop,0,0,0);
}

