/*
 * $Id: stream64.c,v 1.5 2004/10/27 09:52:51 sponcec3 Exp $
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stream64.c,v $ $Revision: 1.5 $ $Date: 2004/10/27 09:52:51 $ CERN/IT/PDP/DM F. Hemmer, A. Trannoy, F. Hassine, P. Gaillardon";
#endif /* not lint */

/* stream64.c       Remote File I/O - Version 3 streaming routines        */

#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include <errno.h>
#include <string.h>
#include <syslog.h>             /* system logger                        */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "rfio.h"               /* remote file I/O definitions          */
#include "rfio_rfilefdt.h"
#include "rfcntl.h"             /* remote file control mapping macros   */
#include "u64subr.h"
#include <pwd.h>
#include <signal.h>
#include "Castor_limits.h"
#include <Cpwd.h>
#include <Cnetdb.h>

#if !defined(_WIN32)
#include <sys/time.h>
#endif
#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>
#endif
#include <stdlib.h>

#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#include <arpa/inet.h>          /* for inet_ntoa()                      */
#if ((defined(IRIX5) || defined(IRIX6)) && ! (defined(LITTLE_ENDIAN) && defined(BIG_ENDIAN) && defined(PDP_ENDIAN)))
#ifdef LITTLE_ENDIAN
#undef LITTLE_ENDIAN
#endif
#define	LITTLE_ENDIAN	1234
#ifdef BIG_ENDIAN
#undef BIG_ENDIAN
#endif
#define	BIG_ENDIAN	4321
#ifdef PDP_ENDIAN
#undef PDP_ENDIAN
#endif
#define	PDP_ENDIAN	3412
#endif
#include <netinet/tcp.h>
#endif

EXTERN_C int DLL_DECL data_rfio_connect _PROTO((char *, int *, int, int));

static void rfio_setup64_ext_v3(iop,uid,gid,passwd)
RFILE   *iop;
int 	uid;
int 	gid;
int	passwd;
{
   extern char * getenv() ; 	/* External declaration		*/
   char * cp ; 			/* Character pointer		*/
   int v ;

   if ( cp= getenv("RFIO_READOPT") ) {
      v = atoi(cp) ;
      rfiosetopt(RFIO_READOPT, &v , 4) ; 
   }

   iop->magic = RFIO_MAGIC;
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
   iop->ftype = FFTYPE_C;
   iop->binary = 0;                 /* no translation needed        */
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
   iop->version3   = 1; 
   iop->offset64   = 0;
   iop->lseekoff64 = 0;
   strcpy(iop->host,"????????");
}

int DLL_DECL rfio_open64_v3(filepath, flags, mode)
char    * filepath ;
int     flags,mode ;
{
   char rh[1] ;
   rh[0]='\0' ;

   return(rfio_open64_ext_v3(filepath, flags, mode,(uid_t)0,(gid_t)0,0,rh));
}


/*
 * Remote file open.
 */
int	rfio_open64_ext_v3(filepath, flags, mode,uid,gid,passwd,reqhost) 
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
   int	    rcode ;     /* Remote errno			*/
   int        len ;     /* Request length               */
   int       rlen ;     /* Reply   length               */
   int        save_errno, save_serrno;
   char    * host ;     /* Host name                    */
   char * filename;     /* File name                    */
   char  * account;     /* VM vestiges ?                */
   char       * p ;	/* Pointer to rfio buffer	*/
   RFILE    * rfp ;	/* Remote file pointer          */
   WORD	      req ;     /* Request code                 */
   struct passwd *pw;   /* User identification          */
   int         rt ;     /* daemon in site(0) or not (1) */	
   int    bufsize ; 	/* socket buffer size 		*/	
   struct sockaddr_in      to;
   int       tolen;
   struct  hostent *hp; /* Get the system name          */
   extern void rfio_setup64_ext_v3();
   extern char * getacct() ;
   extern char * getifnam() ;
   int   data_port;
   int         rem;
   int          rb;
   int         yes;     /* Int for socket option        */
   off64_t offset ;     /* Offset at open               */
   char  rfio_buf[BUFSIZ];
   int   rfp_index, parserc;
   char  tmpbuf[21];

   // Avoiding Valgrind error messages about uninitialized data
   memset(rfio_buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1,"rfio","rfio_open64_ext(%s, 0%o, 0%o, %d, %d, %d, %s)",
      filepath,flags,mode,uid,gid,passwd,reqhost) ;

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
      status= open64(filename, flags, mode) ;
      END_TRACE() ;
      rfio_errno = 0;
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
      save_errno = errno;
      TRACE(2, "rfio", "rfio_open64: malloc(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      errno = save_errno;
      return(-1);
   }
   rfio_setup64_ext_v3(rfp,(int)uid,(int)gid,passwd) ;
   TRACE(2, "rfio", "rfio_open64_ext_v3: RFIO descriptor allocated at 0x%X", rfp);
   /*
    * Connecting server.
    */
   rfp->s = rfio_connect(host,&rt);
   if (rfp->s < 0)      {
      save_errno = errno;
      TRACE(2, "rfio", "rfio_open64_ext_v3: Failing Doing first connect");

      TRACE(2, "rfio", "rfio_open64_ext_v3: freeing RFIO descriptor at 0X%X", rfp);
      (void) free(rfp);
      END_TRACE();
      errno = save_errno;
      return(-1);
   }
   tolen=sizeof(to);
   if (getpeername(rfp->s,(struct sockaddr *)&to, &tolen)<0)        {
      syslog(LOG_ALERT, "rfio: rfio_open64_ext_v3: getpeername: %s\n",strerror(errno));
   }
   hp = Cgethostbyaddr((char *) (&to.sin_addr), sizeof(struct in_addr), to.sin_family);
   if (hp == NULL){
      strncpy(rfp->host, (char *)inet_ntoa(to.sin_addr), RESHOSTNAMELEN );
   }
   else    {
      strncpy(rfp->host,hp->h_name, RESHOSTNAMELEN );
   }


   if ( !rt && !rfp->mapping ) {
      rfp->uid=geteuid() ;
      rfp->gid=getegid() ;
      TRACE(3,"rfio", "rfio_open64_ext_v3: re-setting (uid,gid) to %d,%d",rfp->uid,rfp->gid) ;
      rfp->mapping = 1 ;
   }
		
   /*
    * Remote file table is not large enough.
    */
   if ((rfp_index = rfio_rfilefdt_allocentry(rfp->s)) == -1) {
      TRACE(2, "rfio", "rfio_open64_ext_v3: freeing RFIO descriptor at 0X%X", rfp);
      (void) free(rfp);
      END_TRACE();
      errno= EMFILE ;
      return -1 ;
   }
   rfilefdt[rfp_index]=rfp;
   bufsize= DEFIOBUFSIZE ;

   /* reset error code */
   serrno=0;

   /* Initialization needed for RFIO version 3 write and read requests */
   rfp->first_write  = 1;
   rfp->wrbyte_net64 = 0;
   rfp->first_read   = 1;
   rfp->rdbyte_net64 = 0;

   /* Set the keepalive option on socket */
   yes = 1;
   if (setsockopt(rfp->s,SOL_SOCKET,SO_KEEPALIVE,(char *)&yes, sizeof(yes)) < 0) {
      TRACE(2,"rfio","rfio_open64_ext_v3: setsockopt keepalive on ctrl: %s",strerror(errno));
   }
   TRACE(2,"rfio","rfio_open64_ext_v3: setsockopt keepalive on ctrl done");

#if (defined(__osf__) && defined(__alpha) && defined(DUXV4))    	    
   /* Set the keepalive interval to 20 mns instead of the default 2 hours */
   yes = 20 * 60;
   if (setsockopt(rfp->s,IPPROTO_TCP,TCP_KEEPIDLE,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","rfio_open64_ext_v3: setsockopt keepidle on ctrl: %s",strerror(errno));
   }
   TRACE(2,"rfio","rfio_open64_ext_v3: setsockopt keepidle on ctrl done (%d s)",yes);
#endif
	    
#if !(defined(__osf__) && defined(__alpha) && defined(DUXV4))
   yes = 1;
   if (setsockopt(rfp->s,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","rfio_open64_ext_v3: setsockopt nodelay on ctrl: %s",strerror(errno));
   }
   TRACE(2,"rfio","rfio_open64_ext_v3: setsockopt nodelay option set on ctrl socket");
#endif /* !(defined(__osf__) && defined(__alpha) && defined(DUXV4)) */

   /*
    * Allocate, if necessary, an I/O buffer.
    */
   rfp->_iobuf.hsize= 3*LONGSIZE + WORDSIZE ;
   if ( rfioreadopt(RFIO_READOPT) & RFIO_READBUF ) {
      rfp->_iobuf.dsize = bufsize - rfp->_iobuf.hsize;
      rfp->_iobuf.count = 0;
      rfp->_iobuf.ptr = iodata(rfp) ;
   }

   if ( (pw = Cgetpwuid(geteuid()) ) == NULL ) {
      save_errno = errno; save_serrno = serrno;
      TRACE(2, "rfio" ,"rfio_open64_ext_v3: Cgetpwuid() error %s",strerror(errno));
      rfio_cleanup_v3(rfp->s);
      END_TRACE();
      serrno = save_serrno; errno = save_errno;
      return -1 ;
   }
   /*
	 * Building and sending request.
	 */
   /* if ((account = getacct()) == NULL) */ account = "";
   TRACE(2,"rfio","rfio_open64_ext_v3: uid %d gid %d umask %o ftype %d, mode 0%o, flags 0%o",
	 rfp->uid,rfp->gid,rfp->umask,rfp->ftype,mode,flags) ;
   TRACE(2,"rfio","rfio_open64_ext_v3: account: %s",account) ;
   TRACE(2,"rfio","rfio_open64_ext_v3: filename: %s",filename) ;
   if (reqhost != NULL && strlen(reqhost) )
      TRACE(2,"rfio","rfio_open64_ext_v3: requestor's host: %s",reqhost) ;
   p= rfio_buf ;
   len= 5*WORDSIZE + 3*LONGSIZE + strlen(account) + strlen(filename) + strlen(pw->pw_name) +
      strlen(reqhost) + 4 ;
   marshall_WORD(p,RFIO_MAGIC) ;
   marshall_WORD(p,RQST_OPEN64_V3) ;
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
   TRACE(2,"rfio","rfio_open64_v3: sending %d bytes",RQSTSIZE+len) ;
   save_serrno = serrno; serrno = 0; save_errno = errno; errno = 0;
   if (netwrite_timeout(rfp->s,rfio_buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      save_errno = errno; save_serrno = serrno;
      TRACE(2,"rfio","rfio_open64_v3: write(): ERROR occured (errno=%d)", errno) ;
      syslog(LOG_ALERT,
         "rfio: open64_v3: %s (error %d , serrno %d with %s) [uid=%d,gid=%d,pid=%d] in netwrite(%d,0X%X,%d)",
         strerror(errno > 0 ? errno : serrno), errno, serrno, 
         rfp->host, rfp->uid, rfp->gid, getpid(), rfp->s, rfio_buf, 
         RQSTSIZE+len);
      rfio_cleanup_v3(rfp->s) ;
      END_TRACE() ;
      serrno = save_serrno; errno = save_errno;
      return -1 ;
   }
   serrno = save_serrno; errno = save_errno;

   /*
    * Getting status and current offset.
    */
   TRACE(1, "rfio", "rfio_open64_v3: reading %d bytes",rfp->_iobuf.hsize) ;
   save_serrno = serrno; serrno = 0; save_errno = errno; errno = 0; 
   /*
    * We need to increase the timeout on this netread() because if we
    * miss this message the server will hang forever (it will be stuck
    * in an accept() on the data socket). A better solution on the
    * server side is needed to more properly cope with this problem.
    */
   rlen = RQSTSIZE+HYPERSIZE;
   if ((rb = netread_timeout(rfp->s,rfio_buf,rlen,10*RFIO_CTRL_TIMEOUT)) != rlen ) {
#if !defined(_WIN32)
      if (rb == 0 || (rb < 0 && errno == ECONNRESET)) {
#else
      if (rb == 0 || (rb < 0 && serrno == SETIMEDOUT)) {
#endif
         /* Server doesn't support 64 mode, call 32 one*/
	 TRACE(2, "rfio", "rfio_open64_v3: Server doesn't support V3 64bits protocol, call V3 one");
	 rfio_cleanup_v3(rfp->s);
         errno  = 0;
         serrno = 0;
         status = rfio_open_ext_v3(filepath, flags, mode, uid, gid, passwd, reqhost, "");
	 END_TRACE();
	 return(status);
      }
      else {
         save_errno = errno; save_serrno = serrno;
	 TRACE(2, "rfio", "rfio_open64_v3: read(): ERROR %d occured (errno=%d, serrno=%d)",
            rb, errno, serrno);
	 syslog(LOG_ALERT,
            "rfio: open64_v3: %s (error %d, serrno %d with %s) [uid=%d,gid=%d,pid=%d] in netread(%d,0X%X,%d)",
            strerror(errno > 0 ? errno : serrno), errno, serrno, 
            rfp->host, rfp->uid, rfp->gid, getpid(), rfp->s, rfio_buf, 
            rlen);
	 rfio_cleanup_v3(rfp->s);
	 END_TRACE();
         serrno = save_serrno; errno = save_errno;
	 return(-1);
      }
   }
   serrno = save_serrno; errno = save_errno;
   p= rfio_buf ;
   unmarshall_WORD(p,req) ; 
   unmarshall_LONG(p,status) ;
   unmarshall_LONG(p, rcode) ; 
   unmarshall_LONG(p, data_port);
   unmarshall_HYPER(p, offset);
   TRACE(1,"rfio","rfio_open64_v3: return status(%d), rcode(%d), offset %s fd: %d",
      status, rcode, u64tostr(offset,tmpbuf,0), rfp->s) ;

   /* will have to check if server doesn't support OPEN_V3 */
   if ( status < 0 ) {
      if ( rcode >= SEBASEOFF)
	 serrno = rcode ;
      else
	 rfio_errno= rcode ;
      rfio_cleanup_v3(rfp->s) ;
      /* Operation failed but no error message was sent */
      if ( rcode == 0 )
	 serrno = SENORCODE ;
      if (serrno == SEPROTONOTSUP) {
         /* Server doesn't support 64 mode, call 32 one*/
         int   n;
         
         n = rfio_open_ext(filepath,flags,mode,uid,gid,passwd,reqhost,"");
         END_TRACE() ;
	 return(n);
      }
      END_TRACE() ;
      return -1 ;
   }
   else {
      rfp->offset64= offset ;
   }

   /* lseekhow field contains the fd of the data socket */
   /* Add error checking here ! (-1)*/
   rfp->lseekhow = data_rfio_connect(host,&rem,data_port,flags);

   /* In case of network errors, abort */
   if (rfp->lseekhow == -1) {
      save_errno = errno; save_serrno = serrno;
      rfio_cleanup_v3(rfp->s) ;
      END_TRACE() ;
      serrno = save_serrno; errno = save_errno;
      return -1 ;
   }

   /* Set the keepalive option on both sockets */
   yes = 1;
   if (setsockopt(rfp->lseekhow,SOL_SOCKET,SO_KEEPALIVE,(char *)&yes, sizeof(yes)) < 0) {
      TRACE(2,"rfio","open64_v3: setsockopt keepalive on data: %s",strerror(errno));
   }
   TRACE(2,"rfio","open64_v3: setsockopt keepalive on data done");

#if (defined(__osf__) && defined(__alpha) && defined(DUXV4))    	    	    
   /* Set the keepalive interval to 20 mns instead of the default 2 hours */
   yes = 20 * 60;
   if (setsockopt(rfp->lseekhow,IPPROTO_TCP,TCP_KEEPIDLE,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","open64_v3: setsockopt keepidle on data: %s",strerror(errno));
   }
   TRACE(2,"rfio","open64_v3: setsockopt keepidle on data done (%d s)",yes);
#endif	    
#if !(defined(__osf__) && defined(__alpha) && defined(DUXV4))
   yes = 1;
   if (setsockopt(rfp->lseekhow,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","open64_v3: setsockopt nodelay on data: %s",strerror(errno));
   }
   TRACE(2,"rfio","open64_v3: setsockopt nodelay option set on data socket");
#endif /* !(defined(__osf__) && defined(__alpha) && defined(DUXV4)) */
	    
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

void rfio_setup64_v3(iop)
RFILE   *iop;
{
   (void)rfio_setup64_ext_v3(iop,0,0,0);
}

/*
 * Remote file read
 */
int DLL_DECL rfio_read64_v3(ctrl_sock, ptr, size) 
char    *ptr;
int     ctrl_sock, size;
{
   int status ;	/* Return code of called func	*/
   int HsmType, save_errno;
   char   * p ; 	/* Pointer to buffer		*/
   fd_set fdvar;
   struct timeval t;
   int req;
   char *iobuffer; 
   int byte_in_buffer;
   int n;
   char rqstbuf[BUFSIZ];
   char     rfio_buf[BUFSIZ];
   int ctrl_sock_index;
   char      tmpbuf[21];

   // Avoiding Valgrind error messages about uninitialized data
   memset(rfio_buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_read64_v3(%d, %x, %d)", ctrl_sock, ptr, size) ;
   if (size == 0) {
      END_TRACE();
      return(0);
   }

#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logrd(ctrl_sock,size);
#endif

   /*
    * Check HSM type. The CASTOR HSM uses normal RFIO (local or remote)
    * to perform the I/O. Thus we don't call rfio_HsmIf_read().
    */
   HsmType = rfio_HsmIf_GetHsmType(ctrl_sock,NULL);
   if ( HsmType > 0 ) {
       if ( HsmType != RFIO_HSM_CNS ) {
           status = rfio_HsmIf_read(ctrl_sock,ptr,size);
           if ( status == -1 ) {
               save_errno = errno;
               rfio_HsmIf_IOError(ctrl_sock,errno);
               errno = save_errno;
           }
           END_TRACE();
           return(status);
       }
   }

   /*
    * The file is local.
    */
   if ((ctrl_sock_index = rfio_rfilefdt_findentry(ctrl_sock,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_read64_v3: using local read(%d, %x, %d)", ctrl_sock, ptr, size);
      status = read(ctrl_sock, ptr, size);
      if ( HsmType == RFIO_HSM_CNS ) {
          save_errno = errno;
          rfio_HsmIf_IOError(ctrl_sock,errno);
          errno = save_errno;
      }
      END_TRACE();
      rfio_errno = 0;
      return(status);
   }

   /*
    * Checking magic number.
    */
   if (rfilefdt[ctrl_sock_index]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ; 
      rfio_rfilefdt_freeentry(ctrl_sock_index);
      (void) close(ctrl_sock) ;
      END_TRACE();
      return(-1);
   }

   /*
    * Checking mode 64.
    */
   if (!rfilefdt[ctrl_sock_index]->mode64) {
      status = rfio_read_v3(ctrl_sock, ptr, size);
      END_TRACE();
      return(status);
   }

   if (rfilefdt[ctrl_sock_index]->first_read)
   {
      rfilefdt[ctrl_sock_index]->first_read = 0;
      rfilefdt[ctrl_sock_index]->eof_received = 0;
      /*
       * Sending request using control socket.
       */
      p = rfio_buf;
      marshall_WORD(p,RFIO_MAGIC);
      marshall_WORD(p,RQST_READ64_V3);
	    
      TRACE(2, "rfio", "rfio_read64_v3: sending %d bytes",RQSTSIZE) ;
      if (netwrite_timeout(ctrl_sock, rfio_buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
	 TRACE(2,"rfio","rfio_read64_v3: write(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ;
      }

      TRACE(2,"rfio", "rfio_read64_v3: reading %d bytes",RQSTSIZE) ;
      if ((n = netread_timeout(ctrl_sock,rqstbuf,RQSTSIZE,RFIO_CTRL_TIMEOUT)) != RQSTSIZE) {
	 if (n == 0)
	    TRACE(2, "rfio","read64_v3 ctrl socket: read(): %s\n", sstrerror(serrno));
	 else
	    TRACE(2, "rfio","read64_v3 ctrl socket: read(): %s\n", strerror(errno));
	 END_TRACE();
	 return -1 ;
      }
      p = rqstbuf;
      unmarshall_WORD(p,req) ;			
      unmarshall_HYPER(p,rfilefdt[ctrl_sock_index]->filesize64) ;
      TRACE(2,"rfio", "rfio_read64_v3: filesize is %s bytes",
         u64tostr(rfilefdt[ctrl_sock_index]->filesize64,tmpbuf,0)) ;
   }


   iobuffer = ptr;
   byte_in_buffer = 0;

   while (1)
   {
      /* EOF received previously but not all data read */
      if (rfilefdt[ctrl_sock_index]->eof_received &&
	 rfilefdt[ctrl_sock_index]->filesize64 == rfilefdt[ctrl_sock_index]->rdbyte_net64) {
	 /* End of file and all data has been read by the client */
	 TRACE(2,"rfio","rfio_read64_v3: request satisfied eof encountered (read returns %d)",byte_in_buffer);
         END_TRACE();
	 return(byte_in_buffer);
      }

      FD_ZERO(&fdvar);
      /* Bother of ctrl socket till eof has not been received */
      if (!rfilefdt[ctrl_sock_index]->eof_received)
         FD_SET(ctrl_sock,&fdvar);
      FD_SET(rfilefdt[ctrl_sock_index]->lseekhow,&fdvar);
      t.tv_sec = 30;
      t.tv_usec = 0;

      TRACE(2,"rfio","read64_v3: doing select") ;
      if (select(FD_SETSIZE,&fdvar,NULL,NULL,&t) < 0)
      {
         TRACE(2,"rfio","read64_v3: select failed (errno=%d)",errno) ;
         END_TRACE() ;
         return -1 ;
      }
         
      if (FD_ISSET(ctrl_sock,&fdvar)) {
	 int  cause,rcode;
	 int  n;
	 char rqstbuf[BUFSIZ];
	         
	 /* Something received on the control socket */
	 TRACE(2,"rfio", "read64_v3: ctrl socket: reading %d bytes", RQSTSIZE) ;
         n = netread_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
	 if (n != RQSTSIZE) {
	    if (n == 0)
	    {
	       TRACE(2, "rfio","read64_v3: read ctrl socket: close received");
	       END_TRACE() ;
               return(-1);
	    }
	    else
	    {
	       TRACE(2, "rfio","read64_v3: read ctrl socket: read(): %s", strerror(errno));
	       END_TRACE() ;
	       return -1 ;
	    }
	 }
         p = rqstbuf;
         unmarshall_WORD(p,cause) ;                  
         unmarshall_LONG(p,status) ;
         unmarshall_LONG(p,rcode) ;
         if (cause == REP_ERROR)
         {
            TRACE(2,"rfio", "read64_v3: reply error status %d, rcode %d", status, rcode) ;
            rfio_errno = rcode;
   
            TRACE(2,"rfio","read64_v3: sending ack for error") ;
            n = netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
            if (n != RQSTSIZE) {
               TRACE(2,"rfio","read64_v3: write(): ERROR occured (errno=%d)",errno) ;
               END_TRACE() ;
               return -1 ;
            }
   
            if ( HsmType == RFIO_HSM_CNS )
               rfio_HsmIf_IOError(ctrl_sock,(rfio_errno > 0 ? rfio_errno : serrno));
            END_TRACE();
            return(-1);
         }
         if (cause == REP_EOF)
         {
            rfilefdt[ctrl_sock_index]->eof_received = 1;
            TRACE(2,"rfio", "read64_v3: eof received") ;
            if (rfilefdt[ctrl_sock_index]->filesize64 == rfilefdt[ctrl_sock_index]->rdbyte_net64) {
	       /* End of file and all data has been read by the client */
	       TRACE(2,"rfio","rfio_read64_v3: request satisfied eof encountered (read returns %d)",
                  byte_in_buffer);
	       END_TRACE();
	       return(byte_in_buffer);
            }
         }
         
      }

      if (FD_ISSET(rfilefdt[ctrl_sock_index]->lseekhow,&fdvar))
      {
         /* Receiving data using data socket */
         /* Do not use read here because NT doesn't support that with socket fds */
         n = s_nrecv(rfilefdt[ctrl_sock_index]->lseekhow, iobuffer, size-byte_in_buffer);
         if (n <= 0) {
            if (n == 0)
            {
               TRACE(2,"rfio","read64_v3: datasoket %d  closed by remote end",
                  rfilefdt[ctrl_sock_index]->lseekhow) ;
               END_TRACE() ;
               return(-1);
            }
            else
            {
               TRACE(2,"rfio","read64_v3: datasoket %d  read(): ERROR occured (errno=%d)",
                  rfilefdt[ctrl_sock_index]->lseekhow, errno) ;
               END_TRACE() ;
               return -1 ;
            }
         }
         byte_in_buffer += n;
         rfilefdt[ctrl_sock_index]->rdbyte_net64 += n;
         iobuffer += n;

         TRACE(2,"rfio","read64_v3: receiving datasocket=%d bytes,buffer=%d,req=%d",
            n, byte_in_buffer, size) ;

         if (byte_in_buffer == size)
         {
            TRACE(2,"rfio","read64_v3: request satisfied completely"); ;
            END_TRACE();
            return(size);
         }

      }  /* End of FD_ISSET(rfilefdt[ctrl_sock_index]->lseekhow,&fdvar) */
   }
}



/*
 * Remote file write
 */
int DLL_DECL rfio_write64_v3(ctrl_sock, ptr, size) 
char    *ptr;
int     ctrl_sock, size;
{
   int status ;	/* Return code of called func	*/
   int HsmType, save_errno, written_to;
   char   * p ; 	/* Pointer to buffer		*/
   fd_set fdvar;
   struct timeval t;
   char     rfio_buf[BUFSIZ];
   int ctrl_sock_index;

   // Avoiding Valgrind error messages about uninitialized data
   memset(rfio_buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_write64_v3(%d, %x, %d)", ctrl_sock, ptr, size) ;
   if (size == 0) {
      END_TRACE();
      return(size);
   }

#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logwr(ctrl_sock,size);
#endif

   /*
    * Check HSM type and if file has been written to. The CASTOR HSM
    * uses normal RFIO (local or remote) to perform the I/O. Thus we
    * don't call rfio_HsmIf_write().
    */
   HsmType = rfio_HsmIf_GetHsmType(ctrl_sock,&written_to);
   if ( HsmType > 0 ) {
       if ( written_to == 0 && (status = rfio_HsmIf_FirstWrite(ctrl_sock,ptr,size)) < 0 ) {
           END_TRACE();
           return(status);
       }
       if ( HsmType != RFIO_HSM_CNS ) {
           status = rfio_HsmIf_write(ctrl_sock,ptr,size);
           if ( status == -1 ) rfio_HsmIf_IOError(ctrl_sock,errno);
           END_TRACE();
           return(status);
       }
   }

   /*
    * The file is local.
    */
   if ((ctrl_sock_index = rfio_rfilefdt_findentry(ctrl_sock,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_write64_v3: using local write(%d, %x, %d)", ctrl_sock, ptr, size);
      status = write(ctrl_sock, ptr, size);
      if ( HsmType == RFIO_HSM_CNS ) {
          save_errno = errno;
          rfio_HsmIf_IOError(ctrl_sock,errno);
          errno = save_errno;
      }

      END_TRACE();
      rfio_errno = 0;
      return(status);
   }

   /*
    * Checking magic number.
    */
   if (rfilefdt[ctrl_sock_index]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ; 
      rfio_rfilefdt_freeentry(ctrl_sock_index);
      (void) close(ctrl_sock) ;
      END_TRACE();
      return(-1);
   }

   /*
    * Checking mode 64.
    */
   if (!rfilefdt[ctrl_sock_index]->mode64) {
      status = rfio_write_v3(ctrl_sock, ptr, size);
      END_TRACE();
      return(status);
   }

   if (rfilefdt[ctrl_sock_index]->first_write)
   {
      rfilefdt[ctrl_sock_index]->first_write = 0;

      /*
       * Sending request using control socket.
       */
      p = rfio_buf;
      marshall_WORD(p,RFIO_MAGIC);
      marshall_WORD(p,RQST_WRITE64_V3);
	    
      TRACE(2, "rfio", "rfio_write64_v3: sending %d bytes",RQSTSIZE) ;
      if (netwrite_timeout(ctrl_sock, rfio_buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
	 TRACE(2,"rfio","rfio_write64_v3: write(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ;
      }

   }
   FD_ZERO(&fdvar);
   FD_SET(ctrl_sock,&fdvar);

   t.tv_sec = 0; 
   t.tv_usec = 0;

   TRACE(2,"rfio","write64_v3: doing select");
   /* Immediate return here to send data as fast as possible */
   if (select(FD_SETSIZE,&fdvar,NULL,NULL,&t) < 0)
   {
      TRACE(2,"rfio","write64_v3: select failed (errno=%d)",errno) ;
      END_TRACE() ;
      return -1 ;
   }
	    
   if (FD_ISSET(ctrl_sock,&fdvar))
   {
      int cause,rcode;
      int n;
      char rqstbuf[BUFSIZ];

      // Avoiding Valgrind error messages about uninitialized data
      memset(rqstbuf, 0, BUFSIZ);

      /* Something received on the control socket */
      TRACE(2,"rfio", "write64_v3: ctrl socket: reading %d bytes",RQSTSIZE) ;
      if ((n = netread_timeout(ctrl_sock,rqstbuf,RQSTSIZE,RFIO_CTRL_TIMEOUT)) != RQSTSIZE) {
	 if (n == 0)
	    TRACE(2, "rfio","write64_v3: read ctrl socket: read(): %s\n", sstrerror(serrno));
	 else
	    TRACE(2, "rfio","write64_v3: read ctrl socket: read(): %s\n", strerror(errno));
	 END_TRACE();
	 return -1 ;
      }
      p = rqstbuf;
      unmarshall_WORD(p,cause) ;			
      unmarshall_LONG(p,status) ;
      unmarshall_LONG(p,rcode) ;
      if (cause == REP_ERROR)
	 TRACE(2,"rfio", "write64_v3: reply error status %d, rcode %d", status, rcode) ;
      else
	 TRACE(2,"rfio", "write64_v3: unknown error status %d, rcode %d", status, rcode) ;
      rfio_errno = rcode;

      TRACE(2,"rfio","write64_v3: sending ack for error") ;
      if (netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
	 TRACE(2,"rfio","write64_v3: write(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ;
      }
      if ( status < 0 ) rfio_HsmIf_IOError(ctrl_sock,rfio_errno);

      END_TRACE();
      return(-1);
   }

   /* Sending data using data socket */
   TRACE(2,"rfio","write64_v3: sending %d bytes to datasocket filedesc=%d",
      size,rfilefdt[ctrl_sock_index]->lseekhow) ;
   if (netwrite_timeout(rfilefdt[ctrl_sock_index]->lseekhow, ptr, size, RFIO_DATA_TIMEOUT) != size) {
      TRACE(2,"rfio","write64_v3: data write(): ERROR occured (errno=%d)",errno) ;
      END_TRACE() ;
      return -1 ;
   }

   rfilefdt[ctrl_sock_index]->wrbyte_net64 += size;
   END_TRACE();
   return(size);
}

/* Close part */
/*
 * remote file close
 */
int DLL_DECL rfio_close64_v3(s)    
int     s;
{
   int      req;
   char     *p  ; 
   struct {
     unsigned int    rcount;    /* read() count                 */
     unsigned int    wcount;    /* write() count                */
     off64_t         rbcount;   /* byte(s) read                 */
     off64_t         wbcount;   /* byte(s) written              */
   } iostatbuf ;
   int      rcode,status,status1,HsmType;
   struct   timeval t;
   fd_set   fdvar;
   unsigned char *dummy;
   int      sizeofdummy = 128 * 1024;
   int      n;
   char     rfio_buf[BUFSIZ];
   int      s_index;
   int      eod = 0;            /* Close received on data socket*/
   int      save_errno;

   // Avoiding Valgrind error messages about uninitialized data
   memset(rfio_buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_close64_v3(%d)", s);

   /*
    * Check if file is Hsm. For CASTOR HSM files, the file is
    * closed using normal RFIO (local or remote) close().
    */
   HsmType = rfio_HsmIf_GetHsmType(s,NULL);
   if ( HsmType > 0 && HsmType != RFIO_HSM_CNS ) {
       status = rfio_HsmIf_close(s);
       END_TRACE() ;
       return(status);
   }
   /*
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      char upath[CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2];

      if ( HsmType == RFIO_HSM_CNS )
         status1 = rfio_HsmIf_getipath(s,upath);
      TRACE(2, "rfio", "rfio_close64_v3: using local close(%d)",s) ;
      status= close(s) ;
      save_errno = errno;
#if defined (CLIENTLOG)
      /* Client logging */
      rfio_logcl(s);
#endif
      if ( HsmType == RFIO_HSM_CNS ) {
         if ( status1 == 1 )
            status1 = rfio_HsmIf_reqtoput(upath);
            if ( status1 == 0 ) errno = save_errno;
      } else
         status1 = 0;
      END_TRACE() ;
      rfio_errno = 0;
      return (status ? status : status1) ;
   }

   /*
    * Checking mode 64.
    */
   if (!rfilefdt[s_index]->mode64) {
      status = rfio_close_v3(s);
      END_TRACE() ;
      return(status);
   }

#if defined (CLIENTLOG)
   /* Client logging */
   rfio_logcl(s);
#endif
   /*
    * Checking magic number
    */
   if ( rfilefdt[s_index]->magic != RFIO_MAGIC ) {
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
   marshall_WORD(p, RQST_CLOSE64_V3);
   marshall_HYPER(p, rfilefdt[s_index]->wrbyte_net64);
   TRACE(2, "rfio", "rfio_close64_v3: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s, rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_close64_v3: write(): ERROR occured (errno=%d)", errno);
      (void) rfio_cleanup_v3(s) ;
      END_TRACE() ;
      return -1 ;
   }

   while (1) 
   {
      FD_ZERO(&fdvar);
      FD_SET(s,&fdvar);
      if ( rfilefdt[s_index]->lseekhow != INVALID_SOCKET && eod == 0)
          FD_SET(rfilefdt[s_index]->lseekhow,&fdvar);
      
      t.tv_sec = 10;
      t.tv_usec = 0;

      TRACE(2,"rfio","close64_v3: doing select") ;
      if (select(FD_SETSIZE,&fdvar,NULL,NULL,&t) < 0)
      {
	 TRACE(2,"rfio","close64_v3: select failed (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ;
      }
      
      /* CLOSE confirmation received from server */
      if (FD_ISSET(s,&fdvar))
      {
	 /*
	  * Getting data from the network.
	  */
	  
	 TRACE(2, "rfio", "rfio_close64_v3: reading %d bytes",RQSTSIZE) ; 
	 if ((n = netread_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT)) != RQSTSIZE) {
	    if (n == 0)
	       TRACE(2, "rfio", "rfio_close64_v3: read(): ERROR occured (serrno=%d)", serrno);
	    else
	       TRACE(2, "rfio", "rfio_close64_v3: read(): ERROR occured (errno=%d)", errno);
	    (void)rfio_cleanup_v3(s) ; 
	    END_TRACE() ;
	    return -1 ; 
	 }
	  
	 /* Closing data socket after the server has read all the data */
	 TRACE(2, "rfio", "rfio_close64_v3 closing data socket, fildesc=%d",
            rfilefdt[s_index]->lseekhow) ; 
	 if (rfilefdt[s_index]->lseekhow != INVALID_SOCKET &&
            s_close(rfilefdt[s_index]->lseekhow) < 0)
	    TRACE(2, "rfio", "rfio_close64_v3: close(): ERROR occured (errno=%d)", errno);
	 rfilefdt[s_index]->lseekhow = INVALID_SOCKET; 
	 p = rfio_buf ;
	 unmarshall_WORD(p,req) ;
	 unmarshall_LONG(p,status) ;
	 unmarshall_LONG(p, rcode) ;
	  
	 rfio_errno = rcode ;
	 switch(req) {
	  case RQST_CLOSE64_V3:
	     (void) rfio_cleanup_v3(s) ; 
	     TRACE(1, "rfio", "rfio_close64_v3: return status=%d, rcode=%d",status,rcode) ;
	     END_TRACE() ;
	     return status ;
          case REP_ERROR:
             /* It is possible to receive an error reply from the server write64_v3 routine */
	     /* This is a bug fix for the internal rfio error returned by close64_v3 when
		write64_v3 on the server side returns 'not enough space' while the client
		has executing the close64_v3 routine */
	     (void) rfio_cleanup_v3(s) ;
	     TRACE(1, "rfio", "rfio_close64_v3: return status=%d, rcode=%d",status,rcode) ;
             if ( status < 0 ) rfio_HsmIf_IOError(s,rcode);
	     END_TRACE() ;
	     return status ;
	  case REP_EOF: /* If we read exactly the amount of data in the file */
	     TRACE(1, "rfio", "rfio_close64_v3: received REP_EOF at close."); 
	     continue;
	  default:
	     TRACE(1,"rfio","rfio_close64_v3(): Bad control word received") ; 
	     serrno= SEINTERNAL ;
	     (void) rfio_cleanup_v3(s) ; 
	     END_TRACE() ;
	     return -1 ;
	 }
      }

      /* Server is still sending data through data socket */
      /* Reading data socket to unstuck the server which then will
	 be able to handle the CLOSE request we have sent previously */
      if (FD_ISSET(rfilefdt[s_index]->lseekhow,&fdvar))
      {	    
	 TRACE(2, "rfio", "rfio_close64_v3: emptying data socket") ;
	 dummy = (unsigned char *)malloc(sizeof(unsigned char) * sizeofdummy);

	 if (dummy == NULL) {
	     TRACE(2,"rfio","rfio_close64_v3(): Cannot allocate memory") ; 
	     (void) rfio_cleanup_v3(s) ;
	     END_TRACE() ;
	     return -1 ;
	 }	   
         /*  Do not use read here as NT doesn't support this with socket fds */
	 if ((n = s_nrecv(rfilefdt[s_index]->lseekhow, dummy, sizeofdummy)) <= 0) {
	    if (n < 0) /* 0 = continue (data socket closed, waiting for close ack */
	    {
	       TRACE(2,"rfio","close64_v3: read failed (errno=%d)",errno) ;
	       (void) rfio_cleanup_v3(s) ;
	       END_TRACE() ;
	       return -1 ;
	    }
	    else
               if (n == 0) {
                  TRACE(2, "rfio","rfio_close64_v3: close received on data socket %d",
                     rfilefdt[s_index]->lseekhow);
                  eod = 1;
               }
               else {           /* n > 0                      */
	          TRACE(2, "rfio", "rfio_close64_v3: emptying data socket, %d bytes read from data socket",n) ;
               }

	 }
	 free(dummy);
      }
   }
}

/*
 * lseek for RFIO version 3 transfers
 * Must be called only between an open (version 3) 
 * and the first read or write system call
 */
off64_t rfio_lseek64_v3(s, offset, how)   
     int       s;
     off64_t   offset; 
     int       how;
{
   char *p;
   off64_t   offset64_out; 
   char rfio_buf[BUFSIZ];
   int rep;
   int status,rcode;
   int s_index;
   char  tmpbuf[21];

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1, "rfio", "rfio_lseek64_v3(%d,%s,%x)",s,i64tostr(offset,tmpbuf,0),how);

#if defined(CLIENTLOG)
   /* Client logging */
   rfio_logls(s,offset,how);
#endif /* CLIENTLOG */

   /*
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_lseek64_v3: using local lseek(%d,%s,%d)",s,i64tostr(offset,tmpbuf,0),how) ;
      offset64_out = lseek64(s,offset,how); 
      rfio_errno = 0;
      END_TRACE();
      return offset64_out;
   }


   /*
    * Checking mode 64.
    */
   if (!rfilefdt[s_index]->mode64) {
      int     offsetout;
      int     offsetin;
      off64_t off64;
      offsetin  = offset;
      offsetout = rfio_lseek_v3(s,  offsetin, how );
      off64     = offsetout;
      END_TRACE();
      return(off64);
   }
   
   /*
    * Checking 'how' parameter.
    */
   if (how != SEEK_SET) {
      errno = EINVAL;
      END_TRACE();
      return -1; 
   }

   /*
    * Checking magic number
    */
   if (rfilefdt[s_index]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION;
      rfio_rfilefdt_freeentry(s_index);
      (void) close(s);
      END_TRACE();
      return -1;
   }

   /*
    * Sending request.
    */
   p = rfio_buf; 
   marshall_WORD(p,RFIO_MAGIC);
   marshall_WORD(p,RQST_LSEEK64_V3);
   marshall_HYPER(p,offset);
   marshall_LONG(p,how);
   TRACE(2, "rfio", "rfio_lseek64_v3: sending %d bytes",RQSTSIZE);
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_lseek64_v3: write(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return -1;
   }

   /*
    * Receiving reply
    *
    */
   TRACE(2, "rfio", "rfio_lseek64_v3: reading %d bytes",RQSTSIZE); 
   if (netread_timeout(s,rfio_buf,WORDSIZE+HYPERSIZE+LONGSIZE,RFIO_DATA_TIMEOUT) != WORDSIZE+HYPERSIZE+LONGSIZE) {
     TRACE(2,"rfio","rfio_lseek64_v3: read(): ERROR occured (errno=%d)",errno);
     END_TRACE();
     return -1; 
   }
  
   p = rfio_buf;
   unmarshall_WORD(p,rep);
   unmarshall_HYPER(p,offset64_out);
   unmarshall_LONG(p,rcode); 

   if (rep != RQST_LSEEK64_V3) {
     TRACE(1,"rfio","rfio_lseek64_v3(): Bad control word received\n"); 
     serrno = SEINTERNAL;
     END_TRACE();
     return -1;
   }

   if (offset64_out < 0)
     rfio_errno = rcode;

   TRACE(1,"rfio","rfio_lseek64_v3: rep %x, offset64_out %s, rcode %d",
         rep,u64tostr(offset64_out,tmpbuf,0),rcode) ;
   END_TRACE();
   return offset64_out;
}
