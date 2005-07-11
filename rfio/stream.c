/*
 * stream.c,v 1.36 2004/01/23 10:27:46 jdurand Exp
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)stream.c,v 1.36 2004/01/23 10:27:46 CERN/IT/PDP/DM F. Hemmer, A. Trannoy, F. Hassine";
#endif /* not lint */

/* stream.c       Remote File I/O - Version 3 streaming routines        */

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
#include <pwd.h>
#include <signal.h>
#include "Castor_limits.h"
#include <Cglobals.h>
#include <Cpwd.h>
#include <Cnetdb.h>
#define strtok(X,Y) strtok_r(X,Y,&last)

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

int data_rfio_connect();

static void rfio_setup_ext_v3(iop,uid,gid,passwd)
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
   /*
    * We don't allow READAHEAD without buffering 
    * until the Ultra bug ( or mine ) is fixed.
    if ( rfioreadopt(RFIO_READOPT) == 2 ) {
    v = 3 ;
    rfiosetopt( RFIO_READOPT, &v ,4) ; 
    }
   */

   iop->magic    = RFIO_MAGIC;
   iop->version3 = 1;
   iop->mode64   = 0;
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
   strcpy(iop->host,"????????");
}

int     rfio_cleanup_v3(s)         /* cleanup rfio descriptor              */
int     s;
{
  int s_index;
  int     HsmType;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_cleanup_v3(%d)", s);

   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) != -1) {
      if (rfilefdt[s_index]->magic != RFIO_MAGIC && rfilefdt[s_index]->magic != B_RFIO_MAGIC) {
	 serrno = SEBADVERSION ; 
	 END_TRACE();
	 return(-1);
      }
      if (rfilefdt[s_index]->_iobuf.base != NULL)    {
	 TRACE(2, "rfio", "freeing I/O buffer at 0X%X", rfilefdt[s_index]->_iobuf.base);
	 (void) free(rfilefdt[s_index]->_iobuf.base);
      }
      TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rfilefdt[s_index]);
      rfio_rfilefdt_freeentry(s_index);
      TRACE(2, "rfio", "closing %d",s) ;
      HsmType = rfio_HsmIf_GetHsmType(s,NULL);
      if ( HsmType > 0 ) {
          int status = rfio_HsmIf_close(s);
          if ( HsmType != RFIO_HSM_CNS ) {
            END_TRACE();
            return(status);
          }
      }
      (void) close(s) ;
   }
   END_TRACE();
   return(0);
}
	
int DLL_DECL rfio_open_v3(filepath, flags, mode)
char    * filepath ;
int     flags,mode ;
{
   char rh[1] ;
   rh[0]='\0' ;

   return(rfio_open_ext_v3(filepath, flags, mode,(uid_t)0,(gid_t)0,0,rh,rh));
}


/*
 * Remote file open.
 */
int	rfio_open_ext_v3(filepath, flags, mode,uid,gid,passwd,reqhost,vmstr) 
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
   int        save_errno, save_serrno;
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
   socklen_t               tolen;
   struct  hostent *hp;
   extern void rfio_setup_ext_v3();
   extern char * getacct() ;
   extern char * getifnam() ;
   int data_port;
   int rem;
   int rb;
   int yes;
   char     rfio_buf[BUFSIZ];
   int rfp_index, parserc;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1,"rfio","rfio_open_ext(%s, %d, %d, %d, %d, %d, %s, %s)",filepath,flags,mode,uid,gid,passwd,reqhost,vmstr ) ;

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
          TRACE(1,"rfio","rfio_open_ext: %s is an HSM path",
                filename);
          END_TRACE();
          rfio_errno = 0;
          return(rfio_HsmIf_open(filename,flags,mode,0));
      }
      status= open(filename, flags, mode) ;
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
      TRACE(2, "rfio", "rfio_open: malloc(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      errno = save_errno;
      return(-1);
   }
   rfio_setup_ext_v3(rfp,(int)uid,(int)gid,passwd) ;
   TRACE(2, "rfio", "RFIO descriptor allocated at 0x%X", rfp);
   /*
	 * Connecting server.
	 */
   rfp->s = rfio_connect(host,&rt);
   if (rfp->s < 0)      {
      save_errno = errno;
      TRACE(2, "rfio", "Failing Doing first connect");

      TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rfp);
      (void) free(rfp);
      END_TRACE();
      errno = save_errno;
      return(-1);
   }
   tolen=sizeof(to);
   if (getpeername(rfp->s,(struct sockaddr *)&to, &tolen)<0)        {
      syslog(LOG_ALERT, "rfio: open: getpeername: %s\n",strerror(errno));
   }
   if ((hp = Cgethostbyaddr((char *) (&to.sin_addr), sizeof(struct in_addr), to.sin_family)) == NULL){
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
   if ((rfp_index = rfio_rfilefdt_allocentry(rfp->s)) == -1) {
      TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rfp);
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
   rfp->first_write = 1;
   rfp->byte_written_to_network = 0;
   rfp->first_read = 1;
   rfp->byte_read_from_network = 0;

	/* Set the keepalive option on socket */
   yes = 1;
   if (setsockopt(rfp->s,SOL_SOCKET,SO_KEEPALIVE,(char *)&yes, sizeof(yes)) < 0) {
      TRACE(2,"rfio","setsockopt keepalive on ctrl: %s",strerror(errno));
   }
   TRACE(2,"rfio","setsockopt keepalive on ctrl done");

#if (defined(__osf__) && defined(__alpha) && defined(DUXV4))    	    
   /* Set the keepalive interval to 20 mns instead of the default 2 hours */
   yes = 20 * 60;
   if (setsockopt(rfp->s,IPPROTO_TCP,TCP_KEEPIDLE,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","setsockopt keepidle on ctrl: %s",strerror(errno));
   }
   TRACE(2,"rfio","setsockopt keepidle on ctrl done (%d s)",yes);
#endif
	    
#if !(defined(__osf__) && defined(__alpha) && defined(DUXV4))
   yes = 1;
   if (setsockopt(rfp->s,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","setsockopt nodelay on ctrl: %s",strerror(errno));
   }
   TRACE(2,"rfio","setsockopt nodelay option set on ctrl socket");
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
      TRACE(2, "rfio" ,"rfio_open_v3: Cgetpwuid() error %s",strerror(errno));
      rfio_cleanup_v3(rfp->s);
      END_TRACE();
      serrno = save_serrno; errno = save_errno;
      return -1 ;
   }
   /*
	 * Building and sending request.
	 */
   /* if ((account = getacct()) == NULL) */ account = "";
   TRACE(2,"rfio","rfio_open: uid %d gid %d umask %o ftype %d, mode %d, flags %d",
	 rfp->uid,rfp->gid,rfp->umask,rfp->ftype,mode,flags) ;
   TRACE(2,"rfio","rfio_open: account: %s",account) ;
   TRACE(2,"rfio","rfio_open: filename: %s",filename) ;
   if (reqhost != NULL && strlen(reqhost) )
      TRACE(2,"rfio","rfio_open: requestor's host: %s",reqhost) ;
   p= rfio_buf ;
   len= 5*WORDSIZE + 3*LONGSIZE + strlen(account) + strlen(filename) +strlen(pw->pw_name) + strlen(reqhost) + strlen(vmstr) + 5 ;
   marshall_WORD(p,B_RFIO_MAGIC) ;
   marshall_WORD(p,RQST_OPEN_V3) ;
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
   TRACE(2,"rfio","rfio_open_v3: sending %d bytes",RQSTSIZE+len) ;
   save_serrno = serrno; serrno = 0; save_errno = errno; errno = 0;
   if (netwrite_timeout(rfp->s,rfio_buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
      save_errno = errno; save_serrno = serrno;
      TRACE(2,"rfio","rfio_open_v3: write(): ERROR occured (errno=%d)", errno) ;
      syslog(LOG_ALERT, "rfio: open_v3: %s (error %d , serrno %d with %s) [uid=%d,gid=%d,pid=%d] in netwrite(%d,0X%X,%d)",
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
   TRACE(1, "rfio", "rfio_open_v3: reading %d bytes",rfp->_iobuf.hsize) ;
   save_serrno = serrno; serrno = 0; save_errno = errno; errno = 0; 
   /*
    * We need to increase the timeout on this netread() because if we
    * miss this message the server will hang forever (it will be stuck
    * in an accept() on the data socket). A better solution on the
    * server side is needed to more properly cope with this problem.
    */
   if ((rb = netread_timeout(rfp->s,rfio_buf,RQSTSIZE,10*RFIO_CTRL_TIMEOUT)) != RQSTSIZE ) {
      if (rb == 0)
      {
	 TRACE(2, "rfio", "rfio_open_v3: Server doesn't support V3 protocol");
	 serrno = SEPROTONOTSUP;
	 END_TRACE();
	 return(-1);
      }
      else
      {
         save_errno = errno; save_serrno = serrno;
	 TRACE(2, "rfio", "rfio_open_v3: read(): ERROR occured (errno=%d)", errno);
	 syslog(LOG_ALERT, "rfio: open_v3: %s (error %d, serrno %d with %s) [uid=%d,gid=%d,pid=%d] in netread(%d,0X%X,%d)",
		strerror(errno > 0 ? errno : serrno), errno, serrno, 
                rfp->host, rfp->uid, rfp->gid, getpid(), rfp->s, rfio_buf, 
                RQSTSIZE);
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
   TRACE(1,"rfio","rfio_open_v3: return status(%d), rcode(%d), fd: %d",status,rcode,rfp->s) ;

   /* will have to check if server doesn't support OPEN_V3 */
   if ( status < 0 ) {
      if ( rcode >= SEBASEOFF)
	 serrno = rcode ;
      else
	 rfio_errno= rcode ;
      rfio_cleanup_v3(rfp->s) ;
      END_TRACE() ;
      /* Operation failed but no error message was sent */
      if ( rcode == 0 )
	 serrno = SENORCODE ;
      return -1 ;
   }
   else {
      rfp->offset= status ;
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
      TRACE(2,"rfio","setsockopt keepalive on data: %s",strerror(errno));
   }
   TRACE(2,"rfio","setsockopt keepalive on data done");

#if (defined(__osf__) && defined(__alpha) && defined(DUXV4))    	    	    
   /* Set the keepalive interval to 20 mns instead of the default 2 hours */
   yes = 20 * 60;
   if (setsockopt(rfp->lseekhow,IPPROTO_TCP,TCP_KEEPIDLE,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","setsockopt keepidle on data: %s",strerror(errno));
   }
   TRACE(2,"rfio","setsockopt keepidle on data done (%d s)",yes);
#endif	    
#if !(defined(__osf__) && defined(__alpha) && defined(DUXV4))
   yes = 1;
   if (setsockopt(rfp->lseekhow,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
      TRACE(2,"rfio","setsockopt nodelay on data: %s",strerror(errno));
   }
   TRACE(2,"rfio","setsockopt nodelay option set on data socket");
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
#if defined(CRAY)
   rfp->fp._file50= rfp->s ;
#endif	/* CRAY	*/
   END_TRACE() ;
   return (rfp->s) ;
}

void rfio_setup_v3(iop)
RFILE   *iop;
{
   (void)rfio_setup_ext_v3(iop,0,0,0);
}

/*
 * Remote file read
 */
int DLL_DECL rfio_read_v3(ctrl_sock, ptr, size) 
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

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_read_v3(%d, %x, %d)", ctrl_sock, ptr, size) ;

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
      TRACE(2, "rfio", "rfio_read_v3: using local read(%d, %x, %d)", ctrl_sock, ptr, size);
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
   if (rfilefdt[ctrl_sock_index]->mode64) {
      status = rfio_read64_v3(ctrl_sock, ptr, size);
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
      marshall_WORD(p,RQST_READ_V3);
	    
      TRACE(2, "rfio", "rfio_read_v3: sending %d bytes",RQSTSIZE) ;
      if (netwrite_timeout(ctrl_sock, rfio_buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
	 TRACE(2,"rfio","rfio_read_v3: write(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ;
      }

      TRACE(2,"rfio", "rfio_read_v3: reading %d bytes",RQSTSIZE) ;
      if ((n = netread_timeout(ctrl_sock,rqstbuf,RQSTSIZE,RFIO_CTRL_TIMEOUT)) != RQSTSIZE) {
	 if (n == 0)
	    TRACE(2, "rfio","read ctrl socket: read(): %s\n", sstrerror(serrno));
	 else
	    TRACE(2, "rfio","read ctrl socket: read(): %s\n", strerror(errno));
	 END_TRACE();
	 return -1 ;
      }
      p = rqstbuf;
      unmarshall_WORD(p,req) ;			
      unmarshall_LONG(p,rfilefdt[ctrl_sock_index]->filesize) ;
      TRACE(2,"rfio", "rfio_read_v3: filesize is %d bytes",rfilefdt[ctrl_sock_index]->filesize) ;
   }


   iobuffer = ptr;
   byte_in_buffer = 0;

   while (1)
   {
      /* EOF received previously but not all data read */
      if (rfilefdt[ctrl_sock_index]->eof_received) {
	 if (rfilefdt[ctrl_sock_index]->filesize == rfilefdt[ctrl_sock_index]->byte_read_from_network)
	 {
	    /* End of file and all data has been readby the client */
	    TRACE(2,"rfio","rfio_read: request satisfied eof encountered (read returns %d)",byte_in_buffer);
	    END_TRACE();
	    return(byte_in_buffer);
	 }
	 else
	 {
	    int to_be_read;

	    to_be_read = rfilefdt[ctrl_sock_index]->filesize - rfilefdt[ctrl_sock_index]->byte_read_from_network;

	    TRACE(2,"rfio","filesize=%d,byte_read=%d,size=%d,buffer=%d,toberead=%d",rfilefdt[ctrl_sock_index]->filesize,rfilefdt[ctrl_sock_index]->byte_read_from_network,size,byte_in_buffer,to_be_read);
	    if (size-byte_in_buffer <= to_be_read)
	    {
	       /* Receiving data using data socket */
	       TRACE(2,"rfio","datarfio_read_v3: reading %d bytes from datasocket filedesc=%d",size-byte_in_buffer,rfilefdt[ctrl_sock_index]->lseekhow) ;
	       if ((n = netread_timeout(rfilefdt[ctrl_sock_index]->lseekhow, iobuffer, size-byte_in_buffer,RFIO_DATA_TIMEOUT)) != size-byte_in_buffer) {
		  if (n == 0)
		     TRACE(2,"rfio","datarfio_read_v3: read(): ERROR occured (serrno=%d)",serrno) ;
		  else
		     TRACE(2,"rfio","datarfio_read_v3: read(): ERROR occured (errno=%d)",errno) ;
		  END_TRACE() ;
		  return -1 ;
	       }
	       rfilefdt[ctrl_sock_index]->byte_read_from_network += (size - byte_in_buffer);
	       byte_in_buffer = size;
	       TRACE(2,"rfio","rfio_read_v3: request satisfied after eof met"); ;
	       END_TRACE();
	       return(size);
	    }
	    else {
	       if (size-byte_in_buffer > to_be_read)
	       {
		  /* Receiving data using data socket */
		  TRACE(2,"rfio","datarfio_read_v3: reading %d bytes from datasocket (to_be_read)",to_be_read,rfilefdt[ctrl_sock_index]->lseekhow) ;
		  if ((n = netread_timeout(rfilefdt[ctrl_sock_index]->lseekhow, iobuffer, to_be_read, RFIO_DATA_TIMEOUT)) != to_be_read) {
		     if (n == 0)
			TRACE(2,"rfio","datarfio_read_v3: read(): ERROR occured (serrno=%d)",serrno) ;
		     else
			TRACE(2,"rfio","datarfio_read_v3: read(): ERROR occured (errno=%d)",errno) ;
		     END_TRACE() ;
		     return -1 ;
		  }
		  rfilefdt[ctrl_sock_index]->byte_read_from_network += to_be_read;
		  byte_in_buffer += to_be_read;
		  TRACE(2,"rfio","rfio_read_v3: request partially satisfied : %d bytes",byte_in_buffer);
		  END_TRACE();
		  return(byte_in_buffer);
	       }
	    }
	 }
      }
      else
      {
	 FD_ZERO(&fdvar);
	 FD_SET(ctrl_sock,&fdvar);
	 FD_SET(rfilefdt[ctrl_sock_index]->lseekhow,&fdvar);
	 t.tv_sec = 10;
	 t.tv_usec = 0;

	 TRACE(2,"rfio","read_v3: doing select") ;
	 if (select(FD_SETSIZE,&fdvar,NULL,NULL,&t) < 0)
	 {
	    TRACE(2,"rfio","read_v3: select failed (errno=%d)",errno) ;
	    END_TRACE() ;
	    return -1 ;
	 }
	    
	 if (FD_ISSET(ctrl_sock,&fdvar))
	 {
	    int cause,rcode;
	    int n;
	    char rqstbuf[BUFSIZ];
		    
	    /* Something received on the control socket */
	    TRACE(2,"rfio", "ctrl socket: reading %d bytes",RQSTSIZE) ;
	    if ((n = netread_timeout(ctrl_sock,rqstbuf,RQSTSIZE,RFIO_CTRL_TIMEOUT)) != RQSTSIZE) {
	       if (n == 0)
		  TRACE(2, "rfio","read ctrl socket: read(): %s", sstrerror(serrno));
	       else
		  TRACE(2, "rfio","read ctrl socket: read(): %s", strerror(errno));
	       END_TRACE();
	       return -1 ;
	    }
	    p = rqstbuf;
	    unmarshall_WORD(p,cause) ;			
	    unmarshall_LONG(p,status) ;
	    unmarshall_LONG(p,rcode) ;
	    if (cause == REP_ERROR)
	    {
	       TRACE(2,"rfio", "read_v3: reply error status %d, rcode %d", status, rcode) ;
	       rfio_errno = rcode;

	       TRACE(2,"rfio","read_v3: sending ack for error") ;
	       if (netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
		  TRACE(2,"rfio","read_v3: write(): ERROR occured (errno=%d)",errno) ;
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
	       TRACE(2,"rfio", "read_v3: eof received") ;
	    }
	 }

	 if (FD_ISSET(rfilefdt[ctrl_sock_index]->lseekhow,&fdvar))
	 {
	    /* Receiving data using data socket */
	    /* Do not use read here because NT doesn't support that with socket fds */
	    if ((n = s_nrecv(rfilefdt[ctrl_sock_index]->lseekhow, iobuffer, size-byte_in_buffer)) <= 0) {
	       if (n == 0)
		  TRACE(2,"rfio","datarfio_read_v3: read(): ERROR occured (serrno=%d)",serrno) ;
	       else
		  TRACE(2,"rfio","datarfio_read_v3: read(): ERROR occured (errno=%d)",errno) ;
	       END_TRACE() ;
	       return -1 ;
	    }
	    byte_in_buffer += n;
	    rfilefdt[ctrl_sock_index]->byte_read_from_network += n;
	    iobuffer += n;

	    TRACE(2,"rfio","rfio_read: receiving datasocket=%d,buffer=%d,req=%d",n,byte_in_buffer,size) ;

	    if (byte_in_buffer == size)
	    {
	       TRACE(2,"rfio","rfio_read: request satisfied completely"); ;
	       END_TRACE();
	       return(size);
	    }

	 }
      }
   }
}



/*
 * Remote file write
 */
int DLL_DECL rfio_write_v3(ctrl_sock, ptr, size) 
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

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_write_v3(%d, %x, %d)", ctrl_sock, ptr, size) ;

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
      TRACE(2, "rfio", "rfio_write_v3: using local write(%d, %x, %d)", ctrl_sock, ptr, size);
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
   if (rfilefdt[ctrl_sock_index]->mode64) {
      status = rfio_write64_v3(ctrl_sock, ptr, size);
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
      marshall_WORD(p,RQST_WRITE_V3);
	    
      TRACE(2, "rfio", "rfio_write_v3: sending %d bytes",RQSTSIZE) ;
      if (netwrite_timeout(ctrl_sock, rfio_buf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
	 TRACE(2,"rfio","rfio_write_v3: write(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ;
      }

   }
   FD_ZERO(&fdvar);
   FD_SET(ctrl_sock,&fdvar);

   t.tv_sec = 0; 
   t.tv_usec = 0;

   TRACE(2,"rfio","write_v3: doing select");
   /* Immediate return here to send data as fast as possible */
   if (select(FD_SETSIZE,&fdvar,NULL,NULL,&t) < 0)
   {
      TRACE(2,"rfio","write_v3: select failed (errno=%d)",errno) ;
      END_TRACE() ;
      return -1 ;
   }
	    
   if (FD_ISSET(ctrl_sock,&fdvar))
   {
      int cause,rcode;
      int n;
      char rqstbuf[BUFSIZ];

      /* Something received on the control socket */
      TRACE(2,"rfio", "ctrl socket: reading %d bytes",RQSTSIZE) ;
      if ((n = netread_timeout(ctrl_sock,rqstbuf,RQSTSIZE,RFIO_CTRL_TIMEOUT)) != RQSTSIZE) {
	 if (n == 0)
	    TRACE(2, "rfio","read ctrl socket: read(): %s\n", sstrerror(serrno));
	 else
	    TRACE(2, "rfio","read ctrl socket: read(): %s\n", strerror(errno));
	 END_TRACE();
	 return -1 ;
      }
      p = rqstbuf;
      unmarshall_WORD(p,cause) ;			
      unmarshall_LONG(p,status) ;
      unmarshall_LONG(p,rcode) ;
      if (cause == REP_ERROR)
	 TRACE(2,"rfio", "write_v3: reply error status %d, rcode %d", status, rcode) ;
      else
	 TRACE(2,"rfio", "write_v3: unknown error status %d, rcode %d", status, rcode) ;
      rfio_errno = rcode;

      TRACE(2,"rfio","rfio_write: sending ack for error") ;
      if (netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
	 TRACE(2,"rfio","rfio_write_v3: write(): ERROR occured (errno=%d)",errno) ;
	 END_TRACE() ;
	 return -1 ;
      }
      if ( status < 0 ) rfio_HsmIf_IOError(ctrl_sock,rfio_errno);

      END_TRACE();
      return(-1);
   }

   /* Sending data using data socket */
   TRACE(2,"rfio","rfio_write: sending %d bytes to datasocket filedesc=%d",size,rfilefdt[ctrl_sock_index]->lseekhow) ;
   if (netwrite_timeout(rfilefdt[ctrl_sock_index]->lseekhow, ptr, size, RFIO_DATA_TIMEOUT) != size) {
      TRACE(2,"rfio","datarfio_write_v3: write(): ERROR occured (errno=%d)",errno) ;
      END_TRACE() ;
      return -1 ;
   }

   rfilefdt[ctrl_sock_index]->byte_written_to_network += size;
   END_TRACE();
   return(size);
}

/* Close part */
/*
 * remote file close
 */
int DLL_DECL rfio_close_v3(s)    
int     s;
{
   int req;
   char   * p  ; 
   struct {
     unsigned int    rcount; /* read() count                 */
     unsigned int    wcount; /* write() count                */
     unsigned int    rbcount;/* byte(s) read                 */
     unsigned int    wbcount;/* byte(s) written              */
   } iostatbuf ;
   int rcode,status,status1,HsmType;
   struct timeval t;
   fd_set fdvar;
   unsigned char *dummy;
   int sizeofdummy = 128 * 1024;
   int n;
   char     rfio_buf[BUFSIZ];
   int s_index;
   int save_errno;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_close_v3(%d)", s);

   /*
    * Check if file is Hsm. For CASTOR HSM files, the file is
    * closed using normal RFIO (local or remote) close().
    */
   HsmType = rfio_HsmIf_GetHsmType(s,NULL);
   if ( HsmType > 0 && HsmType != RFIO_HSM_CNS ) {
       status = rfio_HsmIf_close(s);
       END_TRACE();
       return(status);
   }
   /*
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      char upath[CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2];

      if ( HsmType == RFIO_HSM_CNS )
         status1 = rfio_HsmIf_getipath(s,upath);
      TRACE(2, "rfio", "rfio_close_v3: using local close(%d)",s) ;
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
   if (rfilefdt[s_index]->mode64) {
      status = rfio_close64_v3(s);
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
   marshall_WORD(p, RQST_CLOSE_V3);
   marshall_LONG(p, rfilefdt[s_index]->byte_written_to_network);
   TRACE(2, "rfio", "rfio_close_v3: sending %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(s, rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_close_v3: write(): ERROR occured (errno=%d)", errno);
      (void) rfio_cleanup_v3(s) ;
      END_TRACE() ;
      return -1 ;
   }

   while (1) 
   {
      FD_ZERO(&fdvar);
      FD_SET(s,&fdvar);
      if ( rfilefdt[s_index]->lseekhow != INVALID_SOCKET )
          FD_SET(rfilefdt[s_index]->lseekhow,&fdvar);
      
      t.tv_sec = 10;
      t.tv_usec = 0;

      TRACE(2,"rfio","close_v3: doing select") ;
      if (select(FD_SETSIZE,&fdvar,NULL,NULL,&t) < 0)
      {
	 TRACE(2,"rfio","close_v3: select failed (errno=%d)",errno) ;
	 (void) rfio_cleanup_v3(s) ;
	 END_TRACE() ;
	 return -1 ;
      }
      
      /* CLOSE confirmation received from server */
      if (FD_ISSET(s,&fdvar))
      {
	 /*
	  * Getting data from the network.
	  */
	  
	 TRACE(2, "rfio", "rfio_close: reading %d bytes",RQSTSIZE) ; 
	 if ((n = netread_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT)) != RQSTSIZE) {
	    if (n == 0)
	       TRACE(2, "rfio", "rfio_close_v3: read(): ERROR occured (serrno=%d)", serrno);
	    else
	       TRACE(2, "rfio", "rfio_close_v3: read(): ERROR occured (errno=%d)", errno);
	    (void)rfio_cleanup_v3(s) ; 
	    END_TRACE() ;
	    return -1 ; 
	 }
	  
	 /* Closing data socket after the server has read all the data */
	 TRACE(2, "rfio", "rfio_close_v3 closing data socket, fildesc=%d",rfilefdt[s_index]->lseekhow) ; 
	 if (rfilefdt[s_index]->lseekhow != INVALID_SOCKET &&
             s_close(rfilefdt[s_index]->lseekhow) < 0)
	    TRACE(2, "rfio", "rfio_close_v3: close(): ERROR occured (errno=%d)", errno);
	 rfilefdt[s_index]->lseekhow = INVALID_SOCKET; 
	 p = rfio_buf ;
	 unmarshall_WORD(p,req) ;
	 unmarshall_LONG(p,status) ;
	 unmarshall_LONG(p, rcode) ;
	  
	 rfio_errno = rcode ;
	 switch(req) {
	  case RQST_CLOSE_V3:
	     status1 = rfio_cleanup_v3(s) ; 
	     TRACE(1, "rfio", "rfio_close_v3: return status=%d, rcode=%d",status,rcode) ;
	     END_TRACE() ; 
	     return (status ? status : status1) ;
          case REP_ERROR: /* It is possible to receive an error reply from the server write_v3 routine */
	     /* This is a bug fix for the internal rfio error returned by close_v3 when
		write_v3 on the server side returns 'not enough space' while the client
		has executing the close_v3 routine */
	     (void) rfio_cleanup_v3(s) ;
	     TRACE(1, "rfio", "rfio_close_v3: return status=%d, rcode=%d",status,rcode) ;
             if ( status < 0 ) rfio_HsmIf_IOError(s,rcode);
	     END_TRACE() ;
	     return status ;
	  case REP_EOF: /* If we read exactly the amount of data in the file */
	     TRACE(1, "rfio", "rfio_close_v3: received REP_EOF at close."); 
	     continue;
	  default:
	     TRACE(1,"rfio","rfio_close_v3(): Bad control word received") ; 
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
	 TRACE(2, "rfio", "rfio_close_v3: emptying data socket") ;
	 dummy = (unsigned char *)malloc(sizeof(unsigned char) * sizeofdummy);

	 if (dummy == NULL) {
	     TRACE(2,"rfio","rfio_close_v3(): Cannot allocate memory") ; 
	     (void) rfio_cleanup_v3(s) ;
	     END_TRACE() ; 
	     return -1 ;
	 }	   
         /*  Do not use read here as NT doesn't support this with socket fds */
	 if ((n = s_nrecv(rfilefdt[s_index]->lseekhow, dummy, sizeofdummy)) <= 0) {
	    if (n < 0) /* 0 = continue (data socket closed, waiting for close ack */
	    {
	       TRACE(2,"rfio","close_v3: read failed (errno=%d)",errno) ;
	       (void) rfio_cleanup_v3(s) ;
	       END_TRACE() ;
	       return -1 ;
	    }
	    else
	       TRACE(2, "rfio", "rfio_close_v3: emptying data socket, %d bytes read from data socket",n) ;

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
int rfio_lseek_v3(s, offset, how)   
     int s;
     int offset; 
     int how;
{
   char *p;
   char rfio_buf[BUFSIZ];
   int rep;
   int status,rcode;
   int s_index;

   INIT_TRACE("RFIO_TRACE") ;
   TRACE(1, "rfio", "rfio_lseek_v3(%d,%d,%x)",s,offset,how);

#if defined(CLIENTLOG)
   /* Client logging */
   rfio_logls(s,offset,how);
#endif /* CLIENTLOG */

   /*
    * The file is local
    */
   if ((s_index = rfio_rfilefdt_findentry(s,FINDRFILE_WITHOUT_SCAN)) == -1) {
      TRACE(2, "rfio", "rfio_lseek_v3: using local lseek(%d,%d,%d)",s,offset,how) ;
      status = lseek(s,offset,how); 
      rfio_errno = 0;
      END_TRACE();
      return status;
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
    * Checking mode 64.
    */
   if (rfilefdt[s_index]->mode64) {
      status = rfio_lseek64_v3(s, offset,how);
      END_TRACE();
      return(status);
   }

   /*
    * Sending request.
    */
   p = rfio_buf; 
   marshall_WORD(p,RFIO_MAGIC);
   marshall_WORD(p,RQST_LSEEK_V3);
   marshall_LONG(p,offset);
   marshall_LONG(p,how);
   TRACE(2, "rfio", "rfio_lseek_v3: sending %d bytes",RQSTSIZE);
   if (netwrite_timeout(s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
      TRACE(2, "rfio", "rfio_lseek_v3: write(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return -1;
   }

   /*
    * Receiving reply
    *
    */
   TRACE(2, "rfio", "rfio_lseek_v3: reading %d bytes",RQSTSIZE); 
   if (netread_timeout(s,rfio_buf,WORDSIZE+2*LONGSIZE,RFIO_DATA_TIMEOUT) != WORDSIZE+2*LONGSIZE) {
     TRACE(2,"rfio","rfio_lseek_v3: read(): ERROR occured (errno=%d)",errno);
     END_TRACE();
     return -1; 
   }
  
   p = rfio_buf;
   unmarshall_WORD(p,rep);
   unmarshall_LONG(p,status);
   unmarshall_LONG(p,rcode); 

   if (rep != RQST_LSEEK_V3) {
     TRACE(1,"rfio","rfio_lseek_v3(): Bad control word received\n"); 
     serrno = SEINTERNAL;
     END_TRACE(); 
     return -1;
   }

   if (status < 0)
     rfio_errno = rcode;

   TRACE(1,"rfio","rfio_lseek_v3: rep %x, status %d, rcode %d",rep,status,rcode) ;
   END_TRACE(); 
   return status;
}

extern char     *getconfent();
extern char     *getenv();      /* get environmental variable value     */


static int 	lasthost_key = -1; /* key to hold the last connect host name in TLS */

extern int rfio_nodeHasPort(char *node, char *host, int *port);


int     data_rfio_connect(node,remote,port,flags)       /* Connect <node>'s rfio server */
char    *node;                  /* remote host to connect               */
int     *remote  ;              /* connected host is remote or not      */
int     port;
int     flags;
{
  
   register int    s;      /* socket descriptor                    */
   struct hostent  *hp;    /* host entry pointer                   */
   struct sockaddr_in sin; /* socket address (internet) struct     */
   char    *host;          /* host name chararcter string          */
   char    *p, *cp;        /* character string pointers            */
   char *last = NULL;
   register int    retrycnt; /* number of NOMORERFIO retries       */
   register int    retryint; /* interval between NOMORERFIO retries*/
   register int    crtycnt = 0; /* connect retry count             */
   register int    crtyint = 0; /* connect retry interval          */
   register int    crtyattmpt = 0; /* connect retry attempts done  */
   register int    crtycnts = 0 ;
   struct    stat statbuf ; /* NOMORERFIO stat buffer              */
   char    nomorebuf1[BUFSIZ], nomorebuf2[BUFSIZ]; /* NOMORERFIO buffers */
   int setsock_ceiling = 1 * 1024 * 1024;
   int max_rcvbuf;
   int max_sndbuf;
   int i;
   char *lasthost = NULL;
   int   lasthost_len = 256;
#if !defined(_WIN32)
   int optlen,maxseg;
#endif
/*	int max_rcvlow; */
   char tmphost[CA_MAXHOSTNAMELEN+1];

   INIT_TRACE("RFIO_TRACE");

 /*BC First parse the node name to check whether it contains the port*/
   {
     int rc;
     int tmpport;
     

     rc = rfio_nodeHasPort(node, tmphost, &tmpport);
     if (rc == 1) {
       TRACE(2, "rfio", "data_rfio_connect: Hostname includes port(%s, %s, %d)",
             node, tmphost, port);
       node = tmphost;
     }  
   }
   
   /*BC End parse the node name to check whether it contains the port*/


/*
 * Should we use an alternate name ?
 */

/*
 * Under some circumstances (heavy load, use of inetd) the server fails
 * to accept a connection. A simple retry mechanism is therefore
 * implemented here.
 */
   if ( rfioreadopt(RFIO_NETRETRYOPT) != RFIO_NOTIME2RETRY ) {
      if ((p = getconfent("RFIO", "CONRETRY", 0)) != NULL)        {
	 if ((crtycnt = atoi(p)) <= 0)     {
	    crtycnt = 0;
	 }
      }
      serrno = 0 ;
      if ((p = getconfent("RFIO", "CONRETRYINT", 0)) != NULL)        {
	 if ((crtyint = atoi(p)) <= 0)     {
	    crtyint = 0;
	 }
      }
   }
   crtycnts = crtycnt ;
/*
 * When the NOMORERFIO file exists, or if NOMORERFIO.host file exists,
 * the RFIO service is suspended. By default it will retry for ever every
 * DEFRETRYINT seconds.
 */
   if ((p=getconfent("RFIO", "RETRY", 0)) == NULL) {
      retrycnt=DEFRETRYCNT;
   }
   else    {
      retrycnt=atoi(p);
   }
   if ((p=getconfent("RFIO", "RETRYINT", 0)) == NULL) {
      retryint=DEFRETRYINT;
   }
   else    {
      retryint=atoi(p);
   }

   if ( rfioreadopt(RFIO_NETOPT) != RFIO_NONET ) {
      if ((host = getconfent("NET",node,1)) == NULL)  {
	 host = node;
      }
      else {
	 TRACE(3,"rfio","set of hosts: %s",host);
      }
   }
   else    {
      host = node;
   }

   serrno = 0; /* reset the errno could be SEENTRYNFND */
   rfio_errno = 0;

   TRACE(1, "rfio", "datarfio_connect: connecting(%s) using port %d",host,port);

   memset(&sin,0,sizeof(sin));

   sin.sin_family = AF_INET;
   sin.sin_port = htons(port);

   cp = strtok(host," \t") ;
   if (cp == NULL ) {
      TRACE(1,"rfio","host specified incorrect");
      serrno = SENOSHOST;
      END_TRACE();
      return(-1);
   }

  conretryall:
   TRACE(2, "rfio", "datarfio_connect: Cgethostbyname(%s)", cp);
   hp = Cgethostbyname(cp);
   if (hp == NULL) {
      if (strchr(cp,'.') != NULL) { /* Dotted notation */
	 TRACE(2, "rfio", "datarfio_connect: using %s", cp);
	 sin.sin_addr.s_addr = htonl(inet_addr(cp));
      }
      else    {
	 TRACE(2, "rfio", "datarfio_connect: %s: no such host",cp);
	 serrno = SENOSHOST;     /* No such host                 */
	 END_TRACE();
	 return(-1);
      }
   }
   else    {
      sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;
   }

#if defined(_WIN32)
   if (strncmp (NOMORERFIO, "%SystemRoot%\\", 13) == 0 &&
       (p = getenv ("SystemRoot")))
      sprintf (nomorebuf1, "%s%s", p, strchr (NOMORERFIO, '\\'));
#else
   strcpy(nomorebuf1, NOMORERFIO);
#endif
   sprintf(nomorebuf2, "%s.%s", nomorebuf1, cp);
  retry:
   if (!stat(nomorebuf1,&statbuf)) {
      if (retrycnt-- >=0)  {
	 syslog(LOG_ALERT, "rfio: dataconnect: all RFIO service suspended (pid=%d)\n", getpid());
	 sleep(retryint);
	 goto retry;
      } else {
	 syslog(LOG_ALERT, "rfio: dataconnect: all RFIO service suspended (pid=%d), retries exhausted\n", getpid());
	 serrno=SERTYEXHAUST;
	 END_TRACE();
	 return(-1);
      }
   }
   if (!stat(nomorebuf2, &statbuf)) {
      if (retrycnt-- >=0)  {
	 syslog(LOG_ALERT, "rfio: dataconnect: RFIO service to <%s> suspended (pid=%d)\n", cp, getpid());
	 sleep(retryint);
	 goto retry;
      } else {
	 syslog(LOG_ALERT, "rfio: dataconnect: RFIO service to <%s> suspended (pid=%d), retries exhausted\n", cp, getpid());
	 serrno=SERTYEXHAUST;
	 END_TRACE();
	 return(-1);
      }
   }

  conretry:
   TRACE(2, "rfio", "rfio_dataconnect: socket(%d, %d, %d)",
	 AF_INET, SOCK_STREAM, 0);
   if ((s = socket(AF_INET, SOCK_STREAM, 0)) < 0)  {
      TRACE(2, "rfio", "datarfio_connect: socket(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return(-1);
   }

   max_rcvbuf = setsock_ceiling;
   max_sndbuf = setsock_ceiling;

/* A bit ugly but this is the only solution because the posix
   flag O_ACCMODE is not defined under NT with MS C++ compiler */
#if defined(_WIN32)
#ifndef O_ACCMODE
#define O_ACCMODE 3
#endif
#endif
   /* Do the RCV_BUF setsockopt only if we are reading a file */
   /* This is a fix for observed performance problems */
   /* between DECs and SGIs using HIPPI */
   if ((flags & O_ACCMODE) == O_RDONLY)
   {
      for (i = setsock_ceiling ; i >= 16 * 1024 ; i >>= 1)
      {
	 if (set_rcv_sockparam(s,i) == i)
	 {
	    max_rcvbuf = i;
	    break;
	 }
      }
      TRACE(2,"rfio","setsockopt maxrcv=%d",max_rcvbuf);
   }

   /* Do the SND_BUF setsockopt only if we are reading a file */
   /* This is a fix for observed performance problems */
   /* between DECs and SGIs using HIPPI */
   if ((flags & O_ACCMODE) == O_WRONLY)
   {
      for (i = setsock_ceiling ; i >= 16 * 1024 ; i >>= 1)
      {
	 if (set_snd_sockparam(s,i) == i)
	 {
	    max_sndbuf = i;
	    break;
	 }
      }
      TRACE(2,"rfio","setsockopt maxsnd=%d",max_sndbuf);
   }

   TRACE(2, "rfio", "rfio_dataconnect: connect(%d, %x, %d)", s, &sin, sizeof(struct sockaddr_in));
   if (connect(s, (struct sockaddr *)&sin, sizeof(struct sockaddr_in)) < 0)   {
      TRACE(2, "rfio", "rfio_dataconnect: connect(): ERROR occured (errno=%d)", errno);
#if defined(_WIN32)
      if (errno == WSAECONNREFUSED)	
#else
	 if (errno == ECONNREFUSED)      
#endif
	 {	    syslog(LOG_ALERT, "rfio: dataconnect: %d failed to connect %s", getpid(), cp);
	 if (crtycnt-- > 0)       {
	    if (crtyint) sleep(crtyint);
	    syslog(LOG_ALERT, "rfio: dataconnect: %d retrying to connect %s", getpid(), cp);
/*
 * connect() returns "Invalid argument when called twice,
 * so socket needs to be closed and recreated first
 */

	    (void) close(s);
	    crtyattmpt ++ ;
	    goto conretry;
	 }
	 if ( ( cp = strtok(NULL," \t")) != NULL ) 	{
	    crtycnt =  crtycnts ;
	    syslog(LOG_ALERT, "rfio: dataconnect: after ECONNREFUSED, changing host to %s", cp) ;
	    TRACE(3,"rfio","rfio: dataconnect: after ECONNREFUSED, changing host to %s", cp) ;
	    (void) close(s);
	    goto conretryall;
	 }
	 }
#if defined(_WIN32)
      if (errno==WSAEHOSTUNREACH || errno==WSAETIMEDOUT)	
#else
      if (errno==EHOSTUNREACH || errno==ETIMEDOUT)	
#endif
	 {
	    if ( ( cp = strtok(NULL," \t")) != NULL ) 	{
	       crtycnt =  crtycnts ;
#if defined(_WIN32)
	       if (errno == WSAEHOSTUNREACH)
#else
	       if (errno == EHOSTUNREACH)
#endif
		  syslog(LOG_ALERT, "rfio: dataconnect: after EHOSTUNREACH, changing host to %s", cp) ;
	       else
		  syslog(LOG_ALERT, "rfio: dataconnect: after ETIMEDOUT, changing host to %s", cp) ;
					
	       (void) close(s);
	       goto conretryall;
	    }
				
	 }
      (void) close(s);
      END_TRACE();
      return(-1);
   }
   if (crtyattmpt) {
      syslog(LOG_ALERT, "rfio: dataconnect: %d recovered connection after %d secs with %s",getpid(), crtyattmpt*crtyint,cp) ;
   }
   {
     int sav_serrno = serrno;
     *remote = isremote( sin.sin_addr, node ) ;
     serrno = sav_serrno; /* Failure or not of isremote(), we continue */
   }
   Cglobals_get(&lasthost_key, (void**)&lasthost, lasthost_len);
   strcpy(lasthost, cp); /* remember to fetch back remote errs     */
   TRACE(2, "rfio", "rfio_dataconnect: connected");
   TRACE(2, "rfio", "rfio_dataconnect: calling setnetio(%d)", s);
   if (setnetio(s) < 0)    {
      close(s);
      END_TRACE();
      return(-1);
   }

   TRACE(1, "rfio", "rfio_dataconnect: return socket %d", s);
   END_TRACE();
   return(s);
}


int set_rcv_sockparam(s,value)
int s,value;
{
   if (setsockopt(s,SOL_SOCKET,SO_RCVBUF,(char *)&value, sizeof(value)) < 0) {
#if defined(_WIN32)
      if (errno != WSAENOBUFS)
#else
      if (errno != ENOBUFS)
#endif
	 {
	    TRACE(2,"rfio", "setsockopt rcvbuf(): %s\n",strerror(errno));
	    return(-1);
	 }
	 else
	    return(-1);
   }
   /* else */
   return(value);
}


int set_snd_sockparam(s,value)
int s,value;
{
   if (setsockopt(s,SOL_SOCKET,SO_SNDBUF,(char *)&value, sizeof(value)) < 0) {
#if defined(_WIN32)
      if (errno != WSAENOBUFS)
#else
      if (errno != ENOBUFS)
#endif
	 {
	    TRACE(2,"rfio", "setsockopt sndbuf(): %s\n",strerror(errno));
	    return(-1);
	 }
	 else
	    return(-1);
   }
   /* else */
   return(value);
}
