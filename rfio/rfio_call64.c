/*
 * rfio_call64.c,v 1.3 2004/03/22 12:34:03 jdurand Exp
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)rfio_call64.c,v 1.3 2004/03/22 12:34:03 CERN/IT/PDP/DM Frederic Hemmer, Jean-Philippe Baud, Olof Barring, Jean-Damien Durand";
#endif /* not lint */

/*
 * Remote file I/O flags and declarations.
 */
#define DEBUG           0
#define RFIO_KERNEL     1   
#ifndef DOMAINNAME
#define DOMAINNAME "cern.ch"
#endif
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>
#if defined(_WIN32)
#include "syslog.h"
#else
#include <sys/param.h>
#include <syslog.h>                     /* System logger                */
#include <sys/time.h>
#endif
#if defined(_AIX) || defined(hpux) || defined(SOLARIS) || defined(linux)
#include <signal.h>
#endif

#include <Castor_limits.h>
#include "rfio.h"       
#include "rfcntl.h"
#include "log.h"
#include "u64subr.h"

#if defined(_AIX) && defined(_IBMR2)
#include <sys/select.h>
#endif
#include <sys/types.h>
#if !defined(_WIN32)
#include <netinet/in.h>
#if ((defined(IRIX5) || defined(IRIX6)) && ! (defined(LITTLE_ENDIAN) && defined(BIG_ENDIAN) && defined(PDP_ENDIAN)))
#ifdef   LITTLE_ENDIAN
#undef   LITTLE_ENDIAN
#endif
#define  LITTLE_ENDIAN 1234
#ifdef   BIG_ENDIAN
#undef   BIG_ENDIAN
#endif
#define  BIG_ENDIAN 4321
#ifdef   PDP_ENDIAN
#undef   PDP_ENDIAN
#endif
#define  PDP_ENDIAN 3412
#endif
#include <netinet/tcp.h>
#endif
#include <sys/stat.h>

#ifdef linux
#include <sys/uio.h>
#endif

#ifdef CS2
#include <nfs/nfsio.h>
#endif

/* Context for the open/firstwrite/close handlers */
extern void *handler_context;

#include <fcntl.h>

extern int forced_umask;
#define CORRECT_UMASK(this) (forced_umask > 0 ? forced_umask : this)
extern int ignore_uid_gid;

#if defined(HPSS)
#include <dirent.h>
#include <dce/pthread.h>
#include <rfio_hpss.h>
#include "../h/marshall.h"  /* A must on AIX because of clash with dce/marshall.h */
extern struct global_defs global[];
/*
 * Override some system calls which cannot be used in a threaded version
 */
#define setuid(X) 0
#define setgid(X) 0
#define setgroups(X,Y) 0
/*
 * Override the simplest I/O and filesystem calls with HPSS interface
 * Note that the following cannot be overridden: read(), write() and close() because
 * they may be used on sockets as well as normal filedescriptors.
 * The macros below all assumes that the socket "s" and the client "uid" are defined 
 * in calling routine (same rule for global[] declared above). If this is not the 
 * case, the precompiler will hopefully complain otherwise (e.g. s is defined but not 
 * the accept socket) the result will be unpredictable at run-time....
 */
#define unlink(X) rhpss_unlink(X,s,uid,gid)
#define symlink(X,Y) rhpss_symlink(X,Y,s,uid,gid)
#define readlink(X,Y,Z) rhpss_readlink(X,Y,Z,s,uid,gid)
#define chown(X,Y,Z) rhpss_chown(X,Y,Z,s,uid,gid)
#define chmod(X,Y) rhpss_chmod(X,Y,s,uid,gid)
#define umask(X) rhpss_umask(X,s,0,0)
#define mkdir(X,Y) rhpss_mkdir(X,Y,s,uid,gid)
#define rmdir(X) rhpss_rmdir(X,s,uid,gid)
#define rename(X,Y) rhpss_rename(X,Y,s,uid,gid)
#define lockf(X,Y,Z) rhpss_lockf(X,Y,Z,s,uid,gid)
#define lstat(X,Y) rhpss_lstat(X,Y,s,uid,gid)
#define stat(X,Y) rhpss_stat(X,Y,s,uid,gid)
#define fstat(X,Y) rhpss_fstat(X,Y,s,0,0)
#define access(X,Y) rhpss_access(X,Y,s,uid,gid)
#define open(X,Y,Z) rhpss_open(X,Y,Z,s,uid,gid)
#define opendir(X) rhpss_opendir(X,s,uid,gid)
#define readdir(X) rhpss_readdir(X,s,0,0)
#define closedir(X) rhpss_closedir(X,s,0,0)
#define lseek(X,Y,Z) rhpss_lseek(X,Y,Z,s,0,0)
#define popen(X,Y) rhpss_popen(X,Y,s,uid,gid)

#define lockf64(X,Y,Z) rhpss_lockf64(X,Y,Z,s,uid,gid)
#define lstat64(X,Y) rhpss_lstat64(X,Y,s,uid,gid)
#define stat64(X,Y) rhpss_stat64(X,Y,s,uid,gid)
#define fstat64(X,Y) rhpss_fstat64(X,Y,s,0,0)
#define open64(X,Y,Z) rhpss_open(X,Y,Z,s,uid,gid)
#define lseek64(X,Y,Z) rhpss_lseek64(X,Y,Z,s,0,0)
#endif /* HPSS */

/* For multithreading stuff, only tested under Linux at present */
#if !defined(HPSS)
#include <Cthread_api.h>
#include "Csemaphore.h"

/* If DAEMONV3_RDMT is true, reading from disk will be multithreaded
The circular buffer will have in this case DAEMONV3_RDMT_NBUF buffers of
size DAEMONV3_RDMT_BUFSIZE. Defaults values are defined below */
#define DAEMONV3_RDMT (1)
#define DAEMONV3_RDMT_NBUF (4) 
#define DAEMONV3_RDMT_BUFSIZE (256*1024)

static int daemonv3_rdmt, daemonv3_rdmt_nbuf, daemonv3_rdmt_bufsize;

/* If DAEMONV3_WRMT is true, reading from disk will be multithreaded
The circular buffer will have in this case DAEMONV3_WRMT_NBUF buffers of
size DAEMONV3_WRMT_BUFSIZE. Defaults values are defined below */
#define DAEMONV3_WRMT (1)
#define DAEMONV3_WRMT_NBUF (4) 
#define DAEMONV3_WRMT_BUFSIZE (256*1024)

static int daemonv3_wrmt, daemonv3_wrmt_nbuf, daemonv3_wrmt_bufsize;

/* The circular buffer definition */
static struct element {
  char *p;
  int len;
} *array;

/* The two semaphores to synchonize accesses to the circular buffer */
CSemaphore empty64, full64;
/* Number of buffers produced and consumed */
int produced64 = 0;
int consumed64 = 0;

/* Variable used for error reporting between disk writer thread
   and main thread reading from the network */
static int write_error;
/* Variable set by the main thread reading from the network
   to tell the disk reader thread to stop */
static int stop_read;
#endif   /* !HPSS */

extern char *getconfent() ;
extern int     checkkey(int sock, u_short key );
extern int     check_user_perm(int *uid, int *gid, char *hostname, int *ptrcode, char *permstr) ;

#if !defined(HPSS)
#if defined(_WIN32)
#if !defined (MAX_THREADS)
#define MAX_THREADS 64                     
#endif /* MAX_THREADS */

extern  DWORD tls_i;                    /* thread local storage index */
extern struct thData {
  SOCKET ns;            /* control socket */
  struct sockaddr_in from;
  int mode;
  int _is_remote;
  int fd;
/* all globals, which have to be local for thread */
  char *rqstbuf;        /* Request buffer               */
  char *filename;       /* file name                    */
  char *iobuffer;       /* Data communication buffer    */
  int  iobufsiz;        /* Current io buffer size       */
  SOCKET data_s;        /* Data listen socket (v3) */
  SOCKET data_sock;     /* Data accept socket (v3) */
  SOCKET ctrl_sock;     /* the control socket (v3) */
  int first_write;
  int first_read;
  int byte_read_from_network;
  struct rfiostat myinfo;
  char  from_host[MAXHOSTNAMELEN];
} *td;

#define rqstbuf td->rqstbuf
#define filename td->filename
#define iobuffer td->iobuffer
#define iobufsiz td->iobufsiz
#define data_s td->data_s
#define data_sock td->data_sock
#define ctrl_sock td->ctrl_sock
#define first_write td->first_write
#define first_read td->first_read
#define byte_read_from_network td->byte_read_from_network
#define is_remote td->_is_remote
#define myinfo td->myinfo

#else    /* WIN32 */
/*
 * Buffer declarations
 */
extern char     rqstbuf[BUFSIZ] ;       /* Request buffer               */
extern char     filename[MAXFILENAMSIZE];

static char     *iobuffer ;             /* Data communication buffer    */
static int      iobufsiz;               /* Current io buffer size       */
#endif   /* else WIN32 */
#endif   /* !HPSS  */

#if defined(_WIN32)
#define ECONNRESET WSAECONNRESET
#endif   /* WIN32 */

extern int srchkreqsize _PROTO((SOCKET, char *, int));
extern char *forced_filename;
#define CORRECT_FILENAME(filename) (forced_filename != NULL ? forced_filename : filename)

#if !defined(HPSS)
/* Warning : the new sequential transfer mode cannot be used with 
several files open at a time (because of those global variables)*/
#if !defined(_WIN32)    /* moved to global structure            */
static int data_sock;   /* Data socket                          */
static int ctrl_sock;   /* the control socket                   */
static int first_write;
static int first_read;
static off64_t          byte_read_from_network;
static struct rfiostat  myinfo;
#endif   /* WIN32 */
#endif   /* HPSS */


/************************************************************************/
/*                                                                      */
/*                              IO HANDLERS                             */
/*                                                                      */
/************************************************************************/

#if !defined(_WIN32)
int srlockf64(s, infop, fd)
int      s;
struct   rfiostat *infop;
int      fd;
{
   char    *p ;
   LONG    status = 0;
   LONG    len ;
   LONG    mode ;
   int     uid,gid ;
   int     op;
   off64_t siz;
   int     rcode = 0 ;
   LONG    how;
   off64_t offset;
   off64_t offsetout;
   char    tmpbuf[21];

   p = rqstbuf + (2*WORDSIZE) ;
   unmarshall_LONG(p, how) ;
   unmarshall_LONG(p, len) ;

   if ( (status = srchkreqsize(s,p,len)) == -1 ) {
     rcode = errno;
   } else {
     log(LOG_DEBUG, "srlockf64(%d,%d): reading %d bytes\n", s, fd, len);

     if (netread_timeout(s, rqstbuf, len, RFIO_CTRL_TIMEOUT) != len) {
       log(LOG_ERR, "srlockf64:  read(): %s\n", strerror(errno));
       return -1;
     }
     /*
      * Reading request.
      */
     p = rqstbuf ;
     unmarshall_LONG(p, op) ;
     unmarshall_HYPER(p, siz);
     unmarshall_HYPER(p, offset);
     log(LOG_DEBUG, "srlockf64(%d,%d): operation %d, size %s\n", s, fd, op,
         u64tostr(siz,tmpbuf,0));
     infop->lockop++;
     
     /*
      * lseek() if needed.
      */
     if ( how != -1 ) {
       log(LOG_DEBUG, "lseek64(%d,%d): lseek64(%d,%s,%d)\n", s, fd, fd,
           u64tostr(offset,tmpbuf,0), how) ; 
       infop->seekop++;
       if ( (offsetout= lseek64(fd,offset,how)) == -1 ) { 
         rcode= errno ;
         status = -1;
         log(LOG_ERR, "srlockf64(%d,%d): error %d in lseek64\n", s, fd, rcode);
       }
     }
     if (status == 0 ) {
       if ( (status = lockf64(fd, op, siz)) < 0 ) {
         rcode = errno ;
         log(LOG_ERR, "srlockf64(%d,%d): error %d in lockf64(%d,%d,%s)\n",
             s, fd, rcode, fd, op, u64tostr(siz,tmpbuf,0));
       }
       else
         status = 0 ;
     }
   }
   
   p = rqstbuf ;
   marshall_LONG(p, status);
   marshall_LONG(p, rcode);
#if defined(SACCT)
   rfioacct(RQST_LOCKF,-1,-1,s,op,(int)siz,status,rcode,NULL,NULL,NULL);
#endif /* SACCT */
   log(LOG_DEBUG, "srlockf64:  sending back status %d rcode %d\n", status, rcode);
   if (netwrite_timeout(s, rqstbuf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != (2*LONGSIZE))  {
      log(LOG_ERR, "srlockf64:  write(): %s\n", strerror(errno));
      return -1 ; 
   }
   return status ; 
}
#endif  /* !WIN32 */

#if !defined(_WIN32)
int     srlstat64(s, rt, host)
int     s;
int     rt; /* Is it a remote site call ?   */
char *host; /* Where the request comes from */
{
   char *   p ;
   int    status = 0, rcode = 0 ;
   int    len ;
   int    replen;
   struct stat64 statbuf ;
   char   user[CA_MAXUSRNAMELEN+1];
   int    uid,gid;

   p= rqstbuf + 2*WORDSIZE ;
   unmarshall_LONG(p,len) ;
   if ( (status = srchkreqsize(s,p,len)) == -1 ) {
     rcode = errno;
   } else {
     /*
      * Reading stat request.
      */
     log(LOG_DEBUG, "srlstat64(%d): reading %d bytes\n", s, len);
     if ((status = netread_timeout(s,rqstbuf,len,RFIO_CTRL_TIMEOUT)) != len) {
       log(LOG_ERR, "srlstat64: read(): %s\n", strerror(errno));
       return -1;
     }
     p= rqstbuf ;
     uid = gid = 0;
     status = 0;
     *user = *filename = '\0';
     unmarshall_WORD(p,uid);
     unmarshall_WORD(p,gid);
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p,user,CA_MAXUSRNAMELEN+1)) == -1 )
       rcode = E2BIG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p,filename,MAXFILENAMSIZE)) == -1 )
       rcode = SENAMETOOLONG;
     
     if ( (status == 0) && rt ) {
       char to[100];
       int rcd;
       int to_uid, to_gid ;
       
       if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
         log(LOG_ERR, "srlstat64: get_user(): Error opening mapping file\n") ;
         status= -1;
         errno = EINVAL;
         rcode = errno ;
       }

       if ( !status && abs(rcd) == 1 ) {
         log(LOG_ERR, "srlstat64: No entry in mapping file for (%s,%s,%d,%d)\n",
             host, user, uid, gid);
         status= -1;
         errno=EACCES;
         rcode=errno;
       }
       else {
         log(LOG_DEBUG, "srlstat64: (%s,%s,%d,%d) mapped to %s(%d,%d)\n",
             host, user, uid, gid, to, to_uid, to_gid) ;
         uid = to_uid ;
         gid = to_gid ;
       }       
     }
     if ( !status ) {
       if (  (status=check_user_perm(&uid,&gid,host,&rcode,"FTRUST")) < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"WTRUST")) < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"RTRUST")) < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"XTRUST")) < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"OPENTRUST"))   < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"STATTRUST"))   < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"POPENTRUST"))  < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"LINKTRUST"))   < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"CHMODTRUST"))  < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"CHOWNTRUST"))  < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"MKDIRTRUST"))  < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"RMDIRTRUST"))  < 0 &&
             (status=check_user_perm(&uid,&gid,host,&rcode,"RENAMETRUST")) < 0 ) {
         if (status == -2)
           log(LOG_ERR, "srlstat64: uid %d not allowed to stat()\n", uid);
         else
           log(LOG_ERR, "srlstat64: failed at check_user_perm(), rcode %d\n", rcode);
         memset(&statbuf,'\0',sizeof(statbuf));
         status = rcode ;
       }
       else {
         status= ( lstat64(filename, &statbuf) < 0 ) ? errno : 0 ;
         log(LOG_INFO, "srlstat64: file: %s , status %d\n", filename, status) ;
       }
     }
   }
   
   p = rqstbuf ;
   marshall_WORD(p, statbuf.st_dev);
   marshall_HYPER(p, statbuf.st_ino);
   marshall_WORD(p, statbuf.st_mode);
   marshall_WORD(p, statbuf.st_nlink);
   marshall_WORD(p, statbuf.st_uid);
   marshall_WORD(p, statbuf.st_gid);
   marshall_HYPER(p, statbuf.st_size);
   marshall_LONG(p, statbuf.st_atime);
   marshall_LONG(p, statbuf.st_mtime);
   marshall_LONG(p, statbuf.st_ctime);
   /*
    * Bug #2646. This is one of the rare cases when the errno
    * is returned in the status parameter.
    */
   if ( status == -1 && rcode > 0 ) status = rcode;
   marshall_LONG(p, status);
   marshall_LONG(p, statbuf.st_blksize);
   marshall_HYPER(p, statbuf.st_blocks);
   
   replen = 3*HYPERSIZE+5*LONGSIZE+5*WORDSIZE;
   log(LOG_DEBUG, "srlstat64: sending back %d\n", status);
   if (netwrite_timeout(s,rqstbuf, replen, RFIO_CTRL_TIMEOUT) != replen)  {
      log(LOG_ERR, "srlstat64: write(): %s\n", strerror(errno));
      return -1 ;
   }
   return 0 ;
}
#endif        /* !WIN32 */

int     srstat64(s, rt, host)
#if defined(_WIN32)
SOCKET        s;
#else
int     s;
#endif
int       rt; /* Is it a remote site call ?   */
char *host; /* Where the request comes from */
{
   char   *p ;
   int    status = 0, rcode = 0 ; 
   int    len ;
   int    replen; 
   struct stat64 statbuf ;
   char   user[CA_MAXUSRNAMELEN+1];
   int    uid,gid;

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif
   p= rqstbuf + 2*WORDSIZE ;
   unmarshall_LONG(p,len) ;
   if ( (status = srchkreqsize(s,p,len)) == -1 ) {
     rcode = errno;
   } else {
     /*
      * Reading stat request.
      */
     log(LOG_DEBUG, "srstat64(%d): reading %d bytes\n", s, len);
     if ((status = netread_timeout(s,rqstbuf,len,RFIO_CTRL_TIMEOUT)) != len) {
#if defined(_WIN32)
       log(LOG_ERR, "srstat64: read(): %s\n", geterr());
#else      
       log(LOG_ERR, "srstat64: read(): %s\n", strerror(errno));
#endif      
       return -1;
     }
     p = rqstbuf ;
     uid = gid = 0;
     status = 0;     
     *user = *filename = '\0';
     unmarshall_WORD(p,uid);
     unmarshall_WORD(p,gid);
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p,user,CA_MAXUSRNAMELEN+1)) == -1 )
       rcode = E2BIG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p,filename,MAXFILENAMSIZE)) == -1 )
       rcode = SENAMETOOLONG;
     
     if ( (status == 0) && rt ) {
       char to[100];
       int rcd;
       int to_uid, to_gid ;
       
       if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
         log(LOG_ERR, "srstat64: get_user(): Error opening mapping file\n") ;
         status= -1;
         errno = EINVAL;
         rcode = errno ;
       }
       
       if ( !status && abs(rcd) == 1 ) {
         log(LOG_ERR, "srstat64: No entry in mapping file for (%s,%s,%d,%d)\n",
             host, user, uid, gid);
         status= -1;
         errno=EACCES;
         rcode=errno;
       }
       else {
         log(LOG_DEBUG, "srstat64: (%s,%s,%d,%d) mapped to %s(%d,%d)\n",
             host, user, uid, gid, to, to_uid, to_gid) ;
         uid = to_uid ;
         gid = to_gid ;
       }       
     }
     if ( !status ) {
       /*
        * Trust root for stat() if trusted for any other privileged operation
        */
       rcode = 0;
       if (
           (status=check_user_perm(&uid,&gid,host,&rcode,"FTRUST")) < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"WTRUST")) < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"RTRUST")) < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"XTRUST")) < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"OPENTRUST"))   < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"STATTRUST"))   < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"POPENTRUST"))  < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"LINKTRUST"))   < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"CHMODTRUST"))  < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"CHOWNTRUST"))  < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"MKDIRTRUST"))  < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"RMDIRTRUST"))  < 0 &&
           (status=check_user_perm(&uid,&gid,host,&rcode,"RENAMETRUST")) < 0 ) {
         if (status == -2)
           log(LOG_ERR, "srstat64: uid %d not allowed to stat()\n", uid);
         else
           log(LOG_ERR, "srstat64: failed at check_user_perm(), rcode %d\n", rcode);
         memset(&statbuf,'\0',sizeof(statbuf));
         status = rcode ;
       }
       else  {
         status= ( stat64(filename, &statbuf) < 0 ) ? errno : 0 ;
         log(LOG_INFO, "srstat64: file: %s for (%d,%d) status %d\n",
             filename, uid, gid, status) ;
       }
     }
   }
   
   p = rqstbuf ;
   marshall_WORD(p, statbuf.st_dev);
   marshall_HYPER(p, statbuf.st_ino);
   marshall_WORD(p, statbuf.st_mode);
   marshall_WORD(p, statbuf.st_nlink);
   marshall_WORD(p, statbuf.st_uid);
   marshall_WORD(p, statbuf.st_gid);
   marshall_HYPER(p, statbuf.st_size);
   marshall_LONG(p, statbuf.st_atime);
   marshall_LONG(p, statbuf.st_mtime);
   marshall_LONG(p, statbuf.st_ctime);
   /*
    * Bug #2646. This is one of the rare cases when the errno
    * is returned in the status parameter.
    */
   if ( status == -1 && rcode > 0 ) status = rcode;
   marshall_LONG(p, status);
#if !defined(_WIN32)
   marshall_LONG(p, statbuf.st_blksize);
   marshall_HYPER(p, statbuf.st_blocks);
#endif
   replen = 3*HYPERSIZE+5*LONGSIZE+5*WORDSIZE;
   if (netwrite_timeout(s, rqstbuf, replen, RFIO_CTRL_TIMEOUT) != replen)  {
#if defined(_WIN32)
      log(LOG_ERR, "srstat64: write(): %s\n", geterr());
#else      
      log(LOG_ERR, "srstat64: write(): %s\n", strerror(errno));
#endif      
      return -1 ; 
   }
   return 0 ; 
}
       
int  sropen64(s, rt, host)
#if defined(_WIN32)
SOCKET        s;
#else
int     s;
#endif  
int       rt; /* Is it a remote site call ?   */
char *host; /* Where the request comes from */
{
   int     status;
   int     rcode = 0;
   char    *p;
   int     len;
   int     replen;
   int     fd ;
   LONG    flags, mode;
   int     uid,gid;
   WORD    mask, ftype, passwd, mapping;
   char    account[MAXACCTSIZE];           /* account string            */
   char    user[CA_MAXUSRNAMELEN+1];                       /* User name                 */
   char    reqhost[MAXHOSTNAMELEN];
   off64_t offsetin, offsetout;
#if defined(_WIN32)
   SOCKET  sock;
#else
   int        sock;
#endif       
char tmpbuf[21], tmpbuf2[21];
#if defined(HPSS)
   extern char *rtuser;
#endif /* HPSS */

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   p = rqstbuf + 2*WORDSIZE ;
   unmarshall_LONG(p, len) ;
   if ( (status = srchkreqsize(s,p,len)) == -1 ) {
     rcode = errno;
   } else {
     /*
      * Reading open request.
      */
     log(LOG_DEBUG, "sropen64(%d): reading %d bytes\n", s, len) ;
     if ((status = netread_timeout(s,rqstbuf,len,RFIO_CTRL_TIMEOUT)) != len) {
#if defined(_WIN32)
       log(LOG_ERR, "sropen64: read(): %s\n", geterr());
#else      
       log(LOG_ERR, "sropen64: read(): %s\n", strerror(errno)) ;
#endif      
       return -1 ;
     }
     p = rqstbuf ;
     status = 0;
     *account = *filename = *user = *reqhost = '\0';
     unmarshall_WORD(p, uid);
     unmarshall_WORD(p, gid);
     unmarshall_WORD(p, mask);
     unmarshall_WORD(p, ftype);
     unmarshall_LONG(p, flags);
     unmarshall_LONG(p, mode);
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, account,MAXACCTSIZE)) == -1 )
       rcode = E2BIG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, filename, MAXFILENAMSIZE)) == -1 )
       rcode = SENAMETOOLONG;
     if ( (status == 0 ) &&
          (status = unmarshall_STRINGN(p, user, CA_MAXUSRNAMELEN+1)) == -1 )
       rcode = E2BIG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, reqhost, MAXHOSTNAMELEN)) == -1 )
       rcode = E2BIG;
     unmarshall_LONG(p, passwd);
     unmarshall_WORD(p, mapping);

     log(LOG_DEBUG, "sropen64: Opening file %s for remote user: %s\n", CORRECT_FILENAME(filename), user);
     if (rt)
       log(LOG_DEBUG, "sropen64: Mapping : %s\n", mapping ? "yes" : "no" );
     if (rt && !mapping) {
       log(LOG_DEBUG, "sropen64: name : %d, uid: %d, gid: %d\n", passwd, uid, gid);
     }

     /*
      * Someone in the site has tried to specify (uid,gid) directly !
      */
     if ( (status == 0) && !mapping && !rt) {
       log(LOG_INFO, "sropen64: attempt to make non-mapped I/O and modify uid or gid !\n");
       errno=EACCES ;
       rcode=errno ;
       status= -1 ;
     }
              
     if ( (status == 0) && rt ) { 
       openlog("rfio", LOG_PID, LOG_USER);
       syslog(LOG_ALERT, "sropen64: connection %s mapping by %s(%d,%d) from %s",
              (mapping ? "with" : "without"), user, uid, gid, host) ;
       closelog() ;
     }

     /*
      * MAPPED mode: user will be mapped to user "to"
      */
     if ( !status && rt && mapping ) {
       char to[100];
       int rcd,to_uid,to_gid;
       
       log(LOG_DEBUG, "sropen64: Mapping (%s, %d, %d) \n", user, uid, gid );
       if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
         log(LOG_ERR, "sropen64: get_user() error opening mapping file\n") ;
         status = -1;
         errno = EINVAL;
         rcode = SEHOSTREFUSED ;
       }

       else if ( abs(rcd) == 1 ) {
         log(LOG_ERR, "sropen64: No entry found in mapping file for (%s,%s,%d,%d)\n",
             host,user,uid,gid);
         status = -1;
         errno = EACCES;
         rcode = SEHOSTREFUSED;
       }
       else {
         log(LOG_DEBUG, "sropen64: (%s,%s,%d,%d) mapped to %s(%d,%d)\n",
             host, user, uid, gid, to, to_uid, to_gid) ;
         uid = to_uid ;
         gid = to_gid ;
         if ( uid < 100 || gid < 100 ) {
           errno = EACCES;
           status = -1;
           rcode = SEHOSTREFUSED;
         }
       }
     }
     /* 
      * DIRECT access: the user specifies uid & gid by himself 
      */
     if ( !status && rt && !mapping ){
#if defined(HPSS)
       if ( rtuser == NULL || !strcmp(rtuser,"YES") )
#else /* HPSS */
       char * rtuser;
       if ( (rtuser=getconfent ("RTUSER","CHECK",0) ) == NULL || ! strcmp (rtuser,"YES") )
#endif /* HPSS */
       {
         /* Port is also passwd */
#if defined(_WIN32)
         if( (sock = connecttpread(reqhost, passwd)) != INVALID_SOCKET
             && !checkkey(sock, passwd) )  
#else
           if( (sock = connecttpread(reqhost, passwd)) >= 0 && !checkkey(sock, passwd) )  
#endif
           {
             status= -1 ;
             errno = EACCES;
             rcode= errno ;
             log(LOG_ERR, "sropen64: DIRECT mapping : permission denied\n");
           }
#if defined(_WIN32)
         if( sock == INVALID_SOCKET )
#else           
           if( sock < 0 ) 
#endif
           {
             status= -1 ;
             log(LOG_ERR, "sropen64: DIRECT mapping failed: Couldn't connect %s\n", reqhost);
             rcode = EACCES;
           }
       }
       else
         log(LOG_INFO, "sropen64: Any DIRECT rfio request from out of site is authorized\n");
     }
     if ( !status ) {
       log(LOG_DEBUG, "sropen64: uid %d gid %d mask %o ftype %d flags 0%o mode 0%o\n",
           uid, gid, mask, ftype, flags, mode);
       log(LOG_DEBUG, "sropen64: account: %s\n", account);
       log(LOG_INFO, "sropen64: (%s,0%o,0%o) for (%d,%d)\n",
           CORRECT_FILENAME(filename), flags, mode, uid, gid);
       (void) umask((mode_t) CORRECT_UMASK(mask)) ;
       if ( ((status=check_user_perm(&uid,&gid,host,&rcode,((ntohopnflg(flags) & (O_WRONLY|O_RDWR)) != 0) ? "WTRUST" : "RTRUST")) < 0) &&
            ((status=check_user_perm(&uid,&gid,host,&rcode,"OPENTRUST")) < 0) ) {
         if (status == -2)
           log(LOG_ERR, "sropen64: uid %d not allowed to open64()\n", uid);
         else
           log(LOG_ERR, "sropen64: failed at check_user_perm(), rcode %d\n", rcode);
         status = -1 ;
       }
       else
       {
	 int rc;
	 char *pfn;
	 rc = rfio_handle_open(CORRECT_FILENAME(filename),
			       ntohopnflg(flags),
			       mode,
			       uid,
			       gid,
			       &pfn,
			       &handler_context);
	 if (rc < 0) {
	   char alarmbuf[1024];
	   sprintf(alarmbuf,"sropen64(): %s",CORRECT_FILENAME(filename)) ;
	   log(LOG_DEBUG, "sropen64: rfio_handler_open refused open: %s\n", sstrerror(serrno)) ;
	   rcode = serrno;
           rfio_alrm(rcode,alarmbuf) ;
	 }

         errno = 0;
         fd = open64(pfn, ntohopnflg(flags), mode) ;
         log(LOG_DEBUG, "sropen64: open64(%s,0%o,0%o) returned %x (hex)\n",
             CORRECT_FILENAME(filename), flags, mode, fd);
         if (fd < 0) {
           char alarmbuf[1024];
	   if(pfn != NULL) free (pfn);
           sprintf(alarmbuf,"sropen64: %s", CORRECT_FILENAME(filename)) ;
           status= -1 ;
           rcode= errno ;
           log(LOG_DEBUG, "sropen64: open64: %s\n", strerror(errno)) ;
           rfio_alrm(rcode,alarmbuf) ;
         }
         else {
	   if(pfn != NULL) free (pfn);
           /*
            * Getting current offset
            */
           offsetin = 0;
           errno    = 0;
           offsetout= lseek64(fd, offsetin, SEEK_CUR) ;
           log(LOG_DEBUG, "sropen64: lseek64(%d,%s,SEEK_CUR) returned %s\n",
              fd, u64tostr(offsetin,tmpbuf,0), u64tostr(offsetout,tmpbuf2,0)) ; 
           if ( offsetout < 0 ) {
              rcode = errno ;
              status = -1;
           }
           else status = 0;
         }
       }
     }
   }

   /*
    * Sending back status.
    */
   p= rqstbuf ;
   marshall_WORD(p,RQST_OPEN64) ; 
   marshall_LONG(p,status);
   marshall_LONG(p,rcode) ;
   marshall_LONG(p,0) ;
   marshall_HYPER(p,offsetout) ;
#if defined(SACCT)
   rfioacct(RQST_OPEN64,uid,gid,s,(int)ntohopnflg(flags),(int)mode,status,rcode,NULL,CORRECT_FILENAME(filename),NULL);
#endif /* SACCT */
   log(LOG_DEBUG, "sropen64: sending back status(%d) and errno(%d)\n", status, rcode);
   replen = WORDSIZE+3*LONGSIZE+HYPERSIZE;
   if (netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen)  {
#if defined(_WIN32)
      log(LOG_ERR, "sropen64: write(): %s\n", geterr());
#else      
      log(LOG_ERR, "sropen64: write(): %s\n", strerror(errno)) ;
#endif      
      return -1 ;
   }
   return fd ;
}

int srwrite64(s, infop, fd)
#if defined(_WIN32)
SOCKET s;
#else
int     s;
#endif
int     fd;
struct rfiostat * infop ;
{
   int       status;                      /* Return code               */
   int       rcode;                       /* To send back errno        */
   int       how;                         /* lseek mode                */
   off64_t   offset;                      /* lseek offset              */
   off64_t   offsetout;                   /* lseek offset              */
   int       size;                        /* Requeste write size       */
   char      *p;                          /* Pointer to buffer         */
   int       reqlen;                      /* residual request len      */
   int       replen=WORDSIZE+3*LONGSIZE;  /* Reply buffer length       */
   char      tmpbuf[21];

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   if (first_write) {
     first_write = 0;
     rfio_handle_firstwrite(handler_context);
   }
   
   p= rqstbuf + 2*WORDSIZE ; 
   unmarshall_LONG(p, size);
   unmarshall_LONG(p, how) ;

   /*
    * Receiving request: we have 4 bytes more to be read
    */
   p= rqstbuf + RQSTSIZE ;
   reqlen = HYPERSIZE; 
   log(LOG_DEBUG, "srwrite64(%d, %d)): reading %d bytes\n", s, fd, reqlen) ;
   if ((status = netread_timeout(s, p, reqlen, RFIO_CTRL_TIMEOUT)) != reqlen) {
#if defined(_WIN32)
      log(LOG_ERR, "srwrite64: read(): %s\n", geterr());
#else      
      log(LOG_ERR, "srwrite64: read(): %s\n", strerror(errno)) ;
#endif      
      return -1 ;
   }
   unmarshall_HYPER(p,offset) ; 
   log(LOG_DEBUG, "srwrite64(%d, %d): size %d, how %d offset %s\n", s, fd, size,
      how, u64tostr(offset,tmpbuf,0));
   
   /*
    * Checking if buffer is large enough. 
    */
   if (iobufsiz < size)     {
      int     optval ;       /* setsockopt opt value */

      if (iobufsiz > 0)       {
         log(LOG_DEBUG, "srwrite64: freeing %x\n", iobuffer);
         (void) free(iobuffer) ;
      }
      if ((iobuffer = malloc(size)) == NULL)    {
         status= -1 ; 
         rcode= errno ;
         p= rqstbuf ; 
         marshall_WORD(p,RQST_WRITE64) ;                     
         marshall_LONG(p,status) ; 
         marshall_LONG(p,rcode) ; 
         marshall_LONG(p,0) ; 
         log(LOG_ERR, "srwrite64: malloc(): %s\n", strerror(errno)) ;
         if ( netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen ) {
#if defined(_WIN32)
            log(LOG_ERR, "srwrite64: netwrite(): %s\n", geterr());
#else           
            log(LOG_ERR, "srwrite64: netwrite(): %s\n", strerror(errno)) ;
#endif
            return -1 ;
         }
         return -1 ;
      }
      iobufsiz = size ;
      optval = (iobufsiz > 64 * 1024) ? iobufsiz : (64 * 1024);
      log(LOG_DEBUG, "srwrite64: allocated %d bytes at %x\n", size, iobuffer) ;
#if defined(_WIN32)
      if( setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*)&optval, sizeof(optval)) == SOCKET_ERROR ) 
         log(LOG_ERR, "srwrite64: setsockopt(SO_RCVBUF): %s\n", geterr());
#else
      if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char *)&optval, sizeof(optval)) == -1) 
         log(LOG_ERR, "srwrite64: setsockopt(SO_RCVBUF): %s\n", strerror(errno));
#endif      
      else
         log(LOG_DEBUG, "srwrite64: setsockopt(SO_RCVBUF): %d\n", optval) ;
   }
   /*
    * Reading data on the network.
    */
   p= iobuffer;
   if (netread_timeout(s,p,size,RFIO_DATA_TIMEOUT) != size) {
#if defined(_WIN32)
      log(LOG_ERR, "srwrite64: read(): %s\n", geterr());
#else      
      log(LOG_ERR, "srwrite64: read(): %s\n", strerror(errno)) ;
#endif      
      return -1 ;
   }
   /*
    * lseek() if needed.
    */
   if ( how != -1 ) {
      log(LOG_DEBUG, "srwrite64(%d,%d): lseek64(%d,%s,%d)\n", s, fd, fd,
         u64tostr(offset,tmpbuf,0), how) ; 
      infop->seekop++;
      if ( (offsetout = lseek64(fd,offset,how)) == -1 ) { 
         rcode= errno ;
         status = -1;
         p= rqstbuf ; 
         marshall_WORD(p,RQST_WRITE64) ;                     
         marshall_LONG(p,status) ; 
         marshall_LONG(p,rcode) ; 
         marshall_LONG(p,0) ; 
         if ( netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen ) {
#if defined(_WIN32)
            log(LOG_ERR, "srwrite64: netwrite(): %s\n", geterr());
#else           
            log(LOG_ERR, "srwrite64: netwrite(): %s\n", strerror(errno));
#endif           
            return -1 ;
         }
         return -1 ;
      }
   }
   /*
    * Writing data on disk.
    */
   infop->wnbr+= size ;
   log(LOG_DEBUG, "srwrite64: writing %d bytes on %d\n", size, fd);
#if defined(HPSS)
   status = rhpss_write(fd,p,size,s,0,0);
#else /* HPSS */
   status = write(fd,p,size) ;
#endif /* HPSS */
   rcode= ( status < 0 ) ? errno : 0 ;
       
   if ( status < 0 ) {
      char alarmbuf[1024];
      sprintf(alarmbuf,"srwrite64(): %s",filename) ;
      rfio_alrm(rcode,alarmbuf) ;
   }
   p = rqstbuf;
   marshall_WORD(p,RQST_WRITE64) ;                     
   marshall_LONG(p,status) ;
   marshall_LONG(p,rcode) ;
   marshall_LONG(p,0) ;
   log(LOG_DEBUG, "srwrite64: status %d, rcode %d\n", status, rcode) ;
   if (netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen)  {
#if defined(_WIN32)
      log(LOG_ERR, "srwrite64: write(): %s\n", geterr());
#else      
      log(LOG_ERR, "srwrite64: write(): %s\n", strerror(errno)) ;
#endif      
      return -1 ; 
   }
   return status ; 
}

int srread64(s, infop, fd)
#if defined(_WIN32)
SOCKET        s;
#else
int     s;
#endif
int     fd;
struct rfiostat * infop ;
{
   int      status ;          /* Return code               */
   int      rcode ;           /* To send back errno        */
   int      how ;             /* lseek mode                */
   off64_t  offset ;          /* lseek offset              */
   off64_t  offsetout ;       /* lseek offset              */
   int      size ;            /* Requested read size       */
   char     *p ;              /* Pointer to buffer         */
   int      msgsiz ;          /* Message size              */
   int      reqlen;           /* residual request length   */
   int      replen=WORDSIZE+3*LONGSIZE;   /* Reply header length       */
   char     tmpbuf[21];

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   p= rqstbuf + 2*WORDSIZE ; 
   unmarshall_LONG(p, size) ;
   unmarshall_LONG(p,how) ;

   /*
    * Receiving request: we have 8 bytes more to read
    */
   p= rqstbuf + RQSTSIZE ;
   reqlen = HYPERSIZE;
   log(LOG_DEBUG, "srread64(%d, %d): reading %d bytes\n", s, fd, reqlen) ;
   if ((status = netread_timeout(s, p , reqlen, RFIO_CTRL_TIMEOUT)) != reqlen) {
#if defined(_WIN32)
      log(LOG_ERR, "srread64: read(): %s\n", geterr());
#else      
      log(LOG_ERR, "srread64: read(): %s\n", strerror(errno)) ;
#endif      
      return -1 ;
   }
   unmarshall_HYPER(p,offset) ;
    
   /*
    * lseek() if needed.
    */
   if ( how != -1 ) {
      log(LOG_DEBUG, "srread64(%d,%d): lseek64(%d,%s,%d)\n", s, fd, fd,
         u64tostr(offset,tmpbuf,0), how) ; 
      infop->seekop++;
      if ( (offsetout= lseek64(fd,offset,how)) == -1 ) { 
         rcode= errno ;
         status = -1;
         p= rqstbuf ; 
         marshall_WORD(p,RQST_READ64) ;                     
         marshall_LONG(p,status) ; 
         marshall_LONG(p,rcode) ; 
         marshall_LONG(p,0) ;
         if ( netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen ) {
#if defined(_WIN32)
            log(LOG_ERR, "srread64: write(): %s\n", geterr());
#else
            log(LOG_ERR, "srread64: write(): %s\n", strerror(errno));
#endif           
            return -1 ;
         }
         return -1 ;
      }
   }
   /*
    * Allocating buffer if not large enough.
    */
   log(LOG_DEBUG, "srread64(%d, %d): checking buffer size %d\n", s, fd, size);
   if (iobufsiz < (size+replen))     {
      int     optval ;       /* setsockopt opt value       */

      if (iobufsiz > 0)       {
         log(LOG_DEBUG, "srread64: freeing %x\n", iobuffer);
         (void) free(iobuffer);
      }
      if ((iobuffer = malloc(size+replen)) == NULL)    {
         status= -1 ; 
         rcode= errno ;
         log(LOG_ERR, "srread64: malloc(): %s\n", strerror(errno)) ;
         p= rqstbuf ; 
         marshall_WORD(p,RQST_READ64) ;
         marshall_LONG(p,status) ; 
         marshall_LONG(p,rcode) ; 
         marshall_LONG(p,0) ;
         if ( netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen ) {
#if defined(_WIN32)
            log(LOG_ERR, "srread64: netwrite(): %s\n", geterr());
#else           
            log(LOG_ERR, "srread64: write(): %s\n", strerror(errno)) ;
#endif           
            return -1 ;
         }
         return -1 ; 
      }
      iobufsiz = size + replen ;
      log(LOG_DEBUG, "srread64: allocated %d bytes at %x\n", size, iobuffer);
      optval = (iobufsiz > 64 * 1024) ? iobufsiz : (64 * 1024);
#if defined(_WIN32)
      if( setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&optval, sizeof(optval))
         == SOCKET_ERROR )  
         log(LOG_ERR, "srread64: setsockopt(SO_SNDBUF): %s\n", geterr());
#else        
      if( setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&optval, sizeof(optval)) == -1 )  
         log(LOG_ERR, "srread64: setsockopt(SO_SNDBUF): %s\n", strerror(errno));
#endif
      log(LOG_DEBUG, "srread64: setsockopt(SO_SNDBUF): %d\n", optval);
   }
   p = iobuffer + replen;
#if defined(HPSS)
   status = rhpss_read(fd,p,size,s,0,0);
#else /* HPSS */
   status = read(fd, p, size) ;
#endif /* HPSS */
   if ( status < 0 ) {
      char alarmbuf[1024];
      sprintf(alarmbuf,"srread64(): %s",filename) ;
      rcode= errno ;
      msgsiz= replen ;
      rfio_alrm(rcode,alarmbuf) ;
   }  else  {
      rcode= 0 ; 
      infop->rnbr+= status ;
      msgsiz= status+replen ;
   }
   p= iobuffer ;
   marshall_WORD(p,RQST_READ64) ; 
   marshall_LONG(p,status) ;
   marshall_LONG(p,rcode) ;
   marshall_LONG(p,status) ;
   log(LOG_DEBUG, "srread64: returning status %d, rcode %d\n", status, rcode) ;
   if (netwrite_timeout(s,iobuffer,msgsiz,RFIO_CTRL_TIMEOUT) != msgsiz)  {
#if defined(_WIN32)
      log(LOG_ERR, "srread64: write(): %s\n", geterr());
#else      
      log(LOG_ERR, "srread64: write(): %s\n", strerror(errno));
#endif      
      return -1 ;
   }
   return status ;
}

int srreadahd64(s, infop, fd)
#if defined(_WIN32)
SOCKET	s;
#else
int     s;
#endif
int     fd;
struct rfiostat *infop ;
{
   int      status;	/* Return code		*/ 
   int	    rcode;	/* To send back errno	*/
   int	    how;	/* lseek mode		*/
   off64_t  offset;	/* lseek offset		*/
   off64_t  offsetout;	/* lseek offset		*/
   int	    size;	/* Requested read size	*/
   int	    first;	/* First block sent	*/
   char     *p;		/* Pointer to buffer	*/
   int      reqlen;	/* residual request length */
   char     tmpbuf[21];

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   /*
    * Receiving request.
    */
   log(LOG_DEBUG, "rreadahd64(%d, %d)\n",s, fd);
   p= rqstbuf + 2*WORDSIZE ; 
   unmarshall_LONG(p,size);
   unmarshall_LONG(p,how) ;

   /*
    * Receiving request: we have 8 bytes more to read
    */
   p= rqstbuf + RQSTSIZE ;
   reqlen = HYPERSIZE;
   log(LOG_DEBUG, "rreadahd64(%d, %d): reading %d bytes\n", s, fd, reqlen) ;
   if ((status = netread_timeout(s, p , reqlen, RFIO_CTRL_TIMEOUT)) != reqlen) {
#if defined(_WIN32)
      log(LOG_ERR, "rreadahd64: read(): %s\n", geterr());
#else
      log(LOG_ERR, "rreadahd64: read(): %s\n", strerror(errno)) ;
#endif
      return -1 ;
   }
   unmarshall_HYPER(p,offset) ;

   /*
    * lseek() if needed.
    */
   if ( how != -1 ) {
      log(LOG_DEBUG,"rread64(%d,%d): lseek64(%d,%s,%d)\n", s, fd, fd,
         u64tostr(offset,tmpbuf,0), how) ;
      infop->seekop++;
      if ( (offsetout= lseek64(fd,offset,how)) == -1 ) { 
	 rcode= errno ;
	 status= -1 ;
	 p= iobuffer ; 
	 marshall_WORD(p,RQST_FIRSTREAD) ;			
	 marshall_LONG(p,status) ; 
	 marshall_LONG(p,rcode) ; 
	 if ( netwrite_timeout(s,iobuffer,iobufsiz,RFIO_CTRL_TIMEOUT) != iobufsiz ) {
#if defined(_WIN32)
	    log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n",geterr());
#else	    
	    log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n", strerror(errno));
#endif	    
	    return -1 ;
	 }
	 return status ;
      }
   }
   /*
    * Allocating buffer if not large enough.
    */
   log(LOG_DEBUG, "rreadahd64(%d, %d): checking buffer size %d\n", s, fd, size);
   if (iobufsiz < (size+WORDSIZE+3*LONGSIZE))     {
      int     optval ;	/* setsockopt opt value */

      if (iobufsiz > 0)       {
	 log(LOG_DEBUG, "rreadahd64(): freeing %x\n",iobuffer);
	 (void) free(iobuffer); 				   
      }
      if ((iobuffer = malloc(size+WORDSIZE+3*LONGSIZE)) == NULL)    {
	 log(LOG_ERR, "rreadahd64: malloc(): %s\n", strerror(errno)) ;
#if !defined(HPSS)
	 (void) close(s) ;
#endif /* HPSS */
	 return -1 ; 
      }
      iobufsiz = size+WORDSIZE+3*LONGSIZE ;
      optval = (iobufsiz > 64 * 1024) ? iobufsiz : (64 * 1024);
      log(LOG_DEBUG, "rreadahd64(): allocated %d bytes at %x\n",iobufsiz,iobuffer);
#if defined(_WIN32)
      if( setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&optval, sizeof(optval)) == SOCKET_ERROR )
	 log(LOG_ERR, "rreadahd64(): setsockopt(SO_SNDBUF): %s\n", geterr());
#else      
      if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char *)&optval, sizeof(optval)) == -1)
	 log(LOG_ERR, "rreadahd64(): setsockopt(SO_SNDBUF): %s\n",strerror(errno)) ;
#endif      
      else
	 log(LOG_DEBUG, "rreadahd64(): setsockopt(SO_SNDBUF): %d\n",optval) ;
   }
   /*
    * Reading data and sending it. 
    */
   for(first= 1;;first= 0) {
      fd_set fds ;
      struct timeval timeout ;

      /*
       * Has a new request arrived ?
       */
      FD_ZERO(&fds) ; 
      FD_SET(s,&fds) ;
      timeout.tv_sec = 0 ; 
      timeout.tv_usec= 0 ;
#if defined(_WIN32)
      if( select(FD_SETSIZE, &fds, (fd_set*)0, (fd_set*)0, &timeout) == SOCKET_ERROR )  {
	 log(LOG_ERR,"rreadahd64(): select(): %s\n", geterr());
	 return -1;
      }
#else      
      if ( select(FD_SETSIZE,&fds,(fd_set *)0,(fd_set *)0,&timeout) == -1 ) {
	 log(LOG_ERR,"rreadahd64(): select(): %s\n",strerror(errno)) ;
	 return -1 ; 
      }
#endif      
      if ( FD_ISSET(s,&fds) ) {
	 log(LOG_DEBUG,"rreadahd64(): returns because of new request\n") ;
	 return 0 ; 
      }
      /*
       * Reading disk ...
       */
      p= iobuffer + WORDSIZE + 3*LONGSIZE ;
#if defined(HPSS)
      status = rhpss_read(fd,p,size,s,0,0);
#else /* HPSS */
      status = read(fd,p,size);
#endif /* HPSS */
      if (status < 0)        {
	 rcode= errno ;
	 iobufsiz= WORDSIZE+3*LONGSIZE ;
      }
      else	{
	 rcode= 0 ; 
	 infop->rnbr+= status ;
	 iobufsiz = status+WORDSIZE+3*LONGSIZE ;
      }
      log(LOG_DEBUG, "rreadahd64: status %d, rcode %d\n", status, rcode);
      /*
       * Sending data.
       */
      p= iobuffer ;
      marshall_WORD(p,(first)?RQST_FIRSTREAD:RQST_READAHD64) ; 
      marshall_LONG(p,status) ;
      marshall_LONG(p, rcode) ;
      marshall_LONG(p, status) ;
      if ( netwrite_timeout(s, iobuffer, iobufsiz, RFIO_CTRL_TIMEOUT) != iobufsiz ) {
#if defined(_WIN32)
	 log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n", geterr());
#else	     
	 log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n", strerror(errno)) ;
#endif	     
	 return -1 ;
      }
      /*
       * The end of file has been reached
       * or an error was encountered.
       */
      if ( status != size ) {
	 return 0 ; 
      }
   }
}

int     srfstat64(s, infop, fd)
#if defined(_WIN32)
SOCKET   s;
#else
int      s;
#endif
int      fd;
struct rfiostat *infop;
{
   int            status;
   int            rcode = 0;
   int            msgsiz;
   int            headlen = 3*LONGSIZE+WORDSIZE;
   char           *p;
   struct stat64  statbuf;

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   log(LOG_DEBUG, "rfstat64(%d, %d)\n", s, fd) ;
   infop->statop++;
   
   /*
    * Issuing the fstat()
    */
   status= fstat64(fd, &statbuf) ;
   if ( status < 0) {
      rcode= errno ;
      log(LOG_ERR,"rfstat64(%d,%d): fstat64 gave rc %d, errno=%d\n", s, fd, status, errno);
   }
   else errno = 0;
   
#if !defined(_WIN32)
   msgsiz= 5*WORDSIZE + 4*LONGSIZE + 3*HYPERSIZE ;
#else
   msgsiz= 5*WORDSIZE + 3*LONGSIZE + 2*HYPERSIZE ;
#endif
   p = rqstbuf;
   marshall_WORD(p, RQST_FSTAT64) ; 
   marshall_LONG(p, status) ;
   marshall_LONG(p,  rcode) ;
   marshall_LONG(p, msgsiz) ;
   marshall_WORD(p, statbuf.st_dev);
   marshall_HYPER(p, statbuf.st_ino);
   marshall_WORD(p, statbuf.st_mode);
   marshall_WORD(p, statbuf.st_nlink);
   marshall_WORD(p, statbuf.st_uid);
   marshall_WORD(p, statbuf.st_gid);
   marshall_HYPER(p, statbuf.st_size);
   marshall_LONG(p, statbuf.st_atime);
   marshall_LONG(p, statbuf.st_mtime);
   marshall_LONG(p, statbuf.st_ctime);
#if !defined(_WIN32)
   marshall_LONG(p, statbuf.st_blksize);
   marshall_HYPER(p, statbuf.st_blocks);
#endif

   log(LOG_DEBUG, "rfstat64(%d,%d): sending back %d bytes, status=%d\n", s, fd, msgsiz, status) ;
   if (netwrite_timeout(s,rqstbuf,headlen+msgsiz,RFIO_CTRL_TIMEOUT) != (headlen+msgsiz) )  {
#if defined(WIN32)
      log(LOG_ERR,"rfstat64(%d,%d): netwrite(): %s\n", s, fd, geterr());
#else
      log(LOG_ERR,"rfstat64(%d,%d): netwrite(): %s\n", s, fd, strerror(errno));
#endif
      return -1 ;
   }
   return 0 ; 
}

int srlseek64(s, request, infop, fd)
#if defined(_WIN32)
SOCKET       s;
#else
int         s;
#endif
int    request;
int         fd;
struct rfiostat *infop;
{
   int      status;
   int      rcode;
   int      rlen;
   off64_t  offset;
   off64_t  offsetout;
   int      how;
   char     *p;
   char     tmpbuf[21];

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   p= rqstbuf + 2*WORDSIZE ; 
   unmarshall_HYPER(p,offset) ;
   unmarshall_LONG(p,how) ;
   log(LOG_DEBUG, "srlseek64(%d, %d): offset64 %s, how: %x\n", s, fd,
      u64tostr(offset,tmpbuf,0), how) ;
   offsetout = lseek64(fd, offset, how) ;
   if (offsetout == (off64_t)-1 ) status = -1;
   else status = 0;
   rcode= ( status < 0 ) ? errno : 0 ; 
   log(LOG_DEBUG, "srlseek64: status %s, rcode %d\n", u64tostr(offsetout,tmpbuf,0), rcode) ;
   p= rqstbuf ;
   marshall_WORD(p,request) ;
   marshall_HYPER(p, offsetout) ;
   marshall_LONG(p,rcode) ;
   rlen =  WORDSIZE+LONGSIZE+HYPERSIZE;
   if (netwrite_timeout(s,rqstbuf,rlen,RFIO_CTRL_TIMEOUT) != rlen)  {
#if defined(_WIN32)
      log(LOG_ERR, "srlseek64: netwrite(): %s\n", geterr()) ;
#else
      log(LOG_ERR, "srlseek64: write(): %s\n",strerror(errno)) ;
#endif      
      return -1 ;
   }
   return status ;
}

int srpreseek64(s, infop, fd)
#if defined(_WIN32)
SOCKET	s;
#else
int     s;
#endif
int	fd;
struct rfiostat *infop;
{
   int	status;		/* Return code		*/ 
   int  size;		/* Buffer size		*/
   int	nblock; 	/* Nb of blocks to read	*/
   int	first;		/* First block sent	*/
   char *p;		/* Pointer to buffer	*/
   int	reqno;		/* Request number	*/
#if defined(HPSS)
   struct iovec64 *v = NULL; /* List of requests	*/
#else /* HPSS */
   struct iovec64 *v;	/* List of requests	*/
#endif /* HPSS */
   char *trp = NULL;	/* Temporary buffer	*/

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   p= rqstbuf + 2*WORDSIZE ; 
   unmarshall_LONG(p,size) ;
   unmarshall_LONG(p,nblock) ; 
   log(LOG_DEBUG,"rpreseek64(%d, %d)\n",s,fd) ;

   /*
    * A temporary buffer may need to be created 
    * to receive the request.
    */
   if ( nblock*(HYPERSIZE+LONGSIZE) > BUFSIZ ) {
      if ( (trp= (char *) malloc(nblock*(HYPERSIZE+LONGSIZE))) == NULL ) {
	 return -1 ;
      }
   }
   p= ( trp ) ? trp : rqstbuf ;
   /*
    * Receiving the request.
    */
   log(LOG_DEBUG,"rpreseek64: reading %d bytes\n",nblock*(HYPERSIZE+LONGSIZE)) ; 
   if ( netread_timeout(s,p,nblock*(HYPERSIZE+LONGSIZE),RFIO_CTRL_TIMEOUT) != (nblock*(HYPERSIZE+LONGSIZE)) ) {
#if defined(_WIN32)
      log(LOG_ERR,"rpreseek64: netread(): %s\n", geterr());
#else      
      log(LOG_ERR,"rpreseek64: read(): %s\n",strerror(errno)) ;
#endif      
      if ( trp ) (void) free(trp) ;
      return -1 ;
   }	
   /*
    * Allocating space for the list of requests.
    */
   if ( (v= ( struct iovec64 *) malloc(nblock*sizeof(struct iovec64))) == NULL ) {
      log(LOG_ERR, "rpreseek64: malloc(): %s\n",strerror(errno)) ;
      if ( trp ) (void) free(trp) ;
#if !defined(HPSS)
      (void) close(s) ;
#endif /* HPSS */
      return -1 ; 
   }
   /*
    * Filling list request.
    */
   for(reqno= 0;reqno<nblock;reqno++) {
      unmarshall_HYPER(p,v[reqno].iov_base) ; 
      unmarshall_LONG(p,v[reqno].iov_len) ; 
   }
   /*
    * Freeing temporary buffer if needed.
    */ 
   if ( trp ) (void) free(trp) ;
   /*
    * Allocating new data buffer if the 
    * current one is not large enough.
    */
   log(LOG_DEBUG,"rpreseek64(%d, %d): checking buffer size %d\n",s,fd,size) ;
   if (iobufsiz < (size+WORDSIZE+3*LONGSIZE))     {
      int     optval ;	/* setsockopt opt value */

      if (iobufsiz > 0)       {
	 log(LOG_DEBUG, "rpreseek64(): freeing %x\n",iobuffer);
	 (void) free(iobuffer);
      }
      if ((iobuffer = malloc(size+WORDSIZE+3*LONGSIZE)) == NULL)    {
	 log(LOG_ERR, "rpreseek64: malloc(): %s\n", strerror(errno)) ;
#if defined(HPSS)
	 if ( v ) free(v);
#endif /* HPSS */
#if !defined(HPSS)
	 (void) close(s) ;
#endif /* HPSS */
	 return -1 ; 
      }
      iobufsiz = size+WORDSIZE+3*LONGSIZE ;
      optval = (iobufsiz > 64 * 1024) ? iobufsiz : (64 * 1024);
      log(LOG_DEBUG, "rpreseek64(): allocated %d bytes at %x\n",iobufsiz,iobuffer) ;
#if defined(_WIN32)
      if( setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&optval, sizeof(optval)) == SOCKET_ERROR )
	 log(LOG_ERR, "rpreseek64(): setsockopt(SO_SNDBUF): %s\n", geterr());
#else      
      if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char *)&optval, sizeof(optval)) == -1)
	 log(LOG_ERR, "rpreseek64(): setsockopt(SO_SNDBUF): %s\n",strerror(errno)) ;
#endif
      else
	 log(LOG_DEBUG, "rpreseek64(): setsockopt(SO_SNDBUF): %d\n",optval) ;
   }
   /*
    * Reading data and sending it. 
    */
   reqno= 0 ; 
   for(first= 1;;first= 0) {
      struct timeval timeout ;
      fd_set fds ;
      int nbfree ;
      int     nb ;
      off64_t offsetout ;

      /*
       * Has a new request arrived ?
       * The preseek is then interrupted.
       */
      FD_ZERO(&fds) ; 
      FD_SET(s,&fds) ;
      timeout.tv_sec = 0 ; 
      timeout.tv_usec= 0 ;
#if defined(_WIN32)
      if( select(FD_SETSIZE, &fds, (fd_set*)0, (fd_set*)0, &timeout) == SOCKET_ERROR ) {
	 log(LOG_ERR,"rpreseek64(): select(): %s\n", geterr());
	 return -1;
      }
#else      
      if ( select(FD_SETSIZE,&fds,(fd_set *)0,(fd_set *)0,&timeout) == -1 ) {
	 log(LOG_ERR,"rpreseek64(): select(): %s\n",strerror(errno)) ;
#if defined(HPSS)
	 if ( v ) free(v);
#endif /* HPSS */
	 return -1 ; 
      }
#endif 	/* WIN32 */
      if ( FD_ISSET(s,&fds) ) {
	 log(LOG_DEBUG,"rpreseek64(): returns because of new request\n") ;
#if defined(HPSS)
	 if ( v ) free(v);
#endif /* HPSS */
	 return 0 ; 
      }
      /*
       * Filling buffer.
       */
      p= iobuffer + WORDSIZE + 3*LONGSIZE ;
      for(nb= 0, nbfree= size; HYPERSIZE+3*LONGSIZE<nbfree && reqno<nblock; reqno ++, nb ++ ) {
	 nbfree -= HYPERSIZE+3*LONGSIZE ;
	 offsetout= lseek64(fd,v[reqno].iov_base,SEEK_SET) ;
	 if ( offsetout == (off64_t)-1 ) {
	    status= -1 ;
	    marshall_HYPER(p,v[reqno].iov_base) ; 
	    marshall_LONG(p,v[reqno].iov_len) ;
	    marshall_LONG(p,status) ;
	    marshall_LONG(p,errno) ; 
	    continue ;
	 }
	 if ( v[reqno].iov_len <= nbfree ) {
#if defined(HPSS)
	    status = rhpss_read(fd,p+HYPERSIZE+3*LONGSIZE,v[reqno].iov_len,s,0,0);
#else /* HPSS */
	    status= read(fd,p+HYPERSIZE+3*LONGSIZE,v[reqno].iov_len) ; 	
#endif /* HPSS */
	    marshall_HYPER(p,v[reqno].iov_base) ; 
	    marshall_LONG(p,v[reqno].iov_len) ;
	    marshall_LONG(p,status) ;
	    marshall_LONG(p,errno) ; 
	    if ( status != -1 ) {
	       infop->rnbr += status ;
	       nbfree -= status ;	
	       p += status ;
	    }
	 }
	 else	{
	    /* 
	     * The record is broken into two pieces.
	     */
#if defined(HPSS)
	   status = rhpss_read(fd,p+HYPERSIZE+3*LONGSIZE,nbfree,s,0,0);
#else /* HPSS */
	    status= read(fd,p+HYPERSIZE+3*LONGSIZE,nbfree) ;
#endif /* HPSS */
	    marshall_HYPER(p,v[reqno].iov_base) ; 
	    marshall_LONG(p,nbfree) ;
	    marshall_LONG(p,status) ;
	    marshall_LONG(p,errno) ; 
	    if ( status != -1 ) {
	       infop->rnbr += status ;
	       nbfree -= status ;	
	       p += status ;
	       v[reqno].iov_base += status ;
	       v[reqno].iov_len -= status ;
	       reqno -- ;
	    }
	 }
		
      }
      p= iobuffer ;
      if ( first ) {
	 marshall_WORD(p,RQST_FIRSTSEEK) ; 
      }
      else	{
	 marshall_WORD(p,(reqno == nblock) ? RQST_LASTSEEK:RQST_PRESEEK64) ; 
      }
      marshall_LONG(p,nb) ;
      marshall_LONG(p,0) ;
      marshall_LONG(p,size) ;
      log(LOG_DEBUG,"rpreseek64(): sending %d bytes\n",iobufsiz) ;
      if ( netwrite_timeout(s,iobuffer,iobufsiz,RFIO_CTRL_TIMEOUT) != iobufsiz ) {
#if defined(_WIN32)
	 log(LOG_ERR, "rpreseek64(): netwrite_timeout(): %s\n", geterr());
#else	 
	 log(LOG_ERR, "rpreseek64(): netwrite_timeout(): %s\n", strerror(errno)) ;
#endif	 
#if defined(HPSS)
	 if ( v ) free(v);
#endif /* HPSS */
	 return -1 ;
      }
      /*
       * All the data needed has been
       * sent over the network.
       */
#if defined(HPSS)
      if ( reqno == nblock ) {
	 if ( v ) free(v);
	 return 0 ; 
      }
#else /* HPSS */
      if ( reqno == nblock ) 
	 return 0 ; 
#endif /* HPSS */
   }
}
  

int  sropen64_v3(s, rt, host)
#if defined(_WIN32)
SOCKET        s;
#else
int         s;
#endif
int         rt;            /* Is it a remote site call ?          */
char        *host;         /* Where the request comes from        */
{
   int      status;
   int      rcode = 0;
   char     *p;
   int      len;
   int      replen;
   int      fd = -1;
   LONG     flags, mode;
   int      uid,gid;
   WORD     mask, ftype, passwd, mapping;
   char     account[MAXACCTSIZE];         /* account string       */
   char     user[CA_MAXUSRNAMELEN+1];                     /* User name            */
   char     reqhost[MAXHOSTNAMELEN];
#if defined(HPSS)
   extern char *rtuser;
   int      sock;
#else    /* HPSS */
#if defined(_WIN32)
    SOCKET  sock;
#else    /* HPSS */
   int      sock;                         /* Control socket       */
   int      data_s;                       /* Data    socket       */
#endif   /* else WIN32 */
#endif   /* else HPSS  */
#if defined(_AIX)
   socklen_t fromlen;
   socklen_t size_sin;
#else
   int fromlen;
   int size_sin;
#endif
   int      port;
   struct   sockaddr_in sin;
   struct   sockaddr_in from;
   extern int max_rcvbuf;
   extern int max_sndbuf;
   int      optlen;                       /* Socket option length */
   int      yes;                          /* Socket option value  */
   int      maxseg;                       /* Socket option segment*/
   off64_t  offsetout;                    /* Offset               */
   char     tmpbuf[21];
#if defined(_WIN32)
   struct thData *td;
#endif
   
#if defined(_WIN32)
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   /* Initialization of global variables */
   ctrl_sock   = s;
   data_sock   = -1;
   first_write = 1;
   first_read  = 1;
   /* Init myinfo to zeros */
   myinfo.readop = myinfo.writop = myinfo.flusop = myinfo.statop = myinfo.seekop
      = myinfo.presop = 0;
   myinfo.rnbr = myinfo.wnbr = (off64_t)0;
   /* Will remain at this value (indicates that the new sequential transfer mode has been used) */
   myinfo.aheadop = 1;
   byte_read_from_network = (off64_t)0;

   p= rqstbuf + 2*WORDSIZE ;
   unmarshall_LONG(p, len) ;
   if ( (status = srchkreqsize(s,p,len)) == -1 ) {
     rcode = errno;
   } else {
     /*
      * Reading open request.
      */
     log(LOG_DEBUG,"ropen64_v3(%d): reading %d bytes\n", s, len) ;
     if ((status = netread_timeout(s,rqstbuf,len,RFIO_CTRL_TIMEOUT)) != len) {
#if defined(_WIN32)
       log(LOG_ERR,"ropen64_v3: read(): %s\n", geterr());
#else      
       log(LOG_ERR,"ropen64_v3: read(): %s\n",strerror(errno)) ;
#endif
       return -1 ;
     }
     p= rqstbuf ;
     status = 0;
     *account = *filename = *user = *reqhost = '\0';
     unmarshall_WORD(p, uid);
     unmarshall_WORD(p, gid);
     unmarshall_WORD(p, mask);
     unmarshall_WORD(p, ftype);
     unmarshall_LONG(p, flags);
     unmarshall_LONG(p, mode);
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, account, MAXACCTSIZE)) == -1 )
       rcode = E2BIG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, filename, MAXFILENAMSIZE)) == -1 )
       rcode = SENAMETOOLONG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, user, CA_MAXUSRNAMELEN+1)) == -1 )
       rcode = E2BIG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, reqhost, MAXHOSTNAMELEN)) == -1 )
       rcode = E2BIG;
     unmarshall_LONG(p, passwd);
     unmarshall_WORD(p, mapping);
     
     log(LOG_DEBUG,"ropen64_v3: Opening file %s for remote user: %s\n", CORRECT_FILENAME(filename), user);
     if (rt)
       log(LOG_DEBUG,"ropen64_v3: Mapping : %s\n", mapping ? "yes" : "no");
     if (rt && !mapping) {
       log(LOG_DEBUG,"ropen64_v3: user : %d, uid: %d, gid: %d\n", passwd, uid, gid);
     }

     /*
      * Someone in the site has tried to specify (uid,gid) directly !
      */
     if (!mapping && !rt) {
       log(LOG_INFO,"ropen64_v3: attempt to make non-mapped I/O and modify uid or gid !\n");
       errno=EACCES ;
       rcode=errno ;
       status= -1 ;
     }
     
     if ( rt ) { 
       openlog("rfio", LOG_PID, LOG_USER);
       syslog(LOG_ALERT, "rfio: connection %s mapping by %s(%d,%d) from %s\n",
              (mapping ? "with" : "without"), user, uid, gid, host) ;
#if !defined(_WIN32)
       closelog();
#endif      
     }
     
     /*
      * MAPPED mode: user will be mapped to user "to"
      */
     if ( !status && rt && mapping ) {
       char to[100];
       int rcd,to_uid,to_gid;
       
       log(LOG_DEBUG,"ropen64_v3: Mapping (%s, %d, %d) \n", user, uid, gid );
       if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
         log(LOG_ERR,"ropen64_v3: get_user() error opening mapping file\n") ;
         status = -1;
         errno = EINVAL;
         rcode = SEHOSTREFUSED ;
       }
       
       else if ( abs(rcd) == 1 ) {
         log(LOG_ERR,"ropen64_v3: No entry found in mapping file for (%s,%s,%d,%d)\n",
             host, user, uid, gid);
         status = -1;
         errno = EACCES;
         rcode = SEHOSTREFUSED;
       }
       else {
         log(LOG_DEBUG,"ropen64_v3: (%s,%s,%d,%d) mapped to %s(%d,%d)\n",
             host, user, uid, gid, to, to_uid, to_gid) ;
         uid = to_uid ;
         gid = to_gid ;
         if ( uid < 100 || gid < 100 ) {
           errno = EACCES;
           status = -1;
           rcode = SEHOSTREFUSED;
         }
       }
     }
     /* 
      * DIRECT access: the user specifies uid & gid by himself 
      */
     if( !status && rt && !mapping ) {
#if defined(HPSS)
       if ( rtuser == NULL || strcmp(rtuser,"YES") )
#else /* HPSS */
       char *rtuser;
       if( (rtuser = getconfent("RTUSER","CHECK",0) ) == NULL || ! strcmp (rtuser,"YES") )
#endif /* HPSS */
       {
         /* Port is also passwd */
         sock = connecttpread(reqhost, passwd);
#if defined(_WIN32)
         if( (sock != INVALID_SOCKET) && !checkkey(sock, passwd) )  
#else           
         if( (sock >= 0) && !checkkey(sock, passwd) )  
#endif
         {
           status= -1 ;
           errno = EACCES;
           rcode= errno ;
           log(LOG_ERR,"ropen64_v3: DIRECT mapping : permission denied\n");
         }
#if defined(_WIN32)
         if( sock == INVALID_SOCKET )  
#else           
           if (sock < 0) 
#endif
           {
             status= -1 ;
             log(LOG_ERR,"ropen64_v3: DIRECT mapping failed: Couldn't connect %s\n", reqhost);
             rcode = EACCES;
           }
       }
       else
         log(LOG_INFO ,"ropen64_v3: Any DIRECT rfio request from out of site is authorized\n");
     }
     if ( !status ) {

       log(LOG_DEBUG, "ropen64_v3: uid %d gid %d mask %o ftype %d flags 0%o mode 0%o\n",
           uid, gid, mask, ftype, flags, mode);
       log(LOG_DEBUG, "ropen64_v3: account: %s\n", account);
       log(LOG_INFO,  "ropen64_v3: (%s,0%o,0%o) for (%d,%d)\n", CORRECT_FILENAME(filename), flags, mode, uid, gid);
       (void) umask((mode_t) CORRECT_UMASK(mask)) ;
#if !defined(_WIN32)  
       if ( ((status=check_user_perm(&uid,&gid,host,&rcode,((ntohopnflg(flags) & (O_WRONLY|O_RDWR)) != 0) ? "WTRUST" : "RTRUST")) < 0) &&
            ((status=check_user_perm(&uid,&gid,host,&rcode,"OPENTRUST")) < 0) ) {
         if (status == -2)
          log(LOG_ERR,"ropen64_v3: uid %d not allowed to open()\n", uid);
         else
          log(LOG_ERR,"ropen64_v3: failed at check_user_perm(), rcode %d\n", rcode);
         status = -1 ;
      }  else
#endif
      {
	 int rc;
	 char *pfn;
	 rc = rfio_handle_open(CORRECT_FILENAME(filename),
			       ntohopnflg(flags),
			       mode,
			       uid,
			       gid,
			       &pfn,
			       &handler_context);
	 if (rc < 0) {
	   char alarmbuf[1024];
	   sprintf(alarmbuf,"sropen64_v3: %s",CORRECT_FILENAME(filename)) ;
	   log(LOG_DEBUG, "ropen64_v3: rfio_handler_open refused open: %s\n", sstrerror(serrno)) ;
	   rcode = serrno;
           rfio_alrm(rcode,alarmbuf) ;
	 }

         fd = open64(CORRECT_FILENAME(filename), ntohopnflg(flags), mode);
#if defined(_WIN32)
         _setmode( fd, _O_BINARY );       /* default is text mode  */
#endif       
         if (fd < 0)  {
          status= -1 ;
          rcode= errno ;
#if defined(_WIN32)  
          log(LOG_DEBUG,"ropen64_v3: open64(%s,0%o,0%o): %s\n",
              CORRECT_FILENAME(filename), ntohopnflg(flags), mode, geterr()) ;
#else
          log(LOG_DEBUG,"ropen64_v3: open64(%s,0%o,0%o): %s\n",
              CORRECT_FILENAME(filename), ntohopnflg(flags), mode, strerror(errno)) ;
#endif
	  if(pfn != NULL) free (pfn);
         }
         else  {
	   if(pfn != NULL) free (pfn);
           /* File is opened         */
           log(LOG_DEBUG,"ropen64_v3: open64(%s,0%o,0%o) returned %d \n",
               CORRECT_FILENAME(filename), ntohopnflg(flags), mode, fd) ;
           /*
            * Getting current offset
            */
           offsetout = lseek64(fd, (off64_t)0, SEEK_CUR) ; 
           if (offsetout == ((off64_t)-1) ) {
#if defined(_WIN32)  
             log(LOG_ERR,"ropen64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd, geterr()) ;
#else
             log(LOG_ERR,"ropen64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd,strerror(errno)) ;
#endif
             status = -1;
             rcode  = errno;
           }
           else {
             log(LOG_DEBUG,"ropen64_v3: lseek64(%d,0,SEEK_CUR) returned %s\n",
                 fd, u64tostr(offsetout,tmpbuf,0)) ;
             status = 0;
           }
         }
       }
     }

     if (! status && fd >= 0)  {
       data_s = socket(AF_INET, SOCK_STREAM, 0);
#if defined(_WIN32)
       if( data_s == INVALID_SOCKET )  {
         log( LOG_ERR, "ropen64_v3: datasocket(): %s\n", geterr());
         return(-1);
       }
#else    /* WIN32 */   
       if( data_s < 0 )  {
         log(LOG_ERR, "ropen64_v3: datasocket(): %s\n", strerror(errno));
#if defined(HPSS)
         return(-1);
#else
         exit(1);
#endif   /* else HPSS  */
       }
#endif   /* else WIN32 */         
       log(LOG_DEBUG, "ropen64_v3: data socket created fd=%d\n", data_s);
       memset(&sin,0,sizeof(sin));
       port = 0;
       sin.sin_port = htons(port);
       sin.sin_addr.s_addr = htonl(INADDR_ANY);
       sin.sin_family = AF_INET;
       
#if defined(_WIN32)           
       if( bind(data_s, (struct sockaddr*)&sin, sizeof(sin)) == SOCKET_ERROR) 
       {
         log(LOG_ERR, "ropen64_v3: bind: %s\n", geterr());
         return(-1);
       }
#else    /* WIN32 */
       if( bind(data_s, (struct sockaddr*)&sin, sizeof(sin)) < 0 ) {
         log(LOG_ERR, "ropen64_v3: bind: %s\n",strerror(errno));
#if defined(HPSS)
         return(-1);
#else    /* HPSS */
         exit(1);
#endif   /* else HPSS */
       }
#endif   /* else WIN32 */
       
       size_sin = sizeof(sin);
#if defined(_WIN32)      
       if( getsockname(data_s, (struct sockaddr*)&sin, &size_sin) == SOCKET_ERROR )  {
         log(LOG_ERR, "ropen64_v3: getsockname: %s\n", geterr());
         return(-1);
       }
#else
       if( getsockname(data_s, (struct sockaddr*)&sin, &size_sin) < 0 )  {
         log(LOG_ERR, "ropen64_v3: getsockname: %s\n", strerror(errno));
#if defined(HPSS)
         return(-1);
#else    /* HPSS */
         exit(1);
#endif   /* else HPSS  */
       }
#endif   /* else WIN32 */
       log(LOG_DEBUG, "ropen64_v3: assigning data port %d\n", htons(sin.sin_port));
#if defined(_WIN32)           
       if( listen(data_s, 5) == INVALID_SOCKET )   {
         log(LOG_ERR, "ropen64_v3: listen(%d): %s\n", data_s, geterr());
         return(-1);
       }
#else
       if( listen(data_s, 5) < 0 )   {
         log(LOG_ERR, "ropen64_v3: listen(%d): %s\n", data_s, strerror(errno));
#if defined(HPSS)
         return(-1);
#else    /* HPSS */
         exit(1);
#endif   /* else HPSS  */
       }
#endif   /* else WIN32 */
     }
   }
          
   /*
    * Sending back status to the client
    */
   p= rqstbuf ;
   marshall_WORD(p,RQST_OPEN64_V3) ;
   replen = RQSTSIZE+HYPERSIZE;
   marshall_LONG(p,status);
   marshall_LONG(p,rcode) ;
   marshall_LONG(p,ntohs(sin.sin_port)) ;
   marshall_HYPER(p, offsetout);
   log(LOG_DEBUG, "ropen64_v3: sending back status(%d) and errno(%d)\n", status, rcode);
   errno = ECONNRESET;
   if (netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen)  {
#if defined(_WIN32)
      log(LOG_ERR,"ropen64_v3: write(): %s\n", geterr());
#else      
      log(LOG_ERR,"ropen64_v3: write(): %s\n", strerror(errno)) ;
#endif
      return -1 ;
   }

   if (! status && fd >= 0)
   {
      /*
       * The rcvbuf on the data socket must be set _before_
       * the connection is accepted! Otherwise the receiver will
       * only offer the default window, which for large MTU devices
       * (such as HIPPI) is far too small and performance goes down
       * the drain.
       *
       * The sndbuf must be set on the socket returned by accept()
       * as it is not inherited (at least not on SGI).
       * 98/08/05 - Jes
       */
      log(LOG_DEBUG, "ropen64_v3: doing setsockopt %d RCVBUF\n", data_s);
#if defined(_WIN32)
      if( setsockopt(data_s, SOL_SOCKET, SO_RCVBUF, (char*)&max_rcvbuf,
         sizeof(max_rcvbuf)) == SOCKET_ERROR )  {
         log(LOG_ERR, "ropen64_v3: setsockopt %d rcvbuf(%d bytes): %s\n",
            data_s, max_rcvbuf, geterr());
      }
#else      
      if (setsockopt(data_s, SOL_SOCKET, SO_RCVBUF, (char *)&max_rcvbuf,
         sizeof(max_rcvbuf)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt %d rcvbuf(%d bytes): %s\n",
            data_s, max_rcvbuf, strerror(errno));
      }
#endif       /* WIN32 */
      log(LOG_DEBUG, "ropen64_v3: setsockopt rcvbuf on data socket %d (%d bytes)\n",
         data_s, max_rcvbuf);
      for (;;) 
      {
         fromlen = sizeof(from);
         log(LOG_DEBUG, "ropen64_v3: wait for accept to complete\n");
         data_sock = accept(data_s, (struct sockaddr*)&from, &fromlen);
#if defined(_WIN32)
         if( data_sock == INVALID_SOCKET )  {
            log(LOG_ERR, "ropen64_v3: data accept(%d): %s\n", data_s, geterr());
            return(-1);
         }
#else           
         if( data_sock < 0 )  {
            log(LOG_ERR, "ropen64_v3: data accept(%d): %s\n", data_s, strerror(errno));
#if defined(HPSS)
            return(-1);
#else    /* HPSS */
            exit(1);
#endif   /* else HPSS  */
         }
#endif   /* else WIN32 */        
         else break;
      }
      log(LOG_DEBUG, "ropen64_v3: data accept is ok, fildesc=%d\n", data_sock);

      /*
       * Set the send socket buffer on the data socket (see comment
       * above before accept())
       */
      log(LOG_DEBUG, "ropen64_v3: doing setsockopt %d SNDBUF\n", data_sock);
#if defined(_WIN32)
      if( setsockopt(data_sock, SOL_SOCKET, SO_SNDBUF, (char*)&max_sndbuf,
         sizeof(max_sndbuf)) == SOCKET_ERROR)  {
         log(LOG_ERR, "ropen64_v3: setsockopt %d SNDBUF(%d bytes): %s\n",
            data_sock, max_sndbuf, geterr());
      }
#else    /* WIN32*/   
      if (setsockopt(data_sock,SOL_SOCKET,SO_SNDBUF,(char *)&max_sndbuf,
         sizeof(max_sndbuf)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt %d SNDBUFF(%d bytes): %s\n",
            data_sock, max_sndbuf, strerror(errno));
      }
#endif   /* else WIN32 */
      log(LOG_DEBUG, "ropen64_v3: setsockopt SNDBUF on data socket %d(%d bytes)\n",
         data_sock, max_sndbuf);

      /* Set the keepalive option on both sockets */
      yes = 1;
#if defined(_WIN32)
      if( setsockopt(data_sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&yes,
         sizeof(yes)) == SOCKET_ERROR) {
         log(LOG_ERR, "ropen64_v3: setsockopt keepalive on data socket%d: %s\n", data_sock, geterr());
      } 
#else      
      if (setsockopt(data_sock,SOL_SOCKET,SO_KEEPALIVE,(char *)&yes, sizeof(yes)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt keepalive on data socket %d: %s\n",
            data_sock, strerror(errno));
      }
#endif       /* WIN32 */      
      log(LOG_DEBUG, "ropen64_v3: setsockopt keepalive on data socket %d done\n", data_sock);
           
      yes = 1;
#if defined(_WIN32)
      if( setsockopt(ctrl_sock, SOL_SOCKET, SO_KEEPALIVE, (char*)&yes,
         sizeof(yes)) == SOCKET_ERROR )  {
         log(LOG_ERR, "ropen64_v3: setsockopt keepalive on ctrl socket %d: %s\n",
            ctrl_sock, geterr());
      }      
#else
      if (setsockopt(ctrl_sock,SOL_SOCKET,SO_KEEPALIVE,(char *)&yes, sizeof(yes)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt keepalive on ctrl socket %d: %s\n",
            ctrl_sock, strerror(errno));
      }
#endif       /* WIN32 */
      log(LOG_DEBUG, "ropen64_v3: setsockopt keepalive on ctrl socket %d done\n", ctrl_sock);

#if (defined(__osf__) && defined(__alpha) && defined(DUXV4))    
      /* Set the keepalive interval to 20 mns instead of the default 2 hours */
      yes = 20 * 60;
      if (setsockopt(data_sock, IPPROTO_TCP, TCP_KEEPIDLE,(char *)&yes, sizeof(yes)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt keepidle on data socket %d: %s\n",
           data_sock, strerror(errno));
      }
      log(LOG_DEBUG, "ropen64_v3: setsockopt keepidle on data socket %d done (%d sec)\n",
         data_sock, yes);
           
      yes = 20 * 60;
      if (setsockopt(ctrl_sock, IPPROTO_TCP, TCP_KEEPIDLE, (char *)&yes, sizeof(yes)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt keepidle on ctrl socket %d: %s\n",
            ctrl_sock, strerror(errno));
      }
      log(LOG_DEBUG, "ropen64_v3: setsockopt keepidle on ctrl socket %d done (%d sec)\n",
         ctrl_sock, yes);
#endif
#if !(defined(__osf__) && defined(__alpha) && defined(DUXV4))    
      /*
       * TCP_NODELAY seems to cause small Hippi packets on Digital UNIX 4.x
       */
      yes = 1;
#if defined(_WIN32)
      if( setsockopt(data_sock, IPPROTO_TCP, TCP_NODELAY, (char*)&yes,
         sizeof(yes))  == SOCKET_ERROR )  {
         log(LOG_ERR, "ropen64_v3: setsockopt nodelay on data socket %d: %s\n",
            data_sock, geterr());
      }
#else      
      if (setsockopt(data_sock,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt nodelay on data socket %d: %s\n",
            data_sock, strerror(errno));
      }
#endif       /* WIN32 */      
      log(LOG_DEBUG,"ropen64_v3: setsockopt nodelay option set on data socket %d\n", data_sock);
#endif /* !(defined(__osf__) && defined(__alpha) && defined(DUXV4)) */
      
      yes = 1;
#if defined(_WIN32)
      if( setsockopt(ctrl_sock, IPPROTO_TCP, TCP_NODELAY, (char*)&yes,
         sizeof(yes)) == SOCKET_ERROR )  {
         log(LOG_ERR, "ropen64_v3: setsockopt nodelay on ctrl socket %d: %s\n",
            ctrl_sock, geterr());
      } 
#else      
      if (setsockopt(ctrl_sock,IPPROTO_TCP,TCP_NODELAY,(char *)&yes,sizeof(yes)) < 0) {
         log(LOG_ERR, "ropen64_v3: setsockopt nodelay on ctrl socket %d: %s\n",
            ctrl_sock, strerror(errno));
      }
#endif  /* WIN32 */
      log(LOG_DEBUG,"ropen64_v3: setsockopt nodelay option set on ctrl socket %d\n", ctrl_sock);
   }
#if defined(SACCT)
   rfioacct(RQST_OPEN64_V3,uid,gid,s,(int)ntohopnflg(flags),(int)mode,status,rcode,NULL,CORRECT_FILENAME(filename),NULL);
#endif /* SACCT */
   return fd ;
}


int   srclose64_v3(s, infop, fd)
#if defined(_WIN32)
SOCKET        s;
#else
int       s;
#endif   
int     fd;
struct rfiostat *infop;
{
   int         status;
   int         rcode;
   char        *p;
   char        tmpbuf[21], tmpbuf2[21];

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif
   
   /* Issue statistics                                        */
   log(LOG_INFO,"rclose64_v3(%d, %d): %d read, %d readahead, %d write, %d flush, %d stat, %d lseek and %d lockf\n",
      s, fd, myinfo.readop, myinfo.aheadop, myinfo.writop, myinfo.flusop, myinfo.statop,
      myinfo.seekop, myinfo.lockop);
   log(LOG_INFO,"rclose64_v3(%d, %d): %s bytes read and %s bytes written\n",
      s, fd, u64tostr(myinfo.rnbr,tmpbuf,0), u64tostr(myinfo.wnbr,tmpbuf2,0)) ;

   rfio_handle_close(handler_context);

   /* Close the local file                                    */
#if defined(HPSS)
   status = rhpss_close(fd,s,0,0);
#else    /* HPSS */
   status = close(fd) ;
#endif   /* else HPSS */
   rcode = ( status < 0 ) ? errno : 0 ;
   
   /* Close the data socket */
   if (data_sock >= 0) {
#if defined(_WIN32)
      if( closesocket(data_sock) == SOCKET_ERROR )
         log(LOG_DEBUG, "rclose64_v3: Error closing data socket fildesc=%d, errno=%d\n",
            data_sock, WSAGetLastError());
#else    /* WIN32 */
      if( close(data_sock) < 0 )
         log(LOG_DEBUG, "rclose64_v3 : Error closing data socket fildesc=%d,errno=%d\n",
            data_sock, errno);
#endif  /* else WIN32 */
      else
         log(LOG_DEBUG, "rclose64_v3 : closing data socket fildesc=%d\n", data_sock);
      data_sock = -1;
   }
   
   /* Issue accounting                                        */
#if defined(SACCT)
   rfioacct(RQST_CLOSE64_V3,0,0,s,0,0,status,rcode,&myinfo,NULL,NULL);
#endif /* SACCT */

   /* Send the answer to the client via ctrl_sock             */
   p= rqstbuf; 
   marshall_WORD(p, RQST_CLOSE64_V3);
   marshall_LONG(p, status);
   marshall_LONG(p, rcode);
   
   errno = ECONNRESET;
   if (netwrite_timeout(s, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT) != RQSTSIZE)  {
#if defined(_WIN32)
      log(LOG_ERR, "rclose64_v3: netwrite(): %s\n", geterr());
#else    /* WIN32 */
      log(LOG_ERR, "rclose64_v3: netwrite(): %s\n", strerror(errno));
#endif   /* else WIN32 */
      return -1 ;
   }
   
   /* If not HPSS close the ctrl_sock                         */
#if !defined(HPSS)
#if defined(_WIN32)
   if( closesocket(s) == SOCKET_ERROR )
      log(LOG_DEBUG, "rclose64_v3: Error closing control socket fildesc=%d, errno=%d\n",
         s, WSAGetLastError());
#else    /* WIN32 */
   if( close(s) < 0 )
      log(LOG_DEBUG, "rclose64_v3 : Error closing control socket fildesc=%d,errno=%d\n",
         s, errno);
#endif  /* else WIN32 */
   else
      log(LOG_DEBUG, "rclose64_v3 : closing ctrl socket fildesc=%d\n", s);
#endif /* HPSS */

   return status ;
}

#if !defined(HPSS)
static void *produce64_thread(int *ptr)
{        
   int      fd = *ptr;
   int      byte_read = -1;
   int      error = 0;
   off64_t  total_produced = 0;
   char     tmpbuf[21];

   while ((! error) && (byte_read != 0)) {
      if (Cthread_mutex_lock(&stop_read)) {
         log(LOG_ERR,"produce64_thread: Cannot get mutex : serrno=%d\n", serrno);
         return(NULL);
      }
      if (stop_read)
         return (NULL);
      if (Cthread_mutex_unlock(&stop_read)) {
         log(LOG_ERR,"produce64_thread: Cannot release mutex : serrno=%d\n", serrno);
         return(NULL);
      }
	   log(LOG_DEBUG, "produce64_thread: calling Csemaphore_down(&empty64)\n");
      Csemaphore_down(&empty64);
      
	  log(LOG_DEBUG, "produce64_thread: read() at %s:%d\n", __FILE__, __LINE__);
      byte_read = read(fd,array[produced64 % daemonv3_rdmt_nbuf].p,daemonv3_rdmt_bufsize);

      if (byte_read > 0) {
         total_produced += byte_read; 
         /* printf("Has read in buf %d (len %d)\n",produced64 % daemonv3_rdmt_nbuf,byte_read);  */
		 log(LOG_DEBUG, "Has read in buf %d (len %d)\n",produced64 % daemonv3_rdmt_nbuf,byte_read);
         array[produced64 % daemonv3_rdmt_nbuf].len = byte_read;
      }
      else {
         if (byte_read == 0)  {
            log(LOG_DEBUG,"produce64_thread: End of reading : total produced = %s,buffers=%d\n",
               u64tostr(total_produced,tmpbuf,0),produced64); 
            /* array[produced64 % daemonv3_rdmt_nbuf].p = NULL; */
            array[produced64 % daemonv3_rdmt_nbuf].len = 0;
         }
         else {
            array[produced64 % daemonv3_rdmt_nbuf].len = -errno; 
            error = -1;
         }
      }
      produced64++;
	  log(LOG_DEBUG, "produce64_thread: calling Csemaphore_up(&full64)\n");
      Csemaphore_up(&full64);
   }
   return(NULL);
}

static void *consume64_thread(int *ptr)
{        
   int      fd = *ptr;
   int      byte_written = -1;
   int      error = 0, end = 0;
   off64_t  total_consumed = 0;
   char     *buffer_to_write;
   int      len_to_write;
   int      saved_errno;
   char     tmpbuf[21];

   while ((! error) && (! end)) {
	  log(LOG_DEBUG, "consume64_thread: calling Csemaphore_down(&full64)\n");
      Csemaphore_down(&full64);
      
      buffer_to_write = array[consumed64 % daemonv3_wrmt_nbuf].p;
      len_to_write    = array[consumed64 % daemonv3_wrmt_nbuf].len;

      if (len_to_write > 0) {
         log(LOG_DEBUG,"consume64_thread: Trying to write %d bytes from %X\n",
            len_to_write, buffer_to_write);

         byte_written = write(fd, buffer_to_write, len_to_write);
         saved_errno = errno;
         log(LOG_DEBUG, "consume64_thread: succeeded to write %d bytes\n", byte_written);
         /* If the write is successfull but incomplete (fs is full) we
         report the ENOSPC error immediately in order to simplify the
         code */
         if ((byte_written >= 0) && (byte_written != len_to_write)) {
            byte_written = -1;
            saved_errno = errno = ENOSPC;
         }

         /* Error reporting to global var */
         if (byte_written == -1)   {
            error = 1;
            if (Cthread_mutex_lock(&write_error)) {
               log(LOG_ERR,"consume64_thread: Cannot get mutex : serrno=%d\n", serrno);
               return(NULL);
            }

            write_error = saved_errno;

            if (Cthread_mutex_unlock(&write_error)) {
               log(LOG_ERR,"consume64_thread: Cannot release mutex : serrno=%d\n", serrno);
               return(NULL);
            }
   
            log(LOG_DEBUG,"consume64_thread: Error when writing : buffers=%d, error=%d\n",
               consumed64,saved_errno); 
         }
         else {
            /* All bytes written to disks */
            total_consumed += byte_written; 
            log(LOG_DEBUG,"consume64_thread: Has written buf %d to disk (len %d)\n",
               consumed64 % daemonv3_wrmt_nbuf, byte_written); 
         }
      }  /* if  (len_to_write > 0) */
      else {
         if (len_to_write == 0) {
            log(LOG_DEBUG,"consume64_thread: End of writing : total consumed = %s, buffers=%d\n",
               u64tostr(total_consumed,tmpbuf,0), consumed64);  
            end = 1;
         }
         else {
            /* Error indicated by the thread reading from network, this thread just terminates */
            log(LOG_DEBUG,"consume64_thread: Error on reading : total consumed = %s, buffers=%d\n",
               u64tostr(total_consumed,tmpbuf,0), consumed64);  
            error = 1;
         }
      }  /* else  (len_to_write > 0) */
      
      consumed64++;
	  log(LOG_DEBUG, "consume64_thread: calling Csemaphore_up(&empty64)\n");
      Csemaphore_up(&empty64);
   }  /* End of while   */
   return(NULL);
}

static void wait_consumer64_thread(int cid)
{
   log(LOG_DEBUG,"wait_consumer64_thread: Entering wait_consumer64_thread\n");
   /* Indicate to the consumer thread that an error has occured */
   /* The consumer thread will then terminate */
   Csemaphore_down(&empty64);
   array[produced64 % daemonv3_wrmt_nbuf].len = -1;
   produced64++;
   Csemaphore_up(&full64);    

   log(LOG_INFO, "wait_consumer64_thread: Joining thread\n");
   if (Cthread_join(cid,NULL) < 0) {
      log(LOG_ERR,"wait_consumer64_thread: Error joining consumer thread after error in main thread, serrno=%d\n",serrno);
      return;
   }
}
#endif /* !HPSS */


/* This function is used when finding an error condition on read64_v3
   - Close the data stream
   - Trace statistics
*/
static int   readerror64_v3(s, infop)
#if defined(_WIN32)
SOCKET         s;
#else    /* WIN32 */
int            s;
#endif   /* else WIN32 */
struct rfiostat* infop;
{

char tmpbuf[21], tmpbuf2[21];
#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif
   
   /* We close the data socket                                        */
   if (data_sock >= 0) {
#if defined(_WIN32)
      if( closesocket(data_sock) == SOCKET_ERROR )
         log(LOG_DEBUG, "readerror64_v3: Error closing data socket fildesc=%d, errno=%d\n",
            data_sock, WSAGetLastError());
#else    /* WIN32 */
      if( close(data_sock) < 0 )
         log(LOG_DEBUG, "readerror64_v3 : Error closing data socket fildesc=%d,errno=%d\n",
            data_sock, errno);
#endif  /* else WIN32 */
      else
         log(LOG_DEBUG, "readerror64_v3 : closing data socket fildesc=%d\n", data_sock);
      data_sock = -1;
   }
   
   /* Issue statistic messages - There is no accounting record generated */
   log(LOG_INFO,
      "readerror64_v3(%d): %d read, %d readahead, %d write, %d flush, %d stat, %d lseek and %d lockf\n",
      s, infop->readop, infop->aheadop, infop->writop, infop->flusop, infop->statop,
      infop->seekop, infop->lockop); 
   log(LOG_INFO,"readerror64_v3(%d): %s bytes read and %s bytes written\n",
      s, u64tostr(infop->rnbr,tmpbuf,0), u64tostr(infop->wnbr,tmpbuf2,0)) ;
   
   /* Free allocated memory                                              */
#if !defined(HPSS)
   if (!daemonv3_rdmt) {
      log(LOG_DEBUG,"readerror64_v3: freeing iobuffer at 0X%X\n", iobuffer);
      if (iobufsiz > 0) {
         free(iobuffer);
         iobuffer = NULL;
         iobufsiz = 0;
      }
   }
   else {
      if (array) {
         int      el;
         for (el=0; el < daemonv3_rdmt_nbuf; el++) {
            log(LOG_DEBUG,"readerror64_v3: freeing array element %d at 0X%X\n", el,array[el].p);
            free(array[el].p);
            array[el].p = NULL;
         }
         log(LOG_DEBUG,"readerror64_v3: freeing array at 0X%X\n", array);
         free(array);
         array = NULL;
      }
   }
#else    /* HPSS */
   log(LOG_DEBUG,"readerror64_v3: freeing iobuffer at 0X%X\n", iobuffer);
   if (iobufsiz > 0) {
      free(iobuffer);
      iobuffer = NULL;
      iobufsiz = 0;
   }
#endif   /* else HPSS */
    
   return 0;
}

#if defined(_WIN32)
int srread64_v3(s, infop, fd)
SOCKET         s;
#else    /* WIN32 */
#if defined(HPSS)
int srread64_v3(s, infop, fd)
int            s;
#else    /* HPSS */
int srread64_v3(ctrl_sock, infop, fd)
int            ctrl_sock;
#endif   /* else HPSS  */
#endif   /* else WIN32 */
int            fd;
struct rfiostat* infop;
{
   int         status;              /* Return code                */
   int         rcode;               /* To send back errno         */
   int         how;                 /* lseek mode                 */
   off64_t     offset;              /* lseek offset               */
   off64_t     offsetout;           /* lseek offset               */
   int         size;                /* Requested write size       */
   char        *p;                  /* Pointer to buffer          */
#if !defined(_WIN32) && !defined(HPSS)
   char        *iobuffer;
#endif
   off64_t     bytes2send;
   fd_set      fdvar, fdvar2;
   extern int  max_sndbuf;
   int         optlen;
   struct stat64 st;
   char        rfio_buf[BUFSIZ];
   int         eof_met;
   int         join_done;
#if defined(HPSS)
   extern int  DISKBUFSIZE_READ;
#else    /* HPSS */
   int         DISKBUFSIZE_READ = (1 * 1024 * 1024); 
#endif   /* else HPSS */
   int         n;
   int         cid1;
   int         el;
   char        tmpbuf[21];

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);

   ctrl_sock = s;
#endif   /* WIN32 */
#if defined(HPSS)
   ctrl_sock = s;
#endif   /* HPSS */
   /*
    * Receiving request,
    */
   log(LOG_DEBUG, "rread64_v3(%d,%d)\n",ctrl_sock, fd);
  
   if (first_read) {
      char *p;
      first_read = 0;
      eof_met = 0;
      join_done = 0;

#if !defined(HPSS)
      if( (p = getconfent("RFIO", "DAEMONV3_RDSIZE", 0)) != NULL ) {
         if (atoi(p) > 0)
           DISKBUFSIZE_READ = atoi(p);
      }

      daemonv3_rdmt = DAEMONV3_RDMT;
      if( (p = getconfent("RFIO", "DAEMONV3_RDMT", 0)) != NULL )
         if (*p == '0')
            daemonv3_rdmt = 0;
         else
            daemonv3_rdmt = 1;

      daemonv3_rdmt_nbuf = DAEMONV3_RDMT_NBUF;
      if( (p = getconfent("RFIO", "DAEMONV3_RDMT_NBUF", 0)) != NULL )
         if (atoi(p) > 0)
            daemonv3_rdmt_nbuf = atoi(p);

      daemonv3_rdmt_bufsize = DAEMONV3_RDMT_BUFSIZE;
      if( (p = getconfent("RFIO", "DAEMONV3_RDMT_BUFSIZE", 0)) != NULL )
         if (atoi(p) > 0)
            daemonv3_rdmt_bufsize = atoi(p);

      log(LOG_DEBUG,
         "rread64_v3(%d) : daemonv3_rdmt=%d,daemonv3_rdmt_nbuf=%d,daemonv3_rdmt_bufsize=%d\n",
         ctrl_sock, daemonv3_rdmt,daemonv3_rdmt_nbuf,daemonv3_rdmt_bufsize);

      if (daemonv3_rdmt) {
         /* Indicates we are using RFIO V3 and multithreadding while reading */
         myinfo.aheadop = 3;
         /* Allocating circular buffer itself */
         log(LOG_DEBUG, "rread64_v3: allocating circular buffer : %d bytes\n",
            sizeof(struct element) * daemonv3_rdmt_nbuf);
         array = (struct element *)malloc(sizeof(struct element) * daemonv3_rdmt_nbuf);
         if (array == NULL)  {
            log(LOG_ERR, "rread64_v3: malloc array: ERROR occured (errno=%d)\n", errno);
            readerror64_v3(ctrl_sock, &myinfo);
            return -1 ;
         }
         log(LOG_DEBUG, "rread64_v3: malloc array allocated : 0X%X\n", array);      

         /* Allocating memory for each element of circular buffer */
         for (el=0; el < daemonv3_rdmt_nbuf; el++) { 
            log(LOG_DEBUG, "rread64_v3: allocating circular buffer element %d: %d bytes\n",
               el, daemonv3_rdmt_bufsize);
            if ((array[el].p = (char *)malloc(daemonv3_rdmt_bufsize)) == NULL)  {
               log(LOG_ERR, "rread64_v3: malloc array element %d, size %d: ERROR %d occured\n",
                  el, daemonv3_rdmt_bufsize, errno);
               readerror64_v3(ctrl_sock, &myinfo);
               return -1 ;
            }
            log(LOG_DEBUG, "rread64_v3: malloc array element %d allocated : 0X%X\n",
               el, array[el].p);      
         }
      }
      else {
         log(LOG_DEBUG, "rread64_v3: allocating malloc buffer : %d bytes\n", DISKBUFSIZE_READ);
         if ((iobuffer = (char *)malloc(DISKBUFSIZE_READ)) == NULL)  {
            log(LOG_ERR, "rread64_v3: malloc: ERROR occured (errno=%d)\n", errno);
            readerror64_v3(ctrl_sock, &myinfo);
            return -1 ;
         }
         log(LOG_DEBUG, "rread64_v3: malloc buffer allocated : 0X%X\n", iobuffer);      
         iobufsiz = DISKBUFSIZE_READ;
      }
      
#else    /* !HPSS */
      /*
       * For HPSS we reuse the buffer defined in the global structure
       * for this thread rather than reserving a new local buffer.
       */
      if ( iobufsiz>0 && iobufsiz < DISKBUFSIZE_READ) {
         free(iobuffer);
         iobufsiz = 0;
         iobuffer = NULL;
      }
      if ( iobufsiz <= 0 ) {
         log(LOG_DEBUG, "rread64_v3: allocating malloc buffer : %d bytes\n", DISKBUFSIZE_READ);
         if ((iobuffer = (char *)malloc(DISKBUFSIZE_READ)) == NULL)  {
            log(LOG_ERR, "rread64_v3: malloc: ERROR occured (errno=%d)\n", errno);
            readerror64_v3(ctrl_sock, &myinfo);
            return -1 ;
         }
         log(LOG_DEBUG, "rread64_v3: malloc buffer allocated : 0X%X\n", iobuffer);      
         iobufsiz = DISKBUFSIZE_READ;
      }
#endif   /* else !HPSS */

      if (fstat64(fd,&st) < 0) {
         log(LOG_ERR, "rread64_v3: fstat(): ERROR occured (errno=%d)\n", errno);
         readerror64_v3(ctrl_sock, &myinfo);
         return -1 ;
      }
      
      log(LOG_DEBUG, "rread64_v3: filesize : %s bytes\n", u64tostr(st.st_size,tmpbuf,0));
      offsetout = lseek64(fd,0L,SEEK_CUR);
      if (offsetout == (off64_t)-1) {
#if defined(_WIN32)
         log(LOG_ERR, "rread64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd, geterr());
#else        
         log(LOG_ERR, "rread64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd, strerror(errno)) ;
#endif   /* WIN32 */
         readerror64_v3(ctrl_sock, &myinfo);
         return -1 ;
      }
      bytes2send = st.st_size - offsetout;
      if (bytes2send < 0) bytes2send = 0;
      log(LOG_DEBUG, "rread64_v3: %s bytes to send (offset taken into account)\n",
         u64tostr(bytes2send,tmpbuf,0));
      p = rfio_buf;
      marshall_WORD(p,RQST_READ64_V3);
      marshall_HYPER(p,bytes2send);

      log(LOG_DEBUG, "rread64_v3: sending %d bytes\n", RQSTSIZE);
      errno = ECONNRESET;
      n = netwrite_timeout(ctrl_sock, rfio_buf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
      if (n != RQSTSIZE) {
#if defined(_WIN32)
         log(LOG_ERR, "rread64_v3: netwrite() ERROR: %s\n", geterr());
#else    /* WIN32 */
         log(LOG_ERR, "rread64_v3: netwrite() ERROR: %s\n", strerror(errno)) ;
#endif   /* else WIN32 */
         readerror64_v3(ctrl_sock, &myinfo);
         return -1 ;
      }

#ifdef CS2
      if (ioctl(fd,NFSIOCACHE,CLIENTNOCACHE) < 0)
         log(LOG_ERR, "rread64_v3: ioctl client nocache error occured (errno=%d)\n", errno);
      
      if (ioctl(fd,NFSIOCACHE,SERVERNOCACHE) < 0) 
         log(LOG_ERR, "rread64_v3: ioctl server nocache error occured (errno=%d)\n", errno);
      log(LOG_DEBUG,"rread64_v3: CS2: clientnocache, servernocache\n");
#endif

#if !defined(HPSS)
      if (daemonv3_rdmt) {
         Csemaphore_init(&empty64,daemonv3_rdmt_nbuf);
         Csemaphore_init(&full64,0);

         if ((cid1 = Cthread_create((void *(*)(void *))produce64_thread,(void *)&fd)) < 0) {
            log(LOG_ERR,"rread64_v3: Cannot create producer thread : serrno=%d,errno=%d\n",
               serrno, errno);
            readerror64_v3(ctrl_sock, &myinfo);
            return(-1);       
         }
      }
#endif /* !HPSS */
   }  /* end of if (first_read)  */
  
   /*
    * Reading data from the network.
    */
   while (1) 
   {
      struct timeval t;
      fd_set *write_fdset;
      
      FD_ZERO(&fdvar);
      FD_SET(ctrl_sock, &fdvar);
      
      FD_ZERO(&fdvar2);
      if (data_sock >= 0) FD_SET(data_sock, &fdvar2);
      
      t.tv_sec = 10;
      t.tv_usec = 0;
      
      if (eof_met)
         write_fdset = NULL;
      else
         write_fdset = &fdvar2;
      
      log(LOG_DEBUG,"srread64_v3: doing select\n") ;
#if defined(_WIN32)
      if( select(FD_SETSIZE, &fdvar, write_fdset, NULL, &t) == SOCKET_ERROR ) {
         log(LOG_ERR, "srread64_v3: select failed: %s\n", geterr());
         return -1;
      }
#else      
      if( select(FD_SETSIZE, &fdvar, write_fdset, NULL, &t) < 0 ) {
         log(LOG_ERR, "srread64_v3: select failed: %s\n", strerror(errno));
         readerror64_v3(ctrl_sock, &myinfo);
         return -1;
      }
#endif
      
      if( FD_ISSET(ctrl_sock, &fdvar) )  {
         int n, magic, code;
         
         /* Something received on the control socket */
         log(LOG_DEBUG, "srread64_v3: ctrl socket: reading %d bytes\n", RQSTSIZE) ;
         errno = ECONNRESET;
         n = netread_timeout(ctrl_sock,rqstbuf,RQSTSIZE,RFIO_CTRL_TIMEOUT);
         if (n != RQSTSIZE) {
#if defined(_WIN32)
            log(LOG_ERR, "srread64_v3: read ctrl socket %d: netread(): %s\n",
               ctrl_sock, geterr());
#else           
            log(LOG_ERR, "srread64_v3: read ctrl socket %d: read(): %s\n",
               ctrl_sock, strerror(errno));
#endif
            readerror64_v3(ctrl_sock, &myinfo);
            return -1 ;
         }
         p = rqstbuf ; 
         unmarshall_WORD(p,magic) ;
         unmarshall_WORD(p,code) ;
         
         /* what to do ? */
         if (code == RQST_CLOSE64_V3)  {
            log(LOG_DEBUG,"srread64_v3: close request: magic: %x code: %x\n", magic, code);
#if !defined(HPSS)
            if (!daemonv3_rdmt) {
               log(LOG_DEBUG,"srread64_v3: freeing iobuffer at 0X%X\n", iobuffer);
               if (iobufsiz > 0) {
                  free(iobuffer);
                  iobufsiz = 0;
                  iobuffer = NULL;
               }
            }
            else {
               if(!join_done) {
                  if (Cthread_mutex_lock(&stop_read)) {
                     log(LOG_ERR,"srread64_v3: Cannot get mutex : serrno=%d\n", serrno);
                     return(-1);
                  }
                  stop_read = 1;
                  if (Cthread_mutex_unlock(&stop_read)) {
                     log(LOG_ERR,"srread64_v3: Cannot release mutex : serrno=%d\n", serrno);
                     return(-1);
                  }
                  Csemaphore_up(&empty64);
                  if (Cthread_join(cid1,NULL) < 0) {       
                     log(LOG_ERR,"srread64_v3: Error joining producer, serrno=%d\n", serrno);
                     readerror64_v3(ctrl_sock, &myinfo);
                     return(-1);
                  }
               }
               for (el=0; el < daemonv3_rdmt_nbuf; el++) {
                  log(LOG_DEBUG,"srread64_v3: freeing array element %d at 0X%X\n", el,array[el].p);
                  free(array[el].p);
                  array[el].p = NULL;
               }
               log(LOG_DEBUG,"srread64_v3: freeing array at 0X%X\n", array);
               free(array);
               array = NULL;
            }
#else    /* HPSS */
            log(LOG_DEBUG,"srread64_v3: freeing iobuffer at 0X%X\n", iobuffer);
            if (iobufsiz > 0) {
               free(iobuffer);
               iobufsiz = 0;
               iobuffer = NULL;
            }
#endif   /* else HPSS */
            srclose64_v3(ctrl_sock,&myinfo,fd);
            return 0;
         }
         else  {
            log(LOG_ERR,"srread64_v3: unknown request:  magic: %x code: %x\n", magic,code);
            readerror64_v3(ctrl_sock, &myinfo);
            return(-1);
         }
      }  /* if( FD_ISSET(ctrl_sock, &fdvar) ) */
      
      /*
       * Reading data on disk.
       */
      
      if( !eof_met && data_sock >= 0 && (FD_ISSET(data_sock, &fdvar2)) )  {
#if defined(HPSS)
         status = rhpss_read(fd,iobuffer,DISKBUFSIZE_READ,s,0,0);
#else    /* HPSS */
         if (daemonv3_rdmt) {       
            Csemaphore_down(&full64);

            if (array[consumed64 % daemonv3_rdmt_nbuf].len > 0) {
               iobuffer = array[consumed64 % daemonv3_rdmt_nbuf].p;
               status = array[consumed64 % daemonv3_rdmt_nbuf].len;
            }
            else
               if (array[consumed64 % daemonv3_rdmt_nbuf].len == 0) {
                  status = 0;
                  iobuffer = NULL;
                  log(LOG_DEBUG,"srread64_v3: Waiting for producer thread\n");
                  if (Cthread_join(cid1,NULL) < 0) {       
                     log(LOG_ERR,"srread64_v3: Error joining producer, serrno=%d\n", serrno);
                     readerror64_v3(ctrl_sock, &myinfo);
                     return(-1);
                  }
                  join_done = 1;
               }
            else
               if (array[consumed64 % daemonv3_rdmt_nbuf].len < 0) {
                  status = -1;
                  errno = -(array[consumed64 % daemonv3_rdmt_nbuf].len);
                }
            consumed64++;
         }
         else
            status = read(fd,iobuffer,DISKBUFSIZE_READ);
#endif   /* else HPSS */

         rcode = (status < 0) ? errno:0;
         log(LOG_DEBUG, "srread64_v3: %d bytes have been read on disk\n", status) ;

         if (status == 0)  {
#if !defined(HPSS)
			 if (daemonv3_rdmt) {
				 log(LOG_DEBUG, "srread64_v3: calling Csemaphore_up(&empty64)\n");
               Csemaphore_up(&empty64);
			 }
#endif
            eof_met = 1;
            p = rqstbuf;
            marshall_WORD(p,REP_EOF) ;                     
            log(LOG_DEBUG, "rread64_v3: eof\n") ;
            errno = ECONNRESET;
            n = netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
            if (n != RQSTSIZE)  {
#if defined(_WIN32)
               log(LOG_ERR,"rread64_v3: netwrite(): %s\n", geterr());   
#else    /* WIN32 */          
               log(LOG_ERR,"rread64_v3: netwrite(): %s\n", strerror(errno)) ;
#endif   /* else WIN32 */              
               readerror64_v3(ctrl_sock, &myinfo);
               return -1 ; 
            }
         }  /*  status == 0 */
         else {
            if (status < 0)  {
#if !defined(HPSS)
               if (daemonv3_rdmt)
                  Csemaphore_up(&empty64);
#endif   /* !HPSS */
               p = rqstbuf;
               marshall_WORD(p, REP_ERROR);                     
               marshall_LONG(p, status);
               marshall_LONG(p, rcode);
               log(LOG_DEBUG, "rread64_v3: status %d, rcode %d\n", status, rcode) ;
               errno = ECONNRESET;
               n = netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
               if (n != RQSTSIZE)  {
#if defined(_WIN32)
                  log(LOG_ERR, "rread64_v3: netwrite(): %s\n", geterr()); 
#else                
                  log(LOG_ERR, "rread64_v3: netwrite(): %s\n", strerror(errno)) ;
#endif  /* WIN32 */
                  readerror64_v3(ctrl_sock, &myinfo);
                  return -1 ; 
               }
               log(LOG_DEBUG, "read64_v3: waiting ack for error\n");
               n = netread_timeout(ctrl_sock,rqstbuf,RQSTSIZE,RFIO_CTRL_TIMEOUT);
               if (n != RQSTSIZE) {
                  if (n == 0)  {
#if defined(_WIN32)
                     WSASetLastError(WSAECONNRESET);
#else    /* WIN32 */
                     errno = ECONNRESET;
#endif   /* else WIN32 */
                  }
#if defined(_WIN32)
                  log(LOG_ERR, "read64_v3: read ctrl socket %d: read(): %s\n",
                     ctrl_sock, geterr());
#else    /* WIN32 */
                  log(LOG_ERR, "read64_v3: read ctrl socket %d: read(): %s\n",
                     ctrl_sock, strerror(errno));
#endif   /* else WIN32 */
                  readerror64_v3(ctrl_sock, &myinfo);
                  return -1;
               }  /* n != RQSTSIZE */
               return(-1);
            }  /* status < 0  */
            else  {
               /* status > 0, reading was succesfully */
               log(LOG_DEBUG, "rread64_v3: writing %d bytes to data socket %d\n", status, data_sock) ;
#if defined(_WIN32)
               WSASetLastError(WSAECONNRESET);
               if( (n = send(data_sock, iobuffer, status, 0)) != status )  {
                  log(LOG_ERR, "rread64_v3: send() (to data sock): %s\n", geterr() );
                  readerror64_v3(ctrl_sock, &myinfo);
                  return -1;
               }
#else    /* WIN32 */
               errno = ECONNRESET;  
               if( (n = netwrite(data_sock, iobuffer, status)) != status ) {
                  log(LOG_ERR, "rread64_v3: netwrite(): %s\n", strerror(errno));
                  readerror64_v3(ctrl_sock, &myinfo);
                  return -1 ; 
               }
#endif   /* else WIN32 */
#if !defined(HPSS)
               if (daemonv3_rdmt)
                  Csemaphore_up(&empty64);
#endif   /* HPSS */
               myinfo.rnbr += status;
               myinfo.readop++;
            }  /* else status < 0 */
         }  /* else status == 0 */
      }  /* if( !eof_met && (FD_ISSET(data_sock, &fdvar2)) ) */
   }  /* while (1) */
}

/* This function is used when finding an error condition on write64_v3
   - send a REP on the control stream
   - empty the DATA stream
   - wait for acknowledge on control stream
*/
static int   writerror64_v3(s, rcode, infop)
#if defined(_WIN32)
SOCKET         s;
#else    /* WIN32 */
int            s;
#endif   /* else WIN32 */
int            rcode;  
struct rfiostat   *infop;
{
   char        *p;            /* Pointer to buffer          */
   int         status;
   int         n;
   int         el;
   fd_set      fdvar2;
   struct      timeval t;
   int         sizeofdummy;
   char        tmpbuf[21], tmpbuf2[21];
   /*
    * Put dummy on heap to avoid large arrays in thread stack
    */
   unsigned    char *dummy = NULL;

#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif
   
   /* Send back error message  */
   status = -1;
   p = rqstbuf;
   marshall_WORD(p, REP_ERROR);                     
   marshall_LONG(p, status);
   marshall_LONG(p, rcode);
   log(LOG_ERR, "writerror64_v3: status %d, rcode %d (%s)\n", status, rcode, strerror(rcode));
   errno = ECONNRESET;
   n = netwrite_timeout(s, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
   if (n != RQSTSIZE)  {
#if defined(_WIN32)
      log(LOG_ERR, "writerror64_v3: write(): %s\n", ws_strerr(errno));
#else                
      log(LOG_ERR, "writerror64_v3: write(): %s\n", strerror(errno)) ;
#endif
      /* Consumer thread already exited after error */
   }

   /*
    * To avoid overflowing the local thread stack we must
    * put dummy on heap
    */
   sizeofdummy = 256 * 1024;
   dummy = (unsigned char *)malloc(sizeof(unsigned char) * sizeofdummy);
   if (dummy == NULL)
      log(LOG_ERR, "writerror64_v3: malloc(%d) for dummy: %s\n",
         sizeofdummy, strerror(errno)) ;

   /* 
    There is a potential deadlock here since the client may be stuck
    in netwrite (cf rfio_write64_v3), trying to write data on the data 
    socket while both socket buffers (client + server) are full.
    To avoid this problem, we empty the data socket while waiting
    for the ack to be received on the control socket
    */

   while (dummy)  {
      FD_ZERO(&fdvar2);
      FD_SET(s, &fdvar2);
      if (data_sock >= 0) FD_SET(data_sock, &fdvar2);
        
      t.tv_sec = 1;
      t.tv_usec = 0;
        
      log(LOG_DEBUG,"writerror64_v3: doing select after error writing on disk\n") ;
#if defined(_WIN32)
      if( select(FD_SETSIZE, &fdvar2, NULL, NULL, &t) == SOCKET_ERROR ) 
#else
      if( select(FD_SETSIZE, &fdvar2, NULL, NULL, &t) < 0 )
#endif
      {
#if defined(_WIN32)
         errno = WSAGetLastError();
#endif
         log(LOG_ERR, "writerror64_v3: select fdvar2 failed (errno=%d)\n", errno) ;
         /* Consumer thread already exited after error */
         break ;
      }
         
      if ( FD_ISSET(s, &fdvar2) )  {
         /* The ack has been received on the control socket */
           
         log(LOG_DEBUG, "writerror64_v3: waiting ack for error\n");
         n = netread_timeout(s, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
         if (n != RQSTSIZE)  {
            /* Connexion dropped (n==0) or some another error */
            if (n == 0)  errno = ECONNRESET;
#if defined(_WIN32)
            log(LOG_ERR, "writerror64_v3: read ctrl socket: read(): %s\n", ws_strerr(errno));  
#else                        
            log(LOG_ERR, "writerror64_v3: read ctrl socket: read(): %s\n", strerror(errno));
#endif                        
            /* Consumer thread already exited after error */
            break;
         }
         
         /* We have got the reply    */
         break;
      }  /* if ( FD_ISSET(ctrl_sock, &fdvar2) ) */

      if (data_sock >= 0 && FD_ISSET(data_sock, &fdvar2))  {
         /* Read as much data as possible from the data socket */
           
         log(LOG_DEBUG, "writerror64_v3: emptying data socket (last disk write)\n");
#if defined(_WIN32)
         n = recv(data_sock, dummy, sizeofdummy, 0);
         if( (n == 0) || (n == SOCKET_ERROR) ) 
#else
         n = read(data_sock, dummy, sizeofdummy);
         if ( n <= 0 )
#endif  /* WIN32 */
         {
            /* Connexion dropped (n==0) or some another error */
            if (n == 0) errno = ECONNRESET;
#if defined(_WIN32)
            log(LOG_ERR, "writerror64_v3: read emptying data socket: recv(): %s\n",
               ws_strerr(errno));
#else                        
            log(LOG_ERR, "writerror64_v3: read emptying data socket: read(): %s\n",
               strerror(errno));
#endif       /* WIN32 */
            /* Consumer thread already exited after error     */
            break;
         }
         log(LOG_DEBUG, "writerror64_v3: emptying data socket, %d bytes read\n", n);
      }  /* if (FD_ISSET(data_sock,&fdvar2)) */
   }  /* End of while (1) : drop the data */
   
   /* Close the data socket                                   */
   if (data_sock >= 0) {
#if defined(_WIN32)
      if( closesocket(data_sock) == SOCKET_ERROR )
         log(LOG_DEBUG, "writerror64_v3: Error closing data socket fildesc=%d, errno=%d\n",
            data_sock, WSAGetLastError());
#else    /* WIN32 */
      if( close(data_sock) < 0 )
         log(LOG_DEBUG, "writerror64_v3 : Error closing data socket fildesc=%d,errno=%d\n",
            data_sock, errno);
#endif  /* else WIN32 */
      else
         log(LOG_DEBUG, "writerror64_v3 : closing data socket fildesc=%d\n", data_sock);
      data_sock = -1;
   }
   
   /* Issue statistic messages - There is no accounting record generated */
   log(LOG_INFO,
      "writerror64_v3(%d): %d read, %d readahead, %d write, %d flush, %d stat, %d lseek and %d lockf\n",
      s, infop->readop, infop->aheadop, infop->writop, infop->flusop, infop->statop,
      infop->seekop, infop->lockop); 
   log(LOG_INFO,"writerror64_v3(%d): %s bytes read and %s bytes written\n",
      s, u64tostr(infop->rnbr,tmpbuf,0), u64tostr(infop->wnbr,tmpbuf2,0)) ;
   
   /* free the allocated ressources     */
   free(dummy);
   dummy = NULL;
   
#if !defined(HPSS)
   if (!daemonv3_wrmt) {
      log(LOG_DEBUG,"writerror64_v3: freeing iobuffer at 0X%X\n", iobuffer);
      free(iobuffer);
      iobuffer = NULL;
      iobufsiz = 0;
   }
   else {
      if (array) {
         for (el=0; el < daemonv3_wrmt_nbuf; el++) {
            log(LOG_DEBUG,"writerror64_v3: freeing array element %d at 0X%X\n",
               el,array[el].p);
            free(array[el].p);
            array[el].p = NULL;
         }
         log(LOG_DEBUG,"writerror64_v3: freeing array at 0X%X\n", array);
         free(array);
         array = NULL;
      }
   }
#endif   /* !HPSS */
   return 0;
}

int srwrite64_v3(s, infop, fd)
#if defined(_WIN32)
SOCKET         s;   
#else
int            s;
#endif
int            fd;
struct rfiostat   *infop;
{
   int         status;        /* Return code                */
   int         rcode;         /* To send back errno         */
   int         how;           /* lseek mode                 */
   int         offset;        /* lseek offset               */
   int         size;          /* Requested write size       */
   char        *p;            /* Pointer to buffer          */
#if !defined(_WIN32) && !defined(HPSS)
   char        *iobuffer;
#endif   
   fd_set      fdvar;
   off64_t     byte_written_by_client = 0;
   int         eof_received       = 0;
   int         invalid_received   = 0;
   extern int  max_rcvbuf;
   int         maxseg;
#if defined(_AIX)
   socklen_t optlen;
#else
   int    optlen;
#endif
   int         byte_in_diskbuffer = 0;
   char        *iobuffer_p;
   int         max_rcv_wat;
   struct      timeval t;
#if defined(HPSS)
   extern int  DISKBUFSIZE_WRITE;
#else /* HPSS */
   int         DISKBUFSIZE_WRITE = (1*1024*1024); 
#endif /* HPSS */
   int         el;
   int         cid2;
   int         saved_errno;
   char        tmpbuf[21], tmpbuf2[21];
#if defined(_WIN32)
   struct thData *td;
   td = (struct thData*)TlsGetValue(tls_i);
#endif

   /*
    * Receiving request,
    */
   log(LOG_DEBUG, "rwrite64_v3(%d, %d)\n",s, fd);
   if( first_write )  {
      char *p;
      
      first_write = 0;
      rfio_handle_firstwrite(handler_context);
#if !defined(HPSS)
      if ((p = getconfent("RFIO","DAEMONV3_WRSIZE",0)) != NULL)  {
         if (atoi(p) > 0)
            DISKBUFSIZE_WRITE = atoi(p);
      }
#endif /* HPSS */

#if !defined(HPSS)
      daemonv3_wrmt = DAEMONV3_WRMT;
      if( (p = getconfent("RFIO", "DAEMONV3_WRMT", 0)) != NULL )
         if (*p == '0')
            daemonv3_wrmt = 0;
         else
            daemonv3_wrmt = 1;

      daemonv3_wrmt_nbuf = DAEMONV3_WRMT_NBUF;
      if( (p = getconfent("RFIO", "DAEMONV3_WRMT_NBUF", 0)) != NULL )
         if (atoi(p) > 0)
            daemonv3_wrmt_nbuf = atoi(p);

      daemonv3_wrmt_bufsize = DAEMONV3_WRMT_BUFSIZE;
      DISKBUFSIZE_WRITE = DAEMONV3_WRMT_BUFSIZE;
      if( (p = getconfent("RFIO", "DAEMONV3_WRMT_BUFSIZE", 0)) != NULL )
         if (atoi(p) > 0) {
            daemonv3_wrmt_bufsize = atoi(p);
            DISKBUFSIZE_WRITE = atoi(p);
         }

      log(LOG_DEBUG,
         "rwrite64_v3: daemonv3_wrmt=%d,daemonv3_wrmt_nbuf=%d,daemonv3_wrmt_bufsize=%d\n",
         daemonv3_wrmt,daemonv3_wrmt_nbuf,daemonv3_wrmt_bufsize);

      if (daemonv3_wrmt) {
         /* Indicates we are using RFIO V3 and multithreading while writing */
         myinfo.aheadop = 3;
         /* Allocating circular buffer itself */
         log(LOG_DEBUG, "rwrite64_v3: allocating circular buffer: %d bytes\n",
            sizeof(struct element) * daemonv3_wrmt_nbuf);
         array = (struct element *)malloc(sizeof(struct element) * daemonv3_wrmt_nbuf);
         if (array == NULL)  {
            log(LOG_ERR, "rwrite64_v3: malloc array: ERROR occured (errno=%d)\n", errno);
            return -1 ;
         }
         log(LOG_DEBUG, "rwrite64_v3: malloc array allocated at 0X%X\n", array);      

         /* Allocating memory for each element of circular buffer */
         for (el=0; el < daemonv3_wrmt_nbuf; el++) { 
            log(LOG_DEBUG, "rwrite64_v3: allocating circular buffer element %d: %d bytes\n",
               el,daemonv3_wrmt_bufsize);
            if ((array[el].p = (char *)malloc(daemonv3_wrmt_bufsize)) == NULL)  {
               log(LOG_ERR, "rwrite64_v3: malloc array element %d: ERROR occured (errno=%d)\n",
                  el, errno);
               return -1 ;
            }
            log(LOG_DEBUG, "rwrite64_v3: malloc array element %d allocated at 0X%X\n",
               el, array[el].p);      
         }
      }
#endif   /* !HPSS */
      
#if defined(HPSS)
      /*
       * For HPSS we reuse the buffer defined in the global structure
       * for this thread rather than reserving a new local buffer.
       */
      if ( iobufsiz>0 && iobufsiz<DISKBUFSIZE_WRITE) {
         free(iobuffer);
         iobufsiz = 0;
         iobuffer = NULL;
      }
      if ( iobufsiz <= 0 ) {
         log(LOG_DEBUG, "rwrite64_v3: allocating malloc buffer: %d bytes\n", DISKBUFSIZE_WRITE);
         if ((iobuffer = (char *)malloc(DISKBUFSIZE_WRITE)) == NULL) {
            log(LOG_ERR, "rwrite64_v3: malloc: ERROR occured (errno=%d)\n", errno);
            return -1 ;
         }
         log(LOG_DEBUG, "rwrite64_v3: malloc buffer allocated at 0X%X\n", iobuffer);
         iobufsiz = DISKBUFSIZE_WRITE;
      }
#else    /* HPSS */

      /* Don't allocate this buffer if we are multithreaded */
      if (!daemonv3_wrmt) {
         log(LOG_DEBUG, "rwrite64_v3: allocating malloc buffer: %d bytes\n", DISKBUFSIZE_WRITE);
         if ((iobuffer = (char *)malloc(DISKBUFSIZE_WRITE)) == NULL) {
            log(LOG_ERR, "rwrite64_v3: malloc: ERROR occured (errno=%d)\n", errno);
            return -1 ;
         }
         log(LOG_DEBUG, "rwrite64_v3: malloc buffer allocated at 0X%X\n", iobuffer);
         iobufsiz = DISKBUFSIZE_WRITE;
      }
#endif   /* else HPSS */
      
      byte_in_diskbuffer = 0;
#if !defined(HPSS)
      if (daemonv3_wrmt)
         iobuffer_p = NULL; /* For safety */
      else
         iobuffer_p = iobuffer;
#else
      iobuffer_p = iobuffer;
#endif   /* else !HPSS */

#if !defined(_WIN32)
      optlen = sizeof(maxseg);
      if (getsockopt(data_sock,IPPROTO_TCP,TCP_MAXSEG,(char *)&maxseg,&optlen) < 0) {
         log(LOG_ERR, "rwrite64_v3: getsockopt: ERROR occured (errno=%d)\n", errno) ;
         return -1 ;
      }
      log(LOG_DEBUG,"rwrite64_v3: max TCP segment: %d\n", maxseg);
#endif       /* WIN32 */       
#ifdef CS2
      log(LOG_DEBUG,"rwrite64_v3: CS2: clientnocache, servercache\n");
      if (ioctl(fd,NFSIOCACHE,CLIENTNOCACHE) < 0)
         log(LOG_ERR, "rwrite64_v3: ioctl client nocache error occured (errno=%d)\n", errno);
      
      if (ioctl(fd,NFSIOCACHE,SERVERCACHE) < 0) 
         log(LOG_ERR, "rwrite64_v3: ioctl server cache error occured (errno=%d)\n", errno);
#endif

#if !defined(HPSS)
      if (daemonv3_wrmt) {
         Csemaphore_init(&empty64,daemonv3_wrmt_nbuf);
         Csemaphore_init(&full64,0);

         if ((cid2 = Cthread_create((void *(*)(void *))consume64_thread,(void *)&fd)) < 0) {
            log(LOG_ERR,"rwrite64_v3: Cannot create consumer thread : serrno=%d, errno=%d\n",
               serrno, errno);
            return(-1);       
         }
      }
#endif /* !HPSS */

   }  /* if (firstwrite) */
  
   /*
    * Reading data from the network.
    */
  
   while (1)  {
      FD_ZERO(&fdvar);
      FD_SET(ctrl_sock, &fdvar);
      if (data_sock >= 0) FD_SET(data_sock, &fdvar);
      
      t.tv_sec = 10;
      t.tv_usec = 0;
      
      log(LOG_DEBUG,"rwrite64_v3: doing select\n") ;
#if defined(_WIN32)
      if( select(FD_SETSIZE, &fdvar, NULL, NULL, &t) == SOCKET_ERROR ) {
         log(LOG_ERR, "rwrite64_v3: select: %s\n", geterr());
         if (daemonv3_wrmt)
            wait_consumer64_thread(cid2);
         return -1;
      }
#else    /* WIN32 */     
      if( select(FD_SETSIZE, &fdvar, NULL, NULL, &t) < 0 )  {
         log(LOG_ERR, "rwrite64_v3: select: %s\n", strerror(errno));
#if !defined(HPSS)
         if (daemonv3_wrmt)
            wait_consumer64_thread(cid2);
#endif   /* !HPSS */
         return -1 ;
      }
#endif   /* else WIN32 */
      
      /* Anything received on control socket ?  */
      if( FD_ISSET(ctrl_sock, &fdvar) )  {
         int  n, magic, code;
         
         /* Something received on the control socket */
         log(LOG_DEBUG, "rwrite64_v3: ctrl socket: reading %d bytes\n", RQSTSIZE) ;
         n = netread_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
         if ( n != RQSTSIZE )  {
            if (n == 0)  errno = ECONNRESET;
#if defined(_WIN32)
            log(LOG_ERR, "rwrite64_v3: read ctrl socket: netread(): %s\n", ws_strerr(errno));
#else
            log(LOG_ERR, "rwrite64_v3: read ctrl socket: netread(): %s\n", strerror(errno));
#endif      /* WIN32 */
#if !defined(HPSS)
            if (daemonv3_wrmt)
               wait_consumer64_thread(cid2);
#endif   /* !HPSS */
            return -1;
         }
         p = rqstbuf ; 
         unmarshall_WORD(p, magic) ;
         unmarshall_WORD(p, code) ;
         unmarshall_HYPER(p, byte_written_by_client);
         
         if (code == RQST_CLOSE64_V3) {
            log(LOG_DEBUG,"rwrite64_v3: close request:  magic: %x code: %x\n", magic, code) ;
            eof_received = 1;
         }
         else {
            log(LOG_ERR,"rwrite64_v3: unknown request:  magic: %x code: %x\n", magic, code) ;
            writerror64_v3(ctrl_sock, EINVAL, &myinfo);
            return -1;
         }
         
         log(LOG_DEBUG, "rwrite64_v3: data socket: read_from_net=%s, written_by_client=%s\n",
            u64tostr(byte_read_from_network,tmpbuf,0), u64tostr(byte_written_by_client,tmpbuf2,0));
        
      }  /* if (FD_ISSET(ctrl_sock, &fdvar)) */
         
         
      /* Anything received on data  socket ?  */
      if (data_sock >= 0 && FD_ISSET(data_sock, &fdvar)) {
         int n,can_be_read; 
      
#if !defined(HPSS)
         if ((daemonv3_wrmt) && (byte_in_diskbuffer == 0)) {
            log(LOG_DEBUG, "rwrite64_v3: Data received on data socket, new buffer %d requested\n",
               produced64 % daemonv3_wrmt_nbuf);
            Csemaphore_down(&empty64);
            iobuffer = iobuffer_p = array[produced64 % daemonv3_wrmt_nbuf].p;
         }
#endif   /* !HPSS */
         
         log(LOG_DEBUG,"rwrite64_v3: iobuffer_p = %X,DISKBUFSIZE_WRITE = %d, data socket = %d\n",
            iobuffer_p, DISKBUFSIZE_WRITE, data_sock);
#if defined(_WIN32)
         n = recv(data_sock, iobuffer_p, DISKBUFSIZE_WRITE-byte_in_diskbuffer, 0);
         if( (n == 0) || n == SOCKET_ERROR )
#else    
         n = read(data_sock, iobuffer_p, DISKBUFSIZE_WRITE-byte_in_diskbuffer);       
         if( n <= 0 )
#endif
         {
            if (n == 0) errno = ECONNRESET;
#if defined(_WIN32)
            log(LOG_ERR, "rwrite64_v3: read data socket: recv(): %s\n", ws_strerr(errno));
#else
            log(LOG_ERR, "rwrite64_v3: read data socket: read(): %s\n", strerror(errno));
#endif      /* WIN32 */              
#if !defined(HPSS)
            if (daemonv3_wrmt)
               wait_consumer64_thread(cid2);
#endif   /* !HPSS */
            return -1;
         }
      
         log(LOG_DEBUG, "rwrite64_v3: read data socket %d: %d bytes\n", data_sock, n);
         byte_read_from_network += n;
         byte_in_diskbuffer += n;
         iobuffer_p += n;
      }  /* if (FD_ISSET(data_sock,&fdvar)) */
 
      /*
       * Writing data on disk.
       */
      if (byte_in_diskbuffer && byte_in_diskbuffer == DISKBUFSIZE_WRITE ||
         (eof_received && byte_written_by_client == byte_read_from_network) )  {
         log(LOG_DEBUG, "rwrite64_v3: writing %d bytes on disk\n", byte_in_diskbuffer);
#if defined(HPSS)
         status = rhpss_write(fd, iobuffer, byte_in_diskbuffer, s, 0,0);
         /* If the write is successfull but incomplete (fs is full) we
            report the ENOSPC error immediately in order to simplify the
            code */
         if ((status > 0) && (status != byte_in_diskbuffer)) {
            status = -1;
            errno = ENOSPC;
         }
#else    /* HPSS */
         if (daemonv3_wrmt) {
            array[produced64 % daemonv3_wrmt_nbuf].len = byte_in_diskbuffer;
            produced64++;
            Csemaphore_up(&full64);
            
         }  /* if (daemonv3_wrmt) */
         else {
            status = write(fd, iobuffer, byte_in_diskbuffer);
            /* If the write is successfull but incomplete (fs is full) we
               report the ENOSPC error immediately in order to simplify the
               code */
            if ((status > 0) && (status != byte_in_diskbuffer))  {
               status = -1;
               errno = ENOSPC;
            }
         }

         /* Get the status value                                        */
         if (daemonv3_wrmt) {
            /* Caution: the write is done asynchroneously               */
            /* So the status is not related to the above write!         */
            
            /* Get the last write status within a mutex transaction     */
            if (Cthread_mutex_lock(&write_error)) {
               log(LOG_ERR,"rwrite64_v3: Cannot get mutex : serrno=%d\n", serrno);
               return(-1);
            }
            if (write_error) {
               status = -1;
               saved_errno = write_error;
            }
            else
               status = byte_in_diskbuffer;
           
            if (Cthread_mutex_unlock(&write_error)) {
               log(LOG_ERR,"rwrite64_v3: Cannot release mutex : serrno=%d\n", serrno);
               return(-1);
            }
            /* End of mutex transaction                                 */
         }
         if ((daemonv3_wrmt) && (status == -1)) errno = saved_errno;
#endif   /* elde HPSS */

         rcode = (status < 0) ? errno:0;
         if (status < 0)  {
            /* Error in writting buffer                                 */
            writerror64_v3(ctrl_sock, rcode, &myinfo );
            return -1;
         }  /* if (status < 0) */
         
         /* The data has be written on disk  */
         myinfo.wnbr += byte_in_diskbuffer;
         myinfo.writop++;
         byte_in_diskbuffer = 0;
         iobuffer_p = iobuffer;
      }  /* if (byte_in_diskbuffer == DISKBUFSIZE_WRITE || eof_received || ...) */
      
      /* Have we finished ?    */
      if ( eof_received && byte_written_by_client == myinfo.wnbr ) {
#if !defined(HPSS)
         if (!daemonv3_wrmt) {
            log(LOG_DEBUG,"rwrite64_v3: freeing iobuffer at 0X%X\n", iobuffer);
            free(iobuffer);
            iobuffer = NULL;
            iobufsiz = 0;
         }
         else {
	        /* Indicate to the consumer thread that writing is finished */
	        /* if the actual buffer wasn't empty                         */
            log(LOG_DEBUG, "rwrite64_v3: Terminates thread by null buffer %d requested\n",
               produced64 % daemonv3_wrmt_nbuf);
            Csemaphore_down(&empty64);
            array[produced64 % daemonv3_wrmt_nbuf].len = 0;
            produced64++;
            Csemaphore_up(&full64);
            
            /* Wait for consumer thread                                 */
            /* We can then safely catch deferred disk write errors      */
            log(LOG_INFO,"Joining thread\n");
            if (Cthread_join(cid2,NULL) < 0) {       
               log(LOG_ERR,"Error joining consumer, serrno=%d\n", serrno);
               return(-1);
            }
            
            /* Get the last write status within a mutex transaction     */
            if (Cthread_mutex_lock(&write_error)) {
               log(LOG_ERR,"rwrite64_v3: Cannot get mutex : serrno=%d\n", serrno);
               return(-1);
            }
            if (write_error) {
               status = -1;
               rcode  = write_error;
            }
            else  status = 0;
           
            if (Cthread_mutex_unlock(&write_error)) {
               log(LOG_ERR,"rwrite64_v3: Cannot release mutex : serrno=%d\n", serrno);
               return(-1);
            }
            /* End of mutex transaction                                 */
            
            /* If status is bad, bad news....                           */
            if (status < 0) {
               writerror64_v3(ctrl_sock, rcode, &myinfo );
               return -1;
            }  /* if (status < 0) */
            
            /* Free the buffers                                         */
            for (el=0; el < daemonv3_wrmt_nbuf; el++) {
               log(LOG_DEBUG,"rwrite64_v3: freeing array element %d at 0X%X\n", el, array[el].p);
               free(array[el].p);
               array[el].p = NULL;
            }
            log(LOG_DEBUG,"rwrite64_v3: freeing array at 0X%X\n", array);
            free(array);
            array = NULL;
         }
#endif /* !HPSS */
         /* Send back final status and close sockets... and return     */
         srclose64_v3(ctrl_sock, &myinfo, fd);
         return 0;
      }  /* if ( eof_received && byte_written_by_client == myinfo.wnbr ) */
      
   }  /* while (1)  */
}

