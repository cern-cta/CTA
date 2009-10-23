/*
 * rfio_call64.c,v 1.3 2004/03/22 12:34:03 jdurand Exp
 */

/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

/* NOTE(fuji): AIX might support O_DIRECT as well, to be verified. */
#if defined(linux) && !defined(__ia64__)
#define _GNU_SOURCE  /* O_DIRECT */
#endif

/*
 * Remote file I/O flags and declarations.
 */
#define DEBUG           0
#define RFIO_KERNEL     1
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <pwd.h>
#include <grp.h>
#include "common.h"
#include <zlib.h>
#include <attr/xattr.h>
#if defined(_WIN32)
#include "syslog.h"
#include "ws_errmsg.h"
#include <io.h>
#else
#include <sys/param.h>
#include <syslog.h>                     /* System logger                */
#include <sys/time.h>
#endif
#if defined(_AIX) || defined(hpux) || defined(SOLARIS) || defined(linux)
#include <signal.h>
#endif

#ifdef USE_XFSPREALLOC
#include "rfio_xfsprealloc.h"
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
#include "rfio_alignedbuf.h"
#endif

#ifdef CS2
#include <nfs/nfsio.h>
#endif

#include "rfio_callhandlers.h"
#include "rfioacct.h"
#include "checkkey.h"
#include "alrm.h"
#include <fcntl.h>

extern int forced_umask;
#define CORRECT_UMASK(this) (forced_umask > 0 ? forced_umask : this)
extern int ignore_uid_gid;

/* For multithreading stuff, only tested under Linux at present */
#include <Cthread_api.h>
#include "Csemaphore.h"

/* If DAEMONV3_RDMT is true, reading from disk will be multithreaded
   The circular buffer will have in this case DAEMONV3_RDMT_NBUF buffers of
   size DAEMONV3_RDMT_BUFSIZE. Defaults values are defined below */
#define DAEMONV3_RDMT (1)
#define DAEMONV3_RDMT_NBUF (4)
#define DAEMONV3_RDMT_BUFSIZE (256*1024)

#ifdef _WIN32
#define malloc_page_aligned(x) malloc(x)
#define free_page_aligned(x) free(x)
#endif

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
} *array = NULL;

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
static volatile int stop_read;

extern char *getconfent();
extern int  checkkey(int sock, u_short key);
extern int  check_user_perm(int *uid, int *gid, char *hostname, int *ptrcode, char *permstr);

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
  /* Context for the open/firstwrite/close handlers */
  void *handler_context;
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
#define handler_context td->handler_context

#else    /* WIN32 */
/*
 * Buffer declarations
 */
extern char     rqstbuf[BUFSIZ];       /* Request buffer               */
extern char     filename[MAXFILENAMSIZE];

static char     *iobuffer;             /* Data communication buffer    */
static int      iobufsiz;              /* Current io buffer size       */
#endif   /* else WIN32 */

#if defined(_WIN32)
#define ECONNRESET WSAECONNRESET
#endif   /* WIN32 */

extern int srchkreqsize _PROTO((SOCKET, char *, int));
extern char *forced_filename;
#define CORRECT_FILENAME(filename) (forced_filename != NULL ? forced_filename : filename)
extern const char *rfio_all_perms[];
extern int check_path_whitelist _PROTO((const char *, const char *, const char **, char *, size_t, int));

/* Warning : the new sequential transfer mode cannot be used with
   several files open at a time (because of those global variables)*/
#if !defined(_WIN32)    /* moved to global structure            */
static int data_sock;   /* Data socket                          */
static int ctrl_sock;   /* the control socket                   */
static int first_write;
static int first_read;
static off64_t          byte_read_from_network;
static struct rfiostat  myinfo;
/* Context for the open/firstwrite/close handlers */
extern void *handler_context;
#endif   /* WIN32 */


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
  char    *p;
  LONG    status = 0;
  LONG    len;
  int     op;
  off64_t siz;
  int     rcode = 0;
  LONG    how;
  off64_t offset;
  off64_t offsetout;
  char    tmpbuf[21];

  p = rqstbuf + (2*WORDSIZE);
  unmarshall_LONG(p, how);
  unmarshall_LONG(p, len);

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
    p = rqstbuf;
    unmarshall_LONG(p, op);
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
          u64tostr(offset,tmpbuf,0), how);
      infop->seekop++;
      if ( (offsetout= lseek64(fd,offset,how)) == -1 ) {
        rcode= errno;
        status = -1;
        log(LOG_ERR, "srlockf64(%d,%d): error %d in lseek64\n", s, fd, rcode);
      }
    }
    if (status == 0 ) {
      if ( (status = lockf64(fd, op, siz)) < 0 ) {
        rcode = errno;
        log(LOG_ERR, "srlockf64(%d,%d): error %d in lockf64(%d,%d,%s)\n",
            s, fd, rcode, fd, op, u64tostr(siz,tmpbuf,0));
      }
      else
        status = 0;
    }
  }

  p = rqstbuf;
  marshall_LONG(p, status);
  marshall_LONG(p, rcode);
#if defined(SACCT)
  rfioacct(RQST_LOCKF,-1,-1,s,op,(int)siz,status,rcode,NULL,NULL,NULL);
#endif /* SACCT */
  log(LOG_DEBUG, "srlockf64:  sending back status %d rcode %d\n", status, rcode);
  if (netwrite_timeout(s, rqstbuf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != (2*LONGSIZE))  {
    log(LOG_ERR, "srlockf64:  write(): %s\n", strerror(errno));
    return -1;
  }
  return status;
}
#endif  /* !WIN32 */

#if !defined(_WIN32)
int     srlstat64(s, rt, host)
     int     s;
     int     rt; /* Is it a remote site call ?   */
     char *host; /* Where the request comes from */
{
  char *   p;
  int    status = 0, rcode = 0;
  int    len;
  int    replen;
  struct stat64 statbuf;
  char   user[CA_MAXUSRNAMELEN+1];
  int    uid,gid;

  p= rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p,len);
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
    p= rqstbuf;
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
      int to_uid, to_gid;

      if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
        log(LOG_ERR, "srlstat64: get_user(): Error opening mapping file\n");
        status= -1;
        errno = EINVAL;
        rcode = errno;
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
            host, user, uid, gid, to, to_uid, to_gid);
        uid = to_uid;
        gid = to_gid;
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
        status = rcode;
      }
      else {
        char ofilename[MAXFILENAMSIZE];
        strcpy(ofilename, filename);
        if (forced_filename != NULL || !check_path_whitelist(host, filename, rfio_all_perms, ofilename, sizeof(ofilename),0)) {
          status= ( lstat64(CORRECT_FILENAME(ofilename), &statbuf) < 0 ) ? errno : 0;
        } else {
          status = errno;
        }
        log(LOG_INFO, "srlstat64: file: %s , status %d\n", CORRECT_FILENAME(filename), status);
      }
    }
  }

  p = rqstbuf;
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
    return -1;
  }
  return 0;
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
  char   *p;
  int    status = 0, rcode = 0;
  int    len;
  int    replen;
  struct stat64 statbuf;
  char   user[CA_MAXUSRNAMELEN+1];
  int    uid,gid;

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);
#endif
  p= rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p,len);
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
    p = rqstbuf;
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
      int to_uid, to_gid;

      if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
        log(LOG_ERR, "srstat64: get_user(): Error opening mapping file\n");
        status= -1;
        errno = EINVAL;
        rcode = errno;
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
            host, user, uid, gid, to, to_uid, to_gid);
        uid = to_uid;
        gid = to_gid;
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
        status = rcode;
      }
      else  {
        char ofilename[MAXFILENAMSIZE];
        strcpy(ofilename, filename);
        if (forced_filename != NULL || !check_path_whitelist(host, filename, rfio_all_perms, ofilename, sizeof(ofilename),1)) {
          status= ( stat64(CORRECT_FILENAME(ofilename), &statbuf) < 0 ) ? errno : 0;
        } else {
          status = errno;
        }
        log(LOG_INFO, "srstat64: file: %s for (%d,%d) status %d\n",
            CORRECT_FILENAME(filename), uid, gid, status);
      }
    }
  }

  p = rqstbuf;
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
    return -1;
  }
  return 0;
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
  int     fd;
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

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);
#endif

  /* Initialization of global variables */
  /* cf. initialization in sropen64_v3(); here we only
     initialise what we know about! */
  first_write = 1;
  first_read  = 1;

  p = rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p, len);
  if ( (status = srchkreqsize(s,p,len)) == -1 ) {
    rcode = errno;
  } else {
    /*
     * Reading open request.
     */
    log(LOG_DEBUG, "sropen64(%d): reading %d bytes\n", s, len);
    if ((status = netread_timeout(s,rqstbuf,len,RFIO_CTRL_TIMEOUT)) != len) {
#if defined(_WIN32)
      log(LOG_ERR, "sropen64: read(): %s\n", geterr());
#else
      log(LOG_ERR, "sropen64: read(): %s\n", strerror(errno));
#endif
      return -1;
    }
    p = rqstbuf;
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
      errno=EACCES;
      rcode=errno;
      status= -1;
    }

    if ( (status == 0) && rt ) {
      log(LOG_ALERT, "sropen64: connection %s mapping by %s(%d,%d) from %s",
          (mapping ? "with" : "without"), user, uid, gid, host);
    }

    /*
     * MAPPED mode: user will be mapped to user "to"
     */
    if ( !status && rt && mapping ) {
      char to[100];
      int rcd,to_uid,to_gid;

      log(LOG_DEBUG, "sropen64: Mapping (%s, %d, %d) \n", user, uid, gid );
      if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
        log(LOG_ERR, "sropen64: get_user() error opening mapping file\n");
        status = -1;
        errno = EINVAL;
        rcode = SEHOSTREFUSED;
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
            host, user, uid, gid, to, to_uid, to_gid);
        uid = to_uid;
        gid = to_gid;
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
      char * rtuser;
      if ( (rtuser=getconfent ("RTUSER","CHECK",0) ) == NULL || ! strcmp (rtuser,"YES") )
        {
          /* Port is also passwd */
#if defined(_WIN32)
          if( (sock = connecttpread(reqhost, passwd)) != INVALID_SOCKET
              && !checkkey(sock, passwd) )
#else
            if( (sock = connecttpread(reqhost, passwd)) >= 0 && !checkkey(sock, passwd) )
#endif
              {
                status= -1;
                errno = EACCES;
                rcode= errno;
                log(LOG_ERR, "sropen64: DIRECT mapping : permission denied\n");
              }
#if defined(_WIN32)
          if( sock == INVALID_SOCKET )
#else
            if( sock < 0 )
#endif
              {
                status= -1;
                log(LOG_ERR, "sropen64: DIRECT mapping failed: Couldn't connect %s\n", reqhost);
                rcode = EACCES;
              }
        }
      else
        log(LOG_INFO, "sropen64: Any DIRECT rfio request from out of site is authorized\n");
    }
    if ( !status ) {
      int rc;
      char *pfn = NULL;
      int need_user_check = 1;

      log(LOG_DEBUG, "sropen64: uid %d gid %d mask %o ftype %d flags 0%o mode 0%o\n",
          uid, gid, mask, ftype, flags, mode);
      log(LOG_DEBUG, "sropen64: account: %s\n", account);
      log(LOG_INFO, "sropen64: (%s,0%o,0%o) for (%d,%d)\n",
          CORRECT_FILENAME(filename), flags, mode, uid, gid);
      (void) umask((mode_t) CORRECT_UMASK(mask));

      rc = rfio_handle_open(CORRECT_FILENAME(filename),
                            ntohopnflg(flags),
                            mode,
                            uid,
                            gid,
                            &pfn,
                            &handler_context,
                            &need_user_check);
      if (rc < 0) {
        char alarmbuf[1024];
        sprintf(alarmbuf,"sropen64(): %s",CORRECT_FILENAME(filename));
        log(LOG_DEBUG, "sropen64: rfio_handler_open refused open: %s\n", sstrerror(serrno));
        rcode = serrno;
        rfio_alrm(rcode,alarmbuf);
      }

      if (need_user_check &&  ((status=check_user_perm(&uid,&gid,host,&rcode,(((ntohopnflg(flags)) & (O_WRONLY|O_RDWR)) != 0) ? "WTRUST" : "RTRUST")) < 0) &&
          ((status=check_user_perm(&uid,&gid,host,&rcode,"OPENTRUST")) < 0) ) {
        if (status == -2)
          log(LOG_ERR, "sropen64: uid %d not allowed to open64()\n", uid);
        else
          log(LOG_ERR, "sropen64: failed at check_user_perm(), rcode %d\n", rcode);
        status = -1;
      }
      else
        {
          const char *perm_array[3];
          char ofilename[MAXFILENAMSIZE];
          perm_array[0] = (((ntohopnflg(flags)) & (O_WRONLY|O_RDWR)) != 0) ? "WTRUST" : "RTRUST";
          perm_array[1] = "OPENTRUST";
          perm_array[2] = NULL;

          errno = 0;
          fd = -1;
          if (forced_filename!=NULL || !check_path_whitelist(host, pfn, perm_array, ofilename, sizeof(ofilename),1)) {
            fd = open64((forced_filename!=NULL)?pfn:ofilename, ntohopnflg(flags),
                        ((forced_filename != NULL) && (((ntohopnflg(flags)) & (O_WRONLY|O_RDWR)) != 0)) ? 0644 : mode);
            log(LOG_DEBUG, "sropen64: open64(%s,0%o,0%o) returned %x (hex)\n",
                CORRECT_FILENAME(filename), flags, mode, fd);
          }
          if (fd < 0) {
            char alarmbuf[1024];
            sprintf(alarmbuf,"sropen64: %s", CORRECT_FILENAME(filename));
            status= -1;
            rcode= errno;
            log(LOG_DEBUG, "sropen64: open64: %s\n", strerror(errno));
            rfio_alrm(rcode,alarmbuf);
          }
          else {
            /*
             * Getting current offset
             */
            offsetin = 0;
            errno    = 0;
            offsetout= lseek64(fd, offsetin, SEEK_CUR);
            log(LOG_DEBUG, "sropen64: lseek64(%d,%s,SEEK_CUR) returned %s\n",
                fd, u64tostr(offsetin,tmpbuf,0), u64tostr(offsetout,tmpbuf2,0));
            if ( offsetout < 0 ) {
              rcode = errno;
              status = -1;
            }
            else status = 0;
          }
        }
      if(pfn != NULL) free (pfn);
    }
  }

  /*
   * Sending back status.
   */
  p= rqstbuf;
  marshall_WORD(p,RQST_OPEN64);
  marshall_LONG(p,status);
  marshall_LONG(p,rcode);
  marshall_LONG(p,0);
  marshall_HYPER(p,offsetout);
#if defined(SACCT)
  rfioacct(RQST_OPEN64,uid,gid,s,(int)ntohopnflg(flags),(int)mode,status,rcode,NULL,CORRECT_FILENAME(filename),NULL);
#endif /* SACCT */
  log(LOG_DEBUG, "sropen64: sending back status(%d) and errno(%d)\n", status, rcode);
  replen = WORDSIZE+3*LONGSIZE+HYPERSIZE;
  if (netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen)  {
#if defined(_WIN32)
    log(LOG_ERR, "sropen64: write(): %s\n", geterr());
#else
    log(LOG_ERR, "sropen64: write(): %s\n", strerror(errno));
#endif
    return -1;
  }
  return fd;
}

// useful function to answer the client
int rfio_call64_answer_client_internal
(char* rqstbuf, int code, int status, int s) {
  char* p;
  p = rqstbuf;
  marshall_WORD(p,RQST_WRITE64);
  marshall_LONG(p,status);
  marshall_LONG(p,code);
  marshall_LONG(p,0);
  log(LOG_DEBUG, "srwrite64: status %d, rcode %d\n", status, code);
  if ( netwrite_timeout(s,rqstbuf,WORDSIZE+3*LONGSIZE,RFIO_CTRL_TIMEOUT) != WORDSIZE+3*LONGSIZE ) {
#if defined(_WIN32)
    log(LOG_ERR, "srwrite64: netwrite(): %s\n", geterr());
#else
    log(LOG_ERR, "srwrite64: netwrite(): %s\n", strerror(errno));
#endif
    return -1;
  }
  return 0;
}

int srwrite64(s, infop, fd)
#if defined(_WIN32)
     SOCKET s;
#else
     int     s;
#endif
     int     fd;
     struct rfiostat * infop;
{
  int       status;                      /* Return code               */
  int       rcode;                       /* To send back errno        */
  int       how;                         /* lseek mode                */
  off64_t   offset;                      /* lseek offset              */
  off64_t   offsetout;                   /* lseek offset              */
  int       size;                        /* Requeste write size       */
  char      *p;                          /* Pointer to buffer         */
  int       reqlen;                      /* residual request len      */
  char      tmpbuf[21];

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);
#endif

  if (first_write) {
    first_write = 0;
    status = rfio_handle_firstwrite(handler_context);
    if (status != 0) {
      log(LOG_ERR, "srwrite64: rfio_handle_firstwrite(): %s\n", strerror(serrno));
      rfio_call64_answer_client_internal(rqstbuf, serrno, status, s);
      return -1;
    }
  }

  p= rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p, size);
  unmarshall_LONG(p, how);

  /*
   * Receiving request: we have 4 bytes more to be read
   */
  p= rqstbuf + RQSTSIZE;
  reqlen = HYPERSIZE;
  log(LOG_DEBUG, "srwrite64(%d, %d)): reading %d bytes\n", s, fd, reqlen);
  if ((status = netread_timeout(s, p, reqlen, RFIO_CTRL_TIMEOUT)) != reqlen) {
#if defined(_WIN32)
    log(LOG_ERR, "srwrite64: read(): %s\n", geterr());
#else
    log(LOG_ERR, "srwrite64: read(): %s\n", strerror(errno));
#endif
    return -1;
  }
  unmarshall_HYPER(p,offset);
  log(LOG_DEBUG, "srwrite64(%d, %d): size %d, how %d offset %s\n", s, fd, size,
      how, u64tostr(offset,tmpbuf,0));

  /*
   * Checking if buffer is large enough.
   */
  if (iobufsiz < size)     {
    int     optval;       /* setsockopt opt value */

    if (iobufsiz > 0)       {
      log(LOG_DEBUG, "srwrite64: freeing %x\n", iobuffer);
      (void) free(iobuffer);
    }
    if ((iobuffer = malloc(size)) == NULL)    {
      rfio_call64_answer_client_internal(rqstbuf, errno, -1, s);
      return -1;
    }
    iobufsiz = size;
    optval = (iobufsiz > 64 * 1024) ? iobufsiz : (64 * 1024);
    log(LOG_DEBUG, "srwrite64: allocated %d bytes at %x\n", size, iobuffer);
#if defined(_WIN32)
    if( setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*)&optval, sizeof(optval)) == SOCKET_ERROR )
      log(LOG_ERR, "srwrite64: setsockopt(SO_RCVBUF): %s\n", geterr());
#else
    if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char *)&optval, sizeof(optval)) == -1)
      log(LOG_ERR, "srwrite64: setsockopt(SO_RCVBUF): %s\n", strerror(errno));
#endif
    else
      log(LOG_DEBUG, "srwrite64: setsockopt(SO_RCVBUF): %d\n", optval);
  }
  /*
   * Reading data on the network.
   */
  p= iobuffer;
  if (netread_timeout(s,p,size,RFIO_DATA_TIMEOUT) != size) {
#if defined(_WIN32)
    log(LOG_ERR, "srwrite64: read(): %s\n", geterr());
#else
    log(LOG_ERR, "srwrite64: read(): %s\n", strerror(errno));
#endif
    return -1;
  }
  /*
   * lseek() if needed.
   */
  if ( how != -1 ) {
    log(LOG_DEBUG, "srwrite64(%d,%d): lseek64(%d,%s,%d)\n", s, fd, fd,
        u64tostr(offset,tmpbuf,0), how);
    infop->seekop++;
    if ( (offsetout = lseek64(fd,offset,how)) == -1 ) {
      rfio_call64_answer_client_internal(rqstbuf, errno, -1, s);
      return -1;
    }
  }
  /*
   * Writing data on disk.
   */
  infop->wnbr+= size;
  log(LOG_DEBUG, "srwrite64: writing %d bytes on %d\n", size, fd);
  status = write(fd,p,size);
  rcode= ( status < 0 ) ? errno : 0;

  if ( status < 0 ) {
    char alarmbuf[1024];
    sprintf(alarmbuf,"srwrite64(): %s",filename);
    rfio_alrm(rcode,alarmbuf);
  }
  if (rfio_call64_answer_client_internal(rqstbuf, rcode, status, s) < 0) {
    return -1;
  }
  return status;
}

int srread64(s, infop, fd)
#if defined(_WIN32)
     SOCKET        s;
#else
     int     s;
#endif
     int     fd;
     struct rfiostat * infop;
{
  int      status;          /* Return code               */
  int      rcode;           /* To send back errno        */
  int      how;             /* lseek mode                */
  off64_t  offset;          /* lseek offset              */
  off64_t  offsetout;       /* lseek offset              */
  int      size;            /* Requested read size       */
  char     *p;              /* Pointer to buffer         */
  int      msgsiz;          /* Message size              */
  int      reqlen;          /* residual request length   */
  int      replen=WORDSIZE+3*LONGSIZE;   /* Reply header length       */
  char     tmpbuf[21];

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);
#endif

  p= rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p, size);
  unmarshall_LONG(p,how);

  /*
   * Receiving request: we have 8 bytes more to read
   */
  p= rqstbuf + RQSTSIZE;
  reqlen = HYPERSIZE;
  log(LOG_DEBUG, "srread64(%d, %d): reading %d bytes\n", s, fd, reqlen);
  if ((status = netread_timeout(s, p , reqlen, RFIO_CTRL_TIMEOUT)) != reqlen) {
#if defined(_WIN32)
    log(LOG_ERR, "srread64: read(): %s\n", geterr());
#else
    log(LOG_ERR, "srread64: read(): %s\n", strerror(errno));
#endif
    return -1;
  }
  unmarshall_HYPER(p,offset);

  /*
   * lseek() if needed.
   */
  if ( how != -1 ) {
    log(LOG_DEBUG, "srread64(%d,%d): lseek64(%d,%s,%d)\n", s, fd, fd,
        u64tostr(offset,tmpbuf,0), how);
    infop->seekop++;
    if ( (offsetout= lseek64(fd,offset,how)) == -1 ) {
      rcode= errno;
      status = -1;
      p= rqstbuf;
      marshall_WORD(p,RQST_READ64);
      marshall_LONG(p,status);
      marshall_LONG(p,rcode);
      marshall_LONG(p,0);
      if ( netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen ) {
#if defined(_WIN32)
        log(LOG_ERR, "srread64: write(): %s\n", geterr());
#else
        log(LOG_ERR, "srread64: write(): %s\n", strerror(errno));
#endif
        return -1;
      }
      return -1;
    }
  }
  /*
   * Allocating buffer if not large enough.
   */
  log(LOG_DEBUG, "srread64(%d, %d): checking buffer size %d\n", s, fd, size);
  if (iobufsiz < (size+replen))     {
    int     optval;       /* setsockopt opt value       */

    if (iobufsiz > 0)       {
      log(LOG_DEBUG, "srread64: freeing %x\n", iobuffer);
      (void) free(iobuffer);
    }
    if ((iobuffer = malloc(size+replen)) == NULL)    {
      status= -1;
      rcode= errno;
      log(LOG_ERR, "srread64: malloc(): %s\n", strerror(errno));
      p= rqstbuf;
      marshall_WORD(p,RQST_READ64);
      marshall_LONG(p,status);
      marshall_LONG(p,rcode);
      marshall_LONG(p,0);
      if ( netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen ) {
#if defined(_WIN32)
        log(LOG_ERR, "srread64: netwrite(): %s\n", geterr());
#else
        log(LOG_ERR, "srread64: write(): %s\n", strerror(errno));
#endif
        return -1;
      }
      return -1;
    }
    iobufsiz = size + replen;
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
  status = read(fd, p, size);
  if ( status < 0 ) {
    char alarmbuf[1024];
    sprintf(alarmbuf,"srread64(): %s",filename);
    rcode= errno;
    msgsiz= replen;
    rfio_alrm(rcode,alarmbuf);
  }  else  {
    rcode= 0;
    infop->rnbr+= status;
    msgsiz= status+replen;
  }
  p= iobuffer;
  marshall_WORD(p,RQST_READ64);
  marshall_LONG(p,status);
  marshall_LONG(p,rcode);
  marshall_LONG(p,status);
  log(LOG_DEBUG, "srread64: returning status %d, rcode %d\n", status, rcode);
  if (netwrite_timeout(s,iobuffer,msgsiz,RFIO_CTRL_TIMEOUT) != msgsiz)  {
#if defined(_WIN32)
    log(LOG_ERR, "srread64: write(): %s\n", geterr());
#else
    log(LOG_ERR, "srread64: write(): %s\n", strerror(errno));
#endif
    return -1;
  }
  return status;
}

int srreadahd64(s, infop, fd)
#if defined(_WIN32)
     SOCKET s;
#else
     int     s;
#endif
     int     fd;
     struct rfiostat *infop;
{
  int      status; /* Return code  */
  int     rcode; /* To send back errno */
  int     how; /* lseek mode  */
  off64_t  offset; /* lseek offset  */
  off64_t  offsetout; /* lseek offset  */
  int     size; /* Requested read size */
  int     first; /* First block sent */
  char     *p;  /* Pointer to buffer */
  int      reqlen; /* residual request length */
  char     tmpbuf[21];

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);
#endif

  /*
   * Receiving request.
   */
  log(LOG_DEBUG, "rreadahd64(%d, %d)\n",s, fd);
  p= rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p,size);
  unmarshall_LONG(p,how);

  /*
   * Receiving request: we have 8 bytes more to read
   */
  p= rqstbuf + RQSTSIZE;
  reqlen = HYPERSIZE;
  log(LOG_DEBUG, "rreadahd64(%d, %d): reading %d bytes\n", s, fd, reqlen);
  if ((status = netread_timeout(s, p , reqlen, RFIO_CTRL_TIMEOUT)) != reqlen) {
#if defined(_WIN32)
    log(LOG_ERR, "rreadahd64: read(): %s\n", geterr());
#else
    log(LOG_ERR, "rreadahd64: read(): %s\n", strerror(errno));
#endif
    return -1;
  }
  unmarshall_HYPER(p,offset);

  /*
   * lseek() if needed.
   */
  if ( how != -1 ) {
    log(LOG_DEBUG,"rread64(%d,%d): lseek64(%d,%s,%d)\n", s, fd, fd,
        u64tostr(offset,tmpbuf,0), how);
    infop->seekop++;
    if ( (offsetout= lseek64(fd,offset,how)) == -1 ) {
      rcode= errno;
      status= -1;
      p= iobuffer;
      marshall_WORD(p,RQST_FIRSTREAD);
      marshall_LONG(p,status);
      marshall_LONG(p,rcode);
      if ( netwrite_timeout(s,iobuffer,iobufsiz,RFIO_CTRL_TIMEOUT) != iobufsiz ) {
#if defined(_WIN32)
        log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n",geterr());
#else
        log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n", strerror(errno));
#endif
        return -1;
      }
      return status;
    }
  }
  /*
   * Allocating buffer if not large enough.
   */
  log(LOG_DEBUG, "rreadahd64(%d, %d): checking buffer size %d\n", s, fd, size);
  if (iobufsiz < (size+WORDSIZE+3*LONGSIZE))     {
    int     optval; /* setsockopt opt value */

    if (iobufsiz > 0)       {
      log(LOG_DEBUG, "rreadahd64(): freeing %x\n",iobuffer);
      (void) free(iobuffer);
    }
    if ((iobuffer = malloc(size+WORDSIZE+3*LONGSIZE)) == NULL)    {
      log(LOG_ERR, "rreadahd64: malloc(): %s\n", strerror(errno));
#if !defined(_WIN32)
      (void) close(s);
#else
      closesocket(s);
#endif /* _WIN32 */
      return -1;
    }
    iobufsiz = size+WORDSIZE+3*LONGSIZE;
    optval = (iobufsiz > 64 * 1024) ? iobufsiz : (64 * 1024);
    log(LOG_DEBUG, "rreadahd64(): allocated %d bytes at %x\n",iobufsiz,iobuffer);
#if defined(_WIN32)
    if( setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&optval, sizeof(optval)) == SOCKET_ERROR )
      log(LOG_ERR, "rreadahd64(): setsockopt(SO_SNDBUF): %s\n", geterr());
#else
    if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char *)&optval, sizeof(optval)) == -1)
      log(LOG_ERR, "rreadahd64(): setsockopt(SO_SNDBUF): %s\n",strerror(errno));
#endif
    else
      log(LOG_DEBUG, "rreadahd64(): setsockopt(SO_SNDBUF): %d\n",optval);
  }
  /*
   * Reading data and sending it.
   */
  for(first= 1;;first= 0) {
    fd_set fds;
    struct timeval timeout;

    /*
     * Has a new request arrived ?
     */
    FD_ZERO(&fds);
    FD_SET(s,&fds);
    timeout.tv_sec = 0;
    timeout.tv_usec= 0;
#if defined(_WIN32)
    if( select(FD_SETSIZE, &fds, (fd_set*)0, (fd_set*)0, &timeout) == SOCKET_ERROR )  {
      log(LOG_ERR,"rreadahd64(): select(): %s\n", geterr());
      return -1;
    }
#else
    if ( select(FD_SETSIZE,&fds,(fd_set *)0,(fd_set *)0,&timeout) == -1 ) {
      log(LOG_ERR,"rreadahd64(): select(): %s\n",strerror(errno));
      return -1;
    }
#endif
    if ( FD_ISSET(s,&fds) ) {
      log(LOG_DEBUG,"rreadahd64(): returns because of new request\n");
      return 0;
    }
    /*
     * Reading disk ...
     */
    p= iobuffer + WORDSIZE + 3*LONGSIZE;
    status = read(fd,p,size);
    if (status < 0)        {
      rcode= errno;
      iobufsiz= WORDSIZE+3*LONGSIZE;
    }
    else {
      rcode= 0;
      infop->rnbr+= status;
      iobufsiz = status+WORDSIZE+3*LONGSIZE;
    }
    log(LOG_DEBUG, "rreadahd64: status %d, rcode %d\n", status, rcode);
    /*
     * Sending data.
     */
    p= iobuffer;
    marshall_WORD(p,(first)?RQST_FIRSTREAD:RQST_READAHD64);
    marshall_LONG(p,status);
    marshall_LONG(p, rcode);
    marshall_LONG(p, status);
    if ( netwrite_timeout(s, iobuffer, iobufsiz, RFIO_CTRL_TIMEOUT) != iobufsiz ) {
#if defined(_WIN32)
      log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n", geterr());
#else
      log(LOG_ERR, "rreadahd64(): netwrite_timeout(): %s\n", strerror(errno));
#endif
      return -1;
    }
    /*
     * The end of file has been reached
     * or an error was encountered.
     */
    if ( status != size ) {
      return 0;
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

  log(LOG_DEBUG, "rfstat64(%d, %d)\n", s, fd);
  infop->statop++;

  /*
   * Issuing the fstat()
   */
  status= fstat64(fd, &statbuf);
  if ( status < 0) {
    rcode= errno;
    log(LOG_ERR,"rfstat64(%d,%d): fstat64 gave rc %d, errno=%d\n", s, fd, status, errno);
  }
  else errno = 0;

#if !defined(_WIN32)
  msgsiz= 5*WORDSIZE + 4*LONGSIZE + 3*HYPERSIZE;
#else
  msgsiz= 5*WORDSIZE + 3*LONGSIZE + 2*HYPERSIZE;
#endif
  p = rqstbuf;
  marshall_WORD(p, RQST_FSTAT64);
  marshall_LONG(p, status);
  marshall_LONG(p,  rcode);
  marshall_LONG(p, msgsiz);
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

  log(LOG_DEBUG, "rfstat64(%d,%d): sending back %d bytes, status=%d\n", s, fd, msgsiz, status);
  if (netwrite_timeout(s,rqstbuf,headlen+msgsiz,RFIO_CTRL_TIMEOUT) != (headlen+msgsiz) )  {
#if defined(WIN32)
    log(LOG_ERR,"rfstat64(%d,%d): netwrite(): %s\n", s, fd, geterr());
#else
    log(LOG_ERR,"rfstat64(%d,%d): netwrite(): %s\n", s, fd, strerror(errno));
#endif
    return -1;
  }
  return 0;
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

  p= rqstbuf + 2*WORDSIZE;
  unmarshall_HYPER(p,offset);
  unmarshall_LONG(p,how);
  log(LOG_DEBUG, "srlseek64(%d, %d): offset64 %s, how: %x\n", s, fd,
      u64tostr(offset,tmpbuf,0), how);
  offsetout = lseek64(fd, offset, how);
  if (offsetout == (off64_t)-1 ) status = -1;
  else status = 0;
  rcode= ( status < 0 ) ? errno : 0;
  log(LOG_DEBUG, "srlseek64: status %s, rcode %d\n", u64tostr(offsetout,tmpbuf,0), rcode);
  p= rqstbuf;
  marshall_WORD(p,request);
  marshall_HYPER(p, offsetout);
  marshall_LONG(p,rcode);
  rlen =  WORDSIZE+LONGSIZE+HYPERSIZE;
  if (netwrite_timeout(s,rqstbuf,rlen,RFIO_CTRL_TIMEOUT) != rlen)  {
#if defined(_WIN32)
    log(LOG_ERR, "srlseek64: netwrite(): %s\n", geterr());
#else
    log(LOG_ERR, "srlseek64: write(): %s\n",strerror(errno));
#endif
    return -1;
  }
  return status;
}

int srpreseek64(s, infop, fd)
#if defined(_WIN32)
     SOCKET s;
#else
     int     s;
#endif
     int fd;
     struct rfiostat *infop;
{
  int status;  /* Return code  */
  int  size;  /* Buffer size  */
  int nblock;  /* Nb of blocks to read */
  int first;  /* First block sent */
  char *p;  /* Pointer to buffer */
  int reqno;  /* Request number */
  struct iovec64 *v; /* List of requests */
  char *trp = NULL; /* Temporary buffer */

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);
#endif

  p= rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p,size);
  unmarshall_LONG(p,nblock);
  log(LOG_DEBUG,"rpreseek64(%d, %d)\n",s,fd);

  /*
   * A temporary buffer may need to be created
   * to receive the request.
   */
  if ( nblock*(HYPERSIZE+LONGSIZE) > BUFSIZ ) {
    if ( (trp= (char *) malloc(nblock*(HYPERSIZE+LONGSIZE))) == NULL ) {
      return -1;
    }
  }
  p= ( trp ) ? trp : rqstbuf;
  /*
   * Receiving the request.
   */
  log(LOG_DEBUG,"rpreseek64: reading %d bytes\n",nblock*(HYPERSIZE+LONGSIZE));
  if ( netread_timeout(s,p,nblock*(HYPERSIZE+LONGSIZE),RFIO_CTRL_TIMEOUT) != (nblock*(HYPERSIZE+LONGSIZE)) ) {
#if defined(_WIN32)
    log(LOG_ERR,"rpreseek64: netread(): %s\n", geterr());
#else
    log(LOG_ERR,"rpreseek64: read(): %s\n",strerror(errno));
#endif
    if ( trp ) (void) free(trp);
    return -1;
  }
  /*
   * Allocating space for the list of requests.
   */
  if ( (v= ( struct iovec64 *) malloc(nblock*sizeof(struct iovec64))) == NULL ) {
    log(LOG_ERR, "rpreseek64: malloc(): %s\n",strerror(errno));
    if ( trp ) (void) free(trp);
    (void) close(s);
    return -1;
  }
  /*
   * Filling list request.
   */
  for(reqno= 0;reqno<nblock;reqno++) {
    unmarshall_HYPER(p,v[reqno].iov_base);
    unmarshall_LONG(p,v[reqno].iov_len);
  }
  /*
   * Freeing temporary buffer if needed.
   */
  if ( trp ) (void) free(trp);
  /*
   * Allocating new data buffer if the
   * current one is not large enough.
   */
  log(LOG_DEBUG,"rpreseek64(%d, %d): checking buffer size %d\n",s,fd,size);
  if (iobufsiz < (size+WORDSIZE+3*LONGSIZE))     {
    int     optval; /* setsockopt opt value */

    if (iobufsiz > 0)       {
      log(LOG_DEBUG, "rpreseek64(): freeing %x\n",iobuffer);
      (void) free(iobuffer);
    }
    if ((iobuffer = malloc(size+WORDSIZE+3*LONGSIZE)) == NULL)    {
      log(LOG_ERR, "rpreseek64: malloc(): %s\n", strerror(errno));
      (void) close(s);
      return -1;
    }
    iobufsiz = size+WORDSIZE+3*LONGSIZE;
    optval = (iobufsiz > 64 * 1024) ? iobufsiz : (64 * 1024);
    log(LOG_DEBUG, "rpreseek64(): allocated %d bytes at %x\n",iobufsiz,iobuffer);
#if defined(_WIN32)
    if( setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&optval, sizeof(optval)) == SOCKET_ERROR )
      log(LOG_ERR, "rpreseek64(): setsockopt(SO_SNDBUF): %s\n", geterr());
#else
    if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char *)&optval, sizeof(optval)) == -1)
      log(LOG_ERR, "rpreseek64(): setsockopt(SO_SNDBUF): %s\n",strerror(errno));
#endif
    else
      log(LOG_DEBUG, "rpreseek64(): setsockopt(SO_SNDBUF): %d\n",optval);
  }
  /*
   * Reading data and sending it.
   */
  reqno= 0;
  for(first= 1;;first= 0) {
    struct timeval timeout;
    fd_set fds;
    int nbfree;
    int     nb;
    off64_t offsetout;

    /*
     * Has a new request arrived ?
     * The preseek is then interrupted.
     */
    FD_ZERO(&fds);
    FD_SET(s,&fds);
    timeout.tv_sec = 0;
    timeout.tv_usec= 0;
#if defined(_WIN32)
    if( select(FD_SETSIZE, &fds, (fd_set*)0, (fd_set*)0, &timeout) == SOCKET_ERROR ) {
      log(LOG_ERR,"rpreseek64(): select(): %s\n", geterr());
      return -1;
    }
#else
    if ( select(FD_SETSIZE,&fds,(fd_set *)0,(fd_set *)0,&timeout) == -1 ) {
      log(LOG_ERR,"rpreseek64(): select(): %s\n",strerror(errno));
      return -1;
    }
#endif  /* WIN32 */
    if ( FD_ISSET(s,&fds) ) {
      log(LOG_DEBUG,"rpreseek64(): returns because of new request\n");
      return 0;
    }
    /*
     * Filling buffer.
     */
    p= iobuffer + WORDSIZE + 3*LONGSIZE;
    for(nb= 0, nbfree= size; HYPERSIZE+3*LONGSIZE<nbfree && reqno<nblock; reqno ++, nb ++ ) {
      nbfree -= HYPERSIZE+3*LONGSIZE;
      offsetout= lseek64(fd,v[reqno].iov_base,SEEK_SET);
      if ( offsetout == (off64_t)-1 ) {
        status= -1;
        marshall_HYPER(p,v[reqno].iov_base);
        marshall_LONG(p,v[reqno].iov_len);
        marshall_LONG(p,status);
        marshall_LONG(p,errno);
        continue;
      }
      if ( v[reqno].iov_len <= nbfree ) {
        status= read(fd,p+HYPERSIZE+3*LONGSIZE,v[reqno].iov_len);
        marshall_HYPER(p,v[reqno].iov_base);
        marshall_LONG(p,v[reqno].iov_len);
        marshall_LONG(p,status);
        marshall_LONG(p,errno);
        if ( status != -1 ) {
          infop->rnbr += status;
          nbfree -= status;
          p += status;
        }
      }
      else {
        /*
         * The record is broken into two pieces.
         */
        status= read(fd,p+HYPERSIZE+3*LONGSIZE,nbfree);
        marshall_HYPER(p,v[reqno].iov_base);
        marshall_LONG(p,nbfree);
        marshall_LONG(p,status);
        marshall_LONG(p,errno);
        if ( status != -1 ) {
          infop->rnbr += status;
          nbfree -= status;
          p += status;
          v[reqno].iov_base += status;
          v[reqno].iov_len -= status;
          reqno --;
        }
      }

    }
    p= iobuffer;
    if ( first ) {
      marshall_WORD(p,RQST_FIRSTSEEK);
    }
    else {
      marshall_WORD(p,(reqno == nblock) ? RQST_LASTSEEK:RQST_PRESEEK64);
    }
    marshall_LONG(p,nb);
    marshall_LONG(p,0);
    marshall_LONG(p,size);
    log(LOG_DEBUG,"rpreseek64(): sending %d bytes\n",iobufsiz);
    if ( netwrite_timeout(s,iobuffer,iobufsiz,RFIO_CTRL_TIMEOUT) != iobufsiz ) {
#if defined(_WIN32)
      log(LOG_ERR, "rpreseek64(): netwrite_timeout(): %s\n", geterr());
#else
      log(LOG_ERR, "rpreseek64(): netwrite_timeout(): %s\n", strerror(errno));
#endif
      return -1;
    }
    /*
     * All the data needed has been
     * sent over the network.
     */
    if ( reqno == nblock )
      return 0;
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
  char     user[CA_MAXUSRNAMELEN+1];     /* User name            */
  char     reqhost[MAXHOSTNAMELEN];
  const char *value = NULL;
  char     *buf = NULL;
  char     *dp  = NULL;
  char     *dp2 = NULL;
  long     low_port  = RFIO_LOW_PORT_RANGE;
  long     high_port = RFIO_HIGH_PORT_RANGE;
#if defined(_WIN32)
  SOCKET  sock;
#else    /* WIN32 */
  int      sock;                         /* Control socket       */
  int      data_s;                       /* Data    socket       */
#endif   /* else WIN32 */
  socklen_t fromlen;
  socklen_t size_sin;
  int      port;
  struct   sockaddr_in sin;
  struct   sockaddr_in from;
  extern int max_rcvbuf;
  extern int max_sndbuf;
  int      yes;                          /* Socket option value  */
  off64_t  offsetout;                    /* Offset               */
  char     tmpbuf[21];
#if defined(_WIN32)
  struct thData *td;
#endif
  struct timeval tv;
  fd_set read_fds;
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
  /* Initialize the fd set */
  FD_ZERO(&read_fds);
  /* Will remain at this value (indicates that the new sequential transfer mode has been used) */
  myinfo.aheadop = 1;
  byte_read_from_network = (off64_t)0;

  p= rqstbuf + 2*WORDSIZE;
  unmarshall_LONG(p, len);
  if ( (status = srchkreqsize(s,p,len)) == -1 ) {
    rcode = errno;
  } else {
    /*
     * Reading open request.
     */
    log(LOG_DEBUG,"ropen64_v3(%d): reading %d bytes\n", s, len);
    if ((status = netread_timeout(s,rqstbuf,len,RFIO_CTRL_TIMEOUT)) != len) {
#if defined(_WIN32)
      log(LOG_ERR,"ropen64_v3: read(): %s\n", geterr());
#else
      log(LOG_ERR,"ropen64_v3: read(): %s\n",strerror(errno));
#endif
      return -1;
    }
    p= rqstbuf;
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
      errno=EACCES;
      rcode=errno;
      status= -1;
    }

    if ( rt ) {
      log(LOG_ALERT, "rfio: connection %s mapping by %s(%d,%d) from %s\n",
          (mapping ? "with" : "without"), user, uid, gid, host);
    }

    /*
     * MAPPED mode: user will be mapped to user "to"
     */
    if ( !status && rt && mapping ) {
      char to[100];
      int rcd,to_uid,to_gid;

      log(LOG_DEBUG,"ropen64_v3: Mapping (%s, %d, %d) \n", user, uid, gid );
      if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
        log(LOG_ERR,"ropen64_v3: get_user() error opening mapping file\n");
        status = -1;
        errno = EINVAL;
        rcode = SEHOSTREFUSED;
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
            host, user, uid, gid, to, to_uid, to_gid);
        uid = to_uid;
        gid = to_gid;
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
      char *rtuser;
      if( (rtuser = getconfent("RTUSER","CHECK",0) ) == NULL || ! strcmp (rtuser,"YES") )
        {
          /* Port is also passwd */
          sock = connecttpread(reqhost, passwd);
#if defined(_WIN32)
          if( (sock != INVALID_SOCKET) && !checkkey(sock, passwd) )
#else
            if( (sock >= 0) && !checkkey(sock, passwd) )
#endif
              {
                status= -1;
                errno = EACCES;
                rcode= errno;
                log(LOG_ERR,"ropen64_v3: DIRECT mapping : permission denied\n");
              }
#if defined(_WIN32)
          if( sock == INVALID_SOCKET )
#else
            if (sock < 0)
#endif
              {
                status= -1;
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
      (void) umask((mode_t) CORRECT_UMASK(mask));
#if !defined(_WIN32)
      if ( ((status=check_user_perm(&uid,&gid,host,&rcode,(((ntohopnflg(flags)) & (O_WRONLY|O_RDWR)) != 0) ? "WTRUST" : "RTRUST")) < 0) &&
           ((status=check_user_perm(&uid,&gid,host,&rcode,"OPENTRUST")) < 0) ) {
        if (status == -2)
          log(LOG_ERR,"ropen64_v3: uid %d not allowed to open()\n", uid);
        else
          log(LOG_ERR,"ropen64_v3: failed at check_user_perm(), rcode %d\n", rcode);
        status = -1;
      }  else
#endif
        {
          char *pfn = NULL;
          int rc;
          int unused_dummy;
          rc = rfio_handle_open(CORRECT_FILENAME(filename),
                                ntohopnflg(flags),
                                mode,
                                uid,
                                gid,
                                &pfn,
                                &handler_context,
                                &unused_dummy);
          if (rc < 0) {
            char alarmbuf[1024];
            sprintf(alarmbuf,"sropen64_v3: %s",CORRECT_FILENAME(filename));
            log(LOG_DEBUG, "ropen64_v3: rfio_handler_open refused open: %s\n", sstrerror(serrno));
            rcode = serrno;
            rfio_alrm(rcode,alarmbuf);
          }

          /* NOTE(fuji): from now on, flags is in host byte-order... */
          flags = ntohopnflg(flags);
          myinfo.directio = 0;
          if ( getconfent("RFIOD", "DIRECTIO", 0) ) {
            myinfo.directio = 1;
          }
          myinfo.xfsprealloc = 0;
          if ( (p = getconfent("RFIOD", "XFSPREALLOC", 0)) ) {
            myinfo.xfsprealloc = atoi(p);
          }
          if (myinfo.directio) {
#if defined(linux)
            log(LOG_INFO, "%s: O_DIRECT requested\n", __func__);
            flags |= O_DIRECT;
#elif defined(_WIN32)
            log(LOG_INFO, "%s: O_DIRECT requested but ignored.", __FUNCTION__);
#else
            log(LOG_INFO, "%s: O_DIRECT requested but ignored.", __func__);
#endif
          }
          {
            const char *perm_array[3];
            char ofilename[MAXFILENAMSIZE];
            perm_array[0] = ((flags & (O_WRONLY|O_RDWR)) != 0) ? "WTRUST" : "RTRUST";
            perm_array[1] = "OPENTRUST";
            perm_array[2] = NULL;

            strcpy(ofilename, filename);
            fd = -1;
            if (forced_filename!=NULL || !check_path_whitelist(host, filename, perm_array, ofilename, sizeof(ofilename),1)) {
              fd = open64(CORRECT_FILENAME(ofilename), flags,
                          ((forced_filename != NULL) && ((flags & (O_WRONLY|O_RDWR)) != 0)) ? 0644 : mode);
#if defined(_WIN32)
              _setmode( fd, _O_BINARY );       /* default is text mode  */
#endif
            }
          }
          if (fd < 0)  {
            status= -1;
            rcode= errno;
#if defined(_WIN32)
            log(LOG_DEBUG,"ropen64_v3: open64(%s,0%o,0%o): %s\n",
                CORRECT_FILENAME(filename), flags, mode, geterr());
#else
            log(LOG_DEBUG,"ropen64_v3: open64(%s,0%o,0%o): %s\n",
                CORRECT_FILENAME(filename), flags, mode, strerror(errno));
#endif
          }
          else  {
            /* File is opened         */
            log(LOG_DEBUG,"ropen64_v3: open64(%s,0%o,0%o) returned %d \n",
                CORRECT_FILENAME(filename), flags, mode, fd);
            /*
             * Getting current offset
             */
            offsetout = lseek64(fd, (off64_t)0, SEEK_CUR);
            if (offsetout == ((off64_t)-1) ) {
#if defined(_WIN32)
              log(LOG_ERR,"ropen64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd, geterr());
#else
              log(LOG_ERR,"ropen64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd,strerror(errno));
#endif
              status = -1;
              rcode  = errno;
            }
            else {
              log(LOG_DEBUG,"ropen64_v3: lseek64(%d,0,SEEK_CUR) returned %s\n",
                  fd, u64tostr(offsetout,tmpbuf,0));
              status = 0;
            }
          }
          if(pfn != NULL) free (pfn);
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
        exit(1);
      }
#endif   /* else WIN32 */
      log(LOG_DEBUG, "ropen64_v3: data socket created fd=%d\n", data_s);

      memset(&sin, 0, sizeof(sin));
      sin.sin_addr.s_addr = htonl(INADDR_ANY);
      sin.sin_family = AF_INET;

      /* Check to see if there is a user defined RFIOD/PORT_RANGE configured */
      if ((value = getconfent("RFIOD", "PORT_RANGE", 0)) != NULL) {
        if ((buf = strdup(value)) == NULL) {
#if defined(_WIN32)
          log(LOG_ERR, "ropen64_v3: strdup: %s\n", geterr());
          return(-1);
#else    /* WIN32 */
          log(LOG_ERR, "ropen64_v3: strdup: %s\n", strerror(errno));
          exit(1);
#endif   /* else WIN32 */
        }
        if ((p = strchr(buf, ',')) != NULL) {
          *p = '\0';
          p++;
          /* Check that the values are valid */
          if ((((low_port = strtol(buf, &dp, 10)) <= 0) || (*dp != '\0')) ||
              (((high_port = strtol(p, &dp2, 10)) <= 0) || (*dp2 != '\0')) ||
              (high_port <= low_port) ||
              ((low_port < 1024) || (low_port > 65535)) ||
              ((high_port < 1024) || (high_port > 65535))) {
            log(LOG_ERR, "ropen64_v3: invalid port range: %s, using default %d,%d\n",
                value, RFIO_LOW_PORT_RANGE, RFIO_HIGH_PORT_RANGE);
            low_port  = RFIO_LOW_PORT_RANGE;
            high_port = RFIO_HIGH_PORT_RANGE;
          } else {
            log(LOG_DEBUG, "ropen64_v3: using port range: %d,%d\n", low_port, high_port);
          }
        } else {
          log(LOG_ERR, "ropen64_v3: invalid port range: %s, using default %d,%d\n",
              value, RFIO_LOW_PORT_RANGE, RFIO_HIGH_PORT_RANGE);
        }
        (void) free(buf);
      }

      /* Set random seed */
      gettimeofday(&tv, NULL);
      srand(tv.tv_usec * tv.tv_sec);

      /* Loop over all the ports in the specified range starting at a random
       * offset
       */
      port = (rand() % (high_port - (low_port + 1))) + low_port;
      while (port++) {
        /* If we reach the maximum allowed port, reset it! */
        if (port > high_port) {
          port = low_port;
          sleep(5);  /* sleep between complete loops, prevents CPU thrashing */
          continue;
        }

        /* Attempt to bind to the port */
        sin.sin_port = htons(port);
        if (bind(data_s, (struct sockaddr*)&sin, sizeof(sin)) == 0) {
          /* Just because the bind was successfull, doesn't mean the listen
           * will succeed!
           */
#if defined(_WIN32)
          if (listen(data_s, 5) == INVALID_SOCKET) {
            if (WSAGetLastError() == WSAEADDRINUSE) {
              log(LOG_DEBUG, "ropen64_v3: listen(%d): %s, attempting another port\n", data_s, geterr());
              /* close and recreate the socket */
              close(data_s);
              data_s = socket(AF_INET, SOCK_STREAM, 0);
              if( data_s == INVALID_SOCKET )  {
                log( LOG_ERR, "datasocket(): %s\n", geterr());
                return(-1);
              }
              sleep(1);
              continue;
            } else {
              log(LOG_ERR, "ropen64_v3: listen(%d): %s\n", data_s, geterr());
              return(-1);
            }
          }
#else    /* WIN32 */
          if (listen(data_s, 5) < 0) {
            if (errno == EADDRINUSE) {
              log(LOG_DEBUG, "ropen64_v3: listen(%d): %s, attempting another port\n", data_s, strerror(errno));
              /* close and recreate the socket */
              close(data_s);
              data_s = socket(AF_INET, SOCK_STREAM, 0);
              if( data_s < 0 )  {
                log(LOG_ERR, "datasocket(): %s\n", strerror(errno));
                exit(1);
              }
              sleep(1);
              continue;
            } else {
              log(LOG_ERR, "ropen64_v3: listen(%d): %s\n", data_s, strerror(errno));
              exit(1);
            }
          }
          break;
#endif   /* else WIN32 */
        } else {
#if defined(_WIN32)
          log(LOG_DEBUG, "ropen64_v3: bind(%d:%d): %s, trying again\n", data_s, port, geterr());
#else    /* WIN32 */
          log(LOG_DEBUG, "ropen64_v3: bind(%d:%d): %s, trying again\n", data_s, port, strerror(errno));
#endif   /* else WIN32 */
        }
      }

      size_sin = sizeof(sin);
#if defined(_WIN32)
      if (getsockname(data_s, (struct sockaddr*)&sin, &size_sin) == SOCKET_ERROR )  {
        log(LOG_ERR, "ropen64_v3: getsockname: %s\n", geterr());
        return(-1);
      }
#else    /* WIN32 */
      if (getsockname(data_s, (struct sockaddr*)&sin, &size_sin) < 0 )  {
        log(LOG_ERR, "ropen64_v3: getsockname: %s\n", strerror(errno));
        exit(1);
      }
#endif   /* else WIN32 */

      log(LOG_DEBUG, "ropen64_v3: assigning data port %d\n", htons(sin.sin_port));
    }
  }

  /*
   * Sending back status to the client
   */
  p= rqstbuf;
  marshall_WORD(p,RQST_OPEN64_V3);
  replen = RQSTSIZE+HYPERSIZE;
  marshall_LONG(p,status);
  marshall_LONG(p,rcode);
  marshall_LONG(p,ntohs(sin.sin_port));
  marshall_HYPER(p, offsetout);
  log(LOG_DEBUG, "ropen64_v3: sending back status(%d) and errno(%d)\n", status, rcode);
  errno = ECONNRESET;
  if (netwrite_timeout(s,rqstbuf,replen,RFIO_CTRL_TIMEOUT) != replen)  {
#if defined(_WIN32)
    log(LOG_ERR,"ropen64_v3: write(): %s\n", geterr());
#else
    log(LOG_ERR,"ropen64_v3: write(): %s\n", strerror(errno));
#endif
    return -1;
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
          log(LOG_DEBUG, "ropen64_v3: doing select\n");
          FD_ZERO(&read_fds);
          FD_SET(data_s, &read_fds);
          tv.tv_sec  = 10;
          tv.tv_usec = 0;
#if defined (_WIN32)
          if ( select(FD_SETSIZE, &read_fds, NULL, NULL, &tv) == SOCKET_ERROR ) {
            log(LOG_ERR, "ropen64_v3: select failed: %s\n", geterr());
            return -1;
          }
#else
          if ( select(FD_SETSIZE, &read_fds, NULL, NULL, &tv) < 0 ) {
            log(LOG_ERR, "ropen64_v3: select failed: %s\n", strerror(errno));
            return -1;
          }
#endif
          /* Anything received on the data socket ? */
          if ( !FD_ISSET(data_s, &read_fds) ) {
            log(LOG_ERR, "ropen64_v3: timeout in accept(%d)\n", data_s);
            return(-1);
          }
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
            exit(1);
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
  rfioacct(RQST_OPEN64_V3,uid,gid,s,(int)flags,(int)mode,status,rcode,NULL,CORRECT_FILENAME(filename),NULL);
#endif /* SACCT */
  return fd;
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
  struct stat filestat;
  int rc;
  int ret;

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);
#endif

  /* Issue statistics                                        */
  log(LOG_INFO,"rclose64_v3(%d, %d): %d read, %d readahead, %d write, %d flush, %d stat, %d lseek and %d lockf\n",
      s, fd, myinfo.readop, myinfo.aheadop, myinfo.writop, myinfo.flusop, myinfo.statop,
      myinfo.seekop, myinfo.lockop);
  log(LOG_INFO,"rclose64_v3(%d, %d): %s bytes read and %s bytes written\n",
      s, fd, u64tostr(myinfo.rnbr,tmpbuf,0), u64tostr(myinfo.wnbr,tmpbuf2,0));

  /* sync the file to be sure that filesize in correct in following stats.
     this is needed by some ext3 bug/feature
     Still ignore the output of fsync */
  fsync(fd);

  /* Stat the file to be able to provide that information
     to the close handler */
  memset(&filestat,0,sizeof(struct stat));
  rc = fstat(fd, &filestat);

  /* Close the local file                                    */
  status = close(fd);
  rcode = ( status < 0 ) ? errno : 0;

  ret=rfio_handle_close(handler_context, &filestat, rcode);
  if (ret<0){
    log(LOG_ERR, "srclose: rfio_handle_close failed\n");
    if (status>=0) {
      /* we have to set status = -1 and fill rcode with serrno, that should be filled by rfio_handle_close */
      status=-1;
      rcode=serrno;
    }
    /* we already have status<0 in error case here and will send a reply for client with rcode */
  }

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
    return -1;
  }

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

  return status;
}

static void *produce64_thread(int *ptr)
{
  int      fd = *ptr;
  int      byte_read = -1;
  int      error = 0;
  off64_t  total_produced = 0;
  char     tmpbuf[21];
  unsigned int ckSum;
  char     ckSumbuf[CA_MAXCKSUMLEN+1]; /* max check sum 256bit 32x8+'\0'*/
  char     ckSumbufdisk[CA_MAXCKSUMLEN+1];
  char     useCksum;
  char     *conf_ent;
  char     *ckSumalg="ADLER32";
  int      xattr_len;

  /* Check if checksum support is enabled */
  useCksum = 1;
  if ((conf_ent = getconfent("RFIOD","USE_CKSUM",0)) != NULL)
    if (!strncasecmp(conf_ent,"no",2)) useCksum = 0;

  if (useCksum) {
    if ((xattr_len = fgetxattr(fd,"user.castor.checksum.value",ckSumbufdisk,CA_MAXCKSUMLEN)) == -1) {
      log(LOG_ERR,"produce64_thread: fgetxattr failed for user.castor.checksum.value, error=%d\n",errno);
      log(LOG_ERR,"produce64_thread: skipping checksums check\n");
      useCksum = 0;
    } else {
      ckSumbufdisk[xattr_len] = '\0';
      ckSum = adler32(0L,Z_NULL,0);
      log(LOG_DEBUG,"produce64_thread: checksum init for %s\n",ckSumalg);
      log(LOG_DEBUG,"produce64_thread: disk file checksum=0x%s\n",ckSumbufdisk);
    }
  }

  while ((! error) && (byte_read != 0)) {
    if (stop_read)
      return (NULL);

    log(LOG_DEBUG, "produce64_thread: calling Csemaphore_down(&empty64)\n");
    Csemaphore_down(&empty64);

    log(LOG_DEBUG, "produce64_thread: read() at %s:%d\n", __FILE__, __LINE__);
    byte_read = read(fd,array[produced64 % daemonv3_rdmt_nbuf].p,daemonv3_rdmt_bufsize);

    if (byte_read > 0) {
      total_produced += byte_read;
      log(LOG_DEBUG, "Has read in buf %d (len %d)\n",produced64 % daemonv3_rdmt_nbuf,byte_read);
      array[produced64 % daemonv3_rdmt_nbuf].len = byte_read;
      /* Check for checksum mismatch. */
      if (useCksum) {
        ckSum = adler32(ckSum,(unsigned char*)array[produced64 % daemonv3_rdmt_nbuf].p,(unsigned int)byte_read);
        log(LOG_DEBUG,"produce64_thread: current checksum=0x%lx\n",ckSum);
      }
    }
    else {
      if (byte_read == 0)  {
        log(LOG_DEBUG,"produce64_thread: End of reading : total produced = %s,buffers=%d\n",
            u64tostr(total_produced,tmpbuf,0),produced64);
        array[produced64 % daemonv3_rdmt_nbuf].len = 0;
        if (useCksum) {
          sprintf(ckSumbuf,"%x", ckSum);
          log(LOG_DEBUG,"produce64_thread: file checksum=0x%s\n",ckSumbuf);
          if (strncmp(ckSumbufdisk,ckSumbuf,CA_MAXCKSUMLEN)==0) {
            log(LOG_DEBUG,"produce64_thread: checksums OK!\n");
          }
          else {
            log(LOG_ERR,"produce64_thread: checksum error detected reading file: %s (recorded checksum: %s calculated checksum: %s)\n",
                CORRECT_FILENAME(filename), ckSumbufdisk, ckSumbuf);
            array[produced64 % daemonv3_rdmt_nbuf].len = -(SECHECKSUM); /* setting errno= Bad checksum */
            error = -1;
          }
        }
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
  unsigned int ckSum;
  char     ckSumbuf[CA_MAXCKSUMLEN+1]; /* max check sum 256bit 32x8+'\0'*/
  char     ckSumbufdisk[CA_MAXCKSUMLEN+1];
  char     useCksum;
  char     *conf_ent;
  char     *ckSumalg="ADLER32";
  int      mode;

  /* Check if checksum support is enabled */
  useCksum = 1;
  if ((conf_ent=getconfent("RFIOD","USE_CKSUM",0)) != NULL)
    if (!strncasecmp(conf_ent,"no",2)) useCksum = 0;

  /* Deal with cases where checksums should not be calculated */
  if (useCksum) {
    mode = fcntl(fd,F_GETFL);
    if (mode == -1) {
      log(LOG_ERR,"consume64_thread: fcntl (F_GETFL) failed, error=%d\n",errno);
      useCksum = 0;
    }
    /* Checksums on updates are not supported */
    else if (mode & O_RDWR) {
      log(LOG_INFO,"consume64_thread: file opened in O_RDWR, skipping checksums\n");
      useCksum = 0;
    }
    /* If we are writing to the file and a checksum already exists, we
     * remove the checksum value but leave the type.
     */
    else if ((mode & O_WRONLY) &&
             (fgetxattr(fd,"user.castor.checksum.type",ckSumbufdisk,CA_MAXCKSUMLEN) != -1)) {
      log(LOG_INFO,"consume64_thread: file opened in O_WRONLY and checksum already exists, removing checksum\n");
      useCksum = 0;
    } else {
      ckSum = adler32(0L,Z_NULL,0);
      log(LOG_DEBUG,"consume64_thread: checksum init for %s\n",ckSumalg);
    }
  }
  /* Always remove the checksum value */
  fremovexattr(fd,"user.castor.checksum.value");

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
        if (useCksum) {
          ckSum = adler32(ckSum,(unsigned char*)buffer_to_write,(unsigned int)byte_written);
          log(LOG_DEBUG,"consume64_thread: current checksum=0x%lx\n",ckSum);
        }
      }
    }  /* if  (len_to_write > 0) */
    else {
      if (len_to_write == 0) {
        log(LOG_DEBUG,"consume64_thread: End of writing : total consumed = %s, buffers=%d\n",
            u64tostr(total_consumed,tmpbuf,0),consumed64);
        end = 1;
      } else {
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

  /* Record the checksum value and type in the extended attributes of the file.
   * This is done in all cases, including errors!
   */
  if (useCksum) {
    sprintf(ckSumbuf,"%x",ckSum);
    log(LOG_DEBUG,"consume64_thread: file checksum=0x%s\n",ckSumbuf);
    /* Double check whether the checksum is set on disk. If yes, it means
       it appeared while we were writing. this means that concurrent writing
       is taking place. Thus we reset the checksum rather than setting it */
    if (fgetxattr(fd,"user.castor.checksum.value",ckSumbufdisk,CA_MAXCKSUMLEN) != -1) {
      log(LOG_INFO,"consume64_thread: concurrent writing detected, removing checksum\n");
      fremovexattr(fd,"user.castor.checksum.value");
    } else {
      /* Always try and set the type first! */
      if (fsetxattr(fd,"user.castor.checksum.type",ckSumalg,strlen(ckSumalg),0))
        log(LOG_ERR,"consume64_thread: fsetxattr failed for user.castor.checksum.type, error=%d\n",errno);
      else if (fsetxattr(fd,"user.castor.checksum.value",ckSumbuf,strlen(ckSumbuf),0))
        log(LOG_ERR,"consume64_thread: fsetxattr failed for user.castor.checksum.value, error=%d\n",errno);
    }
  }
  return(NULL);
}

static void wait_consumer64_thread(int *cidp)
{
  log(LOG_DEBUG,"wait_consumer64_thread: Entering wait_consumer64_thread\n");
  if (*cidp<0) return;

  /* Indicate to the consumer thread that an error has occured */
  /* The consumer thread will then terminate */
  Csemaphore_down(&empty64);
  array[produced64 % daemonv3_wrmt_nbuf].len = -1;
  produced64++;
  Csemaphore_up(&full64);

  log(LOG_INFO, "wait_consumer64_thread: Joining thread\n");
  if (Cthread_join(*cidp,NULL) < 0) {
    log(LOG_ERR,"wait_consumer64_thread: Error joining consumer thread, serrno=%d\n",serrno);
    return;
  }
  *cidp = -1;
}

static void wait_producer64_thread(int *cidp)
{
  log(LOG_DEBUG,"wait_producer64_thread: Entering wait_producer64_thread\n");
  if (*cidp<0) return;

  if (!stop_read) {
    stop_read = 1;
    Csemaphore_up(&empty64);
  }

  log(LOG_INFO, "wait_producer64_thread: Joining thread\n");
  if (Cthread_join(*cidp,NULL) < 0) {
    log(LOG_ERR,"wait_producer64_thread: Error joining producer thread, serrno=%d\n",serrno);
    return;
  }
  *cidp = -1;
}


/* This function is used when finding an error condition on read64_v3
   - Close the data stream
   - Trace statistics
*/
static int   readerror64_v3(s, infop, cidp)
#if defined(_WIN32)
     SOCKET         s;
#else    /* WIN32 */
     int            s;
#endif   /* else WIN32 */
     struct rfiostat* infop;
     int           *cidp;
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
      s, u64tostr(infop->rnbr,tmpbuf,0), u64tostr(infop->wnbr,tmpbuf2,0));

  /* Free allocated memory                                              */
  if (!daemonv3_rdmt) {
    log(LOG_DEBUG,"readerror64_v3: freeing iobuffer at 0X%X\n", iobuffer);
    if (iobufsiz > 0) {
      if (myinfo.directio) {
        free_page_aligned(iobuffer);
      } else {
        free(iobuffer);
      }
      iobuffer = NULL;
      iobufsiz = 0;
    }
  }
  else {
    wait_producer64_thread(cidp);
    if (array) {
      int      el;
      for (el=0; el < daemonv3_rdmt_nbuf; el++) {
        log(LOG_DEBUG,"readerror64_v3: freeing array element %d at 0X%X\n", el,array[el].p);
        if (myinfo.directio) {
          free_page_aligned(array[el].p);
        } else {
          free(array[el].p);
        }
        array[el].p = NULL;
      }
      log(LOG_DEBUG,"readerror64_v3: freeing array at 0X%X\n", array);
      free(array);
      array = NULL;
    }
  }

  return 0;
}

#if defined(_WIN32)
int srread64_v3(s, infop, fd)
     SOCKET         s;
#else    /* WIN32 */
     int srread64_v3(ctrl_sock, infop, fd)
          int            ctrl_sock;
#endif   /* else WIN32 */
          int            fd;
          struct rfiostat* infop;
{
  int         status;              /* Return code                */
  int         rcode;               /* To send back errno         */
  off64_t     offsetout;           /* lseek offset               */
  char        *p;                  /* Pointer to buffer          */
#if !defined(_WIN32)
  char        *iobuffer;
#endif
  off64_t     bytes2send;
  fd_set      fdvar, fdvar2;
  extern int  max_sndbuf;
  struct stat64 st;
  char        rfio_buf[BUFSIZ];
  int         eof_met;
  int         DISKBUFSIZE_READ = (1 * 1024 * 1024);
  int         n;
  int         cid1 = -1;
  int         el;
  char        tmpbuf[21];

#if defined(_WIN32)
  struct thData *td;
  td = (struct thData*)TlsGetValue(tls_i);

  ctrl_sock = s;
#endif   /* WIN32 */
  /*
   * Receiving request,
   */
  log(LOG_DEBUG, "rread64_v3(%d,%d)\n",ctrl_sock, fd);

  if (first_read) {
    char *p;
    first_read = 0;
    eof_met = 0;

    if( (p = getconfent("RFIO", "DAEMONV3_RDSIZE", 0)) != NULL ) {
      if (atoi(p) > 0)
        DISKBUFSIZE_READ = atoi(p);
    }

    daemonv3_rdmt = DAEMONV3_RDMT;
    if( (p = getconfent("RFIO", "DAEMONV3_RDMT", 0)) != NULL ) {
      if (*p == '0')
        daemonv3_rdmt = 0;
      else
        daemonv3_rdmt = 1;
    }

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
        readerror64_v3(ctrl_sock, &myinfo, &cid1);
        return -1;
      }
      log(LOG_DEBUG, "rread64_v3: malloc array allocated : 0X%X\n", array);

      /* Allocating memory for each element of circular buffer */
      for (el=0; el < daemonv3_rdmt_nbuf; el++) {
        log(LOG_DEBUG, "rread64_v3: allocating circular buffer element %d: %d bytes\n",
            el, daemonv3_rdmt_bufsize);
        if (myinfo.directio) {
          array[el].p = (char *)malloc_page_aligned(daemonv3_rdmt_bufsize);
        } else {
          array[el].p = (char *)malloc(daemonv3_rdmt_bufsize);
        }
        if ( array[el].p == NULL)  {
          log(LOG_ERR, "rread64_v3: malloc array element %d, size %d: ERROR %d occured\n",
              el, daemonv3_rdmt_bufsize, errno);
          readerror64_v3(ctrl_sock, &myinfo, &cid1);
          return -1;
        }
        log(LOG_DEBUG, "rread64_v3: malloc array element %d allocated : 0X%X\n",
            el, array[el].p);
      }
    }
    else {
      log(LOG_DEBUG, "rread64_v3: allocating malloc buffer : %d bytes\n", DISKBUFSIZE_READ);
      if (myinfo.directio) {
        iobuffer = (char *)malloc_page_aligned(DISKBUFSIZE_READ);
      } else {
        iobuffer = (char *)malloc(DISKBUFSIZE_READ);
      }
      if ( iobuffer == NULL)  {
        log(LOG_ERR, "rread64_v3: malloc: ERROR occured (errno=%d)\n", errno);
        readerror64_v3(ctrl_sock, &myinfo, &cid1);
        return -1;
      }
      log(LOG_DEBUG, "rread64_v3: malloc buffer allocated : 0X%X\n", iobuffer);
      iobufsiz = DISKBUFSIZE_READ;
    }

    if (fstat64(fd,&st) < 0) {
      log(LOG_ERR, "rread64_v3: fstat(): ERROR occured (errno=%d)\n", errno);
      readerror64_v3(ctrl_sock, &myinfo, &cid1);
      return -1;
    }

    log(LOG_DEBUG, "rread64_v3: filesize : %s bytes\n", u64tostr(st.st_size,tmpbuf,0));
    offsetout = lseek64(fd,0L,SEEK_CUR);
    if (offsetout == (off64_t)-1) {
#if defined(_WIN32)
      log(LOG_ERR, "rread64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd, geterr());
#else
      log(LOG_ERR, "rread64_v3: lseek64(%d,0,SEEK_CUR): %s\n", fd, strerror(errno));
#endif   /* WIN32 */
      readerror64_v3(ctrl_sock, &myinfo, &cid1);
      return -1;
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
      log(LOG_ERR, "rread64_v3: netwrite() ERROR: %s\n", strerror(errno));
#endif   /* else WIN32 */
      readerror64_v3(ctrl_sock, &myinfo, &cid1);
      return -1;
    }

#ifdef CS2
    if (ioctl(fd,NFSIOCACHE,CLIENTNOCACHE) < 0)
      log(LOG_ERR, "rread64_v3: ioctl client nocache error occured (errno=%d)\n", errno);

    if (ioctl(fd,NFSIOCACHE,SERVERNOCACHE) < 0)
      log(LOG_ERR, "rread64_v3: ioctl server nocache error occured (errno=%d)\n", errno);
    log(LOG_DEBUG,"rread64_v3: CS2: clientnocache, servernocache\n");
#endif

    if (daemonv3_rdmt) {
      Csemaphore_init(&empty64,daemonv3_rdmt_nbuf);
      Csemaphore_init(&full64,0);
      stop_read = 0;

      if ((cid1 = Cthread_create((void *(*)(void *))produce64_thread,(void *)&fd)) < 0) {
        log(LOG_ERR,"rread64_v3: Cannot create producer thread : serrno=%d,errno=%d\n",
            serrno, errno);
        readerror64_v3(ctrl_sock, &myinfo, &cid1);
        return(-1);
      }
    }
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

      log(LOG_DEBUG,"srread64_v3: doing select\n");
#if defined(_WIN32)
      if( select(FD_SETSIZE, &fdvar, write_fdset, NULL, &t) == SOCKET_ERROR ) {
        log(LOG_ERR, "srread64_v3: select failed: %s\n", geterr());
        readerror64_v3(ctrl_sock, &myinfo, &cid1);
        return -1;
      }
#else
      if( select(FD_SETSIZE, &fdvar, write_fdset, NULL, &t) < 0 ) {
        log(LOG_ERR, "srread64_v3: select failed: %s\n", strerror(errno));
        readerror64_v3(ctrl_sock, &myinfo, &cid1);
        return -1;
      }
#endif

      if( FD_ISSET(ctrl_sock, &fdvar) )  {
        int n, magic, code;

        /* Something received on the control socket */
        log(LOG_DEBUG, "srread64_v3: ctrl socket: reading %d bytes\n", RQSTSIZE);
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
          readerror64_v3(ctrl_sock, &myinfo, &cid1);
          return -1;
        }
        p = rqstbuf;
        unmarshall_WORD(p,magic);
        unmarshall_WORD(p,code);

        /* what to do ? */
        if (code == RQST_CLOSE64_V3)  {
          log(LOG_DEBUG,"srread64_v3: close request: magic: %x code: %x\n", magic, code);
          if (!daemonv3_rdmt) {
            log(LOG_DEBUG,"srread64_v3: freeing iobuffer at 0X%X\n", iobuffer);
            if (iobufsiz > 0) {
              if (myinfo.directio) {
                free_page_aligned(iobuffer);
              } else {
                free(iobuffer);
              }
              iobufsiz = 0;
              iobuffer = NULL;
            }
          }
          else {
            wait_producer64_thread(&cid1);
            if(cid1 >= 0) {
              log(LOG_ERR,"srread64_v3: Error joining producer, serrno=%d\n", serrno);
              readerror64_v3(ctrl_sock, &myinfo, &cid1);
              return(-1);
            }
            for (el=0; el < daemonv3_rdmt_nbuf; el++) {
              log(LOG_DEBUG,"srread64_v3: freeing array element %d at 0X%X\n", el,array[el].p);
              if (myinfo.directio) {
                free_page_aligned(array[el].p);
              } else {
                free(array[el].p);
              }
              array[el].p = NULL;
            }
            log(LOG_DEBUG,"srread64_v3: freeing array at 0X%X\n", array);
            free(array);
            array = NULL;
          }
          srclose64_v3(ctrl_sock,&myinfo,fd);
          return 0;
        }
        else  {
          log(LOG_ERR,"srread64_v3: unknown request:  magic: %x code: %x\n", magic,code);
          readerror64_v3(ctrl_sock, &myinfo, &cid1);
          return(-1);
        }
      }  /* if( FD_ISSET(ctrl_sock, &fdvar) ) */

      /*
       * Reading data on disk.
       */

      if( !eof_met && data_sock >= 0 && (FD_ISSET(data_sock, &fdvar2)) )  {
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
                readerror64_v3(ctrl_sock, &myinfo, &cid1);
                return(-1);
              }
              cid1 = -1;
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

        rcode = (status < 0) ? errno:0;
        log(LOG_DEBUG, "srread64_v3: %d bytes have been read on disk\n", status);

        if (status == 0)  {
          if (daemonv3_rdmt) {
            log(LOG_DEBUG, "srread64_v3: calling Csemaphore_up(&empty64)\n");
            Csemaphore_up(&empty64);
          }
          eof_met = 1;
          p = rqstbuf;
          marshall_WORD(p,REP_EOF);
          log(LOG_DEBUG, "rread64_v3: eof\n");
          errno = ECONNRESET;
          n = netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
          if (n != RQSTSIZE)  {
#if defined(_WIN32)
            log(LOG_ERR,"rread64_v3: netwrite(): %s\n", geterr());
#else    /* WIN32 */
            log(LOG_ERR,"rread64_v3: netwrite(): %s\n", strerror(errno));
#endif   /* else WIN32 */
            readerror64_v3(ctrl_sock, &myinfo, &cid1);
            return -1;
          }
        }  /*  status == 0 */
        else {
          if (status < 0)  {
            if (daemonv3_rdmt)
              Csemaphore_up(&empty64);
            p = rqstbuf;
            marshall_WORD(p, REP_ERROR);
            marshall_LONG(p, status);
            marshall_LONG(p, rcode);
            log(LOG_DEBUG, "rread64_v3: status %d, rcode %d\n", status, rcode);
            errno = ECONNRESET;
            n = netwrite_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
            if (n != RQSTSIZE)  {
#if defined(_WIN32)
              log(LOG_ERR, "rread64_v3: netwrite(): %s\n", geterr());
#else
              log(LOG_ERR, "rread64_v3: netwrite(): %s\n", strerror(errno));
#endif  /* WIN32 */
              readerror64_v3(ctrl_sock, &myinfo, &cid1);
              return -1;
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
            }  /* n != RQSTSIZE */
            readerror64_v3(ctrl_sock, &myinfo, &cid1);
            return(-1);
          }  /* status < 0  */
          else  {
            /* status > 0, reading was succesfully */
            log(LOG_DEBUG, "rread64_v3: writing %d bytes to data socket %d\n", status, data_sock);
#if defined(_WIN32)
            WSASetLastError(WSAECONNRESET);
            if( (n = send(data_sock, iobuffer, status, 0)) != status )  {
              log(LOG_ERR, "rread64_v3: send() (to data sock): %s\n", geterr() );
              readerror64_v3(ctrl_sock, &myinfo, &cid1);
              return -1;
            }
#else    /* WIN32 */
            errno = ECONNRESET;
            if( (n = netwrite(data_sock, iobuffer, status)) != status ) {
              log(LOG_ERR, "rread64_v3: netwrite(): %s\n", strerror(errno));
              readerror64_v3(ctrl_sock, &myinfo, &cid1);
              return -1;
            }
#endif   /* else WIN32 */
            if (daemonv3_rdmt)
              Csemaphore_up(&empty64);
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
static int   writerror64_v3(s, rcode, infop, cidp)
#if defined(_WIN32)
     SOCKET         s;
#else    /* WIN32 */
     int            s;
#endif   /* else WIN32 */
     int            rcode;
     struct rfiostat   *infop;
     int           *cidp;
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
    log(LOG_ERR, "writerror64_v3: write(): %s\n", strerror(errno));
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
        sizeofdummy, strerror(errno));

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

    log(LOG_DEBUG,"writerror64_v3: doing select after error writing on disk\n");
#if defined(_WIN32)
    if( select(FD_SETSIZE, &fdvar2, NULL, NULL, &t) == SOCKET_ERROR )
#else
      if( select(FD_SETSIZE, &fdvar2, NULL, NULL, &t) < 0 )
#endif
        {
#if defined(_WIN32)
          errno = WSAGetLastError();
#endif
          log(LOG_ERR, "writerror64_v3: select fdvar2 failed (errno=%d)\n", errno);
          /* Consumer thread already exited after error */
          break;
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
      s, u64tostr(infop->rnbr,tmpbuf,0), u64tostr(infop->wnbr,tmpbuf2,0));

  /* free the allocated ressources     */
  free(dummy);
  dummy = NULL;

  if (!daemonv3_wrmt) {
    log(LOG_DEBUG,"writerror64_v3: freeing iobuffer at 0X%X\n", iobuffer);
    if (myinfo.directio) {
      free_page_aligned(iobuffer);
    } else {
      free(iobuffer);
    }
    iobuffer = NULL;
    iobufsiz = 0;
  }
  else {
    wait_consumer64_thread(cidp);
    if (array) {
      for (el=0; el < daemonv3_wrmt_nbuf; el++) {
        log(LOG_DEBUG,"writerror64_v3: freeing array element %d at 0X%X\n",
            el,array[el].p);
        if (myinfo.directio) {
          free_page_aligned(array[el].p);
        } else {
          free(array[el].p);
        }
        array[el].p = NULL;
      }
      log(LOG_DEBUG,"writerror64_v3: freeing array at 0X%X\n", array);
      free(array);
      array = NULL;
    }
  }
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
  char        *p;            /* Pointer to buffer          */
#if !defined(_WIN32)
  char        *iobuffer;
#endif
  fd_set      fdvar;
  off64_t     byte_written_by_client = 0;
  int         eof_received       = 0;
  extern int  max_rcvbuf;
  int         maxseg;
  socklen_t optlen;
  int         byte_in_diskbuffer = 0;
  char        *iobuffer_p;
  struct      timeval t;
  int         DISKBUFSIZE_WRITE = (1*1024*1024);
  int         el;
  int         cid2 = -1;
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

#ifdef USE_XFSPREALLOC
    if (myinfo.xfsprealloc) {
      rfio_xfs_resvsp64(fd, myinfo.xfsprealloc);
    }
#endif

    first_write = 0;
    status = rfio_handle_firstwrite(handler_context);
    if (status != 0)  {
      log(LOG_ERR, "srwrite64_v3: rfio_handle_firstwrite: %s\n", strerror(serrno));
      writerror64_v3(ctrl_sock, EBUSY, &myinfo);
      return -1;
    }

    daemonv3_wrmt = DAEMONV3_WRMT;
    if( (p = getconfent("RFIO", "DAEMONV3_WRMT", 0)) != NULL ) {
      if (*p == '0')
        daemonv3_wrmt = 0;
      else
        daemonv3_wrmt = 1;
    }

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
        return -1;
      }
      log(LOG_DEBUG, "rwrite64_v3: malloc array allocated at 0X%X\n", array);

      /* Allocating memory for each element of circular buffer */
      for (el=0; el < daemonv3_wrmt_nbuf; el++) {
        log(LOG_DEBUG, "rwrite64_v3: allocating circular buffer element %d: %d bytes\n",
            el,daemonv3_wrmt_bufsize);
        if (myinfo.directio) {
          array[el].p = (char *)malloc_page_aligned(daemonv3_wrmt_bufsize);
        } else {
          array[el].p = (char *)malloc(daemonv3_wrmt_bufsize);
        }
        if ( array[el].p == NULL)  {
          log(LOG_ERR, "rwrite64_v3: malloc array element %d: ERROR occured (errno=%d)\n",
              el, errno);
          return -1;
        }
        log(LOG_DEBUG, "rwrite64_v3: malloc array element %d allocated at 0X%X\n",
            el, array[el].p);
      }
    }

    /* Don't allocate this buffer if we are multithreaded */
    if (!daemonv3_wrmt) {
      log(LOG_DEBUG, "rwrite64_v3: allocating malloc buffer: %d bytes\n", DISKBUFSIZE_WRITE);
      if ((iobuffer = (char *)malloc(DISKBUFSIZE_WRITE)) == NULL) {
        log(LOG_ERR, "rwrite64_v3: malloc: ERROR occured (errno=%d)\n", errno);
        return -1;
      }
      log(LOG_DEBUG, "rwrite64_v3: malloc buffer allocated at 0X%X\n", iobuffer);
      iobufsiz = DISKBUFSIZE_WRITE;
    }

    byte_in_diskbuffer = 0;
    if (daemonv3_wrmt)
      iobuffer_p = NULL; /* For safety */
    else
      iobuffer_p = iobuffer;

#if !defined(_WIN32)
    optlen = sizeof(maxseg);
    if (getsockopt(data_sock,IPPROTO_TCP,TCP_MAXSEG,(char *)&maxseg,&optlen) < 0) {
      log(LOG_ERR, "rwrite64_v3: getsockopt: ERROR occured (errno=%d)\n", errno);
      return -1;
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

    if (daemonv3_wrmt) {
      Csemaphore_init(&empty64,daemonv3_wrmt_nbuf);
      Csemaphore_init(&full64,0);

      if ((cid2 = Cthread_create((void *(*)(void *))consume64_thread,(void *)&fd)) < 0) {
        log(LOG_ERR,"rwrite64_v3: Cannot create consumer thread : serrno=%d, errno=%d\n",
            serrno, errno);
        return(-1);
      }
    }

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

    log(LOG_DEBUG,"rwrite64_v3: doing select\n");
#if defined(_WIN32)
    if( select(FD_SETSIZE, &fdvar, NULL, NULL, &t) == SOCKET_ERROR ) {
      log(LOG_ERR, "rwrite64_v3: select: %s\n", geterr());
      if (daemonv3_wrmt)
        wait_consumer64_thread(&cid2);
      return -1;
    }
#else    /* WIN32 */
    if( select(FD_SETSIZE, &fdvar, NULL, NULL, &t) < 0 )  {
      log(LOG_ERR, "rwrite64_v3: select: %s\n", strerror(errno));
      if (daemonv3_wrmt)
        wait_consumer64_thread(&cid2);
      return -1;
    }
#endif   /* else WIN32 */

    /* Anything received on control socket ?  */
    if( FD_ISSET(ctrl_sock, &fdvar) )  {
      int  n, magic, code;

      /* Something received on the control socket */
      log(LOG_DEBUG, "rwrite64_v3: ctrl socket: reading %d bytes\n", RQSTSIZE);
      n = netread_timeout(ctrl_sock, rqstbuf, RQSTSIZE, RFIO_CTRL_TIMEOUT);
      if ( n != RQSTSIZE )  {
        if (n == 0)  errno = ECONNRESET;
#if defined(_WIN32)
        log(LOG_ERR, "rwrite64_v3: read ctrl socket: netread(): %s\n", ws_strerr(errno));
#else
        log(LOG_ERR, "rwrite64_v3: read ctrl socket: netread(): %s\n", strerror(errno));
#endif      /* WIN32 */
        if (daemonv3_wrmt)
          wait_consumer64_thread(&cid2);
        return -1;
      }
      p = rqstbuf;
      unmarshall_WORD(p, magic);
      unmarshall_WORD(p, code);
      unmarshall_HYPER(p, byte_written_by_client);

      if (code == RQST_CLOSE64_V3) {
        log(LOG_DEBUG,"rwrite64_v3: close request:  magic: %x code: %x\n", magic, code);
        eof_received = 1;
      }
      else {
        log(LOG_ERR,"rwrite64_v3: unknown request:  magic: %x code: %x\n", magic, code);
        writerror64_v3(ctrl_sock, EINVAL, &myinfo, &cid2);
        return -1;
      }

      log(LOG_DEBUG, "rwrite64_v3: data socket: read_from_net=%s, written_by_client=%s\n",
          u64tostr(byte_read_from_network,tmpbuf,0), u64tostr(byte_written_by_client,tmpbuf2,0));

    }  /* if (FD_ISSET(ctrl_sock, &fdvar)) */


    /* Anything received on data  socket ?  */
    if (data_sock >= 0 && FD_ISSET(data_sock, &fdvar)) {
      int n;

      if ((daemonv3_wrmt) && (byte_in_diskbuffer == 0)) {
        log(LOG_DEBUG, "rwrite64_v3: Data received on data socket, new buffer %d requested\n",
            produced64 % daemonv3_wrmt_nbuf);
        Csemaphore_down(&empty64);
        iobuffer = iobuffer_p = array[produced64 % daemonv3_wrmt_nbuf].p;
      }

      log(LOG_DEBUG,"rwrite64_v3: iobuffer_p = %X, DISKBUFSIZE_WRITE = %d, data socket = %d\n",
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
          if (daemonv3_wrmt)
            wait_consumer64_thread(&cid2);
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
    if (byte_in_diskbuffer && (byte_in_diskbuffer == DISKBUFSIZE_WRITE ||
                               (eof_received && byte_written_by_client == byte_read_from_network)) )  {
      log(LOG_DEBUG, "rwrite64_v3: writing %d bytes on disk\n", byte_in_diskbuffer);
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

      rcode = (status < 0) ? errno:0;
      if (status < 0)  {
        /* Error in writting buffer                                 */
        writerror64_v3(ctrl_sock, rcode, &myinfo, &cid2);
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
      if (!daemonv3_wrmt) {
        log(LOG_DEBUG,"rwrite64_v3: freeing iobuffer at 0X%X\n", iobuffer);
        if (myinfo.directio) {
          free_page_aligned(iobuffer);
        } else {
          free(iobuffer);
        }
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
        cid2 = -1;

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
          writerror64_v3(ctrl_sock, rcode, &myinfo, &cid2);
          return -1;
        }  /* if (status < 0) */

        /* Free the buffers                                         */
        for (el=0; el < daemonv3_wrmt_nbuf; el++) {
          log(LOG_DEBUG,"rwrite64_v3: freeing array element %d at 0X%X\n", el, array[el].p);
          if (myinfo.directio) {
            free_page_aligned(array[el].p);
          } else {
            free(array[el].p);
          }
          array[el].p = NULL;
        }
        log(LOG_DEBUG,"rwrite64_v3: freeing array at 0X%X\n", array);
        free(array);
        array = NULL;
      }
#ifdef USE_XFSPREALLOC
      if (myinfo.xfsprealloc) {
        rfio_xfs_unresvsp64(fd, myinfo.xfsprealloc, myinfo.wnbr);
      }
#endif
      /* Send back final status and close sockets... and return     */
      srclose64_v3(ctrl_sock, &myinfo, fd);
      return 0;
    }  /* if ( eof_received && byte_written_by_client == myinfo.wnbr ) */

  }  /* while (1)  */
}
