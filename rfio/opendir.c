/*
 * Copyright (C) 1990-1997 by CERN/IT/PDP/IP
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)opendir.c	1.2 06/02/98 O.Barring";
#endif /* not lint */

/* opendir.c       Remote File I/O - open a directory                   */
#if !defined(_WIN32)
#define RFIO_KERNEL     1       /* system part of Remote File I/O       */

#include <syslog.h>             /* system logger                        */
#include <pwd.h>
#include <stdlib.h>
#include "rfio.h"               /* remote file I/O definitions          */
#include "rfcntl.h"             /* remote file control mapping macros   */

#ifndef linux
extern char *sys_errlist[];     /* system error list                    */
#endif /* linux */

RDIR  *rdirfdt[MAXRFD] =        /* File descriptors tables             */
{
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
NULL, NULL, NULL, NULL, NULL, NULL };

/*
 * Forward declaration
 */
RDIR *rfio_opendir_ext();

/*
 * General purpose buffer.
 * It is used also in fstat.c, read.c
 * write.c, lseek.c and close.c
 */
char     rfio_buf[BUFSIZ] ;

static void rfio_dirsetup_ext(iop,uid,gid,passwd)
     RDIR   *iop;
     int    uid;
     int    gid;
     int    passwd;
{
  iop->magic = RFIO_MAGIC;
  iop->s = -1;
  if (uid || gid)
    iop->mapping = 0;
  else
    iop->mapping = 1;
  iop->passwd = passwd;     /* used only if mapping == 0 */
  iop->uid = (uid==0 ? geteuid() : uid);
  iop->gid = (gid==0 ? getegid() : gid);
  INIT_TRACE("RFIO_TRACE");
  TRACE ( 1,"rfio","rfio_dirsetup_ext(%d,%d,%d)",iop,uid,gid);
  TRACE ( 2,"rfio","rfio_dirsetup_ext: owner s uid is %d",iop->uid);
  TRACE ( 2,"rfio","rfio_dirsetup_ext: owner s gid is %d",iop->gid);
  END_TRACE();
  iop->offset = 0;
  strcpy(iop->host,"????????");
}

int rfio_dircleanup(s)      /* cleanup rfio dir. descriptor            */
int s;
{
  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_dircleanup(%d)", s);
  
  if (rdirfdt[s] != NULL) {
    if (rdirfdt[s]->magic != RFIO_MAGIC && rdirfdt[s]->magic != B_RFIO_MAGIC) {
      serrno = SEBADVERSION ; 
      END_TRACE();
      return(-1);
    }
    TRACE(2, "rfio", "closing %d",s) ;
    (void) close(s) ;
    TRACE(2, "rfio", "freeing RFIO directory descriptor at 0X%X", rdirfdt[s]);
    (void) free((char *)rdirfdt[s]->dp.dd_buf);
    (void) free((char *)rdirfdt[s]);
    rdirfdt[s] = NULL;
  }
  END_TRACE();
  return(0);
}

RDIR *rfio_opendir(dirpath)
     char *dirpath;
{
  char rh[1];
  rh[0] = '\0';
  return(rfio_opendir_ext(dirpath,(uid_t)0,(gid_t)0,0,rh,rh));
}

RDIR *rfio_opendir_ext(dirpath,uid,gid,passwd,reqhost,vmstr)
     char *dirpath;
     uid_t uid;
     gid_t gid;
     int passwd;
     char 	* reqhost; /* In case of a Non-mapped I/O with uid & gid 
			      sepcified, which host will be contacted
			      for key check ? */
     char  	*vmstr ;
{
  int     status ;	/* Return code 			*/
  int	 rcode ;	/* Remote errno			*/
  int        len ;
  char    * host ; 
  char  * account;
  char  *dirname ;
  char       * p ;	/* Pointer to rfio buffer	*/
  RDIR      *rdp ;      /* Remote directory pointer     */
  RDIR      * dp ;      /* Local directory pointer      */
  WORD	   req ;
  struct passwd *pw;
  int 	    rt ; 	/* daemon in site(0) or not (1) */	
  int    bufsize ; 	/* socket buffer size 		*/	
  struct sockaddr_in      to;
  int                     tolen;
  struct  hostent *hp;
  extern void rfio_dirsetup_ext();

  INIT_TRACE("RFIO_TRACE");
  TRACE(1,"rfio","rfio_opendir(%s)",dirpath);
  /*
   * The directory is local.
   */
  if ( ! rfio_parse(dirpath,&host,&dirname) ) {
    dp = (RDIR *)opendir(dirname);
    END_TRACE() ; 
    return(dp);
  }
  /*
   * Allocate and initialize a remote directory descriptor.
   */
  if ((rdp = (RDIR *)malloc(sizeof(RDIR))) == NULL)        {
    TRACE(2, "rfio", "rfio_opendir: malloc(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    return(NULL);
  }
  (void) memset(rdp,'\0',sizeof(RDIR));
  rfio_dirsetup_ext(rdp,(int)uid,(int)gid,passwd) ;
  rdp->s = rfio_connect(host,&rt);
  if (rdp->s < 0)      {
    TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rdp);
    (void) free(rdp);
    END_TRACE();
    return(NULL);
  }
  tolen=sizeof(to);
  if (getpeername(rdp->s,(struct sockaddr *)&to, &tolen)<0)        {
    syslog(LOG_ALERT, "rfio: opendir: getpeername: %s\n",sys_errlist[errno]);
  }
  if ((hp = gethostbyaddr((char *) (&to.sin_addr), sizeof(struct in_addr), to.sin_family)) == NULL){
    strncpy(rdp->host, (char *)inet_ntoa(to.sin_addr), RESHOSTNAMELEN );
  }
  else    {
    strncpy(rdp->host,hp->h_name, RESHOSTNAMELEN );
  }
  
  
  if ( !rt && !rdp->mapping ) {
    rdp->uid=geteuid() ;
    rdp->gid=getegid() ;
    TRACE(3,"rfio", "re-setting (uid,gid) to %d,%d",rdp->uid,rdp->gid) ;
    rdp->mapping = 1 ;
  }
  /*
   * Remote file table is not large enough.
   */
  if ( rdp->s >= MAXRFD ) {
    TRACE(2, "rfio", "freeing RFIO descriptor at 0X%X", rdp);
    (void) free(rdp);
    END_TRACE();
    errno= EMFILE ;
    return(NULL) ;
  }
  rdirfdt[rdp->s]=rdp;
  /*
   * Reserve space for the dirent buffer associated with this directory stream
   */
  if ( (p = (char *)malloc(sizeof(struct dirent)+MAXFILENAMSIZE)) == NULL ) {
    TRACE(2, "rfio", "rfio_opendir: malloc(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    (void) free(rdp);
    return(NULL);
  }
  rdp->dp.dd_buf = p;
  rdp->dp.dd_size = sizeof(struct dirent)+MAXFILENAMSIZE;

  TRACE(2, "rfio", "rfio_opendir: setsockopt(SOL_SOCKET, SO_KEEPALIVE)");
  rcode = 1 ;
  if (setsockopt(rdp->s, SOL_SOCKET, SO_KEEPALIVE,(char *)&rcode, sizeof (int) ) == -1) {
    TRACE(2, "rfio" ,"rfio_opendir: setsockopt(SO_KEEPALIVE) failed");
    syslog(LOG_ALERT, "rfio: opendir: setsockopt(SO_KEEPALIVE): %s", sys_errlist[errno]);
  }
  if ( (pw = getpwuid(geteuid()) ) == NULL ) {
    TRACE(2, "rfio" ,"rfio_opendir: getpwuid() error %s",sys_errlist[errno]);
    rfio_dircleanup(rdp->s);
    END_TRACE();
    return(NULL) ;
  }
  /*
   * Building and sending request.
   */
  if ((account = getacct()) == NULL) account = "";
  TRACE(2,"rfio","rfio_opendir: uid %d gid %d",
	rdp->uid,rdp->gid) ;
  TRACE(2,"rfio","rfio_opendir: account: %s",account) ;
  TRACE(2,"rfio","rfio_opendir: dirname: %s",dirname) ;
  if (reqhost != NULL && strlen(reqhost) )
    TRACE(2,"rfio","rfio_opendir: requestor's host: %s",reqhost) ;
  p= rfio_buf ;
  len= 3*WORDSIZE + LONGSIZE + strlen(account) + strlen(dirname) +strlen(pw->pw_name) + strlen(reqhost) + strlen(vmstr) + 5 ;
  marshall_WORD(p,RFIO_MAGIC) ;
  marshall_WORD(p,RQST_OPENDIR) ;
  marshall_LONG(p,len) ;
  p= rfio_buf + RQSTSIZE ;
  marshall_WORD(p,rdp->uid) ;
  marshall_WORD(p,rdp->gid) ;
  marshall_STRING(p,account) ;
  marshall_STRING(p,dirname) ;
  marshall_STRING(p,pw->pw_name) ;
  marshall_STRING(p,reqhost) ;
  marshall_LONG(p,rdp->passwd);
  marshall_WORD(p,rdp->mapping);
  marshall_STRING(p, vmstr) ;
  TRACE(2,"rfio","rfio_opendir: sending %d bytes",RQSTSIZE+len) ;
  if (netwrite(rdp->s,rfio_buf,RQSTSIZE+len) != RQSTSIZE+len) {
    TRACE(2,"rfio","rfio_opendir: write(): ERROR occured (errno=%d)", errno) ;
    syslog(LOG_ALERT, "rfio: opendir: %s (error %d with %s) [uid=%d,gid=%d,pid=%d] in netwrite(%d,0X%X,%d)",
	   sys_errlist[errno], errno, rdp->host, rdp->uid, rdp->gid, getpid(), rdp->s, rfio_buf, RQSTSIZE+len);
    rfio_dircleanup(rdp->s) ;
    END_TRACE() ;
    return(NULL) ;
  }
  /*
   * Getting status.
   */
  TRACE(1, "rfio", "rfio_opendir: reading %d bytes",WORDSIZE+3*LONGSIZE) ;
  if (netread(rdp->s,rfio_buf,WORDSIZE+3*LONGSIZE) != WORDSIZE+3*LONGSIZE ) {
    TRACE(2, "rfio", "rfio_opendir: read(): ERROR occured (errno=%d)", errno);
    syslog(LOG_ALERT, "rfio: opendir: %s (error %d with %s) [uid=%d,gid=%d,pid=%d] in netread(%d,0X%X,%d)",
	   sys_errlist[errno], errno, rdp->host, rdp->uid, rdp->gid, getpid(), rdp->s, 
	   rfio_buf, WORDSIZE+3*LONGSIZE);
    rfio_dircleanup(rdp->s);
    END_TRACE();
    return(NULL);
  }
  p= rfio_buf ;
  unmarshall_WORD(p,req) ; 
  unmarshall_LONG(p,status) ;
  unmarshall_LONG(p, rcode) ; 
  TRACE(1,"rfio","rfio_opendir: return status(%d), rcode(%d), fd: %d",status,rcode,rdp->s) ;
  if ( status < 0 ) {
    if ( rcode >= SEBASEOFF)
      serrno = rcode ;
    else
      rfio_errno= rcode ;
    /* Operation failed but no error message was sent */
    if ( rcode == 0 )
      serrno = SENORCODE ;
    rfio_dircleanup(rdp->s) ;
    END_TRACE() ;
    return(NULL);
  }
  /*
   * The directory is open, update rdp->dp
   */
  rdp->dp.dd_fd = rdp->s;
  /*
   * Logical (dirent) offset in directory
   */
  rdp->dp.dd_loc = 0;

  END_TRACE() ;
  return(rdp);
}
#endif /* _WIN32 */
