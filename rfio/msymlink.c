/*
 * $Id: msymlink.c,v 1.3 2001/11/13 17:37:07 jdurand Exp $
 */


/*
 * Copyright (C) 1995-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: msymlink.c,v $ $Revision: 1.3 $ $Date: 2001/11/13 17:37:07 $ CERN/IT/PDP/DM Jean-Damien Durand";
#endif /* not lint */


#include <stdlib.h>
#include "Cmutex.h"
#include "Castor_limits.h"
#include <osdep.h>
#include "log.h"
#define RFIO_KERNEL 1
#include "rfio.h"
#include <Cglobals.h>
#include <Cpwd.h>
#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

typedef struct socks {
  char host[CA_MAXHOSTNAMELEN+1];
  int s ;
  int Tid;
#ifdef _WIN32
  int pid;
#else
  pid_t pid;
#endif
} msymlink_connects ;
static msymlink_connects msymlink_tab[MAXMCON]; /* UP TO MAXMCON connections simultaneously */

static int rfio_smsymlink _PROTO((int,  char *, char *));
static int rfio_msymlink_allocentry _PROTO((char *, int, int));
static int rfio_msymlink_findentry _PROTO((char *,int));
static int rfio_symend_this _PROTO((int,int));

int DLL_DECL rfio_msymlink(n1,file2)
     char *n1 ;
     char *file2 ;
{
  int rt ,rc ,i ,fd, rfindex, Tid;
  char *host , *filename ;

  INIT_TRACE("RFIO_TRACE");
  
  Cglobals_getTid(&Tid);
  
  TRACE(1, "rfio", "rfio_msymlink(\"%s\",\"%s\"), Tid=%d", n1, file2, Tid);
  if (!rfio_parseln(file2,&host,&filename,NORDLINKS)) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      rfio_errno = 0;
      serrno = SEOPNOTSUP;
      rc = -1;
      END_TRACE();
      return(rc);
    }
    /* The file is local */
#if ! defined(_WIN32)
      rc = symlink(n1,filename) ;
#else
    { serrno = SEOPNOTSUP; status = -1;}
#endif
    rfio_errno = 0;
    END_TRACE();
    return (rc) ;
  } else {
    /* Look if already in */
    serrno = 0;
    rfindex = rfio_msymlink_findentry(host,Tid);
    TRACE(2, "rfio", "rfio_msymlink: rfio_msymlink_findentry(host=%s,Tid=%d) returns %d", host, Tid, rfindex);
    if (rfindex >= 0) {
      rc = rfio_smsymlink(msymlink_tab[rfindex].s,n1,filename) ;
      END_TRACE();
      return ( rc) ;
    }
    rc = 0;
    fd=rfio_connect(host,&rt) ;
    if ( fd < 0 ) {
      END_TRACE();
      return (-1) ;
    }
    rfindex = rfio_msymlink_allocentry(host,Tid,fd);
    TRACE(2, "rfio", "rfio_msymlink: rfio_msymlink_allocentry(host=%s,Tid=%d,s=%d) returns %d", host, Tid, fd, rfindex);
    serrno = 0;
    if ( rfindex >= 0 ) {
      rc = rfio_smsymlink(fd,n1,filename);
    } else {
      rc = rfio_smsymlink(fd,n1,filename) ;
      if ( rc != -1 ) {
        TRACE(2,"rfio","rfio_msymlink() overflow connect table, Tid=%d. Closing %d",Tid,fd);
        (void)close(fd);
      }
      fd = -1;
    }
  }
  END_TRACE();
  return (rc)  ;
}

static int pw_key = -1;
static int old_uid_key = -1;

static int rfio_smsymlink(s,n1,filename) 
     int s ;
     char * n1 ;
     char * filename ;
{
  char     buf[256];
  int             status;         /* remote fopen() status        */
  int     len;
  int     rc, ans_req, rcode;
  char    *p=buf;
  int     uid;
  int     gid;
  int *old_uid = NULL;
  struct passwd *pw_tmp;
  struct passwd *pw = NULL;
  char *nbuf ;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_smsymlink(%s)", filename);

  if ( Cglobals_get(&old_uid_key, (void**)&old_uid, sizeof(int)) > 0 )
    *old_uid = -1;
  Cglobals_get(&pw_key, (void**)&pw, sizeof(struct passwd));
  
  len = strlen(filename)+1;
  uid = geteuid() ;
  gid = getegid () ;
  if ( uid != *old_uid ) {
    pw_tmp = Cgetpwuid(uid);
    if( pw_tmp  == NULL ) {
      TRACE(2, "rfio" ,"rfio_smsymlink: Cgetpwuid(): ERROR occured (errno=%d)",errno);
      END_TRACE();
      rfio_symend_this(s,1);
      return(-1);
    }	
    memcpy(pw, pw_tmp, sizeof(struct passwd));
    *old_uid = uid;
  }
  marshall_WORD(p, B_RFIO_MAGIC);
  marshall_WORD(p, RQST_MSYMLINK);
  status = strlen(pw->pw_name)+strlen(n1)+strlen(filename)+3+2*WORDSIZE;
  marshall_LONG(p, status) ;

  if (netwrite_timeout(s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio", "smsymlink: write(): ERROR occured (errno=%d)",
          errno);
    END_TRACE();
    rfio_symend_this(s,0);
    return(-1);
  }
  nbuf = (char *) malloc( status ) ;
  if ( nbuf == NULL ) {
    TRACE(2, "rfio", "smsymlink:  malloc () failed");
    END_TRACE();
    rfio_symend_this(s,1);
    return(-1);
  }
  
  p = nbuf ;

  marshall_WORD(p,uid) ;
  marshall_WORD(p,gid) ;
  marshall_STRING( p, n1 ) ;
  marshall_STRING( p, filename ) ;
  marshall_STRING( p, pw->pw_name) ;
	
  if (netwrite_timeout(s,nbuf,status,RFIO_CTRL_TIMEOUT) != status ) {
    TRACE(2, "rfio", "smsymlink: write(): ERROR occured (errno=%d)",errno);
    END_TRACE();
    rfio_symend_this(s,0);
    free(nbuf) ;
    return(-1);
  }
  free(nbuf) ;

  /*
   * Getting back status
   */ 
  if ((rc = netread_timeout(s, buf, WORDSIZE + 2*LONGSIZE, RFIO_CTRL_TIMEOUT)) != (WORDSIZE+ 2*LONGSIZE))  {
    TRACE(2, "rfio", "rfio_smsymlink: read(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    rfio_symend_this(s, (rc <= 0 ? 0 : 1));
    return(-1);
  }
  p = buf;
  unmarshall_WORD( p, ans_req );
  unmarshall_LONG( p, status ) ;
  unmarshall_LONG( p, rcode ) ;

  if ( ans_req != RQST_MSYMLINK ) {
    TRACE(1,"rfio","rfio_smsymlink: ERROR: answer does not correspond to request !");
    END_TRACE();
    rfio_symend_this(s,1);
    return(-1);
  }
  
  TRACE(1,"rfio","rfio_smsymlink: return %d",rcode);
  rfio_errno = rcode ;
  if ( status < 0 ) {
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(0) ;
}

int DLL_DECL rfio_symend()
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
  int rc = 0;
#ifdef _WIN32
  int pid = getpid();
#else
  pid_t pid = getpid();
#endif

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(3,"rfio","rfio_symend entered, Tid=%d", Tid);


  if (Cmutex_lock((void *) msymlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_symend : Cmutex_lock(msymlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    /* TRACE(3,"rfio","msymlink_tab[i]={host=\"%s\", s=%d, Tid=%d}", msymlink_tab[i].host, msymlink_tab[i].s, msymlink_tab[i].Tid); */
    if ((msymlink_tab[i].pid > 0) && (msymlink_tab[i].pid != pid)) {
      TRACE(3,"rfio","rfio_symend: msymlink_tab[%d].s=%d : Found different pid=%d (current pid is %d) - Cleaning this entry", i, msymlink_tab[i].s, msymlink_tab[i].pid, pid);
      msymlink_tab[i].s = -1;
      msymlink_tab[i].host[0] = '\0';
      msymlink_tab[i].Tid = -1;
      msymlink_tab[i].pid = 0;
      continue;
    }
    if (msymlink_tab[i].Tid == Tid) {
      if ((msymlink_tab[i].s >= 0) && (msymlink_tab[i].host[0] != '\0')) {
        marshall_WORD(p, RFIO_MAGIC);
        marshall_WORD(p, RQST_END);
        marshall_LONG(p, j);
        TRACE(3,"rfio","rfio_symend: close(msymlink_tab[%d].s=%d), Tid=%d",i,msymlink_tab[i].s, Tid);
        if (netwrite_timeout(msymlink_tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
          TRACE(3, "rfio", "rfio_symend: write(): ERROR occured (errno=%d), Tid=%d", errno, Tid);
          rc = -1;
        }
        (void) close(msymlink_tab[i].s);
      }
      msymlink_tab[i].s = -1;
      msymlink_tab[i].host[0] = '\0';
      msymlink_tab[i].Tid = -1;
      msymlink_tab[i].pid = 0;
    }
  }
   
  if (Cmutex_unlock((void *) msymlink_tab) != 0) {
    TRACE(3,"rfio","rfio_symend : Cmutex_unlock(msymlink_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

/* This is a simplified version of rfio_symend() that just free entry in the table */
/* If flag is set a clean close (write on the socket) is tried */
static int rfio_symend_this(s,flag)
     int s;
     int flag;
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
  int rc = 0;
#ifdef _WIN32
  int pid = getpid();
#else
  pid_t pid = getpid();
#endif

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(3,"rfio","rfio_symend_this(s=%d,flag=%d) entered, Tid=%d", s, flag, Tid);

  if (Cmutex_lock((void *) msymlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_symend_this : Cmutex_lock(msymlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    /* TRACE(3,"rfio","msymlink_tab[i]={host=\"%s\", s=%d, sec=%d, Tid=%d}", msymlink_tab[i].host, msymlink_tab[i].s, msymlink_tab[i].sec, msymlink_tab[i].Tid); */
    if ((msymlink_tab[i].pid > 0) && (msymlink_tab[i].pid != pid)) {
      TRACE(3,"rfio","rfio_symend_this: msymlink_tab[%d].s=%d : Found different pid=%d (current pid is %d) - Cleaning this entry", i, msymlink_tab[i].s, msymlink_tab[i].pid, pid);
      msymlink_tab[i].s = -1;
      msymlink_tab[i].host[0] = '\0';
      msymlink_tab[i].Tid = -1;
      msymlink_tab[i].pid = 0;
      continue;
    }
    if (msymlink_tab[i].Tid == Tid) {
      if ((msymlink_tab[i].s == s) && (msymlink_tab[i].host[0] != '\0')) {
        if (flag) {
          marshall_WORD(p, RFIO_MAGIC);
          marshall_WORD(p, RQST_END);
          marshall_LONG(p, j);
          TRACE(3,"rfio","rfio_symend_this: close(msymlink_tab[%d].s=%d), Tid=%d",i,msymlink_tab[i].s, Tid);
          if (netwrite_timeout(msymlink_tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
            TRACE(3, "rfio", "rfio_symend_this: netwrite_timeout(): ERROR occured (errno=%d), Tid=%d", errno, Tid);
          }
        }
        (void) close(msymlink_tab[i].s);
        msymlink_tab[i].s = -1;
        msymlink_tab[i].host[0] = '\0';
        msymlink_tab[i].Tid = -1;
        msymlink_tab[i].pid = 0;
      }
    }
  }
   
  if (Cmutex_unlock((void *) msymlink_tab) != 0) {
    TRACE(3,"rfio","rfio_symend_this : Cmutex_unlock(msymlink_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

/*
 * Seach for a free index in the msymlink_tab table
 */
static int rfio_msymlink_allocentry(hostname,Tid,s)
     char *hostname;
     int Tid;
     int s;
{
  int i;
  int rc;
#ifdef _WIN32
  int pid = getpid();
#else
  pid_t pid = getpid();
#endif

  INIT_TRACE("RFIO_TRACE");

  if (Cmutex_lock((void *) msymlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_msymlink_allocentry : Cmutex_lock(msymlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if ((msymlink_tab[i].pid > 0) && (msymlink_tab[i].pid != pid)) {
      TRACE(3,"rfio","rfio_msymlink_allocentry: msymlink_tab[%d].s=%d : Found different pid=%d (current pid is %d) - Cleaning this entry", i, msymlink_tab[i].s, msymlink_tab[i].pid, pid);
      msymlink_tab[i].s = -1;
      msymlink_tab[i].host[0] = '\0';
      msymlink_tab[i].Tid = -1;
      msymlink_tab[i].pid = 0;
    }
    if (msymlink_tab[i].host[0] == '\0') {
      rc = i;
      strncpy(msymlink_tab[i].host,hostname,CA_MAXHOSTNAMELEN);
      msymlink_tab[i].host[CA_MAXHOSTNAMELEN] = '\0';
      msymlink_tab[i].Tid = Tid;
      msymlink_tab[i].s = s;
      msymlink_tab[i].pid = pid;
      goto _rfio_msymlink_allocentry_return;
    }
  }
  
  serrno = ENOENT;
  rc = -1;
  
 _rfio_msymlink_allocentry_return:
  if (Cmutex_unlock((void *) msymlink_tab) != 0) {
    TRACE(3,"rfio","rfio_msymlink_allocentry : Cmutex_unlock(msymlink_tab) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(rc);
}

/*
 * Seach for a given index in the msymlink_tab table
 */
static int rfio_msymlink_findentry(hostname,Tid)
     char *hostname;
     int Tid;
{
  int i;
  int rc;
#ifdef _WIN32
  int pid = getpid();
#else
  pid_t pid = getpid();
#endif

  INIT_TRACE("RFIO_TRACE");

  if (Cmutex_lock((void *) msymlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_msymlink_findentry : Cmutex_lock(msymlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if ((msymlink_tab[i].pid > 0) && (msymlink_tab[i].pid != pid)) {
      TRACE(3,"rfio","rfio_msymlink_findentry: msymlink_tab[%d].s=%d : Found different pid=%d (current pid is %d) - Cleaning this entry", i, msymlink_tab[i].s, msymlink_tab[i].pid, pid);
      msymlink_tab[i].s = -1;
      msymlink_tab[i].host[0] = '\0';
      msymlink_tab[i].Tid = -1;
      msymlink_tab[i].pid = 0;
    }
    if ((strcmp(msymlink_tab[i].host,hostname) == 0) && (msymlink_tab[i].Tid == Tid)) {
      rc = i;
      goto _rfio_msymlink_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_msymlink_findentry_return:
  if (Cmutex_unlock((void *) msymlink_tab) != 0) {
    TRACE(3,"rfio","rfio_msymlink_findentry : Cmutex_unlock(msymlink_tab) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(rc);
}
