/*
 * $Id: munlink.c,v 1.4 2001/11/14 10:44:28 jdurand Exp $
 */


/*
 * Copyright (C) 1995-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: munlink.c,v $ $Revision: 1.4 $ $Date: 2001/11/14 10:44:28 $ CERN/IT/PDP/DM Jean-Damien Durand";
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

#ifdef _WIN32
typedef int _rfio_pgrp_t;
#else
typedef pid_t _rfio_pgrp_t;
#endif

typedef struct socks {
  char host[CA_MAXHOSTNAMELEN+1];
  int s ;
  int Tid;
  _rfio_pgrp_t pgrp;
} munlink_connects ;
static munlink_connects munlink_tab[MAXMCON]; /* UP TO MAXMCON connections simultaneously */

static int rfio_smunlink _PROTO((int, char *));
static int rfio_munlink_allocentry _PROTO((char *, int, int));
static int rfio_munlink_findentry _PROTO((char *,int));
static int rfio_unend_this _PROTO((int,int));
static int rfio_munlink_findpgrp _PROTO((int));

int DLL_DECL rfio_munlink(file)
     char *file ;
{
  int rt ,rc ,i ,fd, rfindex, Tid;
  char *host , *filename ;

  INIT_TRACE("RFIO_TRACE");
  
  Cglobals_getTid(&Tid);
  
  TRACE(1, "rfio", "rfio_munlink(\"%s\"), Tid=%d", file, Tid);
  if (!rfio_parseln(file,&host,&filename,NORDLINKS)) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      rfio_errno = 0;
      rc = rfio_HsmIf_unlink(filename);
      END_TRACE();
      return(rc);
    }
    /* The file is local */
    rc = unlink(filename) ;
    rfio_errno = 0;
    END_TRACE();
    return (rc) ;
  } else {
    /* Look if already in */
    serrno = 0;
    rfindex = rfio_munlink_findentry(host,Tid);
    TRACE(2, "rfio", "rfio_munlink: rfio_munlink_findentry(host=%s,Tid=%d) returns %d", host, Tid, rfindex);
    if (rfindex >= 0) {
      rc = rfio_smunlink(munlink_tab[rfindex].s,filename) ;
      END_TRACE();
      return ( rc) ;
    }
    rc = 0;
    fd=rfio_connect(host,&rt) ;
    if ( fd < 0 ) {
      END_TRACE();
      return (-1) ;
    }
    rfindex = rfio_munlink_allocentry(host,Tid,fd);
    TRACE(2, "rfio", "rfio_munlink: rfio_munlink_allocentry(host=%s,Tid=%d,s=%d) returns %d", host, Tid, fd, rfindex);
    serrno = 0;
    if ( rfindex >= 0 ) {
      rc = rfio_smunlink(fd,filename);
    } else {
      rc = rfio_smunlink(fd,filename) ;
      if ( rc != -1 ) {
        TRACE(2,"rfio","rfio_munlink() overflow connect table, host=%s, Tid=%d. Closing %d",host,Tid,fd);
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

static int rfio_smunlink(s,filename) 
     int s ;
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
  char *n1 = "";

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_smunlink(%s)", filename);

  if ( Cglobals_get(&old_uid_key, (void**)&old_uid, sizeof(int)) > 0 )
    *old_uid = -1;
  Cglobals_get(&pw_key, (void**)&pw, sizeof(struct passwd));
  
  len = strlen(filename)+1;
  uid = geteuid() ;
  gid = getegid () ;
  if ( uid != *old_uid ) {
    pw_tmp = Cgetpwuid(uid);
    if( pw_tmp  == NULL ) {
      TRACE(2, "rfio" ,"rfio_smunlink: Cgetpwuid(): ERROR occured (errno=%d)",errno);
      END_TRACE();
      rfio_unend_this(s,1);
      return(-1) ;
    }	
    memcpy(pw, pw_tmp, sizeof(struct passwd));
    *old_uid = uid;
  }
  marshall_WORD(p, B_RFIO_MAGIC);
  marshall_WORD(p, RQST_MSYMLINK);
  status = strlen(pw->pw_name)+strlen(n1)+strlen(filename)+3+2*WORDSIZE;
  marshall_LONG(p, status) ;

  if (netwrite_timeout(s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
    TRACE(2, "rfio", "smunlink: write(): ERROR occured (errno=%d)",
          errno);
    END_TRACE();
    rfio_unend_this(s,0);
    return(-1);
  }
  nbuf = (char *) malloc( status ) ;
  if ( nbuf == NULL ) {
    TRACE(2, "rfio", "smunlink:  malloc () failed");
    END_TRACE();
    rfio_unend_this(s,1);
    return(-1);
  }
  
  p = nbuf ;

  marshall_WORD(p,uid) ;
  marshall_WORD(p,gid) ;
  marshall_STRING( p, n1 ) ;
  marshall_STRING( p, filename ) ;
  marshall_STRING( p, pw->pw_name) ;
	
  if (netwrite_timeout(s,nbuf,status,RFIO_CTRL_TIMEOUT) != status ) {
    TRACE(2, "rfio", "smunlink: write(): ERROR occured (errno=%d)",errno);
    END_TRACE();
    rfio_unend_this(s,0);
    free(nbuf);
    return(-1);
  }
  free(nbuf) ;

  /*
   * Getting back status
   */ 
  if ((rc = netread_timeout(s, buf, WORDSIZE + 2*LONGSIZE, RFIO_CTRL_TIMEOUT)) != (WORDSIZE+ 2*LONGSIZE))  {
    TRACE(2, "rfio", "rfio_smunlink: read(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    rfio_unend_this(s, (rc <= 0 ? 0 : 1));
    return(-1);
  }
  p = buf;
  unmarshall_WORD( p, ans_req );
  unmarshall_LONG( p, status ) ;
  unmarshall_LONG( p, rcode ) ;

  if ( ans_req != RQST_MSYMLINK ) {
    TRACE(1,"rfio","rfio_smunlink: ERROR: answer does not correspond to request !");
    END_TRACE();
    rfio_unend_this(s,1);
    return(-1);
  }
  
  TRACE(1,"rfio","rfio_smunlink: return %d",rcode);
  rfio_errno = rcode ;
  if ( status < 0 ) {
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(0) ;
}

static int rfio_munlink_findpgrp(Tid)
     int Tid;
{
  if (Tid < 0) {
    return(getpid());
  } else {
#ifdef linux
    return(getpgrp());
#else
    return(getpid());
#endif
  }
}

int DLL_DECL rfio_unend()
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
  int rc = 0;
  _rfio_pgrp_t pgrp;

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);
  pgrp = rfio_munlink_findpgrp(Tid);

  TRACE(3,"rfio","rfio_unend entered, Tid=%d, pgrp=%d", Tid, (int) pgrp);

  TRACE(3,"rfio","rfio_unend: Lock munlink_tab");
  if (Cmutex_lock((void *) munlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_unend: Cmutex_lock(munlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    if ((munlink_tab[i].pgrp > 0) && (munlink_tab[i].pgrp != pgrp)) {
      TRACE(3,"rfio","rfio_unend: munlink_tab[%d].s=%d - Found different pgrp=%d (current pgrp is %d) - Cleaning this entry", i, munlink_tab[i].s, munlink_tab[i].pgrp, pgrp);
      munlink_tab[i].s = -1;
      munlink_tab[i].host[0] = '\0';
      munlink_tab[i].Tid = -1;
      munlink_tab[i].pgrp = 0;
      continue;
    }
    if (munlink_tab[i].Tid == Tid) {
      if ((munlink_tab[i].s >= 0) && (munlink_tab[i].host[0] != '\0')) {
        marshall_WORD(p, RFIO_MAGIC);
        marshall_WORD(p, RQST_END);
        marshall_LONG(p, j);
        TRACE(3,"rfio","rfio_unend: close(munlink_tab[%d].s=%d), host=%s, Tid=%d",i,munlink_tab[i].s, munlink_tab[i].host, Tid);
        if (netwrite_timeout(munlink_tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
          TRACE(3, "rfio", "rfio_unend: write(): ERROR occured (errno=%d), Tid=%d", errno, Tid);
          rc = -1;
        }
        (void) close(munlink_tab[i].s);
      }
      munlink_tab[i].s = -1;
      munlink_tab[i].host[0] = '\0';
      munlink_tab[i].Tid = -1;
      munlink_tab[i].pgrp = 0;
    }
  }
   
  TRACE(3,"rfio","rfio_unend: Unlock munlink_tab");
  if (Cmutex_unlock((void *) munlink_tab) != 0) {
    TRACE(3,"rfio","rfio_unend: Cmutex_unlock(munlink_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

/* This is a simplified version of rfio_unend() that just free entry in the table */
/* If flag is set a clean close (write on the socket) is tried */
static int rfio_unend_this(s,flag)
     int s;
     int flag;
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
  int rc = 0;
  _rfio_pgrp_t pgrp;

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);
  pgrp = rfio_munlink_findpgrp(Tid);

  TRACE(3,"rfio","rfio_unend_this(s=%d,flag=%d) entered, Tid=%d, pgrp=%d", s, flag, Tid, (int) pgrp);

  TRACE(3,"rfio","rfio_unend_this: Lock munlink_tab");
  if (Cmutex_lock((void *) munlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_unend_this: Cmutex_lock(munlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    if ((munlink_tab[i].pgrp > 0) && (munlink_tab[i].pgrp != pgrp)) {
      TRACE(3,"rfio","rfio_unend_this: munlink_tab[%d].s=%d - Found different pgrp=%d (current pgrp is %d) - Cleaning this entry", i, munlink_tab[i].s, munlink_tab[i].pgrp, pgrp);
      munlink_tab[i].s = -1;
      munlink_tab[i].host[0] = '\0';
      munlink_tab[i].Tid = -1;
      munlink_tab[i].pgrp = 0;
      continue;
    }
    if (munlink_tab[i].Tid == Tid) {
      if ((munlink_tab[i].s == s) && (munlink_tab[i].host[0] != '\0')) {
        if (flag) {
          marshall_WORD(p, RFIO_MAGIC);
          marshall_WORD(p, RQST_END);
          marshall_LONG(p, j);
          TRACE(3,"rfio","rfio_unend_this: close(munlink_tab[%d].s=%d), host=%s, Tid=%d",i,munlink_tab[i].s, munlink_tab[i].host, Tid);
          if (netwrite_timeout(munlink_tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
            TRACE(3, "rfio", "rfio_unend_this: netwrite_timeout(): ERROR occured (errno=%d), Tid=%d", errno, Tid);
          }
        }
        (void) close(munlink_tab[i].s);
        munlink_tab[i].s = -1;
        munlink_tab[i].host[0] = '\0';
        munlink_tab[i].Tid = -1;
        munlink_tab[i].pgrp = 0;
      }
    }
  }
   
  TRACE(3,"rfio","rfio_unend_this: Unlock munlink_tab");
  if (Cmutex_unlock((void *) munlink_tab) != 0) {
    TRACE(3,"rfio","rfio_unend_this: Cmutex_unlock(munlink_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

/*
 * Seach for a free index in the munlink_tab table
 */
static int rfio_munlink_allocentry(hostname,Tid,s)
     char *hostname;
     int Tid;
     int s;
{
  int i;
  int rc;
  _rfio_pgrp_t pgrp;

  pgrp = rfio_munlink_findpgrp(Tid);

  INIT_TRACE("RFIO_TRACE");

  TRACE(3,"rfio","rfio_munlink_allocentry: Lock munlink_tab");
  if (Cmutex_lock((void *) munlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_munlink_allocentry: Cmutex_lock(munlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if ((munlink_tab[i].pgrp > 0) && (munlink_tab[i].pgrp != pgrp)) {
      TRACE(3,"rfio","rfio_munlink_allocentry: munlink_tab[%d].s=%d - Found different pgrp=%d (current pgrp is %d) - Cleaning this entry", i, munlink_tab[i].s, munlink_tab[i].pgrp, pgrp);
      munlink_tab[i].s = -1;
      munlink_tab[i].host[0] = '\0';
      munlink_tab[i].Tid = -1;
      munlink_tab[i].pgrp = 0;
    }
    if (munlink_tab[i].host[0] == '\0') {
      rc = i;
      strncpy(munlink_tab[i].host,hostname,CA_MAXHOSTNAMELEN);
      munlink_tab[i].host[CA_MAXHOSTNAMELEN] = '\0';
      munlink_tab[i].Tid = Tid;
      munlink_tab[i].s = s;
      munlink_tab[i].pgrp = pgrp;
      goto _rfio_munlink_allocentry_return;
    }
  }
  
  serrno = ENOENT;
  rc = -1;
  
 _rfio_munlink_allocentry_return:
  TRACE(3,"rfio","rfio_munlink_allocentry: Unlock munlink_tab");
  if (Cmutex_unlock((void *) munlink_tab) != 0) {
    TRACE(3,"rfio","rfio_munlink_allocentry: Cmutex_unlock(munlink_tab) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(rc);
}

/*
 * Seach for a given index in the munlink_tab table
 */
static int rfio_munlink_findentry(hostname,Tid)
     char *hostname;
     int Tid;
{
  int i;
  int rc;
  _rfio_pgrp_t pgrp;

  pgrp = rfio_munlink_findpgrp(Tid);

  INIT_TRACE("RFIO_TRACE");

  TRACE(3,"rfio","rfio_munlink_findentry: Lock munlink_tab");
  if (Cmutex_lock((void *) munlink_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_munlink_findentry: Cmutex_lock(munlink_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if ((munlink_tab[i].pgrp > 0) && (munlink_tab[i].pgrp != pgrp)) {
      TRACE(3,"rfio","rfio_munlink_findentry: munlink_tab[%d].s=%d - Found different pgrp=%d (current pgrp is %d) - Cleaning this entry", i, munlink_tab[i].s, munlink_tab[i].pgrp, pgrp);
      munlink_tab[i].s = -1;
      munlink_tab[i].host[0] = '\0';
      munlink_tab[i].Tid = -1;
      munlink_tab[i].pgrp = 0;
    }
    if ((strcmp(munlink_tab[i].host,hostname) == 0) && (munlink_tab[i].Tid == Tid)) {
      rc = i;
      goto _rfio_munlink_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_munlink_findentry_return:
  TRACE(3,"rfio","rfio_munlink_findentry: Unlock munlink_tab");
  if (Cmutex_unlock((void *) munlink_tab) != 0) {
    TRACE(3,"rfio","rfio_munlink_findentry: Cmutex_unlock(munlink_tab) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(rc);
}
