/*
 * $Id: mstat.c,v 1.21 2002/02/18 09:47:11 jdurand Exp $
 */


/*
 * Copyright (C) 1995-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: mstat.c,v $ $Revision: 1.21 $ $Date: 2002/02/18 09:47:11 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */


#include "Cmutex.h"
#include "Castor_limits.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#if defined(_WIN32)
#define MAXHOSTNAMELEN 64
#else
#include <sys/param.h>
#endif
#include <osdep.h>
#include "log.h"
#define RFIO_KERNEL 1
#include "rfio.h"
#include <Cglobals.h>
#include <Cpwd.h>
#include <net.h>

typedef struct socks {
  char host[CA_MAXHOSTNAMELEN+1];
  int s ;
  int sec;
  int Tid;
} mstat_connects ;
static mstat_connects mstat_tab[MAXMCON]; /* UP TO MAXMCON connections simultaneously */

EXTERN_C int DLL_DECL rfio_smstat _PROTO((int, char *, struct stat *, int));
static int rfio_mstat_allocentry _PROTO((char *, int, int, int));
static int rfio_mstat_findentry _PROTO((char *,int));
static int rfio_end_this _PROTO((int,int));

int DLL_DECL rfio_mstat(file,statb)
     char *file ;
     struct stat *statb;

{
  int rt ,rc ,i ,fd, rfindex, Tid;
  char *host , *filename ;

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(1, "rfio", "rfio_mstat(%s, %x), Tid=%d", file, statb, Tid);
  if (!rfio_parseln(file,&host,&filename,NORDLINKS)) {
    /* if not a remote file, must be local or HSM  */
    if ( host != NULL ) {
      /*
       * HSM file
       */
      rfio_errno = 0;
      rc = rfio_HsmIf_stat(filename,statb);
      END_TRACE();
      return(rc);
    }

    /* The file is local */
    rc = stat(filename,statb) ;
    rfio_errno = 0;
    END_TRACE();
    return (rc) ;
  }  else  {
    /* Look if already in */
    serrno = 0;
    rfindex = rfio_mstat_findentry(host,Tid);
    TRACE(2, "rfio", "rfio_mstat: rfio_mstat_findentry(host=%s,Tid=%d) returns %d", host, Tid, rfindex);
    if (rfindex >= 0) {
      if ( mstat_tab[rfindex].sec ) {
        rc = rfio_smstat(mstat_tab[rfindex].s,filename,statb,RQST_MSTAT_SEC ) ;
      } else {
        rc = rfio_smstat(mstat_tab[rfindex].s,filename,statb,RQST_MSTAT ) ;
      }
      END_TRACE();
      return (rc) ;
    }
    rc = 0;
    /*
     * To keep backward compatibility we first try the new secure
     * stat() and then, if it failed, go back to the old one.
     */
    for ( i=0; i<2; i++ ) {
      /* The second pass can occur only if (rc == -1 && serrno == SEPROTONOTSUP) */
      /* In such a case rfio_smstat(fd) would have called rfio_end_this(fd,1) */
      /* itself calling netclose(fd) */
      fd=rfio_connect(host,&rt) ;
      if ( fd < 0 ) {
        END_TRACE();
        return (-1) ;
      }
      rfindex = rfio_mstat_allocentry(host,Tid,fd, (! rc) ? 1 : 0);
      TRACE(2, "rfio", "rfio_mstat: rfio_mstat_allocentry(host=%s,Tid=%d,s=%d,sec=%d) returns %d", host, Tid, fd, (! rc) ? 1 : 0, rfindex);
      serrno = 0;
      if ( rfindex >= 0 ) {
        if ( !rc ) 
          rc = rfio_smstat(fd,filename,statb,RQST_MSTAT_SEC);
        else
          rc = rfio_smstat(fd,filename,statb,RQST_MSTAT);
      } else {
        if ( !rc )
          rc = rfio_smstat(fd,filename,statb,RQST_STAT_SEC) ;
        else
          rc = rfio_smstat(fd,filename,statb,RQST_STAT);
        if ( rc != -1 ) {
          TRACE(2,"rfio","rfio_mstat() overflow connect table, host=%s, Tid=%d. Closing %d",host,Tid,fd);
          netclose(fd);
        }
        fd = -1;
      }
      if ( !(rc == -1 && serrno == SEPROTONOTSUP) ) break;
    }
    END_TRACE();
    return (rc)  ;
  }
}

/* 
 * Simplest operation in stat() : just do a stat() 
 * for 1 filename
 */

static int pw_key = -1;
static int old_uid_key = -1;

int DLL_DECL rfio_smstat(s,filename,statbuf,reqst) 
     int s ;
     char * filename ;
     struct stat *statbuf ;
     int reqst ;

{
  char     buf[256];
  int             status;         /* remote fopen() status        */
  int     len;
  int     rc;
  char    *p=buf;
  int     uid;
  int     gid;
  int *old_uid = NULL;
  struct passwd *pw_tmp;
  struct passwd *pw = NULL;

  INIT_TRACE("RFIO_TRACE");
  TRACE(1, "rfio", "rfio_stat(%s, %x)", filename, statbuf);

  if ( Cglobals_get(&old_uid_key, (void**)&old_uid, sizeof(int)) > 0 )
    *old_uid = -1;
  Cglobals_get(&pw_key, (void**)&pw, sizeof(struct passwd));
  
  len = strlen(filename)+1;
  switch ( reqst ) {
  case RQST_MSTAT_SEC :
  case RQST_STAT_SEC :
    TRACE(2,"rfio","rfio_stat: trying secure stat()");
    marshall_WORD(p, B_RFIO_MAGIC);
    uid = geteuid() ;
    gid = getegid () ;
    if ( uid != *old_uid ) {
      TRACE(2,"rfio","rfio_stat: uid=%d != *old_uid=%d\n", (int) uid, (int) *old_uid);
	  pw_tmp = Cgetpwuid(uid);
	  if( pw_tmp  == NULL ) {
        TRACE(2, "rfio" ,"rfio_stat: Cgetpwuid(): ERROR occured (errno=%d)",errno);
        END_TRACE();
        rfio_end_this(s,1);
        END_TRACE();
        return(-1);
	  }	
	  memcpy(pw, pw_tmp, sizeof(struct passwd));
	  *old_uid = uid;
    }
    marshall_WORD(p, reqst);
    len+=2*WORDSIZE + strlen(pw->pw_name) + 1;
    break;
  case RQST_MSTAT:
  case RQST_STAT:
    marshall_WORD(p, RFIO_MAGIC);
    marshall_WORD(p, reqst);
    break ;
  default:
    END_TRACE();
    return (-1) ;
  }
  marshall_LONG(p, len);
  p= buf + RQSTSIZE;
  if ( reqst == RQST_STAT_SEC || reqst == RQST_MSTAT_SEC ) {
    TRACE(2,"rfio","rfio_stat: using (uid=%d,gid=%d)\n",(int) uid, (int) gid);
    marshall_WORD(p, uid);
    marshall_WORD(p, gid);
    marshall_STRING(p,pw->pw_name);
  }
  marshall_STRING(p, filename);
  TRACE(2,"rfio","rfio_stat: sending %d bytes",RQSTSIZE+len) ;
  if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
    TRACE(2, "rfio", "rfio_stat: write(): ERROR occured (errno=%d)", errno);
    END_TRACE();
    rfio_end_this(s,0);
    END_TRACE();
    return(-1);
  }
  p = buf;
  TRACE(2, "rfio", "rfio_stat: reading %d bytes", 8*LONGSIZE+5*WORDSIZE);
  rc = netread_timeout(s, buf, 8*LONGSIZE+5*WORDSIZE, RFIO_CTRL_TIMEOUT);
  if ( rc == 0 && (reqst == RQST_MSTAT_SEC || reqst == RQST_STAT_SEC ) ) {
    TRACE(2, "rfio", "rfio_stat: Server doesn't support secure stat()");
    serrno = SEPROTONOTSUP;
    rfio_end_this(s,1);
    END_TRACE();
    return(-1);
  }
  if ( rc != 8*LONGSIZE+5*WORDSIZE)  {
    TRACE(2, "rfio", "rfio_stat: read(): ERROR occured (errno=%d)", errno);
    rfio_end_this(s, (rc <= 0 ? 0 : 1));
    END_TRACE();
    return(-1);
  }
  unmarshall_WORD(p, statbuf->st_dev);
  unmarshall_LONG(p, statbuf->st_ino);
  unmarshall_WORD(p, statbuf->st_mode);
  unmarshall_WORD(p, statbuf->st_nlink);
  unmarshall_WORD(p, statbuf->st_uid);
  unmarshall_WORD(p, statbuf->st_gid);
  unmarshall_LONG(p, statbuf->st_size);
  unmarshall_LONG(p, statbuf->st_atime);
  unmarshall_LONG(p, statbuf->st_mtime);
  unmarshall_LONG(p, statbuf->st_ctime);
  unmarshall_LONG(p, status);

  /* 
   * Inserted here to preserve
   * backward compatibility with 
   * former stat () protocol
   */
#if !defined(_WIN32)
  unmarshall_LONG(p, statbuf->st_blksize);
  unmarshall_LONG(p, statbuf->st_blocks);
#endif

  TRACE(1, "rfio", "rfio_stat: return %d",status);
  rfio_errno = status;
  if (status)     {
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(0);

}

int DLL_DECL rfio_end()
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
  int rc = 0;

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(3,"rfio","rfio_end entered, Tid=%d", Tid);

  TRACE(3,"rfio","rfio_end: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_end: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    if (mstat_tab[i].Tid == Tid) {
      if ((mstat_tab[i].s >= 0) && (mstat_tab[i].host[0] != '\0')) {
        marshall_WORD(p, RFIO_MAGIC);
        marshall_WORD(p, RQST_END);
        marshall_LONG(p, j);
        TRACE(3,"rfio","rfio_end: close(mstat_tab[%d].s=%d), host=%s, Tid=%d",i,mstat_tab[i].s, mstat_tab[i].host, Tid);
        if (netwrite_timeout(mstat_tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
          TRACE(3, "rfio", "rfio_end: netwrite_timeout(): ERROR occured (errno=%d), Tid=%d", errno, Tid);
          rc = -1;
        }
        netclose(mstat_tab[i].s);
      }
      mstat_tab[i].s = -1;
      mstat_tab[i].host[0] = '\0';
      mstat_tab[i].sec = -1;
      mstat_tab[i].Tid = -1;
    }
  }
   
  TRACE(3,"rfio","rfio_end: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_end: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

/* This is a simplified version of rfio_end() that just free entry in the table */
/* If flag is set a clean close is tried (write on the socket) */
static int rfio_end_this(s,flag)
     int s;
     int flag;
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
  int rc = 0;

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(3,"rfio","rfio_end_this(s=%d,flag=%d) entered, Tid=%d", s, flag, Tid);

  TRACE(3,"rfio","rfio_end: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_end_this: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    if (mstat_tab[i].Tid == Tid) {
      if ((mstat_tab[i].s == s) && (mstat_tab[i].host[0] != '\0')) {
        if (flag) {
          marshall_WORD(p, RFIO_MAGIC);
          marshall_WORD(p, RQST_END);
          marshall_LONG(p, j);
          TRACE(3,"rfio","rfio_end_this: close(mstat_tab[%d].s=%d), host=%s, Tid=%d",i,mstat_tab[i].s, mstat_tab[i].host, Tid);
          if (netwrite_timeout(mstat_tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
            TRACE(3, "rfio", "rfio_end_this: netwrite_timeout(): ERROR occured (errno=%d), Tid=%d", errno, Tid);
          }
        }
        netclose(mstat_tab[i].s);
        mstat_tab[i].s = -1;
        mstat_tab[i].host[0] = '\0';
        mstat_tab[i].sec = -1;
        mstat_tab[i].Tid = -1;
      }
    }
  }
   
  TRACE(3,"rfio","rfio_end: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_end_this: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

/*
 * Seach for a free index in the mstat_tab table
 */
static int rfio_mstat_allocentry(hostname,Tid,s,sec)
     char *hostname;
     int Tid;
     int s;
     int sec;
{
  int i;
  int rc;

  INIT_TRACE("RFIO_TRACE");

  TRACE(3,"rfio","rfio_mstat_allocentry entered, Tid=%d", Tid);

  TRACE(3,"rfio","rfio_mstat_allocentry: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_mstat_allocentry: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if (mstat_tab[i].host[0] == '\0') {
      rc = i;
      strncpy(mstat_tab[i].host,hostname,CA_MAXHOSTNAMELEN);
      mstat_tab[i].host[CA_MAXHOSTNAMELEN] = '\0';
      mstat_tab[i].Tid = Tid;
      mstat_tab[i].s = s;
      mstat_tab[i].sec = sec;
      goto _rfio_mstat_allocentry_return;
    }
  }
  
  serrno = ENOENT;
  rc = -1;
  
 _rfio_mstat_allocentry_return:
  TRACE(3,"rfio","rfio_mstat_allocentry: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_mstat_allocentry: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(rc);
}

/*
 * Seach for a given index in the mstat_tab table
 */
static int rfio_mstat_findentry(hostname,Tid)
     char *hostname;
     int Tid;
{
  int i;
  int rc;

  INIT_TRACE("RFIO_TRACE");

  TRACE(3,"rfio","rfio_mstat_findentry entered, Tid=%d", Tid);

  TRACE(3,"rfio","rfio_mstat_findentry: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_mstat_findentry: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if ((strcmp(mstat_tab[i].host,hostname) == 0) && (mstat_tab[i].Tid == Tid)) {
      rc = i;
      goto _rfio_mstat_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_mstat_findentry_return:
  TRACE(3,"rfio","rfio_mstat_findentry: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_mstat_findentry: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  END_TRACE();
  return(rc);
}

int DLL_DECL rfio_mstat_reset()
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
  int rc = 0;

  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(3,"rfio","rfio_mstat_reset entered, Tid=%d", Tid);

  TRACE(3,"rfio","rfio_mstat_reset: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_mstat_reset: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
    END_TRACE();
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    if (mstat_tab[i].s >= 0) {
        TRACE(3,"rfio","rfio_mstat_reset: Resetting socket fd=%d, host=%s\n", mstat_tab[i].s, mstat_tab[i].host);
        netclose(mstat_tab[i].s);
    }
    mstat_tab[i].s = -1;
    mstat_tab[i].host[0] = '\0';
    mstat_tab[i].sec = -1;
    mstat_tab[i].Tid = -1;
  }
   
  TRACE(3,"rfio","rfio_mstat_reset: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_mstat_reset: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

