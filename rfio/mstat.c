/*
 * $Id: mstat.c,v 1.11 2001/07/12 06:14:52 jdurand Exp $
 */


/*
 * Copyright (C) 1995-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: mstat.c,v $ $Revision: 1.11 $ $Date: 2001/07/12 06:14:52 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */


#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pwd.h>
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

typedef struct socks {
  char host[MAXHOSTNAMELEN];
  int s ;
  int sec;
  int Tid;
} connects ;
static connects tab[MAXMCON]; /* UP TO MAXMCON connections simultaneously */
extern int rfio_smstat() ;

static int rfio_tab_allocentry _PROTO((char *, int, int, int));
static int rfio_tab_findentry _PROTO((char *,int));

int DLL_DECL rfio_mstat(file,statb)
     char *file ;
     struct stat *statb;

{
  int rt ,rc ,i ,fd ,retry, rfindex, Tid;
  char *host , *filename ;
  char     buf[256];

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
      return(rfio_HsmIf_stat(filename,statb));
    }

    /* The file is local */
    rc = stat(filename,statb) ;
    rfio_errno = 0;
    return (rc) ;
  }  else  {
    /* Look if already in */
    serrno = 0;
    rfindex = rfio_tab_findentry(host,Tid);
    TRACE(2, "rfio", "rfio_mstat: rfio_tab_findentry(host=%s,Tid=%d) returns %d", host, Tid, rfindex);
    if (rfindex >= 0) {
      if ( tab[rfindex].sec )
        rc = rfio_smstat(tab[rfindex].s,filename,statb,RQST_MSTAT_SEC ) ;
      else
        rc = rfio_smstat(tab[rfindex].s,filename,statb,RQST_MSTAT ) ;
      return ( rc) ;
    }
    rc = 0;
    /*
     * To keep backward compatibility we first try the new secure
     * stat() and then, if it failed, go back to the old one.
     */
    for ( i=0; i<2; i++ ) {
      fd=rfio_connect(host,&rt) ;
      if ( fd < 0 ) {
        return (-1) ;
      }
      rfindex = rfio_tab_allocentry(host,Tid,fd, (! rc) ? 1 : 0);
      TRACE(2, "rfio", "rfio_mstat: rfio_tab_allocentry(host=%s,Tid=%d,s=%d,sec=%d) returns %d", host, Tid, fd, (! rc) ? 1 : 0, rfindex);
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
          TRACE(2,"rfio","rfio_mstat() overflow connect table, Tid=%d. Closing %d",Tid,fd);
          (void)close(fd);
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
	  pw_tmp = Cgetpwuid(uid);
	  if( pw_tmp  == NULL ) {
        TRACE(2, "rfio" ,"rfio_stat: Cgetpwuid(): ERROR occured (errno=%d)",errno);
        END_TRACE();
        (void) close(s);
        return -1 ;
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
    marshall_WORD(p, uid);
    marshall_WORD(p, gid);
    marshall_STRING(p,pw->pw_name);
  }
  marshall_STRING(p, filename);
  TRACE(2,"rfio","rfio_stat: sending %d bytes",RQSTSIZE+len) ;
  if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
    TRACE(2, "rfio", "rfio_stat: write(): ERROR occured (errno=%d)", errno);
    (void) close(s);
    END_TRACE();
    return(-1);
  }
  p = buf;
  TRACE(2, "rfio", "rfio_stat: reading %d bytes", 8*LONGSIZE+5*WORDSIZE);
  rc = netread_timeout(s, buf, 8*LONGSIZE+5*WORDSIZE, RFIO_CTRL_TIMEOUT);
  if ( rc == 0 && (reqst == RQST_MSTAT_SEC || reqst == RQST_STAT_SEC ) ) {
    TRACE(2, "rfio", "rfio_stat: Server doesn't support secure stat()");
    serrno = SEPROTONOTSUP;
    (void) close(s);
    return(-1);
  }
  if ( rc != 8*LONGSIZE+5*WORDSIZE)  {
    TRACE(2, "rfio", "rfio_stat: read(): ERROR occured (errno=%d)", errno);
    (void) close(s);
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
  return (0);

}

int DLL_DECL rfio_end()
{
  int i,Tid, j=0 ;
  char buf[256];
  char *p=buf ;
   
  INIT_TRACE("RFIO_TRACE");

  Cglobals_getTid(&Tid);

  TRACE(3,"rfio","rfio_end entered, Tid=%d", Tid);


  if (Cmutex_lock((void *) tab,-1) != 0) {
    TRACE(3,"rfio","rfio_end : Cmutex_lock(tab,-1) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    /* TRACE(3,"rfio","tab[i]={host=\"%s\", s=%d, sec=%d, Tid=%d}", tab[i].host, tab[i].s, tab[i].sec, tab[i].Tid); */
    if (tab[i].Tid == Tid) {
      if ((tab[i].s >= 0) && (tab[i].host[0] != '\0')) {
        marshall_WORD(p, RFIO_MAGIC);
        marshall_WORD(p, RQST_END);
        marshall_LONG(p, j);
        TRACE(3,"rfio","rfio_end: close(tab[%d].s=%d), Tid=%d",i,tab[i].s, Tid);
        if (netwrite_timeout(tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
          TRACE(3, "rfio", "rfio_stat: write(): ERROR occured (errno=%d), Tid=%d", errno, Tid);
          END_TRACE();
          return(-1);
        }
        (void) close(tab[i].s);
      }
      tab[i].s = -1;
      tab[i].host[0] = '\0';
      tab[i].sec = -1;
      tab[i].Tid = -1;
    }
  }
   
  if (Cmutex_unlock((void *) tab) != 0) {
    TRACE(3,"rfio","rfio_end : Cmutex_unlock(tab) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }

  END_TRACE();
  return(0);
}

/*
 * Seach for a free index in the tab table
 */
static int rfio_tab_allocentry(hostname,Tid,s,sec)
     char *hostname;
     int Tid;
     int s;
     int sec;
{
  int i;
  int rc;

  if (Cmutex_lock((void *) tab,-1) != 0) {
    TRACE(3,"rfio","rfio_tab_allocentry : Cmutex_lock(tab,-1) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if (tab[i].host[0] == '\0') {
      rc = i;
      strcpy(tab[i].host,hostname);
      tab[i].Tid = Tid;
      tab[i].s = s;
      tab[i].sec = sec;
      goto _rfio_tab_allocentry_return;
    }
  }
  
  serrno = ENOENT;
  rc = -1;
  
 _rfio_tab_allocentry_return:
  if (Cmutex_unlock((void *) tab) != 0) {
    TRACE(3,"rfio","rfio_tab_allocentry : Cmutex_unlock(tab) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  return(rc);
}

/*
 * Seach for a given index in the tab table
 */
static int rfio_tab_findentry(hostname,Tid)
     char *hostname;
     int Tid;
{
  int i;
  int rc;

  if (Cmutex_lock((void *) tab,-1) != 0) {
    TRACE(3,"rfio","rfio_tab_findentry : Cmutex_lock(tab,-1) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if ((strcmp(tab[i].host,hostname) == 0) && (tab[i].Tid == Tid)) {
      rc = i;
      goto _rfio_tab_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_tab_findentry_return:
  if (Cmutex_unlock((void *) tab) != 0) {
    TRACE(3,"rfio","rfio_tab_findentry : Cmutex_unlock(tab) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  return(rc);
}
