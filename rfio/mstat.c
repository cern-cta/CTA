/*
 * $Id: mstat.c,v 1.36 2005/02/22 13:28:41 jdurand Exp $
 */


/*
 * Copyright (C) 1995-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: mstat.c,v $ $Revision: 1.36 $ $Date: 2005/02/22 13:28:41 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */


#include "Cmutex.h"
#include "Castor_limits.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
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
  int m64;
} mstat_connects ;
static mstat_connects mstat_tab[MAXMCON]; /* UP TO MAXMCON connections simultaneously */

EXTERN_C int DLL_DECL rfio_smstat _PROTO((int, char *, struct stat *, int));
static int rfio_mstat_allocentry _PROTO((char *, int, int, int));
static int rfio_mstat_findentry _PROTO((char *,int));
static int rfio_end_this _PROTO((int,int));
extern int rfio_newhost _PROTO((char *));

int DLL_DECL rfio_mstat(file,statb)
char *file ;
struct stat *statb;

{
#if (defined(__alpha) && defined(__osf__))
   return (rfio_mstat64(file,statb));
#else
   int       rc, parserc ;
#if defined(IRIX64) || defined(__ia64__) || defined(__x86_64) || defined(__ppc64__)
   struct stat64 statb64;

   if ((rc = rfio_mstat64(file,&statb64)) == 0)
	(void) stat64tostat(&statb64, statb);
   return (rc);
#else
   int rt ,i ,fd, rfindex, Tid;
   char *host , *filename ;

   INIT_TRACE("RFIO_TRACE");

   Cglobals_getTid(&Tid);

   TRACE(1, "rfio", "rfio_mstat(%s, %x), Tid=%d", file, statb, Tid);
   if (!(parserc = rfio_parseln(file,&host,&filename,NORDLINKS))) {
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
      if ( rc < 0 ) serrno = 0;
      rfio_errno = 0;
      END_TRACE();
      return (rc) ;
   }  else  {
	   if (parserc < 0) {
		   END_TRACE();
		   return(-1);
	   }
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
		  rfio_errno = 0;
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
           if ( (rc != -1) || (rc == -1 && rfio_errno != 0) ) {
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
#endif
#endif
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
   char     buf[BUFSIZ];
   int             status;         /* remote fopen() status        */
   int     len;
   int     rc;
   char    *p=buf;
   int     uid;
   int     gid;
   int *old_uid = NULL;
   struct passwd *pw_tmp;
   struct passwd *pw = NULL;

   // Avoiding Valgrind error messages about uninitialized data
   memset(buf, 0, BUFSIZ);

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
   if ( len > BUFSIZ ) {
     TRACE(2,"rfio","rfio_stat: request too long %d (max %d)",len,BUFSIZ);
     END_TRACE();
     serrno = E2BIG;
     return(-1);
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
      rfio_end_this(s,0);
      END_TRACE();
      return(-1);
   }
   p = buf;
   TRACE(2, "rfio", "rfio_stat: reading %d bytes", 8*LONGSIZE+5*WORDSIZE);
   rc = netread_timeout(s, buf, 8*LONGSIZE+5*WORDSIZE, RFIO_CTRL_TIMEOUT);
   if ( rc == 0 && (reqst == RQST_MSTAT_SEC || reqst == RQST_STAT_SEC ) ) {
      TRACE(2, "rfio", "rfio_stat: Server doesn't support secure stat()");
      rfio_end_this(s,0);
      serrno = SEPROTONOTSUP;
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
   char buf[RQSTSIZE];
   char *p=buf ;
   int rc = 0;

   // Avoiding Valgrind error messages about uninitialized data
   memset(buf, 0, RQSTSIZE);
   
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
        p = buf;
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
      mstat_tab[i].m64 = -1;
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
  char buf[RQSTSIZE];
  int found = 0;
  char *p=buf ;
  int rc = 0;

  // Avoiding Valgrind error messages about uninitialized data
  memset(buf, 0, RQSTSIZE);

  Cglobals_getTid(&Tid);

  TRACE(3,"rfio","rfio_end_this(s=%d,flag=%d) entered, Tid=%d", s, flag, Tid);

  TRACE(3,"rfio","rfio_end: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_end_this: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  for (i = 0; i < MAXMCON; i++) {
    if (mstat_tab[i].Tid == Tid) {
      if ((mstat_tab[i].s == s) && (mstat_tab[i].host[0] != '\0')) {
        found++;
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
        mstat_tab[i].m64 = -1;
      }
    }
  }
  if (! found) netclose(s);
   
  TRACE(3,"rfio","rfio_end: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_end_this: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

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

  TRACE(3,"rfio","rfio_mstat_allocentry entered, Tid=%d", Tid);

  TRACE(3,"rfio","rfio_mstat_allocentry: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_mstat_allocentry: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
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
      mstat_tab[i].m64 = -1;
      goto _rfio_mstat_allocentry_return;
    }
  }
  
  serrno = ENOENT;
  rc = -1;
  
 _rfio_mstat_allocentry_return:
  TRACE(3,"rfio","rfio_mstat_allocentry: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_mstat_allocentry: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
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

  TRACE(3,"rfio","rfio_mstat_findentry entered, Tid=%d", Tid);

  TRACE(3,"rfio","rfio_mstat_findentry: Lock mstat_tab");
  if (Cmutex_lock((void *) mstat_tab,-1) != 0) {
    TRACE(3,"rfio","rfio_mstat_findentry: Cmutex_lock(mstat_tab,-1) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXMCON; i++) {
    if ((strcmp(mstat_tab[i].host,hostname) == 0) && (mstat_tab[i].Tid == Tid)) {
      rc = i;
      /* Lie to rfio_lasthost() */
      rfio_newhost(hostname);
      goto _rfio_mstat_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_mstat_findentry_return:
  TRACE(3,"rfio","rfio_mstat_findentry: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_mstat_findentry: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    return(-1);
  }
  return(rc);
}

int DLL_DECL rfio_mstat_reset()
{
  int i,Tid;
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
    if ((mstat_tab[i].s >= 0) && (mstat_tab[i].host[0] != '\0')) {
        TRACE(3,"rfio","rfio_mstat_reset: Resetting socket fd=%d, host=%s\n", mstat_tab[i].s, mstat_tab[i].host);
        netclose(mstat_tab[i].s);
    }
    mstat_tab[i].s = -1;
    mstat_tab[i].host[0] = '\0';
    mstat_tab[i].sec = -1;
    mstat_tab[i].Tid = -1;
    mstat_tab[i].m64 = -1;
  }
   
  TRACE(3,"rfio","rfio_mstat_reset: Unlock mstat_tab");
  if (Cmutex_unlock((void *) mstat_tab) != 0) {
    TRACE(3,"rfio","rfio_mstat_reset: Cmutex_unlock(mstat_tab) error No %d (%s)", errno, strerror(errno));
    rc = -1;
  }

  END_TRACE();
  return(rc);
}

int DLL_DECL rfio_mstat64(file,statb)
char *file ;
struct stat64 *statb;

{
   int rt ,rc ,i ,fd, rfindex, Tid;
   char *host , *filename ;
   int         fitreqst;                     /*Fitted request           */
   int savsec, parserc;

   INIT_TRACE("RFIO_TRACE");

   Cglobals_getTid(&Tid);

   TRACE(1, "rfio", "rfio_mstat64(%s, %x), Tid=%d", file, statb, Tid);
   if (!(parserc = rfio_parseln(file,&host,&filename,NORDLINKS))) {
      /* if not a remote file, must be local or HSM  */
      if ( host != NULL ) {
          /*
           * HSM file
           */
          rfio_errno = 0;
          rc = rfio_HsmIf_stat64(filename,statb);
          END_TRACE();
          return(rc);
      }

      /* The file is local */
      rc = stat64(filename, statb) ;
      if ( rc < 0 ) serrno = 0;
      rfio_errno = 0;
      END_TRACE();
      return (rc) ;
   } else  {
	   if (parserc < 0) {
		   END_TRACE();
		   return(-1);
	   }
      /* Look if already in */
      serrno = 0;
      rfindex = rfio_mstat_findentry(host,Tid);
      TRACE(2, "rfio", "rfio_mstat64: rfio_mstat_findentry(host=%s,Tid=%d) returns %d", host, Tid, rfindex);
      if (rfindex >= 0) {
         if ( mstat_tab[rfindex].m64 == -1 && mstat_tab[rfindex].sec >= 0) {
            /* Not yet used for 64 bits support                      */
            savsec = mstat_tab[rfindex].sec;
            rc = rfio_smstat64( mstat_tab[rfindex].s, filename, statb, RQST_MSTAT64 ) ;
            if ( rc == 0 || serrno != SEPROTONOTSUP ) {
               if ( rc == 0 )
                  mstat_tab[rfindex].m64 = 1;
               END_TRACE();
               return (rc) ;
            }
            rfio_errno = 0;
            fd = rfio_connect(host,&rt) ;
            if ( fd < 0 ) {
               END_TRACE();
               return (-1) ;
            }
            rfindex = rfio_mstat_allocentry(host,Tid,fd, savsec);
            TRACE(2, "rfio", "rfio_mstat64: rfio_mstat_allocentry(host=%s,Tid=%d,s=%d,sec=%d) returns %d", host, Tid, fd, savsec, rfindex);
            if ( rfindex < 0 ) {
               rc = rfio_smstat64( fd, filename, statb, savsec ? RQST_STAT_SEC : RQST_STAT ) ;
               END_TRACE();
               return (rc) ;
            }
            mstat_tab[rfindex].m64 = 0;
         }
         if (mstat_tab[rfindex].m64)
            fitreqst = RQST_MSTAT64;
         else {
            if (mstat_tab[rfindex].sec)
               fitreqst =  RQST_MSTAT_SEC;
            else
               fitreqst =  RQST_MSTAT;
         }
         rc = rfio_smstat64( mstat_tab[rfindex].s, filename, statb, fitreqst ) ;
         END_TRACE();
         return (rc) ;
      }
      rc = 0;
      /*
       * To keep backward compatibility we first try the new secure
       * stat() and then, if it failed, go back to the old one.
       */
      for ( i=0; i<3; i++ ) {
         int tabreqst [3] = { RQST_MSTAT64, RQST_MSTAT_SEC, RQST_MSTAT };
         int tabreqst2[3] = { RQST_STAT64,  RQST_STAT_SEC,  RQST_STAT };
         int tabm64[3]   = { 1, 0, 0 };
         int tabsec[3]   = { 1, 1, 0 };
         
         /* The second pass can occur only if (rc == -1 && serrno == SEPROTONOTSUP) */
         /* In such a case rfio_smstat(fd) would have called rfio_end_this(fd,1) */
         /* itself calling netclose(fd) */
         rfio_errno = 0;
         fd = rfio_connect(host,&rt) ;
         if ( fd < 0 ) {
            END_TRACE();
            return (-1) ;
         }
         rfindex = rfio_mstat_allocentry(host,Tid,fd, (! rc) ? 1 : 0);
         TRACE(2, "rfio", "rfio_mstat64: rfio_mstat_allocentry(host=%s,Tid=%d,s=%d,sec=%d) returns %d", host, Tid, fd, (! rc) ? 1 : 0, rfindex);
         if ( rfindex >= 0 ) {
            mstat_tab[rfindex].sec = tabsec[i];
            mstat_tab[rfindex].m64 = tabm64[i];
         }
         serrno = 0;
         if ( rfindex >= 0 )
	    rc = rfio_smstat64(fd, filename, statb, tabreqst[i]);
         else {
	    rc = rfio_smstat64(fd, filename, statb, tabreqst2[i]);
            if ( (rc != -1) || (rc == -1 && rfio_errno != 0) ) {
                TRACE(2,"rfio","rfio_mstat() overflow connect table, Tid=%d. Closing %d",Tid,fd);
                netclose(fd);
            }
            fd = -1;
	 }
	 if ( !(rc == -1 && serrno == SEPROTONOTSUP) ) break;
      }  /* End of for (i) */
      END_TRACE();
      return (rc)  ;
   }

}

/* Do the remote stat64() and lstat64()                            */
int DLL_DECL rfio_smstat64(s,filename,statbuf,reqst) 
int s ;
char * filename ;
struct stat64 *statbuf ;
int    reqst ;

{
   char        buf[BUFSIZ];
   int         status;            /* remote fopen() status         */
   int         len;
   int         replen;
   int         rc;
   int         save_errno, save_serrno;
   char        *p=buf;
   int         uid;
   int         gid;
   int         sec;
   int         m64;
   int         *old_uid = NULL;
   long        i32 ;
   struct      passwd *pw_tmp;
   struct      passwd *pw = NULL;

   // Avoiding Valgrind error messages about uninitialized data
   memset(buf, 0, BUFSIZ);

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_smstat64(%s, %x)", filename, statbuf);

   if ( Cglobals_get(&old_uid_key, (void**)&old_uid, sizeof(int)) > 0 )
      *old_uid = -1;
   Cglobals_get(&pw_key, (void**)&pw, sizeof(struct passwd));
  
   len = strlen(filename)+1;
   switch ( reqst ) {
      case RQST_MSTAT64:
      case RQST_STAT64:
      case RQST_LSTAT64:
         TRACE(2,"rfio","rfio_smstat64: trying (l)stat64()");
         m64 = 1;
         sec = 1;
         break;
      case RQST_MSTAT_SEC:
      case RQST_STAT_SEC:
      case RQST_LSTAT_SEC:
         TRACE(2,"rfio","rfio_smstat64: trying secure (l)stat()");
         m64 = 0;
         sec = 1;
         break;
      case RQST_MSTAT:
      case RQST_STAT:
      case RQST_LSTAT:
         TRACE(2,"rfio","rfio_smstat64: trying (l)stat()");
         m64 = 0;
         sec = 0;
         break;
      default:
         TRACE(2,"rfio","rfio_smstat64: Invalid request %x", reqst);
         END_TRACE();
         return (-1) ;
   }
   
   if (sec && !m64) {
      marshall_WORD(p, B_RFIO_MAGIC);
   }
   else {
      marshall_WORD(p, RFIO_MAGIC);
   }
      
   marshall_WORD(p, reqst);
   
   if (sec) {   
      uid = geteuid() ;
      gid = getegid () ;
      if ( uid != *old_uid ) {
         TRACE(2,"rfio","rfio_smstat64: uid=%d != *old_uid=%d\n", (int) uid, (int) *old_uid);
         pw_tmp = Cgetpwuid(uid);
         if( pw_tmp  == NULL ) {
            TRACE(2, "rfio" ,"rfio_smstat64: Cgetpwuid(): ERROR occured (errno=%d)", errno);
            rfio_end_this(s,1);
            END_TRACE();
            return(-1);
         }  
         memcpy(pw, pw_tmp, sizeof(struct passwd));
         *old_uid = uid;
      }
      len += 2*WORDSIZE + strlen(pw->pw_name) + 1;
   }

   if ( len > BUFSIZ ) {
     TRACE(2,"rfio","rfio_smstat64: request too long %d (max %d)",len,BUFSIZ);
     rfio_end_this(s,0);
     END_TRACE();
     serrno = E2BIG;
     return(-1);
   }
   
   marshall_LONG(p, len);
   p= buf + RQSTSIZE;
   
   if ( sec ) {
      TRACE(2,"rfio","rfio_smstat64: using (uid=%d,gid=%d)\n",(int) uid, (int) gid);
      marshall_WORD(p, uid);
      marshall_WORD(p, gid);
      marshall_STRING(p,pw->pw_name);
   }
   marshall_STRING(p, filename);
   TRACE(2,"rfio","rfio_smstat64: sending %d bytes", RQSTSIZE+len) ;
   if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
     TRACE(2, "rfio", "rfio_smstat64: write(): ERROR occured (errno=%d)", errno);
     rfio_end_this(s,0);
     END_TRACE();
     return(-1);
   }
   p = buf;
   if ( m64 )
      replen = 3*HYPERSIZE+5*LONGSIZE+5*WORDSIZE;
   else {
      if (reqst == RQST_LSTAT_SEC || reqst == RQST_LSTAT )
         replen = 6*LONGSIZE+5*WORDSIZE;
      else
         replen = 8*LONGSIZE+5*WORDSIZE;
   }
   
   TRACE(2, "rfio", "rfio_smstat64: reading %d bytes", replen);
   rc = netread_timeout(s, buf, replen, RFIO_CTRL_TIMEOUT);
#if !defined(_WIN32)
   if ( (rc == 0 || (rc < 0 && errno == ECONNRESET)) && sec ) {
#else
   if ( (rc == 0 || (rc < 0 && serrno == SETIMEDOUT)) && sec ) {
#endif
      TRACE(2, "rfio", "rfio_smstat64: Server doesn't support %s()",
         m64 ? "stat64" : "secure stat");
      rfio_end_this(s,0);
      END_TRACE();
      serrno = SEPROTONOTSUP;
      return(-1);
   }
   if ( rc != replen)  {
     TRACE(2, "rfio", "rfio_smstat64: read(): ERROR received %d/%d bytes (errno=%d)",
           rc, replen, errno);
     rfio_end_this(s, (rc <= 0 ? 0 : 1));
     END_TRACE();
     return(-1);
   }
   if (m64) {
      unmarshall_WORD(p, statbuf->st_dev);
      unmarshall_HYPER(p, statbuf->st_ino);
      unmarshall_WORD(p, statbuf->st_mode);
      unmarshall_WORD(p, statbuf->st_nlink);
      unmarshall_WORD(p, statbuf->st_uid);
      unmarshall_WORD(p, statbuf->st_gid);
      unmarshall_HYPER(p, statbuf->st_size);
      unmarshall_LONG(p, statbuf->st_atime);
      unmarshall_LONG(p, statbuf->st_mtime);
      unmarshall_LONG(p, statbuf->st_ctime);
      unmarshall_LONG(p, status);
#if !defined(_WIN32)
      unmarshall_LONG(p, statbuf->st_blksize);
      unmarshall_HYPER(p, statbuf->st_blocks);
#endif
   }
   else {
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
#if !defined(_WIN32)
      if ( reqst != RQST_LSTAT_SEC && reqst != RQST_LSTAT ) {
         unmarshall_LONG(p, statbuf->st_blksize);
         unmarshall_LONG(p, statbuf->st_blocks);
      }
#endif
   }

   TRACE(1, "rfio", "rfio_smstat64: return %d", status);
   rfio_errno = status;
   if (status)     {
      END_TRACE();
      return(-1);
   }
   END_TRACE();
   return (0);

}

int DLL_DECL stat64tostat(statb64, statb)
const struct   stat64   *statb64;
struct         stat     *statb;
{
   statb->st_dev    = statb64->st_dev;
   statb->st_ino    = (ino_t) statb64->st_ino;
   statb->st_mode   = statb64->st_mode;
   statb->st_nlink  = statb64->st_nlink;
   statb->st_uid    = statb64->st_uid;
   statb->st_gid    = statb64->st_gid;
   statb->st_size   = statb64->st_size;
   statb->st_atime  = statb64->st_atime;
   statb->st_mtime  = statb64->st_mtime;
   statb->st_ctime  = statb64->st_ctime;
#if !defined(_WIN32)
   statb->st_blksize= statb64->st_blksize;
   statb->st_blocks = (int) statb64->st_blocks;
#endif
   return (0);
}
