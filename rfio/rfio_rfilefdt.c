/*
 * $Id: rfio_rfilefdt.c,v 1.5 2001/05/12 07:23:25 jdurand Exp $
 */

/*
 * System remote file I/O
 */
#define RFIO_KERNEL     1
#include <fcntl.h>
#if defined(_WIN32)
#define MAXHOSTNAMELEN 64
#else
#include <sys/param.h>          /* For MAXHOSTNAMELEN definition  */
#endif
#include <stdlib.h>
#include "rfio.h"
#include "rfio_rfilefdt.h"

#ifndef linux
extern char *sys_errlist[];     /* system error list                    */
#endif

extern RFILE *rfilefdt[MAXRFD] ;

RFILE dummyrfile;               /* Used to fill with a dummy value */

/*
 * Seach for a free index in the rfilefdt table
 */
int DLL_DECL rfio_rfilefdt_allocentry(s)
     int s;
{
#ifdef _WIN32
  int i;
  int rc;

  if (Cmutex_lock((void *) rfilefdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (rfilefdt[i] == NULL) {
      rc = i;
      rfilefdt[i] = &dummyrfile;
      goto _rfio_rfilefdt_allocentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_rfilefdt_allocentry_return:
  if (Cmutex_unlock((void *) rfilefdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  return(((s >= 0) && (s < MAXRFD)) ? s : -1);
#endif /* _WIN32 */
}

/*
 * Seach for a given index in the rfilefdt table
 * On UNIX, if scanflag is FINDRDIR_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 */
int DLL_DECL rfio_rfilefdt_findentry(s,scanflag)
     int s;
     int scanflag;
{
  int i;
#ifdef _WIN32
  int rc;

  if (Cmutex_lock((void *) rfilefdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (rfilefdt[i] != NULL) {
      if (rfilefdt[i]->s == s) {
        rc = i;
        goto _rfio_rfilefdt_findentry_return;
      }
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_rfilefdt_findentry_return:
  if (Cmutex_unlock((void *) rfilefdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  if (scanflag == FINDRFILE_WITH_SCAN) {
    for (i = 0; i < MAXRFD; i++) {
      if (rfilefdt[i] != NULL) {
        if (rfilefdt[i]->s == s) {
          return(i);
        }
      }
    }
    return(-1);
  } else {
    return(((s >= 0) && (s < MAXRFD) && (rfilefdt[s] != NULL)) ? s : -1);
  }
#endif /* _WIN32 */
}


/*
 * Seach for a given pointer in the rfilefdt table
 * On UNIX, if scanflag is FINDRDIR_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 */
int DLL_DECL rfio_rfilefdt_findptr(ptr,scanflag)
     RFILE *ptr;
     int scanflag;
{
  int i;
#ifdef _WIN32
  int rc;

  if (Cmutex_lock((void *) rfilefdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (rfilefdt[i] == ptr) {
      rc = i;
      goto _rfio_rfilefdt_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_rfilefdt_findentry_return:
  if (Cmutex_unlock((void *) rfilefdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  if (scanflag == FINDRFILE_WITH_SCAN) {
    for (i = 0; i < MAXRFD; i++) {
      if (rfilefdt[i] == ptr) {
        return(i);
      }
    }
    return(-1);
  } else {
    /* This method works only in FINDRFILE_WITH_SCAN mode */
    serrno = EINVAL;
    return(-1);
  }
#endif /* _WIN32 */
}


/*
 * Free a given index in the rfilefdt table
 * Warning : the argument is REALLY an index
 */
int DLL_DECL rfio_rfilefdt_freeentry(s)
     int s;
{
#ifdef _WIN32
  if (Cmutex_lock((void *) rfilefdt,-1) != 0) {
    return(-1);
  }
  if (rfilefdt[s] != NULL) {
    if (rfilefdt[s] != dummyrfile) free(rfilefdt[s]);
    rfilefdt[s] = NULL;
  }
  if (Cmutex_unlock((void *) rfilefdt) != 0) {
    return(-1);
  }
#else /* _WIN32 */
  if ((s >= 0) && (s < MAXRFD) && (rfilefdt[s] != NULL)) {
    if (rfilefdt[s] != dummyrfile) free((char *)rfilefdt[s]);
    rfilefdt[s] = NULL;
  }
#endif /* _WIN32 */
  return(0);
}


