/*
 * $Id: rfio_rdirfdt.c,v 1.6 2001/05/12 07:25:09 jdurand Exp $
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
#include "rfio_rdirfdt.h"

#ifndef linux
extern char *sys_errlist[];     /* system error list                    */
#endif

extern RDIR *rdirfdt[MAXRFD];

RDIR dummyrdir;                 /* Used to fill index */

/*
 * Seach for a free index in the rdirfdt table
 */
int DLL_DECL rfio_rdirfdt_allocentry(s)
     int s;
{
#ifdef _WIN32
  int i;
  int rc;

  if (Cmutex_lock((void *) rdirfdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (rdirfdt[i] == NULL) {
      rc = i;
      rdirfdt[i] = &dummyrdir;
      goto _rfio_rdirfdt_allocentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_rdirfdt_allocentry_return:
  if (Cmutex_unlock((void *) rdirfdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  return(((s >= 0) && (s < MAXRFD)) ? s : -1);
#endif
}

/*
 * Seach for a given index in the rdirfdt table
 * On UNIX, if scanflag is FINDRDIR_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 */
int DLL_DECL rfio_rdirfdt_findentry(s,scanflag)
     int s;
     int scanflag;
{
  int i;
#ifdef _WIN32
  int rc;

  if (Cmutex_lock((void *) rdirfdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (rdirfdt[i] != NULL) {
      if (rdirfdt[i]->s == s) {
        rc = i;
        goto _rfio_rdirfdt_findentry_return;
      }
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_rdirfdt_findentry_return:
  if (Cmutex_unlock((void *) rdirfdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  if (scanflag == FINDRDIR_WITH_SCAN) {
    for (i = 0; i < MAXRFD; i++) {
      if (rdirfdt[i] != NULL) {
        if (rdirfdt[i]->s == s) {
          return(i);
        }
      }
    }
    return(-1);
  } else {
    return(((s >= 0) && (s < MAXRFD) && (rdirfdt[s] != NULL)) ? s : -1);
  }
#endif /* _WIN32 */
}


/*
 * Seach for a given pointer in the rdirfdt table
 * On UNIX, if scanflag is FINDRDIR_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 */
int DLL_DECL rfio_rdirfdt_findptr(ptr,scanflag)
     RDIR *ptr;
     int scanflag;
{
  int i;
#ifdef _WIN32
  int rc;

  if (Cmutex_lock((void *) rdirfdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    if (rdirfdt[i] == ptr) {
      rc = i;
      goto _rfio_rdirfdt_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_rdirfdt_findentry_return:
  if (Cmutex_unlock((void *) rdirfdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  if (scanflag == FINDRDIR_WITH_SCAN) {
    for (i = 0; i < MAXRFD; i++) {
      if (rdirfdt[i] == ptr) {
        return(i);
      }
    }
    return(-1);
  } else {
    /* This method works only in FINDRDIR_WITH_SCAN mode */
    serrno = EINVAL;
    return(-1);
  }
#endif /* _WIN32 */
}


/*
 * Free a given index in the rdirfdt table
 * Warning : the argument is REALLY an index
 */
int DLL_DECL rfio_rdirfdt_freeentry(s)
     int s;
{
#ifdef _WIN32
  if (Cmutex_lock((void *) rdirfdt,-1) != 0) {
    return(-1);
  }
  if (rdirfdt[s] != NULL) {
    if (rdirfdt[s] != &dummyrdir) free(rdirfdt[s]);
    rdirfdt[s] = NULL;
  }
  if (Cmutex_unlock((void *) rdirfdt) != 0) {
    return(-1);
  }
#else /* _WIN32 */
  if ((s >= 0) && (s < MAXRFD) && (rdirfdt[s] != NULL)) {
    if (rdirfdt[s] != &dummyrdir) free((char *)rdirfdt[s]);
    rdirfdt[s] = NULL;
  }
#endif /* _WIN32 */
  return(0);
}


