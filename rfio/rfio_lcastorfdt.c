/*
 * $Id: rfio_lcastorfdt.c,v 1.1 2005/04/11 15:35:11 jdurand Exp $
 */

/* This file maintain a table to remember a peculiar case: */
/* the user opened a CASTOR file but it appears that the */
/* transfer was totally local, using real local file descriptors */
/* Use case: the data is stroed on global filesystem */

#ifdef _WIN32
#include "Cmutex.h"
#endif
#define RFIO_KERNEL     1
#include <fcntl.h>
#if defined(_WIN32)
#define MAXHOSTNAMELEN 64
#else
#include <sys/param.h>          /* For MAXHOSTNAMELEN definition  */
#endif
#include <stdlib.h>
#include "rfio.h"
#include "rfio_lcastorfdt.h"

struct lcastor_fdt {
  int user_fd;             /* The file descriptor that the user will get */
  int internal_fd;         /* The associated internal file descriptor */
};
typedef struct lcastor_fdt lcastor_fdt_t;

static int rfio_lcastorfdt_init _PROTO(());
static lcastor_fdt_t lcastorfdt[MAXRFD] ; /* Note: we assume that MAXRFD will fit our needs */

/*
 * Seach for a free index in the lcastorfdt table
 */
int DLL_DECL rfio_lcastorfdt_allocentry(user_fd)
     int user_fd;
{
#ifdef _WIN32
  int i;
  int rc;

  if (Cmutex_lock((void *) lcastorfdt,-1) != 0) {
    return(-1);
  }

  /* Scan it */
  for (i = 0; i < MAXRFD; i++) {
    /* Note: the array whould be filled with (fd+1) instead of fd ! */
    if (castorfdt[i].user_fd == 0) {
      rc = i;
      goto _rfio_lcastorfdt_allocentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_lcastorfdt_allocentry_return:
  if (Cmutex_unlock((void *) lcastorfdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  return(((user_fd >= 0) && (user_fd < MAXRFD)) ? user_fd : -1);
#endif /* _WIN32 */
}

/*
 * Seach for a given index in the lcastorfdt table
 * On UNIX, if scanflag is FINDRDIR_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 *
 */
int DLL_DECL rfio_lcastorfdt_findentry(user_fd,internal_fd,scanflag)
     int user_fd;
     int *internal_fd;
     int scanflag;
{
  int i;
#ifdef _WIN32
  int rc;

  if (Cmutex_lock((void *) lcastorfdt,-1) != 0) {
    return(-1);
  }
  /* Scan it */

  for (i = 0; i < MAXRFD; i++) {
    /* Note: the array whould be filled with (fd+1) instead of fd ! */
    int sdummy = user_fd + 1;
    if (lcastorfdt[i].user_fd == sdummy) {
      if (internal_fd != NULL) {
	*internal_fd = lcastorfdt[i].internal_fd - 1;
      }
      rc = i;
      goto _rfio_lcastorfdt_findentry_return;
    }
  }

  serrno = ENOENT;
  rc = -1;

 _rfio_lcastorfdt_findentry_return:
  if (Cmutex_unlock((void *) lcastorfdt) != 0) {
    return(-1);
  }
  return(rc);
#else /* _WIN32 */
  if (scanflag == FINDLCASTOR_WITH_SCAN) {
    for (i = 0; i < MAXRFD; i++) {
      /* Note: the array whould be filled with (fd+1) instead of fd ! */
      int sdummy = user_fd + 1;
      if (lcastorfdt[i].user_fd == sdummy) {
	if (internal_fd != NULL) {
	  *internal_fd = lcastorfdt[i].internal_fd - 1;
	}
	return(i);
      }
    }
    return(-1);
  } else {
    if ((user_fd >= 0) && (user_fd < MAXRFD)) {
	if (internal_fd != NULL) {
	  *internal_fd = lcastorfdt[user_fd].internal_fd - 1;
	}
	return(user_fd);
    } else {
      return(-1);
    }
  }
#endif /* _WIN32 */
}


/*
 * Reset a given index in the lcastorfdt table
 * Warning : the argument is REALLY an index
 */
int DLL_DECL rfio_lcastorfdt_freeentry(index)
     int index;
{
#ifdef _WIN32
  if (Cmutex_lock((void *) lcastorfdt,-1) != 0) {
    return(-1);
  }
  if ((index >= 0) && (index < MAXRFD)) {
    lcastorfdt[index].user_fd = 0;
    lcastorfdt[index].internal_fd = 0;
  }
  if (Cmutex_unlock((void *) lcastorfdt) != 0) {
    return(-1);
  }
#else /* _WIN32 */
  if ((index >= 0) && (index < MAXRFD)) {
    lcastorfdt[index].user_fd = 0;
    lcastorfdt[index].internal_fd = 0;
  }
#endif /* _WIN32 */
  return(0);
}


/*
 * Fill a given index in the lcastorfdt table
 *
 * NOTE: We fill with (fd+1) instead of (fd) because
 * array IS INITIALIZED with ZEROS and 0 is a valid fd
 */
int DLL_DECL rfio_lcastorfdt_fillentry(index,user_fd,internal_fd)
     int index;
     int user_fd;
     int internal_fd;
{
#ifdef _WIN32
  if (Cmutex_lock((void *) lcastorfdt,-1) != 0) {
    return(-1);
  }
  lcastorfdt[index].user_fd = user_fd + 1;
  lcastorfdt[index].internal_fd = internal_fd + 1;
  if (Cmutex_unlock((void *) lcastorfdt) != 0) {
    return(-1);
  }
#else /* _WIN32 */
  if ((index >= 0) && (index < MAXRFD)) {
    lcastorfdt[index].user_fd = user_fd + 1;
    lcastorfdt[index].internal_fd = internal_fd + 1;
  }
#endif /* _WIN32 */
  return(0);
}


