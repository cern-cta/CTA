/*
 * $Id: rfio_rdirfdt.c,v 1.9 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * System remote file I/O
 */
#define RFIO_KERNEL     1
#include <fcntl.h>
#include <sys/param.h>          /* For MAXHOSTNAMELEN definition  */
#include <stdlib.h>
#include "rfio.h"
#include "rfio_rdirfdt.h"

extern RDIR *rdirfdt[MAXRFD];

RDIR dummyrdir;                 /* Used to fill index */

/*
 * Seach for a free index in the rdirfdt table
 */
int rfio_rdirfdt_allocentry(int s)
{
  return(((s >= 0) && (s < MAXRFD)) ? s : -1);
}

/*
 * Seach for a given index in the rdirfdt table
 * On UNIX, if scanflag is FINDRDIR_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 */
int rfio_rdirfdt_findentry(int s,
                           int scanflag)
{
  int i;
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
}


/*
 * Seach for a given pointer in the rdirfdt table
 * On UNIX, if scanflag is FINDRDIR_WITH_SCAN,
 * a scan of table content is performed, otherwise
 * only boundary and content within the boundary
 * is performed.
 */
int rfio_rdirfdt_findptr(RDIR *ptr,
                         int scanflag)
{
  int i;
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
}


/*
 * Free a given index in the rdirfdt table
 * Warning : the argument is REALLY an index
 */
int rfio_rdirfdt_freeentry(int s)
{
  if ((s >= 0) && (s < MAXRFD) && (rdirfdt[s] != NULL)) {
    if (rdirfdt[s] != &dummyrdir) free((char *)rdirfdt[s]);
    rdirfdt[s] = NULL;
  }
  return(0);
}
