/*
 * $Id: rfio_rfilefdt.c,v 1.9 2008/07/31 07:09:13 sponcec3 Exp $
 */

/*
 * System remote file I/O
 */
#define RFIO_KERNEL     1
#include <fcntl.h>
#include <sys/param.h>          /* For MAXHOSTNAMELEN definition  */
#include <stdlib.h>
#include "rfio.h"
#include "rfio_rfilefdt.h"

extern RFILE *rfilefdt[MAXRFD] ;

RFILE dummyrfile;               /* Used to fill with a dummy value */

/*
 * Seach for a free index in the rfilefdt table
 */
int DLL_DECL rfio_rfilefdt_allocentry(s)
     int s;
{
  return(((s >= 0) && (s < MAXRFD)) ? s : -1);
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
}


/*
 * Free a given index in the rfilefdt table
 * Warning : the argument is REALLY an index
 */
int DLL_DECL rfio_rfilefdt_freeentry(s)
     int s;
{
  if ((s >= 0) && (s < MAXRFD) && (rfilefdt[s] != NULL)) {
    if (rfilefdt[s] != &dummyrfile) free((char *)rfilefdt[s]);
    rfilefdt[s] = NULL;
  }
  return(0);
}
