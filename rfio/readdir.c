/*
 * $Id: readdir.c,v 1.6 2000/05/03 13:42:37 obarring Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: readdir.c,v $ $Revision: 1.6 $ $Date: 2000/05/03 13:42:37 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/* readdir.c       Remote File I/O - read  a directory entry            */

#if !defined(_WIN32)
#include <syslog.h>             /* system logger 			*/

/*
 * System remote file I/O definitions
 */

#define RFIO_KERNEL     1 
#include "rfio.h"  
#include <Cglobals.h>
/*
 * Remote directory read
 */

static int s_key = -1;

struct dirent *rfio_readdir(dirp)
RDIR *dirp;
{
   int status ;		/* Status and return code from remote   */
   int rcode;
   int req;
   int *s = NULL;
   int namlen;
   struct dirent *de;
   char *p;
   extern RDIR *rdirfdt[MAXRFD];
   char     rfio_buf[BUFSIZ];
  
   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_readdir(%x)", dirp);

   Cglobals_get(&s_key, (void**)&s, sizeof(int));
   
   /*
    * Search internal table for this directory pointer
    * Check first that it's not the last one used
    */
    if ( *s<0 || *s>=MAXRFD || rdirfdt[*s] != dirp )
       for ( *s=0; *s<MAXRFD; (*s)++ ) if ( rdirfdt[*s] == dirp ) break;

   /* 
    * The directory is local.
    */
   if ( *s<0 || *s >= MAXRFD || rdirfdt[*s] == NULL) {
      TRACE(2,"rfio","rfio_readdir: check if HSM directory");
      if ( (de = rfio_HsmIf_readdir((DIR *)dirp)) == NULL ) {
          TRACE(2,"rfio","rfio_readdir: using local readdir(%x)", dirp);
          de = readdir((DIR *)dirp);
      }
      END_TRACE();
      return(de);
   }

   /*
    * Associate the static dirent area allocate with this 
    * directory descriptor. 
    */
  
   de = (struct dirent *)rdirfdt[*s]->dp.dd_buf;
  
   /*
    * Checking magic number.
    */
   if (rdirfdt[*s]->magic != RFIO_MAGIC) {
      serrno = SEBADVERSION ;
      (void) close(*s) ;
      free((char *)rdirfdt[*s]);
      rdirfdt[*s] = NULL;
      END_TRACE();
      return(NULL);
   }
   p = rfio_buf;
   marshall_WORD(p, RFIO_MAGIC);
   marshall_WORD(p, RQST_READDIR) ; 
   TRACE(2,"rfio","rfio_readdir: writing %d bytes",RQSTSIZE) ;
   if (netwrite_timeout(*s,rfio_buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE)  {
      TRACE(2,"rfio","rfio_readdir: write(): ERROR occured (errno=%d)", errno) ;
      END_TRACE() ;
      return(NULL) ; 
   }
   TRACE(2, "rfio", "rfio_readdir: reading %d bytes",WORDSIZE+3*LONGSIZE) ; 
   if ( netread_timeout(*s,rfio_buf,WORDSIZE+3*LONGSIZE,RFIO_CTRL_TIMEOUT) != (WORDSIZE+3*LONGSIZE) ) {
      TRACE(2,"rfio","rfio_readdir: read(): ERROR occured (errno=%d)", errno);
      END_TRACE();
      return(NULL) ; 
   }
   p= rfio_buf ;
   unmarshall_WORD(p,req) ;	/* RQST_READDIR */
   unmarshall_LONG(p,status) ;
   unmarshall_LONG(p, rcode) ;
   unmarshall_LONG(p,namlen) ;
   if ( status < 0 ) {
      rfio_errno= rcode ;
      if ( rcode == 0 ) 
	 serrno = SENORCODE ;
      END_TRACE() ; 
      return(NULL) ;
   }
   if ( namlen > 0 ) {
      TRACE(2, "rfio", "rfio_readdir: reading %d bytes",namlen) ; 
      memset(de->d_name,'\0',MAXFILENAMSIZE);
      /* Directory name is of a small size, so I put RFIO_CTRL_TIMEOUT instead */
      /* of RFIO_DATA_TIMEOUT */
      if ( netread_timeout(*s,de->d_name,namlen,RFIO_CTRL_TIMEOUT) != namlen ) {
	 TRACE(2,"rfio","rfio_readdir: read(): ERROR occured (errno=%d)", errno);
	 END_TRACE();
	 return(NULL) ; 
      }
      /*
       * Update the directory offset.
       */
      dirp->offset++;
      dirp->dp.dd_loc = dirp->offset;
      de->d_reclen = sizeof(struct dirent) + namlen;
#if !defined(SOLARIS) && !defined(sgi) && !defined(linux)
      de->d_namlen = namlen;
#endif
#ifdef SOLARIS
      de->d_off = dirp->offset;
#endif
#ifdef _AIX
      de->d_offset = dirp->offset;
#endif
   } else {
      TRACE(2,"rfio","rfio_readdir: no more directory entries");
      END_TRACE();
      return(NULL);
   }
   END_TRACE();
   return(de);
}
#endif /* _WIN32 */
