/*
 * $Id: lstat.c,v 1.16 2005/02/22 13:28:35 jdurand Exp $
 */

/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: lstat.c,v $ $Revision: 1.16 $ $Date: 2005/02/22 13:28:35 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

/* lstat.c       Remote File I/O - get file status   */

#define RFIO_KERNEL     1
#include <pwd.h>
#include "rfio.h"
#include <Cglobals.h>
#include <Cpwd.h>

static int pw_key = -1;
static int old_uid_key = -1;


int DLL_DECL rfio_lstat(filepath, statbuf)      /* Remote file lstat	*/
char    *filepath;              	/* remote file path  		*/
struct stat *statbuf;           	/* status buffer 		*/
{
#if (defined(__alpha) && defined(__osf__))
   return (rfio_lstat64(filepath,statbuf));
#else
   int      lstatus;		/* remote lstat() status    	*/
#if defined(IRIX64) || defined(__ia64__) || defined(__x86_64) || defined(__ppc64__)
   struct stat64 statb64;

   if ((lstatus = rfio_lstat64(filepath,&statb64)) == 0)
	(void) stat64tostat(&statb64, statbuf);
   return (lstatus);
#else
   register int    s;           /* socket descriptor 		*/
   char     buf[BUFSIZ];      	/* General input/output buffer  */
   int	    len;
   char     *host, *filename;
   char     *p=buf;
   int     uid;
   int     gid;
   int	i;
   struct  passwd *pw_tmp;
   struct  passwd *pw = NULL;
   int	*old_uid = NULL;
   int 		rt,rc,reqst,magic, parserc ;


   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_lstat(%s, %x)", filepath, statbuf);

   if ( Cglobals_get(&old_uid_key, (void**)&old_uid, sizeof(int)) > 0 )
      *old_uid = -1;
   Cglobals_get(&pw_key, (void**)&pw, sizeof(struct passwd));
   if (!(parserc = rfio_parseln(filepath,&host,&filename,NORDLINKS))) {
      /* if not a remote file, must be local or HSM  */
      if ( host != NULL ) {
          /*
           * HSM file
           */
          TRACE(1,"rfio","rfio_stat: %s is an HSM path",
                filename);
          END_TRACE();
          rfio_errno = 0;
          return(rfio_HsmIf_stat(filename,statbuf));
      }
      TRACE(1, "rfio", "rfio_lstat: using local lstat(%s, %x)",
	    filename, statbuf);

      END_TRACE();
      rfio_errno = 0;
#if !defined(_WIN32)
      lstatus = lstat(filename,statbuf);
#else
      lstatus = stat(filename,statbuf);
#endif /* _WIN32 */
      if ( lstatus < 0 ) serrno = 0;
      return(lstatus);
   }
   if (parserc < 0) {
	   END_TRACE();
	   return(-1);
   }

   serrno = 0;
   magic = B_RFIO_MAGIC;
   reqst = RQST_LSTAT_SEC;
   /*
    * To keep backward compatibility we first try the new secure
    * lstat() and then, if it failed, go back to the old one.
    */
   for ( i=0; i<2; i++ ) {
      s = rfio_connect(host,&rt);
      if (s < 0) {
	 END_TRACE();
	 return(-1);
      }

      len = strlen(filename)+1;
      p = buf;
      marshall_WORD(p, magic);
      marshall_WORD(p, reqst);
      if ( reqst == RQST_LSTAT_SEC ) {
	 uid = geteuid();
	 gid = getegid();
	 if ( uid != *old_uid ) {
	    if ( (pw_tmp = Cgetpwuid(uid) ) == NULL ) {
	       TRACE(2, "rfio" ,"rfio_stat: Cgetpwuid(): ERROR occured (errno=%d)",errno);
	       END_TRACE();
	       (void) netclose(s);
	       return -1 ;
	    }
	    memcpy(pw, pw_tmp, sizeof(struct passwd));
	    *old_uid = uid;
	 }
	 len+=2*WORDSIZE + strlen(pw->pw_name) + 1;
   if ( RQSTSIZE+len > BUFSIZ ) {
     TRACE(2,"rfio","rfio_lstat: request too long %d (max %d)",
           RQSTSIZE+len,BUFSIZ);
     END_TRACE();
     (void) netclose(s);
     serrno = E2BIG;
     return(-1);
   }
      }
      marshall_LONG(p, len);
      p= buf + RQSTSIZE;
      if ( reqst == RQST_LSTAT_SEC ) {
	 marshall_WORD(p, uid);
	 marshall_WORD(p, gid);
	 marshall_STRING(p, pw->pw_name);
      }
      marshall_STRING(p, filename);
      TRACE(2,"rfio","rfio_lstat: sending %d bytes",RQSTSIZE+len) ;
      if (netwrite_timeout(s,buf,RQSTSIZE+len,RFIO_CTRL_TIMEOUT) != (RQSTSIZE+len)) {
	 TRACE(2, "rfio", "rfio_lstat: write(): ERROR occured (errno=%d)", errno);
	 (void) netclose(s);
	 END_TRACE();
	 return(-1);
      }
      p = buf;
      TRACE(2, "rfio", "rfio_lstat: reading %d bytes", 6*LONGSIZE+5*WORDSIZE);
      if ((rc = netread_timeout(s, buf, 6*LONGSIZE+5*WORDSIZE,RFIO_CTRL_TIMEOUT)) != (6*LONGSIZE+5*WORDSIZE))  {
	 TRACE(2, "rfio", "rfio_lstat: read(): ERROR occured (errno=%d)", errno);
	 (void) netclose(s);
	 if ( rc == 0 && reqst == RQST_LSTAT_SEC ) {
	    TRACE(2,"rfio","rfio_lstat: Server doesn't support secure lstat()");
	    reqst = RQST_LSTAT;
	    magic = RFIO_MAGIC;
	 } else {
	    END_TRACE();
	    return(-1);
	 }
      } else break;
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
   unmarshall_LONG(p, lstatus);
   TRACE(1, "rfio", "rfio_lstat: return %d",lstatus);
   rfio_errno = lstatus;
   (void) netclose(s);
   if (lstatus)     {
      END_TRACE();
      return(-1);
   }
   END_TRACE();
   return (0);
#endif
#endif
}

int DLL_DECL rfio_lstat64(filepath, statbuf)    /* Remote file lstat    */
char    *filepath;                              /* remote file path     */
struct stat64 *statbuf;                         /* status buffer        */
{
   register int    s;                           /* socket descriptor    */
   int       status ;
   char     *host, *filename;
   int      rt, parserc ;

   INIT_TRACE("RFIO_TRACE");
   TRACE(1, "rfio", "rfio_lstat64(%s, %x)", filepath, statbuf);

   if (!(parserc = rfio_parseln(filepath,&host,&filename,NORDLINKS))) {
      /* if not a remote file, must be local or HSM                     */
      if ( host != NULL ) {
          /*
           * HSM file
           */
          TRACE(1,"rfio","rfio_lstat64: %s is an HSM path", filename);
          END_TRACE();
          rfio_errno = 0;
          return(rfio_HsmIf_stat64(filename,statbuf));
      }
      TRACE(1, "rfio", "rfio_lstat64: using local lstat64(%s, %x)",
         filename, statbuf);

      END_TRACE();
      rfio_errno = 0;
#if !defined(_WIN32)
      status = lstat64(filename,statbuf);
#else
      status = stat64(filename,statbuf);
#endif /* _WIN32 */
      if ( status < 0 ) serrno = 0;
      return(status);
   }
   if (parserc < 0) {
	   END_TRACE();
	   return(-1);
   }

   s = rfio_connect(host,&rt);
   if (s < 0)      {
	   END_TRACE();
      return(-1);
   }
   END_TRACE();
   status = rfio_smstat64(s, filename, statbuf, RQST_LSTAT64) ;
   if ( status == -1 && serrno == SEPROTONOTSUP ) {
      /* The RQST_LSTAT64 is not supported by remote server          */
      /* So, we use RQST_LSTAT_SEC instead                           */
      s = rfio_connect(host,&rt);    /* First: reconnect             */
      if (s < 0)      {
          return(-1);
      }
      status = rfio_smstat64(s, filename, statbuf, RQST_LSTAT_SEC);
      if ( status == -1 && serrno == SEPROTONOTSUP ) {
         /* The RQST_LSTAT_SEC is not supported by remote server     */
         /* So, we use RQST_LSTAT instead                            */
         s = rfio_connect(host,&rt);
         if (s < 0)      {
             return(-1);
         }
         status = rfio_smstat64(s, filename, statbuf, RQST_LSTAT) ;
      }
   }
   (void) netclose(s);
   return (status);
}
