/*
 * $Id: lstat.c,v 1.3 1999/12/09 08:48:35 baran Exp $
 *
 * $Log: lstat.c,v $
 * Revision 1.3  1999/12/09 08:48:35  baran
 * Thread-safe version
 *
 * Revision 1.2  1999/07/20 12:48:02  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */

/*
 * Copyright (C) 1990-1999 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)lstat.c	1.5 08 Jan 1999 CERN CN-SW/DC Felix Hassine";
#endif /* not lint */

/* lstat.c       Remote File I/O - get file status   */

#define RFIO_KERNEL     1
#include <pwd.h>
#include "rfio.h"

static char     buf[256];       /* General input/output buffer          */

int  rfio_lstat(filepath, statbuf)       /* Remote file lstat 		*/
char    *filepath;              	/* remote file path  		*/
struct stat *statbuf;           	/* status buffer 		*/
{
	register int    s;              /* socket descriptor 		*/
	int             lstatus;        /* remote lstat() status    	*/
	int     	len;
	char    	*host, *filename;
	char    	*p=buf;
	int     uid;
	int     gid;
	int     req;
	int	i;
	struct  passwd	*pw_tmp;
	static struct	passwd pw;
	static int	old_uid = -1;
#if defined(vms)
	unsigned short  unix_st_dev;
	unsigned long   unix_st_ino;
#endif /* vms */
	int 		rt,rc,reqst,magic ;
        static char     buf[256];       /* General input/output buffer          */

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_lstat(%s, %x)", filepath, statbuf);

	if (!rfio_parseln(filepath,&host,&filename,NORDLINKS)) {
		/* if not a remote file, must be local  */
		TRACE(1, "rfio", "rfio_lstat: using local lstat(%s, %x)",
			filename, statbuf);

		END_TRACE();
		rfio_errno = 0;
#if !defined(vms)
		return(lstat(filename,statbuf));
#else
		return(stat(filename,statbuf));
#endif /* vms */
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
			if ( uid != old_uid ) {
				if ( (pw_tmp = getpwuid(uid) ) == NULL ) {
					TRACE(2, "rfio" ,"rfio_stat: getpwuid(): ERROR occured (errno=%d)",errno);
					END_TRACE();
					(void) netclose(s);
					return -1 ;
				}
				memcpy(&pw,pw_tmp,sizeof(struct passwd));
				old_uid = uid;
			}
			len+=2*WORDSIZE + strlen(pw.pw_name) + 1;
		}
		marshall_LONG(p, len);
		p= buf + RQSTSIZE;
		if ( reqst == RQST_LSTAT_SEC ) {
		  marshall_WORD(p, uid);
		  marshall_WORD(p, gid);
		  marshall_STRING(p,pw.pw_name);
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
#if !defined(vms)
	unmarshall_WORD(p, statbuf->st_dev);
	unmarshall_LONG(p, statbuf->st_ino);
#else
	unmarshall_WORD(p, unix_st_dev);
	unmarshall_LONG(p, unix_st_ino);
#endif /* vms */
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
}
