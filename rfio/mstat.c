/*
 * $Id: mstat.c,v 1.2 1999/07/20 12:48:03 jdurand Exp $
 *
 * $Log: mstat.c,v $
 * Revision 1.2  1999/07/20 12:48:03  jdurand
 * 20-JUL-1999 Jean-Damien Durand
 *   Timeouted version of RFIO. Using netread_timeout() and netwrite_timeout
 *   on all control and data sockets.
 *
 */


/*
 * Copyright (C) 1995-1999 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)mstat.c	1.9 08 Jan 1999  CERN CN-SW/DC Felix Hassine";
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
#include "log.h"
#define RFIO_KERNEL 1
#include "rfio.h"

static int rfindex = 0 ;
typedef struct socks {
              char host[MAXHOSTNAMELEN];
              int s ;
              int sec;
} connects ;
static connects tab[MAXMCON]; /* UP TO MAXMCON connections simultaneously */
extern int rfio_smstat() ;
static char     buf[256];

int rfio_mstat(file,statb)
char *file ;
struct stat *statb;

{
	
	int rt ,rc ,i ,fd ,retry;
	char *host , *filename ;

	if (!rfio_parseln(file,&host,&filename,NORDLINKS)) {
		/* The file is local */
		rc = stat(filename,statb) ;
		rfio_errno = 0;
		return (rc) ;
	}
	else {
		/* Look if already in */
	        serrno = 0;
		for ( i=0; i<rfindex; i++ ) {
			if ( !strcmp(host,tab[i].host) ) {
				if ( tab[i].sec )
					rc = rfio_smstat(tab[i].s,filename,statb,RQST_MSTAT_SEC ) ;
				else
					rc = rfio_smstat(tab[i].s,filename,statb,RQST_MSTAT ) ;
				return ( rc) ;
			}
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
			if ( rfindex < MAXMCON ) {	
				tab[rfindex].s=fd ;
				strcpy( tab[rfindex].host, host) ;
				if ( !rc ) tab[rfindex].sec = 1;
				else tab[rfindex].sec = 0;
			}
			serrno = 0;
			if ( rfindex < MAXMCON ) {
				if ( !rc ) 
				  rc = rfio_smstat(fd,filename,statb,RQST_MSTAT_SEC);
				else
				  rc = rfio_smstat(fd,filename,statb,RQST_MSTAT);
			} else {
				if ( !rc )
				  rc = rfio_smstat(fd,filename,statb,RQST_STAT_SEC) ;
				else
				  rc = rfio_smstat(fd,filename,statb,RQST_STAT);
			}
			if ( !(rc == -1 && serrno == SEPROTONOTSUP) ) break;
		}
		rfindex++;
		return (rc)  ;
	}

}

/* 
 * Simplest operation in stat() : just do a stat() 
 * for 1 filename
 */
rfio_smstat(s,filename,statbuf,reqst) 
int s ;
char * filename ;
struct stat *statbuf ;
int reqst ;

{
	int             status;         /* remote fopen() status        */
	int     len;
	int     rc;
	char    *p=buf;
        int     uid;
        int     gid;
	static int old_uid = -1;
	struct  passwd *pw_tmp;
	static struct passwd pw;

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_stat(%s, %x)", filename, statbuf);

	len = strlen(filename)+1;
	switch ( reqst ) {
		case RQST_MSTAT_SEC :
		case RQST_STAT_SEC :
		        TRACE(2,"rfio","rfio_stat: trying secure stat()");
		        marshall_WORD(p, B_RFIO_MAGIC);
			uid = geteuid() ;
			gid = getegid () ;
			if ( uid != old_uid ) {
				if ( (pw_tmp = getpwuid(uid) ) == NULL ) {
					TRACE(2, "rfio" ,"rfio_stat: getpwuid(): ERROR occured (errno=%d)",errno);
					END_TRACE();
					(void) close(s);
					return -1 ;
				}	
				memcpy(&pw,pw_tmp,sizeof(struct passwd));
				old_uid = uid;
			}
		        marshall_WORD(p, reqst);
			len+=2*WORDSIZE + strlen(pw.pw_name) + 1;
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
		marshall_STRING(p,pw.pw_name);
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

int rfio_end()
{
	int i,j=0 ;
	char *p=buf ;
	INIT_TRACE("RFIO_TRACE");
	TRACE(3,"rfio","rfio_end entered\n");
	for ( i=0 ; i< rfindex; i++ )  {
		marshall_WORD(p, RFIO_MAGIC);
        	marshall_WORD(p, RQST_END);
        	marshall_LONG(p, j);
        	if (netwrite_timeout(tab[i].s,buf,RQSTSIZE,RFIO_CTRL_TIMEOUT) != RQSTSIZE) {
                	TRACE(2, "rfio", "rfio_stat: write(): ERROR occured (errno=%d)", errno);
                	END_TRACE();
                	return(-1);
		}
               	(void) close(tab[i].s);
        }
        END_TRACE();
	rfindex=0 ;
}

