
/*
 * Copyright (C) 1990-1997 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)symlink.c	1.7 05/06/98  CERN CN-SW/DC F. Hassine";
#endif /* not lint */

#define RFIO_KERNEL     1
#include <pwd.h>
#include "rfio.h"

static  char buf[256];
#ifndef linux
extern char *sys_errlist[];     /* system error list */
#endif

/*
 * returns -1 if an error occured or no information is available at the
 * daemon's host. 0 otherwise.
 */
static int rfio_wlink(n1,n2 )
char *n1 ;
char *n2 ;

{
	int c;
        int status ;
	char *nbuf ;
	int s ;
	char *host ;
	char * filename;
	char *p ;
	int ans_req ,rt ;
	int rcode ;
	int uid ;
	int gid ;
	int un ; 	/* UNlink or SYMlink ? */
	struct passwd *pw ;

        /*
         * The file is local.
         */
	un = (n1[0]=='\0' ? 1:0);
        INIT_TRACE("RFIO_TRACE");
	TRACE( 1, "rfio", " rfio_%slink (%s,%s)",(un ? "un":"sym"),n1,n2 );
        if ( ! rfio_parseln(n2,&host,&filename,NORDLINKS) ) {
		TRACE(2,"rfio","rfio_%slink local %s %s %s",(un ? "un":"sym"),filename,
							(un? "":"-> "),
							(un? "":n1) ) ;
		if (un)
			status = unlink(filename) ;
		else
#if ! defined(_WIN32)
			status = symlink(n1,filename) ;
#else
			{ serrno = SEOPNOTSUP; status = -1;}
#endif
                END_TRACE() ;
		rfio_errno = 0;
                return status ;
        }

        s = rfio_connect(host,&rt);
        if (s < 0)      {
                END_TRACE();
                return(-1);
        }
	uid = geteuid() ;
	gid = getegid () ;
        if ( (pw = getpwuid(uid) ) == NULL ) {
                        TRACE(2, "rfio" ,"rfio_%slink: getpwuid() error %s",
					(un ? "un":"sym"),
					sys_errlist[errno]);
                        END_TRACE();
                        return -1 ;
        }

        p = buf ;
        marshall_WORD(p, B_RFIO_MAGIC);
        marshall_WORD(p, RQST_SYMLINK);

	status = strlen(pw->pw_name)+strlen(n1)+strlen(filename)+3+2*WORDSIZE;
	marshall_LONG(p, status) ;

        if (netwrite(s,buf,RQSTSIZE) != RQSTSIZE) {
                TRACE(2, "rfio", "%slink: write(): ERROR occured (errno=%d)",
					(un ? "un":"sym"),
					errno);
                (void) close(s);
                END_TRACE();
                return(-1);
        }
	nbuf = (char *) malloc( status ) ;
	if ( nbuf == NULL ) {
		TRACE(2, "rfio", "%slink:  malloc () failed",(un ? "un":"sym"));
		(void) close(s);
                END_TRACE();
                return(-1);
        }

	p = nbuf ;

	marshall_WORD(p,uid) ;
	marshall_WORD(p,gid) ;
	marshall_STRING( p, n1 ) ;
	marshall_STRING( p, filename ) ;
	marshall_STRING( p, pw->pw_name) ;
	
	if (netwrite(s,nbuf,status) != status ) {
                TRACE(2, "rfio", "%slink: write(): ERROR occured (errno=%d)",(un ? "un":"sym"),errno);
                (void) close(s);
                END_TRACE();
                return(-1);
        }
	(void) free(nbuf) ;

	/*
 	 * Getting back status
	 */ 
        if ((c=netread(s, buf, WORDSIZE + 2*LONGSIZE)) != WORDSIZE+ 2*LONGSIZE)  {
		if (c == 0 && un == 0) {
			serrno = SEOPNOTSUP;	/* symbolic links not supported on remote machine */
			TRACE(2, "rfio", "rfio_symlink: read(): ERROR occured (serrno=%d)", serrno);
		} else
			TRACE(2, "rfio", "rfio_%slink: read(): ERROR occured (errno=%d)", (un ? "un":"sym"), errno);
                (void) close(s);
                END_TRACE();
                return(-1);
        }
	p = buf;
        unmarshall_WORD( p, ans_req );
        unmarshall_LONG( p, status ) ;
	unmarshall_LONG( p, rcode ) ;

        if ( ans_req != RQST_SYMLINK ) {
                TRACE(1,"rfio","rfio_%slink: ERROR: answer does not correspond to request !",(un ? "un":"sym"));
                (void) close(s);
                END_TRACE();
                return(-1);
        }

        if ( status < 0 ) {
                TRACE(1,"rfio","rfio_%slink: failure, error %d",(un ? "un":"sym"),rcode);
		rfio_errno = rcode ;
                (void) close(s);
                END_TRACE();
                return(status);
        }
	TRACE (2,"rfio","rfio_%slink succeded",(un ? "un":"sym"));
	END_TRACE();
	(void) close (s) ;
	return(status) ;
}

int rfio_unlink(path)
char *path ;
{
	return( rfio_wlink("",path)) ;
}

int rfio_symlink(name1,name2)
char *name1, *name2;
{
	return( rfio_wlink( name1, name2 ) ) ;
}
