/*
 * Copyright (C) 1990-1998 by CERN/CN/SW/DC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)error.c	3.17 05/06/98 CERN CN-SW/DC Frederic Hemmer";
#endif /* not lint */

/* error.c      Remote File I/O - error numbers and message handling    */

/*
 * System remote file I/O definitions
 */
#define RFIO_KERNEL     1 
#include "rfio.h" 

/*
 * RFIO global error number.
 */
int rfio_errno= 0 ;
#ifndef linux 
extern char * sys_errlist[];
#endif
static char rerrlist[256] ; 	/* Message from errlist */

char *rfio_lasthost() ;
char *rfio_serror() ;

/*
 * Get remote error string corresponding to code.
 */
char *rfio_errmsg(s,code)   
int     s;
int     code;
{
	char   * p ;
	LONG   len ;
	static char msg[256];
	char rfio_buf[256];

	INIT_TRACE("RFIO_TRACE");
	TRACE(1, "rfio", "rfio_errmsg(%d, %d)",s,code) ;
	p= rfio_buf ;
	marshall_WORD(p, RFIO_MAGIC);
	marshall_WORD(p, RQST_ERRMSG);
	marshall_LONG(p, code);
	TRACE(2,"rfio","rfio_errmsg: sending %d bytes",RQSTSIZE) ;
	if (netwrite(s,rfio_buf,RQSTSIZE) != RQSTSIZE)  {
		TRACE(2, "rfio" ,"rfio_errmsg: write(): ERROR occured (errno=%d)", errno);
		END_TRACE();
		return((char *) NULL);
	}
	TRACE(2, "rfio", "rfio_errmsg: reading %d bytes", LONGSIZE);
	if (netread(s,rfio_buf, LONGSIZE) != LONGSIZE) {
		TRACE(2, "rfio" ,"rfio_errmsg: read(): ERROR occured (errno=%d)", errno);
		END_TRACE();
		return((char *) NULL);
	}
	p = rfio_buf ;
	unmarshall_LONG(p, len);
	TRACE(2, "rfio", "rfio_errmsg: reading %d bytes", len);
	if (netread(s,rfio_buf,len) != len) {
		TRACE(2, "rfio" ,"rfio_errmsg: read(): ERROR occured (errno=%d)", errno);
		END_TRACE();
		return((char *) NULL);
	}
	p= rfio_buf;
	unmarshall_STRING(p, msg);
	TRACE(1, "rfio", "rfio_errmsg: <%s>", msg);
	END_TRACE();
	return(msg);
}

char * rfio_serror()                /* print an error message               	*/
{
	int             s;
	int		last_rferr ; 	/* to preserve rfio_errno 		*/
	int		last_err ; 	/* to preserve errno 			*/
	int             last_serrno ;   /* to preserve serrno                   */
	int 		rt ;		/* Request is from other network ?  	*/
	char 		*rferrmsg ;

	INIT_TRACE("RFIO_TRACE");
	last_err = errno ;
	last_rferr = rfio_errno ;
	last_serrno = serrno ;
	TRACE(2, "rfio", "rfio_serror: errno=%d, serrno=%d, rfio_errno=%d",
			  errno, serrno, rfio_errno);
	END_TRACE();
	if (last_serrno != 0) {
		return (  sstrerror(serrno) );
	}
	else    {
		if (last_rferr != 0)        {
			if ((s=rfio_connect(rfio_lasthost(),&rt)) == -1)  {
				sprintf(rerrlist,"Unable to fetch remote error %d",last_rferr);
				rfio_errno = last_rferr ;
				return (rerrlist) ;
			}
			else    {
				if ( (rferrmsg = rfio_errmsg(s,last_rferr)) != NULL )
					sprintf(rerrlist, "%s (error %d on %s)",rferrmsg,last_rferr, rfio_lasthost());
				else
					sprintf(rerrlist, " (error %d on %s)" ,last_rferr ,rfio_lasthost());
				netclose(s);
				rfio_errno = last_rferr ;
				return (rerrlist);
			}
		}
		else    {
			if (serrno != 0)  {
				return (  sstrerror(serrno) );
			}
			else {
				return ( sys_errlist[last_err] );
			}
		}
	}
}

void rfio_perror(umsg)
char *umsg ;
{
	char *errmsg ;
	errmsg =  rfio_serror();
        if (errmsg != NULL ) 
               fprintf(stderr,"%s : %s\n",umsg, errmsg);
        else
               fprintf(stderr,"%s : No error message\n",umsg);
}
