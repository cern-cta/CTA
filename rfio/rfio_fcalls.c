/*
 * $Id: rfio_fcalls.c,v 1.1 2004/12/07 12:15:08 jdurand Exp $
 */

/*
 * Copyright (C) 1994-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rfio_fcalls.c,v $ $Revision: 1.1 $ $Date: 2004/12/07 12:15:08 $ CERN/IT/PDP/DM Felix Hassine" ;
#endif /* not lint */

/* rfio_fcalls.c        - Remote file I/O - server FORTRAN calls        */

#define DEBUG           0               /* Debugging flag               */
#define RFIO_KERNEL     1               /* KERNEL part of the programs  */

#include <errno.h>
#include <string.h>
#include <Castor_limits.h>
#include "rfio.h"                       /* Remote file I/O              */
#include <log.h>                        /* Genralized error logger      */
#if defined(_WIN32)
#include "syslog.h"
#else
#include <sys/param.h>                  /* System parameters            */
#endif
#include <pwd.h>

#if defined(HPSS)
#include "../h/marshall.h"
#include <dirent.h>
#include <dce/pthread.h>
#include <rfio_hpss.h>
extern struct global_defs global[];
#endif /* HPSS */

#define SO_BUFSIZE      20*1024         /* Default socket buffer size   */
#define MAXXFERSIZE     200*1024        /* Maximum transfer size        */

/*
 * External declarations for error handling
 * and space allocation.
 */
extern char     *getconfent();
#if !defined(_WIN32)
extern char     *malloc();		/* Some systems forget this	*/
#endif
extern int      checkkey();         /* To check passwd provided     */

/*
 *External functions for RFIOs
 */
extern int	usf_open();
extern int	udf_open();
extern int	usf_write();
extern int	udf_write();
extern int	usf_read();
extern int 	udf_read();
extern int	uf_close();	
extern void	uf_cread();

#if !defined(HPSS)
#if defined(_WIN32)

#if !defined (MAX_THREADS)
#define MAX_THREADS 64
#endif /* MAX_THREADS */
#if !defined (RFIO_MAX_PORTS)
#define RFIO_MAX_PORTS 10
#endif /* RFIO_MAX_PORTS */

extern /*__declspec( thread ) */ DWORD tls_i;  
extern struct thData  {
  SOCKET ns;    /* control socket */
  struct sockaddr_in from;
  int mode;
  int _is_remote;
  int fd;  
/* globals, which have to be local for thread... */
  char *rqstbuf;        /* Request buffer		*/
  char *filename;        /* file name            	*/  
  char *iobuffer; 		/* Data communication buffer    */
  int  iobufsiz;		/* Current io buffer size       */
  SOCKET data_s;     	/* Data listen socket (v3) */
  SOCKET data_sock;  	/* Data accept socket (v3) */
  SOCKET ctrl_sock;  	/* the control socket (v3) */
  int first_write;
  int first_read;
  int byte_read_from_network;
  struct rfiostat myinfo;
  char	from_host[MAXHOSTNAMELEN];
} *td;

#define rqstbuf td->rqstbuf
#define filename td->filename
#define iobuffer td->iobuffer
#define iobufsiz td->iobufsiz
#define data_s td->data_s
#define data_sock td->data_sock
#define ctrl_sock td->ctrl_sock
#define first_write td->first_write
#define first_read td->first_read
#define byte_read_from_network td->byte_read_from_network
#define is_remote td->_is_remote
#define myinfo td->myinfo

#else /* ! _WIN32 */
/*
 * Data buffer.
 */
static char     * iobuffer ;
static int      iobufsiz= 0;
/*
 * External declaration of request buffer.
 * ( Defined in rfio_calls.c ) 
 */
extern char rqstbuf[] ;   
#endif /* _WIN32 */
#endif /* HPSS */

extern int switch_open();
extern int switch_close();
extern int switch_write();
extern int switch_read();
extern int srchkreqsize _PROTO((SOCKET, char *, int));

/************************************************************************/
/*                                                                      */
/*                              IO HANDLERS                             */
/*                                                                      */
/************************************************************************/

int   srxyopen(s, rlun,access, rt,host, bet)
int     s;
int     *rlun;
int     *access;
int     rt;
char    *host;
int 	bet ;

{
	char    *p ;				/* Msg buffer pointer	*/
	int	len ; 				/* Msg length		*/
	WORD    uid, gid, mask, ftype, format ;	/* Open flags		*/
	LONG    lun, lrecl, openopt ;		/* Open flags		*/
	int     append ;			/* Open flag		*/
	int 	trunc;				/* Open flag		*/
	char	account[MAXACCTSIZE] ;		/* client account 	*/
#if !defined(_WIN32) && !defined(HPSS)
	char	filename[MAXFILENAMSIZE] ; 	/* file name            */
#endif
	int	filen ;				/* file name length	*/
	int     status ;			/* Status		*/
        char    user[CA_MAXUSRNAMELEN+1];
        int     passwd,mapping;
	char    reqhost[MAXHOSTNAMELEN];
	int	sock ;

	*rlun= 0 ;
	p= rqstbuf + 2*WORDSIZE ;
	unmarshall_LONG(p,len) ;
   if ( (status = srchkreqsize(s,p,len)) == -1 ) {
     status = errno;
   } else {
     log(LOG_DEBUG,"rxyopen: betel protocol ? %s\n",(bet ? "yes":"no"));
     /*
      * Reading xyopen request.
      */
     log(LOG_DEBUG,"rxyopen: reading %d bytes\n",len) ;
     if ((status = netread_timeout(s,rqstbuf,len,RFIO_CTRL_TIMEOUT)) != len) {
       log(LOG_ERR,"rxyopen: read(): %s\n",strerror(errno));
       return -1 ;
     }
     p= rqstbuf ; 
     status = 0;
     *account = *filename = '\0';
     unmarshall_WORD(p, uid);
     unmarshall_WORD(p, gid);
     unmarshall_WORD(p, mask);
     unmarshall_WORD(p, ftype);
     unmarshall_LONG(p, lun);
     unmarshall_WORD(p, format);
     unmarshall_WORD(p, *access);
     unmarshall_LONG(p, lrecl);
     unmarshall_LONG(p, openopt);
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, account, MAXACCTSIZE)) == -1 )
       status = E2BIG;
     if ( (status == 0) &&
          (status = unmarshall_STRINGN(p, filename, MAXFILENAMSIZE)) == -1 )
       status = SENAMETOOLONG;
     if (bet) {
       if ( (status == 0) &&
            (status = unmarshall_STRINGN(p, user, CA_MAXUSRNAMELEN+1)) == -1 )
         status = E2BIG;
       if ( (status == 0) &&
            (status = unmarshall_STRINGN(p, reqhost, MAXHOSTNAMELEN)) == -1 )
         status = E2BIG;
       unmarshall_LONG(p, passwd);
       unmarshall_WORD(p, mapping);
     }
     
     if (rt) {
       log(LOG_DEBUG, "Mapping: %s\n", (mapping ? "yes" : "no") );
#if !defined(ultrix)
       openlog("rfio",LOG_PID, LOG_USER) ;
#else
       openlog("rfio",LOG_PID) ;
#endif
       syslog(LOG_ALERT, "rfio: connection %s mapping by %s(%d,%d) from %s",(mapping ?"with" : "without"),user,uid,gid,host) ;
       closelog() ;
     }
     
     if (rt && !mapping) {
       log(LOG_DEBUG, "passwd: %d\n",passwd);
       log(LOG_DEBUG, "reqhost: %s\n",reqhost);
     }
     /*
      * Someone in the site has tried to specify (uid,gid) directly !
      */
     if (bet && !mapping && !rt) {
       log(LOG_INFO,"attempt to make non-mapped I/O and modify uid or gid !\n");
       status=EACCES ;
     }
     
     if ( bet )
       log(LOG_DEBUG,"Opening file %s for remote user: %s\n",filename,user);
     /*
      * Do not authorize any request to create file to
      * a user whose uid < 100
      */
     if ( uid < 100 ) {
       log(LOG_INFO,"attempt to start rfiod with (uid,gid)=(%d,%d) rejected\n",uid,gid);
       status=EACCES;
     }
     /*
      * MAPPED mode: user will be mapped to user "to"
      */
     if ( !status && rt && mapping )  {
       char to[100];
       int rcd,to_uid,to_gid ;
       
       if ( (rcd = get_user(host,user,uid,gid,to,&to_uid,&to_gid)) == -ENOENT ) {
         log(LOG_ERR,"get_user(): Error opening mapping file\n") ;
         status=EINVAL ;
       }
       
       if ( abs(rcd) == 1 ) {
         log(LOG_ERR,"No entry found in mapping file for (%s,%s,%d,%d)\n",
             host,user,uid,gid);
         status = EACCES;
       }
       else {
         log(LOG_DEBUG,"(%s,%s,%d,%d) mapped to %s(%d,%d)\n",
             host,user,uid,gid,to,to_uid,to_gid) ;
         uid = to_uid ;
         gid = to_gid ;
       }
     }
     /*
      * DIRECT access: the user specifies uid & gid by himself
      */
     if ( !status && rt && !mapping ) {
	     char *pr ;
	     if ( (pr= getconfent ("RTUSER","CHECK",0)) == NULL || !strcmp ( pr,"YES") ) { 
         log(LOG_INFO ,"Connecting %s for passwd check ...\n",reqhost);
         if ((sock=connecttpread(reqhost,passwd))>=0 && !checkkey(sock,passwd)) {
           status= EACCES;
           log(LOG_ERR,"ropen: DIRECT mapping : permission denied\n");
         }
         if (sock < 0) {
           status= EACCES ;
           log(LOG_ERR,"ropen: DIRECT mapping failed: Couldn't connect %s\n",reqhost);
         }
       }
	     else 
         log(LOG_INFO ,"Any DIRECT rfio request from out of site is authorized\n");
     }
     
     if ( !status ) {
       filen= strlen(filename) ; 
       log(LOG_DEBUG, "rxyopen: uid %d gid %d mask %o ftype %d \n",uid, gid, mask, ftype);
       log(LOG_DEBUG, "rxyopen: lun %d format %d access %d lrecl %d openopt %x\n",
           lun, format, *access, lrecl, openopt);
       log(LOG_DEBUG, "rxyopen: account: %s\n", account);
       log(LOG_DEBUG, "rxyopen: filename: %s\n", filename);
       log(LOG_INFO, "rxyopen(%s) for (%d,%d)\n",filename,uid,gid);
       *rlun = lun;
       (void) umask(mask);
       append = openopt & FFOOPT_A;
       trunc = openopt & FFOOPT_T;
#if !defined(_WIN32)
       if ((setgid(gid)<0) || (setuid(uid)<0))  {
         status = errno;
         log(LOG_ERR, "rxyopen: unable to setuid,gid(%d,%d): %s\n", uid, gid, strerror(errno));
       }
       else
#endif	     
       {
#if ( defined ( _AIX ) && defined(_IBMR2))
         /*
          * for RS6000, setgid() and setuid() is not enough
          */
         if ( setgroups( 0 , NULL) <0 ) {
           status = errno ;
           log(LOG_ERR,"Unable to setup the process to group ID\n");
         }
#endif
         
         status=switch_open(access, &lun, filename, &filen, &lrecl, (LONG *)&append,(LONG *)&trunc,LLTM);
         log(LOG_DEBUG, "rxyopen: %d\n", status);
       }
     }
   }
   
   p= rqstbuf;
   marshall_LONG(p, status);
   log(LOG_DEBUG, "rxyopen: sending back %d\n", status);
   if (netwrite_timeout(s, rqstbuf, LONGSIZE, RFIO_CTRL_TIMEOUT) != LONGSIZE)  {
     log(LOG_ERR, "rxyopen: write(): %s\n", strerror(errno));
     return(-1);
   }
   log(LOG_INFO, "rxyopen(%s): status is: %d\n",filename,status);
   return(status);
}

int   srxyclos(s, infop, lun)
int     s;
int     lun;
struct rfiostat * infop ;
{
	int     status=0;
	char    *p ;
	int     irc;

	log(LOG_INFO, "%d read, %d readahead, %d write, %d flush, %d stat, %d lseek and %d preseek\n",
	infop->readop,infop->aheadop,infop->writop,infop->flusop,infop->statop,infop->seekop,infop->presop); 
	log(LOG_INFO, "%d bytes read and %d bytes written\n",infop->rnbr,infop->wnbr) ; 
	log(LOG_DEBUG, "rxyclos(%d)\n",lun);
	irc=switch_close(&lun);
	if (iobufsiz > 0)       {
		log(LOG_DEBUG, "rxyclos(): freeing %x\n",iobuffer);
		(void) free(iobuffer);
	}
	iobufsiz= 0;
	if (irc != 0){
		status = serrno ? serrno : errno;
	}
	p= rqstbuf ; 
	marshall_LONG(p,status) ;
	if (netwrite_timeout(s,rqstbuf,LONGSIZE,RFIO_CTRL_TIMEOUT) != LONGSIZE)  {
		log(LOG_ERR, "rxyclos: write(): %s\n", strerror(errno));
		return(-1);
	}
	log(LOG_INFO, "rxyclos(%d): status is: %d\n",lun,status);
	return(status);
}

int srxywrit(s, infop, lun, access)
int     s, lun, access ;
struct rfiostat * infop ;
{
	int     status=0, rcode=0;
	char    *ptr;
	char    *p ;
	int     optval;
	int     nrec, nwrit;

	p = rqstbuf + 2*WORDSIZE ;
	unmarshall_LONG(p, nrec);
	unmarshall_LONG(p, nwrit);
	log(LOG_DEBUG, "rxywrit(%d,%d): nrec %d nwrit %d\n",s,lun,nrec,nwrit) ;
	if (iobufsiz < nwrit)     {
		if (iobufsiz > 0)       {
			log(LOG_DEBUG, "rxywrit(): freeing %x\n",iobuffer);
			(void) free(iobuffer);
		}
		if ((iobuffer = malloc(nwrit)) == NULL)    {
			log(LOG_ERR, "rxywrit: malloc(): %s\n", strerror(errno));
			return(-1);
		}
		iobufsiz = nwrit;
		log(LOG_DEBUG, "rxywrit(): allocated %d bytes at %x\n",nwrit,iobuffer);
		optval = iobufsiz;
		if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char *)&optval, sizeof(optval)) == -1)    {
			log(LOG_ERR, "rxywrit(): setsockopt(SO_RCVBUF): %s\n",strerror(errno));
		}
		log(LOG_DEBUG, "rxywrit(): setsockopt(SO_RCVBUF): %d\n",nwrit);
	}
	ptr = iobuffer;
	if (netread_timeout(s, ptr, nwrit, RFIO_DATA_TIMEOUT) != nwrit)       {
		log(LOG_ERR, "rxywrit: read(): %s\n", strerror(errno));
		return(-1);
	}
	status=switch_write(access,&lun,ptr,&nwrit,&nrec,LLTM);
	if (status != 0)        {
		rcode = errno;
	}
	else	{
		infop->wnbr+= nwrit ;
	}

	log(LOG_DEBUG, "rxywrit: status %d, rcode %d\n", status, rcode);
	p = rqstbuf;
	marshall_LONG(p, status);
	marshall_LONG(p, rcode);
	if (netwrite_timeout(s, rqstbuf, 2*LONGSIZE, RFIO_CTRL_TIMEOUT) != 2*LONGSIZE)  {
		log(LOG_ERR, "rxywrit: write(): %s\n", strerror(errno));
		return(-1);
	}
	return(status);
}


int srxyread(s, infop, lun, access)
int     s, lun;
struct rfiostat * infop ;
{
	int     status=0, rcode=0;
	char    *ptr;
	char    *p ;
	int     optval;
	int     nrec, nwant, ngot;
	int     readopt;

	p= rqstbuf + 2*WORDSIZE ; 
	unmarshall_LONG(p, readopt);
	unmarshall_LONG(p, nrec);
	unmarshall_LONG(p, nwant);
	log(LOG_DEBUG, "rxyread(%d,%d): readopt %x nrec %d nwant %d\n",s,lun,readopt,nrec,nwant) ;
	if (iobufsiz < nwant ) 	{
		if (iobufsiz > 0)       {
			log(LOG_DEBUG, "rxyread(): freeing %x\n",iobuffer);
			(void) free(iobuffer);
		}
		if ((iobuffer = malloc(nwant)) == NULL)    {
			log(LOG_ERR, "rxyread: malloc(): %s\n", strerror(errno));
			return(-1);
		}
		iobufsiz = nwant ;
		log(LOG_DEBUG, "rxyread(): allocated %d bytes at %x\n",nwant,iobuffer);
		optval = iobufsiz;
		if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char *)&optval, sizeof(optval)) == -1)    {
			log(LOG_ERR, "rxyread(): setsockopt(SO_SNDBUF): %s\n",strerror(errno));
		}
		log(LOG_DEBUG, "rxyread(): setsockopt(SO_SNDBUF): %d\n",nwant);
	}
	ptr = iobuffer;
	serrno = 0 ;
	status = switch_read(access, &lun, ptr, &nwant, &nrec, readopt, &ngot,LLTM);  

	if (status != 0){
		rcode = serrno ? serrno : errno;
	}
	else	{
		infop->rnbr+= ngot ;
	}

	p = rqstbuf;
	marshall_LONG(p, status);
	marshall_LONG(p, rcode);
	marshall_LONG(p, ngot);
	if (netwrite_timeout(s, rqstbuf, 3*LONGSIZE, RFIO_CTRL_TIMEOUT) != (3*LONGSIZE)) {
		log(LOG_ERR, "rxyread: write(): %s\n", strerror(errno));
		return(-1);
	}
	if ( ngot ) {
		if (netwrite_timeout(s, ptr, ngot, RFIO_DATA_TIMEOUT) != ngot) {
			log(LOG_ERR, "rxyread: read(): %s\n", strerror(errno));
			return(-1);
		}
	}
	log(LOG_DEBUG, "rxyread: status %d, rcode %d\n", status, rcode);
	return status ;
}

