/*
 * $Id: checkkey.c,v 1.5 2003/09/14 06:38:56 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: checkkey.c,v $ $Revision: 1.5 $ $Date: 2003/09/14 06:38:56 $ CERN/IT/PDP/DM Felix Hassine";
#endif /* not lint */

#include <stdio.h>
#include <osdep.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>
#endif
#include <log.h>
#include <string.h>
#include <errno.h>
#include <serrno.h>
#if defined(HPSS) /* Clash with marshall.h in /usr/include/dce */
#include "../h/marshall.h"
#else /* HPSS */
#include <marshall.h>
#endif /* HPSS */
#include <socket_timeout.h>
#include <Cnetdb.h>

#define RFIO2TPREAD_MAGIC 0X0110
#define OK 1

extern int (*recvfunc)();       /* Network receive function */
extern int (*sendfunc)();	/* Network send function */

#define netread         (*recvfunc)
#define netwrite        (*sendfunc)

#ifndef RFIO_CTRL_TIMEOUT
#define RFIO_CTRL_TIMEOUT 10
#endif

int connecttpread(host,aport)
        char * host ;
	u_short aport ;
{
        struct hostent          *hp ;           /* Host entry pointer.          */
        struct sockaddr_in      sin ;           /* An Internet socket address.  */
        int                    sock ;           /* Socket descriptor.           */
        extern char      * getenv() ;           /* Getting env variables        */
        char                  * env ;           /* To store env variables       */

	log(LOG_DEBUG,"Connecting tpread@%s to check key on port %d\n",host,aport);
        /*
         * Creating socket.
         */
        if (( sock= socket(AF_INET,SOCK_STREAM,0)) == -1 ) {
                log(LOG_ERR,"socket(): %s\n",strerror(errno)) ;
                return -1 ;
        }

        if ((hp= Cgethostbyname(host)) == NULL ) {
                serrno = SENOSHOST;
                log(LOG_ERR,"Cgethostbyname(): %s\n",sstrerror(serrno)) ;
                return -1 ;
        }

        /*
         * Building Daemon Internet address.
         */
        if ( (env=getenv("RFIO2TPREAD")) == NULL ) 
                sin.sin_port= aport ;
        else    {
                sin.sin_port= htons(atoi(env)) ;
        }
        sin.sin_family= AF_INET ;
        sin.sin_addr.s_addr= ((struct in_addr *)(hp->h_addr))->s_addr ;

        /*
         * Connecting the socket.
         */
        if ( connect(sock, (struct sockaddr *) &sin, sizeof(sin))  == -1 ) {
                log(LOG_ERR,"connect(): %s\n",strerror(errno)) ;
                return -1 ;
        }

	log(LOG_DEBUG,"Checking that key replier is in site\n");
	if ( isremote(sin.sin_addr, host) ) {
		log(LOG_INFO,"Attempt to give key from outside site rejected\n");
		return  -1 ;
	}
        if ( setnetio(sock) == -1 ) {
                log(LOG_ERR,"error in setnetio()\n") ;
                (void) close(sock) ;
                return -1 ;
        }
        return sock ;
}

/* 
 * Returns 1 if key is valid, 0 otherwise.
 * returns -1 if failure
 */
int checkkey( sock, key )
int sock;
u_short  key;
{
	int rcode ;
	int magic ;
	int answer;
	char marsh_buf[64] ;
	char *ptr;
	ptr = marsh_buf ;

	marshall_LONG(ptr,RFIO2TPREAD_MAGIC);
	marshall_LONG(ptr,(LONG)key);
	marshall_LONG(ptr, 0);
        /*
         * Sending key.
         */
        if ( netwrite_timeout(sock,marsh_buf,3*LONGSIZE,RFIO_CTRL_TIMEOUT) != (3*LONGSIZE) ) {
                log(LOG_ERR,"netwrite(): %s\n", strerror(errno)) ;
                return -1 ;
        }
	/*
	 * Waiting for ok akn.
	 */
	if ( (rcode= netread_timeout(sock,marsh_buf,LONGSIZE*3,RFIO_CTRL_TIMEOUT)) != (LONGSIZE*3) ) {
                log(LOG_ERR,"netread(): %s\n",strerror(errno)) ;
                (void) close(sock) ;
                return -1 ;
	}
	ptr = marsh_buf ;
	if ( rcode == 0 ) {
                log(LOG_ERR,"connection closed by remote end\n") ;
                (void) close(sock) ;
                return -1 ;
        }
	unmarshall_LONG(ptr,magic);
	if ( magic != RFIO2TPREAD_MAGIC ) {
		log(LOG_ERR,"Magic inconsistency. \n");
		return -1 ;
	}
	unmarshall_LONG(ptr,answer);
	if ( answer==OK ) {
		log(LOG_DEBUG,"Key is correct.\n");
		return 1 ;
	}
	else
		return 0 ;
}

