/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char cvsId[] = "@(#)$RCSfile: setnetio.c,v $ $Revision: 1.7 $ $Date: 2005/01/20 16:26:03 $ CERN/IT/PDP/DM Olof Barring";
#endif /* not lint */

/* setnetio.c   Set network input/output characteristics                */

#include <stdio.h>                      /* Standard Input/Output        */
#include <sys/types.h>                  /* Standard data types          */
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <netdb.h>                      /* Network "data base"          */
#endif
#include <errno.h>                      /* Error numbers                */
#include <string.h>
#include <serrno.h>                     /* SHIFT error codes            */
#include <log.h>                        /* Genralized error logger      */
#include <trace.h>                      /* tracing definitions          */
#include <Cnetdb.h>                     /* reentrant netdb funct.       */

extern char     *getifnam();            /* get interface name           */
extern  char    *getconfent();          /* get configuration entry      */

extern int (*recvfunc)();
extern int (*sendfunc)();

extern int s_recv();            /*         Normal recv()                */
extern int s_send();            /*         Normal send()                */
#ifdef MINBLOCKSIZE             /* Minimum blocksize is required        */
extern int seg_recv();          /*         Segmented recv()             */
extern int seg_send();          /*         Segmented send()             */
#endif /* MINBLOCKSIZE */

int DLL_DECL setnetio(s)
int     s;
{

#ifdef MINBLOCKSIZE

	struct  hostent *hp;
	struct sockaddr_in from;
	int fromlen = sizeof(from);

	char    *p1, *p2;

	INIT_TRACE("COMMON_TRACE");
	TRACE(1,"setnetio", "setnetio(0x%x) entered", s)
	if ((p1 = getifnam(s)) != NULL)  {
		log(LOG_INFO, "connection from interface [%s]\n", p1);
		TRACE(1, "setnetio", "connection from interface [%s]\n", p1);
	}
	else    {
		log(LOG_ERR,"fatal: unable to get interface name\n");
		TRACE(1, "setnetio","fatal: unable to get interface name\n");
		END_TRACE();
		return(-1);
	}

	if (getpeername(s,(struct sockaddr *)&from, &fromlen)<0)        {
		log(LOG_ERR, "getpeername: %s\n",strerror(errno));
		TRACE(1, "setnetio", "getpeername: %s\n",strerror(errno));
		END_TRACE();
		return(-1);
	}

	if ((hp = Cgethostbyaddr(&(from.sin_addr), sizeof(struct in_addr), from.sin_family)) == NULL){
		log(LOG_INFO, "unable to get host name for %s\n", inet_ntoa(from.sin_addr));
		TRACE(1, "setnetio", "unable to get host name for %s\n", inet_ntoa(from.sin_addr));
		END_TRACE();
		return(-1);
	}

/*
 * Hack needed for CRAY/LSC Ultranet problem
 */

	if ((p2 = getconfent("SEGMENT", hp->h_name, 0)) == NULL)   {
		serrno = 0;     /* reset global error number    */
		log(LOG_INFO, "using unsegmented network read/write\n");
		TRACE(1, "setnetio", "using unsegmented network read/write\n");
		recvfunc = s_recv;
		sendfunc = s_send;
	}
	else    {
		if (strcmp(p1, p2))      {
			log(LOG_INFO, "using unsegmented network read/write\n");
			TRACE(1, "setnetio", "using unsegmented network read/write\n");
			recvfunc = s_recv;
			sendfunc = s_send;
		}
		else    {
			log(LOG_INFO, "using segmented network read/write\n");
			TRACE(1, "setnetio", "using segmented network read/write\n");

/*
 * Here we switch the read/write functions
 */
			recvfunc = seg_recv;
			sendfunc = seg_send;
		}
	}

#else /* MINBLOCKSIZE */
	recvfunc = s_recv;
	sendfunc = s_send;
#endif /* MINBLOCKSIZE */



	END_TRACE();
	return(0);
}
