/*
 * 
 * Copyright (C) 2004 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: send2expert.c,v $ $Revision: 1.1 $ $Date: 2004/06/30 16:18:36 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#include "Cnetdb.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "expert.h"
#if defined(_WIN32)
extern char *ws_strerr;
#endif

/* send2expert - send a request to the expert daemon */

send2expert(socketp, reqp, reql)
int *socketp;
char *reqp;
int reql;

{
	int c;
	int port;
	char exphost[CA_MAXHOSTNAMELEN + 1];
	char func[16];
	char *getconfent();
	char *getenv();
	struct hostent *hp;
	int magic;
	int n;
	char *p;
	int s;
	struct sockaddr_in sin; /* internet socket */
	struct servent *sp;

	strcpy (func, "send2expert");

	/* Get the server */
    
	if ((p = getenv ("EXPERT_PORT")) || (p = getconfent ("EXPERT", "PORT", 0))) {
	  port = atoi (p);
	}
	else if ((sp = Cgetservbyname ("expert", "tcp")) != NULL) {
	  port = ntohs(sp->s_port);
	}
	else {
	  port = EXPERT_PORT;
	}
	if ((p = getenv ("EXPERT_HOST")) || (p = getconfent ("EXPERT", "HOST", 0)))
	  strncpy (exphost, p, CA_MAXHOSTNAMELEN);
	else
#if defined(EXPERT_HOST)
	  strncpy (exphost, EXPERT_HOST, CA_MAXHOSTNAMELEN);
#else
	  gethostname (exphost, sizeof(exphost));
#endif
	if (socketp == NULL || *socketp < 0) {	/* connection not opened yet */
       		sin.sin_family = AF_INET;
		sin.sin_port = htons ((unsigned short)port);
		if ((hp = Cgethostbyname (exphost)) == NULL) 
		{
		        exp_errmsg (func, EXP09, "Host unknown:", exphost);
			serrno = SENOSHOST;
			return (-1);
		}
		sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

		if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) 
                {
		        exp_errmsg (func, EXP02, "socket", neterror());
			serrno = SECOMERR;
			return (-1);
		}

		if (connect (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
#if defined(_WIN32)
			if (WSAGetLastError() == WSAECONNREFUSED) {
#else
			if (errno == ECONNREFUSED) {
#endif
			  	exp_errmsg (func, EXP00, exphost);
				(void) netclose (s);
				serrno = 21 /*EEXPNACT*/;
				return (-1);
			} else {
			  	exp_errmsg (func, EXP02, "connect", neterror());
				(void) netclose (s);
				serrno = SECOMERR;
				return (-1);
			}
		}
		if (socketp)
			*socketp = s;
	} else
		s = *socketp;

	/* send request to the expert daemon */
	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
		  exp_errmsg (func, EXP02, "send", sys_serrlist[SERRNO]);
		else
		  exp_errmsg (func, EXP02, "send", neterror());
		(void) netclose (s);
		serrno = SECOMERR;
		return (-1);
	}
	return (0);
}

/* get reply from the expert daemon */

getrep (socket, rep_status, errcode, reply_type)
int socket;
int* rep_status;
int* errcode;
int* reply_type;
{
        int s;
        char repheader[4 * LONGSIZE];
        int magic;
        int rep_type;
	int status;
	int ecode;
        int n;
	char *p;
	char func[32];

	strcpy(func, "getrep");
	/* get reply */
        s = socket;
	/* read status */
	if ((n = netread (s, repheader, 4 * LONGSIZE)) <= 0) {
	  if (n == 0)
	    exp_errmsg (func, EXP02, "recv", sys_serrlist[SERRNO]);
	  else
	    exp_errmsg (func, EXP02, "recv", neterror());
	  (void) netclose (s);
	  serrno = SECOMERR;
	  return (-1);
	}
	p = repheader;
	unmarshall_LONG (p, magic);
	unmarshall_LONG (p, rep_type);
	unmarshall_LONG (p, status);
	unmarshall_LONG (p, ecode);
	*reply_type = rep_type;
	*rep_status = status;
	*errcode = ecode;
	return (0);
}
