/*
 * $Id: send2nsd.c,v 1.6 2005/03/15 23:07:37 bcouturi Exp $
 *
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)send2nsd.c,v 1.20 2003/11/19 12:22:40 CERN IT-PDP/DM Jean-Philippe Baud";
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
#include "Cns.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#ifdef CSEC
#include "Csec_api.h"
#endif

/* send2nsd - send a request to the name server and wait for the reply */

send2nsd(socketp, host, reqp, reql, user_repbuf, user_repbuf_len)
int *socketp;
char *host;
char *reqp;
int reql;
char *user_repbuf;
int user_repbuf_len;
{
	int actual_replen = 0;
	int c;
	char Cnshost[CA_MAXHOSTNAMELEN+1];
	char func[16];
	char *getconfent();
	char *getenv();
	struct hostent *hp;
	int magic;
	int n;
	char *p;
	char prtbuf[PRTBUFSZ];
	int rep_type;
	char repbuf[REPBUFSZ];
	int s;
	struct sockaddr_in sin; /* internet socket */
	struct servent *sp;
#ifdef CSEC
	Csec_context_t ctx;
	int secure_connection = 0;
#endif
	strcpy (func, "send2nsd");
#ifdef CSEC
	if (getenv("SECURE_CASTOR") != NULL) secure_connection++;
#endif
	if (socketp == NULL || *socketp < 0) {	/* connection not opened yet */
		sin.sin_family = AF_INET;
#ifdef CSEC
		if (secure_connection) {
		  if ((p = getenv ("SCNS_PORT")) || (p = getconfent ("SCNS", "PORT", 0))) {
		    sin.sin_port = htons ((unsigned short)atoi (p));
		  } else if (sp = Cgetservbyname ("scns", "tcp")) {
		    sin.sin_port = sp->s_port;
		    serrno = 0;
		  } else {
		    sin.sin_port = htons ((unsigned short)SCNS_PORT);
		    serrno = 0;
		  }
		} else {
#endif
		  if ((p = getenv ("CNS_PORT")) || (p = getconfent ("CNS", "PORT", 0))) {
		    sin.sin_port = htons ((unsigned short)atoi (p));
		  } else if (sp = Cgetservbyname ("cns", "tcp")) {
		    sin.sin_port = sp->s_port;
		    serrno = 0;
		  } else {
		    sin.sin_port = htons ((unsigned short)CNS_PORT);
		    serrno = 0;
		  }
#ifdef CSEC
		}
#endif
		if (host && *host)
			strcpy (Cnshost, host);
		else if ((p = getenv ("CNS_HOST")) || (p = getconfent ("CNS", "HOST", 0)))
			strcpy (Cnshost, p);
		else {
#if defined(CNS_HOST)
			strcpy (Cnshost, CNS_HOST);
#else
			gethostname (Cnshost, sizeof(Cnshost));
#endif
			serrno = 0;
		}
		if ((hp = Cgethostbyname (Cnshost)) == NULL) {
			Cns_errmsg (func, NS009, "Host unknown:", Cnshost);
			serrno = SENOSHOST;
			return (-1);
		}
		sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

		if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
			Cns_errmsg (func, NS002, "socket", neterror());
			serrno = SECOMERR;
			return (-1);
		}

		if (connect (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
#if defined(_WIN32)
			if (WSAGetLastError() == WSAECONNREFUSED) {
#else
			if (errno == ECONNREFUSED) {
#endif
				Cns_errmsg (func, NS000, Cnshost);
				(void) netclose (s);
				serrno = ENSNACT;
				return (-1);
			} else {
				Cns_errmsg (func, NS002, "connect", neterror());
				(void) netclose (s);
				serrno = SECOMERR;
				return (-1);
			}
		}

#ifdef CSEC
			if (secure_connection) {

			  if (Csec_client_initContext(&ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
			    Cns_errmsg (func, NS002, "send", "Could not init context");
			    (void) netclose (s);
			    serrno = ESEC_CTX_NOT_INITIALIZED;
			    return -1;
			  }
			
			  if(Csec_client_establishContext(&ctx, s)< 0) {
			    Cns_errmsg (func, "%s: %s\n",
					"send",
					"Could not establish context");
			    (void) netclose (s);
			    serrno = ESEC_NO_CONTEXT;
			    return -1;
			  }

			  Csec_clearContext(&ctx);
			}
#endif


		if (socketp)
			*socketp = s;
	} else
		s = *socketp;

	/* send request to name server */

	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
			Cns_errmsg (func, NS002, "send", sys_serrlist[SERRNO]);
		else
			Cns_errmsg (func, NS002, "send", neterror());
		(void) netclose (s);
		serrno = SECOMERR;
		return (-1);
	}

	/* get reply */

	while (1) {
		if ((n = netread (s, repbuf, 3 * LONGSIZE)) <= 0) {
			if (n == 0)
				Cns_errmsg (func, NS002, "recv", sys_serrlist[SERRNO]);
			else
				Cns_errmsg (func, NS002, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == CNS_IRC)
			return (0);
		if (rep_type == CNS_RC) {
			(void) netclose (s);
			if (c) {
				serrno = c;
				c = -1;
			}
			break;
		}
		if ((n = netread (s, repbuf, c)) <= 0) {
			if (n == 0)
				Cns_errmsg (func, NS002, "recv", sys_serrlist[SERRNO]);
			else
				Cns_errmsg (func, NS002, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		if (rep_type == MSG_ERR) {
			unmarshall_STRING (p, prtbuf);
			Cns_errmsg (NULL, "%s", prtbuf);
		} else if (user_repbuf) {
			if (actual_replen + c <= user_repbuf_len)
				n = c;
			else
				n = user_repbuf_len - actual_replen;
			if (n) {
				memcpy (user_repbuf + actual_replen, repbuf, n);
				actual_replen += n;
			}
		}
	}
	return (c);
}
