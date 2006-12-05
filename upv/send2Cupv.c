/*
 * $Id: send2Cupv.c,v 1.5 2006/12/05 14:00:43 riojac3 Exp $
 *
 * Copyright (C) 1999-2002 by CERN/IT-DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: send2Cupv.c,v $ $Revision: 1.5 $ $Date: 2006/12/05 14:00:43 $ CERN IT-DS/HSM Ben Couturier";
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
#include "Cupv.h"
#include "Cupv_util.h"
#ifdef CSEC
#include "Csec_api.h"
#endif
#if defined(_WIN32)
extern char *ws_strerr;
#endif

/* send2Cupv - send a request to the CUPV daemon and wait for the reply */

int send2Cupv(int *socketp,char *reqp,int reql,char *user_repbuf,int user_repbuf_len)
{
#ifndef USE_CUPV
	serrno = EPERM;
	return(-1);
#else
	int actual_replen = 0;
	int c;
	char Cupvhost[CA_MAXHOSTNAMELEN+1];
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

	strcpy (func, "send2Cupv");
#ifdef CSEC
	if (getenv("SECURE_CASTOR") != NULL) secure_connection++;
#endif
	if (socketp == NULL || *socketp < 0) {	/* connection not opened yet */
		sin.sin_family = AF_INET;
#ifdef CSEC
		if (secure_connection) {
		  if ((p = getenv ("SUPV_PORT")) || (p = getconfent ("SUPV", "PORT", 0))) {
		    sin.sin_port = htons ((unsigned short)atoi (p));
		  } else if (sp = Cgetservbyname ("sCupv", "tcp")) {
		    sin.sin_port = sp->s_port;
		  } else {
		    sin.sin_port = htons ((unsigned short)SCUPV_PORT);
		  }
		} else {
#endif
		  if ((p = getenv ("UPV_PORT")) || (p = getconfent ("UPV", "PORT", 0))) {
		    sin.sin_port = htons ((unsigned short)atoi (p));
		  } else if (sp = Cgetservbyname ("Cupv", "tcp")) {
		    sin.sin_port = sp->s_port;
		  } else {
		    sin.sin_port = htons ((unsigned short)CUPV_PORT);
		  }
#ifdef CSEC
		}
#endif
		if ((p = getenv ("UPV_HOST")) || (p = getconfent ("UPV", "HOST", 0)))
			strcpy (Cupvhost, p);
		else
#if defined(CUPV_HOST)
			strcpy (Cupvhost, CUPV_HOST);
#else
			gethostname (Cupvhost, sizeof(Cupvhost));
#endif
		if ((hp = Cgethostbyname (Cupvhost)) == NULL) {
			Cupv_errmsg (func, CUP09, "Host unknown:", Cupvhost);
			serrno = SENOSHOST;
			return (-1);
		}
		sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

		if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
			Cupv_errmsg (func, CUP02, "socket", neterror());
			serrno = SECOMERR;
			return (-1);
		}

		if (connect (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
#if defined(_WIN32)
			if (WSAGetLastError() == WSAECONNREFUSED) {
#else
			if (errno == ECONNREFUSED) {
#endif
			         Cupv_errmsg (func, CUP00, Cupvhost);
				(void) netclose (s);
				serrno = ECUPVNACT;
				return (-1);
			} else {
				Cupv_errmsg (func, CUP02, "connect", neterror());
				(void) netclose (s);
				serrno = SECOMERR;
				return (-1);
			}
		}
#ifdef CSEC
			if (secure_connection) {
			  if (Csec_client_initContext(&ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
			    Cupv_errmsg (func, CUP02, "send", "Could not init context");
			    (void) netclose (s);
			    serrno = ESEC_CTX_NOT_INITIALIZED;
			    return -1;
			  }
			
			  if(Csec_client_establishContext(&ctx, s)< 0) {
			    Cupv_errmsg (func, "%s: %s\n",
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

	/* send request to Cupv server */

	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
			Cupv_errmsg (func, CUP02, "send", sys_serrlist[SERRNO]);
		else
			Cupv_errmsg (func, CUP02, "send", neterror());
		(void) netclose (s);
		serrno = SECOMERR;
		return (-1);
	}

	/* get reply */

	while (1) {
		if ((n = netread (s, repbuf, 3 * LONGSIZE)) <= 0) {
			if (n == 0)
				Cupv_errmsg (func, CUP02, "recv", sys_serrlist[SERRNO]);
			else
				Cupv_errmsg (func, CUP02, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == CUPV_IRC)
			return (0);
		if (rep_type == CUPV_RC) {
			(void) netclose (s);
			if (c) {
				serrno = c;
				c = -1;
			}
			break;
		}
		if ((n = netread (s, repbuf, c)) <= 0) {
			if (n == 0)
				Cupv_errmsg (func, CUP02, "recv", sys_serrlist[SERRNO]);
			else
				Cupv_errmsg (func, CUP02, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		if (rep_type == MSG_ERR) {
			unmarshall_STRING (p, prtbuf);
			Cupv_errmsg (NULL, "%s", prtbuf);
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
#endif
}









