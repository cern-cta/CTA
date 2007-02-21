/*
 * $Id: send2tpd.c,v 1.4 2007/02/21 16:31:31 wiebalck Exp $
 *
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: send2tpd.c,v $ $Revision: 1.4 $ $Date: 2007/02/21 16:31:31 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

#include <errno.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#include "Cnetdb.h"
#include <stddef.h>
#include "Ctape.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#ifdef CSEC
#include "Csec_api.h"
#endif
#include "Ctape_api.h"

/* send2tpd - send a request to the tape daemon and wait for the reply */

int send2tpd(host, reqp, reql, user_repbuf, user_repbuf_len)
char *host;
char *reqp;
int reql;
char *user_repbuf;
int user_repbuf_len;
{
	int actual_replen = 0;
	int c;
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
	char tapehost[CA_MAXHOSTNAMELEN+1];
#ifdef CSEC
	Csec_context_t ctx;
	int secure_connection = 0;
#endif

	strcpy (func, "send2tpd");
#ifdef CSEC
	if (getenv("SECURE_CASTOR") != NULL) secure_connection++;
#endif
	sin.sin_family = AF_INET;
#ifdef CSEC
	if (secure_connection) {
	  if ((p = getenv ("STAPE_PORT")) || (p = getconfent ("STAPE", "PORT", 0))) {
	    sin.sin_port = htons ((unsigned short)atoi (p));
	  } else if (sp = Cgetservbyname ("stape", "tcp")) {
	    sin.sin_port = sp->s_port;
	    serrno = 0;
	  } else {
	    sin.sin_port = htons ((unsigned short)STAPE_PORT);
	    serrno = 0;
	  }
	} else {
#endif
	  if ((p = getenv ("TAPE_PORT")) || (p = getconfent ("TAPE", "PORT", 0))) {
	    sin.sin_port = htons ((unsigned short)atoi (p));
	  } else if ((sp = Cgetservbyname ("tape", "tcp"))) {
	    sin.sin_port = sp->s_port;
	    serrno = 0;
	  } else {
	    sin.sin_port = htons ((unsigned short)TAPE_PORT);
	    serrno = 0;
	  }
#ifdef CSEC
	}
#endif
	if (host == NULL) {
		gethostname (tapehost, CA_MAXHOSTNAMELEN+1);
	} else {
		strcpy (tapehost, host);
	}
	if ((hp = Cgethostbyname (tapehost)) == NULL) {
		Ctape_errmsg (func, TP051, "Host unknown:", tapehost);
		serrno = SENOSHOST;
		return (-1);
	}
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

	if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		Ctape_errmsg (func, TP002, "socket", neterror());
		serrno = SECOMERR;
		return (-1);
	}

	if (connect (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
#if defined(_WIN32)
		if (WSAGetLastError() == WSAECONNREFUSED) {
#else
		if (errno == ECONNREFUSED) {
#endif
			Ctape_errmsg (func, TP000, tapehost);
			(void) netclose (s);
			serrno = ETDNP;
			return (-1);
		} else {
			Ctape_errmsg (func, TP002, "connect", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
	}
#ifdef CSEC

		if (secure_connection) {
		  
		  if (Csec_client_initContext(&ctx, CSEC_SERVICE_TYPE_TAPE, NULL) <0) {
			  Ctape_errmsg (func, TP002, "send", "Could not init context");
			  (void) netclose (s);
			  serrno = ESEC_CTX_NOT_INITIALIZED;
			  return -1;
			}
			
			if(Csec_client_establishContext(&ctx, s)< 0) {
			  Ctape_errmsg (func, "%s: %s\n",
				      "send",
				      "Could not establish context");
			  (void) netclose (s);
			  serrno = ESEC_NO_CONTEXT;
			  return -1;
			}

			Csec_clearContext(&ctx);
		}
#endif

	/* send request to tape daemon */

	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
			Ctape_errmsg (func, TP002, "send", sys_serrlist[SERRNO]);
		else
			Ctape_errmsg (func, TP002, "send", neterror());
		(void) netclose (s);
		serrno = SECOMERR;
		return (-1);
	}

	if (user_repbuf == NULL) {	/* does not want a reply */
		(void) netclose (s);
		return (0);
	}

	/* get reply */

	while (1) {
		if ((n = netread (s, repbuf, 3 * LONGSIZE)) <= 0) {
			if (n == 0)
				Ctape_errmsg (func, TP002, "recv", sys_serrlist[SERRNO]);
			else
				Ctape_errmsg (func, TP002, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == TAPERC) {
			(void) netclose (s);
			if (c) {
				serrno = c;
				c = -1;
			}
			break;
		}
		if ((n = netread (s, repbuf, c)) <= 0) {
			if (n == 0)
				Ctape_errmsg (func, TP002, "recv", sys_serrlist[SERRNO]);
			else
				Ctape_errmsg (func, TP002, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		if (rep_type == MSG_ERR) {
			unmarshall_STRING (p, prtbuf);
			Ctape_errmsg (NULL, "%s", prtbuf);
		} else {
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
