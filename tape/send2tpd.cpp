/*
 * $Id: send2tpd.c,v 1.4 2007/02/21 16:31:31 wiebalck Exp $
 *
 * Copyright (C) 1993-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include "castor/tape/tapeserver/daemon/Constants.hpp"
#include "h/Cnetdb.h"
#include "h/Ctape.h"
#include "h/Ctape_api.h"
#include "h/getconfent.h"
#include "h/marshall.h"
#include "h/net.h"
#include "h/serrno.h"

#include <errno.h>
#include <sys/types.h>
#include <stdlib.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <stddef.h>

/* send2tpd - send a request to the tape daemon and wait for the reply */

int send2tpd(char *host,
             char *reqp,
             int reql,
             char *user_repbuf,
             int user_repbuf_len)
{
	int actual_replen = 0;
	int c;
	char func[16];
	struct hostent *hp;
	int magic;
	int n;
	char *p;
	char prtbuf[PRTBUFSZ];
	int rep_type;
	char repbuf[REPBUFSZ];
	int s;
	struct sockaddr_in sin; /* internet socket */
	char tapehost[CA_MAXHOSTNAMELEN+1];

	strncpy (func, "send2tpd", 16);
	sin.sin_family = AF_INET;
	if ((p = getconfent ("TapeServer", "AdminPort", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else {
                using namespace castor::tape::tapeserver::daemon;
		sin.sin_port = htons (TAPESERVER_ADMIN_PORT);
		serrno = 0;
	}
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
		if (errno == ECONNREFUSED) {
			Ctape_errmsg (func, TP000, tapehost);
			(void) close (s);
			serrno = ETDNP;
			return (-1);
		} else {
			Ctape_errmsg (func, TP002, "connect", neterror());
			(void) close (s);
			serrno = SECOMERR;
			return (-1);
		}
	}

	/* send request to tape daemon */

	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
			Ctape_errmsg (func, TP002, "send", sys_serrlist[SERRNO]);
		else
			Ctape_errmsg (func, TP002, "send", neterror());
		(void) close (s);
		serrno = SECOMERR;
		return (-1);
	}

	if (user_repbuf == NULL) {	/* does not want a reply */
		(void) close (s);
		return (0);
	}

	/* get reply */

	while (1) {
		if ((n = netread (s, repbuf, 3 * LONGSIZE)) <= 0) {
			if (n == 0)
				Ctape_errmsg (func, TP002, "recv", sys_serrlist[SERRNO]);
			else
				Ctape_errmsg (func, TP002, "recv", neterror());
			(void) close (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == TAPERC) {
			(void) close (s);
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
			(void) close (s);
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
