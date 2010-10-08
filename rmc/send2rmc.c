/*
 * Copyright (C) 2001-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include "Cnetdb.h"
#include "marshall.h"
#include "net.h"
#include "rmc.h"
#include "rmc_api.h"
#include "serrno.h"

/* send2tpd - send a request to the SCSI media changer server and wait for the reply */

int send2rmc(char *host,
             char *reqp,
             int reql,
             char *user_repbuf,
             int user_repbuf_len)
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
	char rmchost[CA_MAXHOSTNAMELEN+1];
	int s;
	struct sockaddr_in sin; /* internet socket */
	struct servent *sp;

	strncpy (func, "send2rmc", 16);
	sin.sin_family = AF_INET;
	if ((p = getenv ("RMC_PORT")) || (p = getconfent ("RMC", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else if ((sp = Cgetservbyname ("rmc", "tcp"))) {
		sin.sin_port = sp->s_port;
		serrno = 0;
	} else {
		sin.sin_port = htons ((unsigned short)RMC_PORT);
		serrno = 0;
	}
	if (host && *host)
		strcpy (rmchost, host);
	else if ((p = getenv ("RMC_HOST")) || (p = getconfent ("RMC", "HOST", 0)))
		strcpy (rmchost, p);
	else {
		gethostname (rmchost, CA_MAXHOSTNAMELEN+1);
		serrno = 0;
	}
	if ((hp = Cgethostbyname (rmchost)) == NULL) {
		rmc_errmsg (func, RMC09, "Host unknown:", rmchost);
		serrno = ERMCUNREC;
		return (-1);
	}
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

	if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		rmc_errmsg (func, RMC02, "socket", neterror());
		serrno = SECOMERR;
		return (-1);
	}

	if (connect (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
		if (errno == ECONNREFUSED) {
			rmc_errmsg (func, RMC00, rmchost);
			(void) netclose (s);
			serrno = ERMCNACT;
			return (-1);
		} else {
			rmc_errmsg (func, RMC02, "connect", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
	}

	/* send request to the SCSI media changer server */

	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
			rmc_errmsg (func, RMC02, "send", sys_serrlist[SERRNO]);
		else
			rmc_errmsg (func, RMC02, "send", neterror());
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
				rmc_errmsg (func, RMC02, "recv", sys_serrlist[SERRNO]);
			else
				rmc_errmsg (func, RMC02, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == RMC_RC) {
			(void) netclose (s);
			if (c) {
				serrno = c;
				c = -1;
			}
			break;
		}
		if ((n = netread (s, repbuf, c)) <= 0) {
			if (n == 0)
				rmc_errmsg (func, RMC02, "recv", sys_serrlist[SERRNO]);
			else
				rmc_errmsg (func, RMC02, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		if (rep_type == MSG_ERR) {
			unmarshall_STRING (p, prtbuf);
			rmc_errmsg (NULL, "%s", prtbuf);
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
