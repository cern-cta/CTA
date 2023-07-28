/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2001-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
#include "rmc_api.h"
#include "rmc_constants.h"
#include "serrno.h"

#define PATH_CONF "/etc/cta/cta-rmcd.conf"
/* send2tpd - send a request to the SCSI media changer server and wait for the reply */

int send2rmc(
	const char *const host,
	const char *const reqp,
	const int reql,
	char *const user_repbuf,
	const int user_repbuf_len)
{
	int actual_replen = 0;
	int c;
	char func[16];
	char *getconfent();
	char *getconfent_fromfile();
	char *getenv();
	struct hostent *hp;
	int magic;
	int n;
	char *p;
	char prtbuf[RMC_PRTBUFSZ];
	int rep_type;
	char repbuf[RMC_REPBUFSZ];
	char rmchost[CA_MAXHOSTNAMELEN+1];
	int s;
	struct sockaddr_in sin; /* internet socket */

	strncpy (func, "send2rmc", 16);
	sin.sin_family = AF_INET;
	if ((p = getenv ("RMC_PORT")) || (p = getconfent_fromfile (PATH_CONF,"RMC", "PORT", 0))) {
		sin.sin_port = htons ((unsigned short)atoi (p));
	} else {
		sin.sin_port = htons ((unsigned short)RMC_PORT);
		serrno = 0;
	}
	if (host && *host) {
		strncpy(rmchost, host, CA_MAXHOSTNAMELEN+1);
	} else {
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
			(void) close (s);
			serrno = ERMCNACT;
			return (-1);
		} else {
			rmc_errmsg (func, RMC02, "connect", neterror());
			(void) close (s);
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
				rmc_errmsg (func, RMC02, "recv", sys_serrlist[SERRNO]);
			else
				rmc_errmsg (func, RMC02, "recv", neterror());
			(void) close (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == RMC_RC) {
			(void) close (s);
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
			(void) close (s);
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
