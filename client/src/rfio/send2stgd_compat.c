/*
 * $Id: send2stgd_compat.c,v 1.4 2007/02/21 09:46:22 sponcec3 Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#if !defined(_WIN32)
#include <sys/wait.h>
#endif
#include <sys/stat.h>
#include "marshall.h"
#include "rfio_api.h"
#include "net.h"
#include "serrno.h"
#include "osdep.h"
#include "Cnetdb.h"
#include "stage_struct.h"
#include "stage_messages.h"
#include "socket_timeout.h"
#include "stage_api.h"

extern int dosymlink _PROTO((char *, char *));
extern void dounlink _PROTO((char *));
extern int rc_shift2castor _PROTO((int,int));
EXTERN_C int DLL_DECL netconnect_timeout _PROTO((int, struct sockaddr *, size_t, int));

int DLL_DECL send2stgd_compat(host, reqp, reql, want_reply, user_repbuf, user_repbuf_len)
		 char *host;
		 char *reqp;
		 int reql;
		 int want_reply;
		 char *user_repbuf;
		 int user_repbuf_len;
{
	int actual_replen = 0;
	int c;
	char file2[CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2];
	char func[16];
	char *getconfent();
	char *getenv();
	struct hostent *hp;
	int link_rc;
	int magic;
	int n;
	char *p;
	char prtbuf[PRTBUFSZ];
	int rep_type;
	char repbuf[REPBUFSZ];
	struct sockaddr_in sin; /* internet socket */
	struct servent *sp = NULL;
	int stg_s;
	char stghost[CA_MAXHOSTNAMELEN+1];
	char *stagehost = STAGEHOST;
	int stg_service = 0;
	int stage_timeout;
	int connect_timeout;
	int connect_rc;
	int save_serrno;
	int on = 1;

	strcpy (func, "send2stgd");
	link_rc = 0;
	if ((p = getenv ("STAGE_PORT")) == NULL &&
		(p = getconfent("STG", "PORT",0)) == NULL) {
		if ((sp = Cgetservbyname (STAGE_NAME, STAGE_PROTO)) == NULL) {
			if ((stg_service = STAGE_PORT) <= 0) {
				stage_errmsg (func, STG09, STAGE_NAME, "service from environment or configuration is <= 0");
				serrno = SENOSSERV;
				return (-1);
			}
		}
	} else {
		if ((stg_service = atoi(p)) <= 0) {
			stage_errmsg (func, STG09, STAGE_NAME, "service from environment or configuration is <= 0");
			serrno = SENOSSERV;
			return (-1);
		}
	}
	if (host == NULL) {
		if ((p = getenv ("STAGE_HOST")) == NULL &&
				(p = getconfent("STG", "HOST",0)) == NULL) {
			strcpy (stghost, stagehost);
		} else {
			strcpy (stghost, p);
		}
	} else {
		strcpy (stghost, host);
	}

	if ((p = getenv ("STAGE_TIMEOUT")) == NULL &&
		(p = getconfent("STG", "TIMEOUT",0)) == NULL) {
		stage_timeout = -1;
	} else {
		stage_timeout = atoi(p);
	}

	if ((p = getenv ("STAGE_CONNTIMEOUT")) == NULL &&
		(p = getconfent("STG", "CONNTIMEOUT",0)) == NULL) {
		connect_timeout = -1;
	} else {
		connect_timeout = atoi(p);
	}

	if ((hp = Cgethostbyname(stghost)) == NULL) {
		stage_errmsg (func, STG09, "Host unknown:", stghost);
		serrno = SENOSHOST;
		return (-1);
	}
	sin.sin_family = AF_INET;
	sin.sin_port = (stg_service > 0 ? htons((u_short) stg_service) : sp->s_port);
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

	if ((stg_s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		stage_errmsg (func, STG02, "", "socket", neterror());
		serrno = SECOMERR;
		return (-1);
	}

#if (defined(SOL_SOCKET) && defined(SO_KEEPALIVE))
	/* Set socket option */
	setsockopt(stg_s,SOL_SOCKET,SO_KEEPALIVE,(char *) &on,sizeof(on));
#endif

	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	serrno = 0;
	if (connect_timeout > 0) {
		connect_rc = netconnect_timeout (stg_s, (struct sockaddr *) &sin, sizeof(sin), connect_timeout);
	} else {
		connect_rc = connect (stg_s, (struct sockaddr *) &sin, sizeof(sin));
	}
	if (connect_rc < 0) {
		if (
#if defined(_WIN32)
				WSAGetLastError() == WSAECONNREFUSED
#else
				errno == ECONNREFUSED
#endif
				) {
			stage_errmsg (func, STG00, stghost, neterror());
			(void) netclose (stg_s);
			serrno = ESTNACT;
			return (-1);
		} else {
			stage_errmsg (func, STG02, stghost, "connect", neterror());
			(void) netclose (stg_s);
			serrno = SECOMERR;
			return (-1);
		}
	}
	if ((n = netwrite_timeout (stg_s, reqp, reql, STGTIMEOUT)) != reql) {
		save_serrno = serrno;
		if (n == 0)
			stage_errmsg (func, STG02, "", "send", sys_serrlist[SERRNO]);
		else
			stage_errmsg (func, STG02, "", "send", neterror());
		(void) netclose (stg_s);
		serrno = save_serrno;
		return (-1);
	}
	if (! want_reply) {
		(void) netclose (stg_s);
		return (0);
	}
	
	while (1) {
		if ((n = netread_timeout(stg_s, repbuf, 3 * LONGSIZE, stage_timeout)) != (3 * LONGSIZE)) {
			save_serrno = serrno;
			if (n == 0)
				stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
			else
				stage_errmsg (func, STG02, "", "recv", neterror());
			(void) netclose (stg_s);
			serrno = save_serrno;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == STAGERC) {
			(void) netclose (stg_s);
			if (c) {
				serrno = rc_shift2castor(magic,c);
				c = -1;
			}
			break;
		}
		if ((n = netread_timeout(stg_s, repbuf, c, stage_timeout)) != c) {
			save_serrno = serrno;
			if (n == 0)
				stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
			else
				stage_errmsg (func, STG02, "", "recv", neterror());
			(void) netclose (stg_s);
			serrno = save_serrno;
			return (-1);
		}
		p = repbuf;
		unmarshall_STRINGN (p, prtbuf, REPBUFSZ);
		switch (rep_type) {
		case MSG_OUT:
		case RTCOPY_OUT:
			if (user_repbuf != NULL) {
				if (actual_replen + c <= user_repbuf_len)
					n = c;
				else
					n = user_repbuf_len - actual_replen;
				if (n) {
					memcpy (user_repbuf + actual_replen, repbuf, n);
					actual_replen += n;
				}
			} else
				stage_outmsg (NULL, "%s", prtbuf);
			break;
		case MSG_ERR:
			stage_errmsg (NULL, "%s", prtbuf);
			break;
		case SYMLINK:
			unmarshall_STRINGN (p, file2, CA_MAXHOSTNAMELEN+CA_MAXPATHLEN+2);
			if ((c = dosymlink (prtbuf, file2)) != 0)
				link_rc = c;
			break;
		case RMSYMLINK:
			dounlink (prtbuf);
			break;
		default:
			break;
		}
	}
	return (c ? c : link_rc);
}
