/*
 * Copyright (C) 1999-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <errno.h>
#include <unistd.h>
#include <stdlib.h>
#include <sys/types.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include "Cnetdb_trunk_r21843.h"
#include "marshall_trunk_r21843.h"
#include "net_trunk_r21843.h"
#include "serrno_trunk_r21843.h"
#include "vmgr_trunk_r21843.h"
#include "vmgr_api_trunk_r21843.h"
#ifdef VMGRCSEC
#include "Csec_api_trunk_r21843.h"
#endif

/* send2vmgr - send a request to the volume manager and wait for the reply */

int send2vmgr(int *socketp,
              char *reqp,
              int reql,
              char *user_repbuf,
              int user_repbuf_len)
{
	int actual_replen = 0;
	int c;
	char vmgrhost[CA_MAXHOSTNAMELEN+1];
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
#ifdef VMGRCSEC
	Csec_context_t ctx;
	int secure_connection = 0;
#endif

	strncpy (func, "send2vmgr", 16);
#ifdef VMGRCSEC
	if (getenv("SECURE_CASTOR") != NULL) secure_connection++;
#endif
	if (socketp == NULL || *socketp < 0) {	/* connection not opened yet */
		sin.sin_family = AF_INET;
#ifdef VMGRCSEC
		if (secure_connection) {
		  if ((p = getenv ("SVMGR_PORT")) || (p = getconfent ("SVMGR", "PORT", 0))) {
		    sin.sin_port = htons ((unsigned short)atoi (p));
		  } else if (sp = Cgetservbyname ("svmgr", "tcp")) {
		    sin.sin_port = sp->s_port;
		    serrno = 0;
		  } else {
		    sin.sin_port = htons ((unsigned short)SVMGR_PORT);
		    serrno = 0;
		  }
		} else {
#endif
		  if ((p = getenv ("VMGR_PORT")) || (p = getconfent ("VMGR", "PORT", 0))) {
		    sin.sin_port = htons ((unsigned short)atoi (p));
		  } else if ((sp = Cgetservbyname ("vmgr", "tcp"))) {
		    sin.sin_port = sp->s_port;
		    serrno = 0;
		  } else {
		    sin.sin_port = htons ((unsigned short)VMGR_PORT);
		    serrno = 0;
		  }
#ifdef VMGRCSEC
		}
#endif
		if ((p = getenv ("VMGR_HOST")) || (p = getconfent ("VMGR", "HOST", 0)))
			strcpy (vmgrhost, p);
		else {
			vmgr_errmsg (func, VMG09, "VMGR HOST", "Not set");
			serrno = EVMGRNOHOST;
			return (-1);
		}
		if ((hp = Cgethostbyname (vmgrhost)) == NULL) {
			vmgr_errmsg (func, VMG09, "Host unknown:", vmgrhost);
			serrno = SENOSHOST;
			return (-1);
		}
		sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

		if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
			vmgr_errmsg (func, VMG02, "socket", neterror());
			serrno = SECOMERR;
			return (-1);
		}

		if (connect (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
			if (errno == ECONNREFUSED) {
				vmgr_errmsg (func, VMG00, vmgrhost);
				(void) netclose (s);
				serrno = EVMGRNACT;
				return (-1);
			} else {
				vmgr_errmsg (func, VMG02, "connect", neterror());
				(void) netclose (s);
				serrno = SECOMERR;
				return (-1);
			}
		}
#ifdef VMGRCSEC
			if (secure_connection) {

			  if (Csec_client_initContext(&ctx, CSEC_SERVICE_TYPE_CENTRAL, NULL) <0) {
			    vmgr_errmsg (func, VMG02, "send", "Could not init context");
			    serrno = ESEC_CTX_NOT_INITIALIZED;
			    (void) netclose (s);
			    return -1;
			}
			
			  if(Csec_client_establishContext(&ctx, s)< 0) {
			    vmgr_errmsg (func, "%s: %s\n",
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

	/* send request to vmgr server */

	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
			vmgr_errmsg (func, VMG02, "send", sys_serrlist[SERRNO]);
		else
			vmgr_errmsg (func, VMG02, "send", neterror());
		(void) netclose (s);
		serrno = SECOMERR;
		return (-1);
	}

	/* get reply */

	while (1) {
		if ((n = netread (s, repbuf, 3 * LONGSIZE)) <= 0) {
			if (n == 0)
				vmgr_errmsg (func, VMG02, "recv", sys_serrlist[SERRNO]);
			else
				vmgr_errmsg (func, VMG02, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == VMGR_IRC)
			return (0);
		if (rep_type == VMGR_RC) {
			(void) netclose (s);
			if (c) {
				serrno = c;
				c = -1;
			}
			break;
		}
		if ((n = netread (s, repbuf, c)) <= 0) {
			if (n == 0)
				vmgr_errmsg (func, VMG02, "recv", sys_serrlist[SERRNO]);
			else
				vmgr_errmsg (func, VMG02, "recv", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		if (rep_type == MSG_ERR) {
			unmarshall_STRING (p, prtbuf);
			vmgr_errmsg (NULL, "%s", prtbuf);
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
