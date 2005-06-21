/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: send2dlf.c,v $ $Revision: 1.6 $ $Date: 2005/06/21 11:21:41 $ CERN IT-ADC/CA Vitaly Motyakov";
#endif /* not lint */

#include <errno.h>
#include <stdlib.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#endif
#include "Cnetdb.h"
#include "marshall.h"
#include "net.h"
#include "serrno.h"
#include "dlf.h"
#include "dlf_api.h"
#include "socket_timeout.h"

#if defined(_WIN32)
extern char *ws_strerr;
#endif

EXTERN_C int DLL_DECL netconnect_timeout _PROTO((SOCKET, struct sockaddr *, size_t, int));

/* send2dlf - send a request to the distributed logging facility server */

send2dlf(socketp, dst, reqp, reql)
int *socketp;
dlf_log_dst_t* dst;
char *reqp;
int reql;

{
	int c;
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
	int on = 1;

	strcpy (func, "send2dlf");
	if (socketp == NULL || *socketp < 0) {	/* connection not opened yet */
       		sin.sin_family = AF_INET;
		sin.sin_port = htons ((unsigned short)dst->port);
		if ((hp = Cgethostbyname (dst->name)) == NULL) 
		{
		        dlf_errmsg (func, DLF09, "Host unknown:", dst->name);
			serrno = SENOSHOST;
			return (-1);
		}
		sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

		if ((s = socket (AF_INET, SOCK_STREAM, 0)) < 0) 
                {
		        dlf_errmsg (func, DLF02, "socket", neterror());
			serrno = SECOMERR;
			return (-1);
		}

#if (defined(SOL_SOCKET) && defined(SO_KEEPALIVE))
		if (setsockopt (s, SOL_SOCKET, SO_KEEPALIVE, (char *)&on, sizeof(on)) < 0) {
		  dlf_errmsg (func, DLF02, "setsockopt", strerror(errno));
		}
#endif
#ifndef __osf__
		/* It appears that on osf TCP_NODELAY was not working well - to check */
#if (defined(IPPROTO_TCP) && defined(TCP_NODELAY))
		if (setsockopt (s, IPPROTO_TCP,TCP_NODELAY, (char *)&on, sizeof(on)) < 0) {
		  dlf_errmsg (func, DLF02, "setsockopt", strerror(errno));
		}
#endif
#endif
		if (netconnect_timeout (s, (struct sockaddr *) &sin, sizeof(sin), DLF_TIMEOUT) < 0) {
		  if (
#if defined(_WIN32)
		  WSAGetLastError() == WSAECONNREFUSED
#else
		  errno == ECONNREFUSED
#endif
		  ) {
			dlf_errmsg (func, DLF00, dst->name);
			(void) netclose (s);
			serrno = EDLFNACT;
			return (-1);
		      } else {
			dlf_errmsg (func, DLF02, "connect", neterror());
			(void) netclose (s);
			serrno = SECOMERR;
			return (-1);
		      }
		}
		if (socketp)
			*socketp = s;
	} else
		s = *socketp;

	/* send request to the dlf server */
	if ((n = netwrite_timeout (s, reqp, reql, DLF_TIMEOUT)) <= 0) {
		if (n == 0)
		  dlf_errmsg (func, DLF02, "netwrite_timeout", sys_serrlist[SERRNO]);
		else
		  dlf_errmsg (func, DLF02, "netwrite_timeout", neterror());
		(void) netclose (s);
		serrno = SECOMERR;
		return (-1);
	}
	return (0);
}

/* get reply from the dlf server */

getrep (socket, user_repbuf, user_repbuf_len, reply_type)
int socket;
char** user_repbuf;
int* user_repbuf_len;
int* reply_type;
{
        int s;
        char repheader[3 * LONGSIZE];
        int c;
        int magic;
        int rep_type;
        int n;
        char* p;
	char func[15];
	char prtbuf[DLF_PRTBUFSZ];

	strcpy (func, "getrep");

	/* get reply */
        s = socket;
		
	/* read header */
	if ((n = netread_timeout (s, repheader, 3 * LONGSIZE, DLF_TIMEOUT)) <= 0) {
	  if (n == 0)
	    dlf_errmsg (func, DLF02, "netread_timeout", sys_serrlist[SERRNO]);
	  else
	    dlf_errmsg (func, DLF02, "netread_timeout", neterror());
	  (void) netclose (s);
	  serrno = SECOMERR;
	  return (-1);
	}
	p = repheader;
	unmarshall_LONG (p, magic) ;
	unmarshall_LONG (p, rep_type) ;
	unmarshall_LONG (p, c) ;
	*reply_type = rep_type;
	if (rep_type == DLF_IRC)
	  return (0);

	if (rep_type == DLF_RC) {
 	  (void) netclose (s);
	  if (c) { 
	    serrno = c;
	    return (-1);
	  }
	  else
	    return (0);
	}

	/* Allocate memory for the user buffer */
	/* This memory must be freed by the caller */

	*user_repbuf = (char*)malloc(c);
	if (*user_repbuf == NULL) {
	  (void) netclose(s);
	  serrno = ENOMEM;
	  return (-1);
	}
	*user_repbuf_len = c;

	if ((n = netread_timeout (s, *user_repbuf, c, DLF_TIMEOUT)) <= 0) {
	  if (n == 0)
	    dlf_errmsg (func, DLF02, "netread_timeout", sys_serrlist[SERRNO]);
	  else
	    dlf_errmsg (func, DLF02, "netread_timeout", neterror());
	  (void) netclose (s);
	  free (*user_repbuf); /* Free memory */
	  serrno = SECOMERR;
	  return (-1);
	}
	p = *user_repbuf;
	if (rep_type == MSG_ERR) {
	  unmarshall_STRING (p, prtbuf);
	  dlf_errmsg (NULL, "%s", prtbuf);
	} 
	return (0);
}
