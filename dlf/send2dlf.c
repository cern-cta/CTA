/*
 * 
 * Copyright (C) 2003 by CERN/IT/ADC
 * All rights reserved
 *
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: send2dlf.c,v $ $Revision: 1.1 $ $Date: 2003/08/20 13:09:18 $ CERN IT-ADC/CA Vitaly Motyakov";
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
#include "dlf.h"
#include "dlf_api.h"
#if defined(_WIN32)
extern char *ws_strerr;
#endif

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

		if (connect (s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
#if defined(_WIN32)
			if (WSAGetLastError() == WSAECONNREFUSED) {
#else
			if (errno == ECONNREFUSED) {
#endif
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
	if ((n = netwrite (s, reqp, reql)) <= 0) {
		if (n == 0)
		  dlf_errmsg (func, DLF02, "send", sys_serrlist[SERRNO]);
		else
		  dlf_errmsg (func, DLF02, "send", neterror());
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
	char prtbuf[PRTBUFSZ];

	strcpy (func, "getrep");

	/* get reply */
        s = socket;
		
	/* read header */
	if ((n = netread (s, repheader, 3 * LONGSIZE)) <= 0) {
	  if (n == 0)
	    dlf_errmsg (func, DLF02, "recv", sys_serrlist[SERRNO]);
	  else
	    dlf_errmsg (func, DLF02, "recv", neterror());
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

	if ((n = netread (s, *user_repbuf, c)) <= 0) {
	  if (n == 0)
	    dlf_errmsg (func, DLF02, "recv", sys_serrlist[SERRNO]);
	  else
	    dlf_errmsg (func, DLF02, "recv", neterror());
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
