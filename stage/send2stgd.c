/*
 * $Id: send2stgd.c,v 1.22 2000/09/13 17:21:49 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: send2stgd.c,v $ $Revision: 1.22 $ $Date: 2000/09/13 17:21:49 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#endif
#ifndef _WIN32
#include <signal.h>
#include <sys/wait.h>
#endif
#include <sys/stat.h>
#include "marshall.h"
#include "rfio_api.h"
#include "net.h"
#include "serrno.h"
#include "osdep.h"
#include "stage.h"
#include "Cnetdb.h"

int nb_ovl;
#ifndef _WIN32
struct sigaction sa;
#endif

int dosymlink _PROTO((char *, char *));
void dounlink _PROTO((char *));
#ifndef _WIN32
void wait4child _PROTO(());
#endif
int rc_shift2castor _PROTO((int));

int rc_shift2castor(rc)
		 int rc;
{
	/* Input  is a SHIFT  return code */
	/* Output is a CASTOR return code */
	switch (rc) {
	case BLKSKPD:
		return(ERTBLKSKPD);
	case TPE_LSZ:
		return(ERTTPE_LSZ);
	case MNYPARI:
		return(ERTMNYPARY);
	case LIMBYSZ:
		return(ERTLIMBYSZ);
	case USERR:
		return(EINVAL);
	case SYERR:
		return(SESYSERR);
	case SHIFT_ESTNACT:
		return(ESTNACT);
	default:
		return(rc);
	}
}

int DLL_DECL send2stgd(host, reqp, reql, want_reply, user_repbuf, user_repbuf_len)
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

	strcpy (func, "send2stgd");
#ifndef _WIN32
	sa.sa_handler = wait4child;
	sa.sa_flags = SA_RESTART;
	sigaction (SIGCHLD, &sa, NULL);
#endif
	link_rc = 0;
	if ((p = getenv ("STAGE_PORT")) == NULL &&
		(p = getconfent("STG", "PORT",0)) == NULL) {
		if ((sp = Cgetservbyname (STG, "tcp")) == NULL) {
			stage_errmsg (func, STG09, STG, "not defined in /etc/services");
			serrno = SENOSSERV;
			return (-1);
		}
	} else {
		if ((stg_service = atoi(p)) <= 0) {
			stage_errmsg (func, STG09, STG, "service from environment or configuration is <= 0");
			serrno = SENOSSERV;
			return (-1);
		}
	}
	nb_ovl = 0;
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
	if ((hp = gethostbyname (stghost)) == NULL) {
		stage_errmsg (func, STG09, "Host unknown:", stghost);
		serrno = SENOSHOST;
		return (-1);
	}
	sin.sin_family = AF_INET;
	sin.sin_port = (stg_service > 0 ? stg_service : sp->s_port);
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

	if ((stg_s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		stage_errmsg (func, STG02, "", "socket", neterror());
		serrno = SECOMERR;
		return (-1);
	}

	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	if (connect (stg_s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
		if (
#if defined(_WIN32)
				WSAGetLastError() == WSAECONNREFUSED
#else
				errno == ECONNREFUSED
#endif
				) {
			stage_errmsg (func, STG00, stghost);
			(void) netclose (stg_s);
			serrno = ESTNACT;
			return (-1);
		} else {
			stage_errmsg (func, STG02, "", "connect", neterror());
			(void) netclose (stg_s);
			serrno = SECOMERR;
			return (-1);
		}
	}
	if ((n = netwrite_timeout (stg_s, reqp, reql, STGTIMEOUT)) != reql) {
		if (n == 0)
			stage_errmsg (func, STG02, "", "send", sys_serrlist[SERRNO]);
		else
			stage_errmsg (func, STG02, "", "send", neterror());
		(void) netclose (stg_s);
		serrno = SECOMERR;
		return (-1);
	}
	if (! want_reply) {
		(void) netclose (stg_s);
		return (0);
	}
	
	while (1) {
		if ((n = netread(stg_s, repbuf, 3 * LONGSIZE)) != (3 * LONGSIZE)) {
			if (n == 0)
				stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
			else
				stage_errmsg (func, STG02, "", "recv", neterror());
			(void) netclose (stg_s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == STAGERC) {
			(void) netclose (stg_s);
			if (c) {
				serrno = rc_shift2castor(c);
				c = -1;
			}
			break;
		}
		if ((n = netread(stg_s, repbuf, c)) != c) {
			if (n == 0)
				stage_errmsg (func, STG02, "", "recv", sys_serrlist[SERRNO]);
			else
				stage_errmsg (func, STG02, "", "recv", neterror());
			(void) netclose (stg_s);
			serrno = SECOMERR;
			return (-1);
		}
		p = repbuf;
		unmarshall_STRING (p, prtbuf);
		switch (rep_type) {
		case MSG_OUT:
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
		case RTCOPY_OUT:
			stage_errmsg (NULL, "%s", prtbuf);
			break;
		case SYMLINK:
			unmarshall_STRING (p, file2);
			if (c = dosymlink (prtbuf, file2))
				link_rc = c;
			break;
		case RMSYMLINK:
			dounlink (prtbuf);
		}
	}
#if !defined(_WIN32)
	while (nb_ovl > 0) sleep (1);
#endif
	return (c ? c : link_rc);
}

int dosymlink (file1, file2)
		 char *file1;
		 char *file2;
{
	char *filename;
	char func[16];
	char *host;
	int remote;
	
	strcpy (func, "send2stgd");
	remote = rfio_parseln (file2, &host, &filename, NORDLINKS);
	serrno = 0;
	if (rfio_symlink (file1, file2) &&
			((!remote && errno != EEXIST) || (remote && rfio_errno != EEXIST))) {
		stage_errmsg (func, STG02, file1, "symlink", rfio_serror());
		if (serrno == SEOPNOTSUP) {
			serrno = ESTLNKNSUP;
			return (-1);
		}
		if ((remote &&
				 (rfio_errno == EACCES || rfio_errno == ENOENT)) ||
				(remote == 0 && (errno == EACCES || errno == ENOENT))) {
			serrno = ESTLNKNCR;
			return (-1);
		} else {
			serrno = SESYSERR;
			return (-1);
		}
	}
	return (0);
}

void
dounlink (path)
		 char *path;
{
	char *filename;
	char func[16];
	char *host;
#if !defined(_WIN32)
	int pid;
	struct stat st;
#endif
	int remote;
	
	strcpy (func, "send2stgd");
	remote = rfio_parseln (path, &host, &filename, NORDLINKS);
	if (rfio_unlink (path)) {
		if ((remote && rfio_errno == ENOENT) ||
				(remote == 0 && errno == ENOENT)) return;
#if !defined(_WIN32)
		if (getuid() || (remote && rfio_errno != EACCES) ||
				(remote == 0 && errno != EACCES) ||
				strncmp (filename, "/afs/", 5) == 0) {
#endif
			stage_errmsg (func, STG02, path, "unlink", rfio_serror());
			return;
		}
#if !defined(_WIN32)
		if (rfio_lstat (path, &st) != 0) {
			stage_errmsg (func, STG02, path, "unlink(lstat)", rfio_serror());
			return;
		}
		pid = fork ();
		if (pid < 0) {
			stage_errmsg (func, STG02, path, "unlink(fork)", rfio_serror());
			return;
		} else if (pid == 0) {
			setgid (st.st_gid);
			setuid (st.st_uid);
			if (rfio_unlink (path)) {
				stage_errmsg (func, STG02, path, "unlink", rfio_serror());
				exit (SYERR);
			}
			exit (0);
		}
		nb_ovl++;
	}
#endif
}

#if !defined(_WIN32)
void wait4child()
{
	int pid;
	int status;
	
	while ((pid = waitpid (-1, &status, WNOHANG)) > 0)
		nb_ovl--;
}
#endif

