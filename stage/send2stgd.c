/*
 * $Id: send2stgd.c,v 1.14 2000/03/08 17:35:35 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: send2stgd.c,v $ $Revision: 1.14 $ $Date: 2000/03/08 17:35:35 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
#if !defined(vms) && !defined(_WIN32)
#include <sys/stat.h>
#include <signal.h>
#include <sys/wait.h>
#endif
#include "marshall.h"
#include "rfio.h"
#include "net.h"
#include "serrno.h"
#include "stage.h"
#include "osdep.h"
extern int rfio_errno;
int nb_ovl;
#ifndef _WIN32
struct sigaction sa;
#endif

int dosymlink _PROTO((char *, char *));
void dounlink _PROTO((char *));
#ifndef _WIN32
void wait4child _PROTO(());
#endif

send2stgd(host, reqp, reql, want_reply)
char *host;
char *reqp;
int reql;
int want_reply;
{
	int c;
	char file2[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
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
	struct servent *sp;
	int stg_s;
	char stghost[64];

#ifndef _WIN32
  sa.sa_handler = wait4child;
  sa.sa_flags = SA_RESTART;
  sigaction (SIGCHLD, &sa, NULL);
#endif
	link_rc = 0;
	nb_ovl = 0;
	if ((sp = getservbyname (STG, "tcp")) == NULL) {
		fprintf (stderr, STG09, STG, "not defined in /etc/services");
		return (CONFERR);
	}
	if (host == NULL) {
		if ((p = getenv ("STAGE_HOST")) == NULL &&
		    (p = getconfent("STG", "HOST",0)) == NULL) {
			fprintf (stderr, STG31);
			return (CONFERR);
		}
		strcpy (stghost, p);
	} else {
		strcpy (stghost, host);
	}
	if ((hp = gethostbyname (stghost)) == NULL) {
		fprintf (stderr, STG09, "Host unknown:", stghost);
		return (CONFERR);
	}
	sin.sin_family = AF_INET;
	sin.sin_port = sp->s_port;
	sin.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

	if ((stg_s = socket (AF_INET, SOCK_STREAM, 0)) < 0) {
		fprintf (stderr, STG02, "", "socket", neterror());
		return (SYERR);
	}

	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	if (connect (stg_s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
#if defined(_WIN32)
		if (WSAGetLastError() == WSAECONNREFUSED) {
#else
		if (errno == ECONNREFUSED) {
#endif
			fprintf (stderr, STG00, stghost);
			(void) netclose (stg_s);
			return (ESTNACT);
		} else {
			fprintf (stderr, STG02, "", "connect", neterror());
			(void) netclose (stg_s);
			return (SYERR);
		}
	}
	if ((n = netwrite_timeout (stg_s, reqp, reql, STGTIMEOUT)) != reql) {
		if (n == 0)
			fprintf (stderr, STG02, "", "send", sys_serrlist[SERRNO]);
		else
			fprintf (stderr, STG02, "", "send", neterror());
		(void) netclose (stg_s);
		return (SYERR);
	}
	if (! want_reply) {
		(void) netclose (stg_s);
		return (0);
	}

	while (1) {
		if ((n = netread(stg_s, repbuf, 3 * LONGSIZE)) != (3 * LONGSIZE)) {
			if (n == 0)
				fprintf (stderr, STG02, "", "recv", sys_serrlist[SERRNO]);
			else
				fprintf (stderr, STG02, "", "recv", neterror());
			(void) netclose (stg_s);
			return (SYERR);
		}
		p = repbuf;
		unmarshall_LONG (p, magic) ;
		unmarshall_LONG (p, rep_type) ;
		unmarshall_LONG (p, c) ;
		if (rep_type == STAGERC) {
			(void) netclose (stg_s);
			break;
		}
		if ((n = netread(stg_s, repbuf, c)) != c) {
			if (n == 0)
				fprintf (stderr, STG02, "", "recv", sys_serrlist[SERRNO]);
			else
				fprintf (stderr, STG02, "", "recv", neterror());
			(void) netclose (stg_s);
			c = SYERR;
			break;
		}
		p = repbuf;
		unmarshall_STRING (p, prtbuf);
		switch (rep_type) {
		case MSG_OUT:
			printf ("%s", prtbuf);
			break;
		case MSG_ERR:
		case RTCOPY_OUT:
			fprintf (stderr, "%s", prtbuf);
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
	char *host;
	int remote;

	remote = rfio_parseln (file2, &host, &filename, NORDLINKS);
	serrno = 0;
	if (rfio_symlink (file1, file2) &&
	    ((!remote && errno != EEXIST) || (remote && rfio_errno != EEXIST))) {
		rfio_perror ("symlink");
		if (serrno == SEOPNOTSUP) return (LNKNSUP);
		if ((remote &&
		    (rfio_errno == EACCES || rfio_errno == ENOENT)) ||
		    (remote == 0 && (errno == EACCES || errno == ENOENT)))
			return (USERR);
		else
			return (SYERR);
	}
	return (0);
}

void dounlink (path)
char *path;
{
	char *filename;
	char *host;
#if !defined(_WIN32)
	int pid;
	struct stat st;
#endif
	int remote;

	remote = rfio_parseln (path, &host, &filename, NORDLINKS);
	if (rfio_unlink (path)) {
		if ((remote && rfio_errno == ENOENT) ||
		    (remote == 0 && errno == ENOENT)) return;
#if !defined(_WIN32)
		if (getuid() || (remote && rfio_errno != EACCES) ||
		    (remote == 0 && errno != EACCES) ||
		    strncmp (filename, "/afs/", 5) == 0) {
#endif
			fprintf (stderr, STG02, path, "unlink", rfio_serror());
			return;
		}
#if !defined(_WIN32)
		if (rfio_lstat (path, &st) != 0) {
			fprintf (stderr, STG02, path, "unlink(lstat)", rfio_serror());
			return;
		}
		pid = fork ();
		if (pid < 0) {
			fprintf (stderr, STG02, path, "unlink(fork)", rfio_serror());
			return;
		} else if (pid == 0) {
			setgid (st.st_gid);
			setuid (st.st_uid);
			if (rfio_unlink (path)) {
				fprintf (stderr, STG02, path, "unlink", rfio_serror());
				exit (SYERR);
			}
			exit (0);
		}
		nb_ovl++;
	}
}
#endif

#if !defined(_WIN32)
void wait4child()
{
  int pid;
  int status;
  
  while ((pid = waitpid (-1, &status, WNOHANG)) > 0)
    nb_ovl--;
}
#endif

