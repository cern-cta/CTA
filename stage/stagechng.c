/*
 * $Id: stagechng.c,v 1.1 2000/06/16 14:34:22 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stagechng.c,v $ $Revision: 1.1 $ $Date: 2000/06/16 14:34:22 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "stage.h"
extern	int	optind;
extern	char	*optarg;
#if !defined(linux)
extern	char	*sys_errlist[];
#endif

void usage _PROTO((char *));
void cleanup _PROTO((int));

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
	void cleanup();
	char *dp;
	int errflg = 0;
	int fun = 0;
	gid_t gid;
	int pid;
	int key;
	int msglen;
	int nargs;
	int ntries = 0;
	char *p;
	char path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	struct passwd *pw;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	nargs = argc;
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	while ((c = getopt (argc, argv, "p:")) != EOF) {
		switch (c) {
		case 'h':
			stghost = optarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (optind >= argc) {
		fprintf (stderr, STG07);
		errflg++;
	}

	if (errflg) {
		usage (argv[0]);
		exit (USERR);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEFILCHG);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 4 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = getpwuid (uid)) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", uid);
		fprintf (stderr, STG11, uidstr);
		exit (SYERR);
	}
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	marshall_WORD (sbp, uid);
	marshall_WORD (sbp, gid);
	pid = getpid();
	marshall_WORD (sbp, pid);

	marshall_WORD (sbp, nargs);
	for (i = 0; i < optind; i++)
		marshall_STRING (sbp, argv[i]);
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
		exit (SYERR);
	}
#endif
	for (i = optind; i < argc; i++) {
		if ((c = build_linkname (argv[i], path, sizeof(path), STAGEUPDC)) == SYERR) {
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (SYERR);
		} else if (c) {
			errflg++;
			continue;
		} else {
			if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
				fprintf (stderr, STG38);
				errflg++;
				break;
			}
			marshall_STRING (sbp, path);
		}
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

#if ! defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if ! defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

	while (1) {
		c = send2stgd (stghost, sendbuf, msglen, 1, NULL, 0);
		if (c == 0 || serrno == EINVAL || serrno == ENOSPC) break;
		if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
		sleep (RETRYI);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (c == 0 ? 0 : serrno);
}

void cleanup(sig)
		 int sig;
{
	signal (sig, SIG_IGN);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (USERR);
}

void usage(cmd)
		 char *cmd;
{
	fprintf (stderr, "usage: %s [-h stagehost] pathname(s)", cmd);
}
