/*
 * $Id: stagechng.c,v 1.7 2001/03/02 18:12:23 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stagechng.c,v $ $Revision: 1.7 $ $Date: 2001/03/02 18:12:23 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <stdlib.h>
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
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "stage.h"
#include "stage_api.h"
#include "Cpwd.h"
#include "Cgetopt.h"

#if !defined(linux)
extern	char	*sys_errlist[];
#endif

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
void usage _PROTO((char *));
void cleanup _PROTO((int));

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
	void cleanup();
	int errflg = 0;
	gid_t gid;
	int pid;
	int msglen;
	int nargs;
	int ntries = 0;
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
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt (argc, argv, "h:")) != -1) {
		switch (c) {
		case 'h':
			stghost = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (Coptind >= argc) {
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

	if ((pw = Cgetpwuid (uid)) == NULL) {
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
	for (i = 0; i < Coptind; i++)
		marshall_STRING (sbp, argv[i]);
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
		exit (SYERR);
	}
#endif
	for (i = Coptind; i < argc; i++) {
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
		c = send2stgd_cmd (stghost, sendbuf, msglen, 1, NULL, 0);
		if (c == 0 || serrno == EINVAL || serrno == ENOSPC) break;
		if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
		sleep (RETRYI);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (c == 0 ? 0 : rc_castor2shift(serrno));
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
