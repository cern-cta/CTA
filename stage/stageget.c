/*
 * $Id: stageget.c,v 1.2 1999/07/20 17:29:21 jdurand Exp $
 *
 * $Log: stageget.c,v $
 * Revision 1.2  1999/07/20 17:29:21  jdurand
 * Added Id and Log CVS's directives
 *
 */

/*
 * Copyright (C) 1996-1997 by CERN/CN/PDP/DS
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)stageget.c	1.4 08/19/97 CERN CN-PDP/DS Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "stage.h"
extern	char	*getenv();
extern	char	*getconfent();
extern	int	optind;
extern	char	*optarg;
static gid_t gid;
static struct passwd *pw;
char *stghost;

main(argc, argv)
int	argc;
char	**argv;
{
	int c, i;
	void cleanup();
	char *dp;
	int errflg = 0;
	int fun = 0;
	struct group *gr;
	int msglen;
	int nargs;
	int ntries = 0;
	char *p, *q;
	char path[MAXHOSTNAMELEN + MAXPATH];
	char *pool_user = NULL;
	char *sbp;
	char sendbuf[REQBUFSZ];
	int uflag = 0;
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
	while ((c = getopt (argc, argv, "h:Pp:U:u:")) != EOF) {
		switch (c) {
		case 'h':
			stghost = optarg;
			break;
		case 'U':
			fun = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				fprintf (stderr, STG06, "-U\n");
				errflg++;
			}
			break;
		case 'u':
			uflag++;
			break;
		case '?':
			errflg++;
			break;
		}
	}
	if (optind >= argc && fun == 0) {
		fprintf (stderr, STG07);
		errflg++;
	}
	if (errflg) {
		usage (argv[0]);
		exit (1);
	}

	/* default parameters will be added to the argv list */

	if (fun)
		nargs++;
	if (uflag == 0 &&
	   (pool_user = getenv ("STAGE_USER")) != NULL)
		nargs += 2;

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEGET);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
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

	marshall_WORD (sbp, nargs);
	for (i = 0; i < optind; i++)
		marshall_STRING (sbp, argv[i]);
	if (uflag == 0 && pool_user) {
		marshall_STRING (sbp, "-u");
		marshall_STRING (sbp, pool_user);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
		exit (SYERR);
	}
#endif
	if ((c = build_linkname (argv[i], path, sizeof(path), STAGEGET)) == SYERR) {
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (SYERR);
	} else if (c) {
		errflg++;
	} else {
		if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
			fprintf (stderr, STG38);
			errflg++;
		}
		marshall_STRING (sbp, path);
	}
	if (fun) {
		if ((c = build_Upath (fun, path, sizeof(path), STAGEGET)) == SYERR) {
#if defined(_WIN32)
			WSACleanup();
#endif
			exit (SYERR);
		} else if (c)
			errflg++;
		else if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
			fprintf (stderr, STG38);
			errflg++;
		} else
			marshall_STRING (sbp, path);
	}
	if (errflg) {
		usage (argv[0]);
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (1);
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

	while (c = send2stgd (stghost, sendbuf, msglen, 1)) {
		if (c == 0 || c == USERR) break;
		if (c == LNKNSUP) {	/* symbolic links not supported on that platform */
			c = 0;
			break;
		}
		if (c != ESTNACT && ntries++ > MAXRETRY) break;
		sleep (RETRYI);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (c);
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

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s",
	  "[-h stage_host] [-P] [-p pool] [-U fun] [-u user] pathname\n");
}
