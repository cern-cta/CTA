/*
 * Copyright (C) 1996-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageget.c,v $ $Revision: 1.5 $ $Date: 1999/12/08 15:57:34 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#include <netinet/in.h>
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

	nargs = argc;
	uid = getuid();
	gid = getgid();
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

	pw = getpwuid (uid);
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
	if ((c = build_linkname (argv[i], path, sizeof(path), STAGEGET)) == SYERR)
		exit (SYERR);
	if (c) {
		errflg++;
	} else {
		if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
			fprintf (stderr, STG38);
			errflg++;
		}
		marshall_STRING (sbp, path);
	}
	if (fun) {
		if (build_Upath (fun, path, STAGEGET))
			exit (SYERR);
		if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
			fprintf (stderr, STG38);
			errflg++;
		} else
			marshall_STRING (sbp, path);
	}
	if (errflg) {
		usage (argv[0]);
		exit (1);
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	signal (SIGHUP, cleanup);
	signal (SIGINT, cleanup);
	signal (SIGQUIT, cleanup);
	signal (SIGTERM, cleanup);

	while (c = send2stgd (stghost, sendbuf, msglen, 1)) {
		if (c == 0 || c == USERR) break;
		if (c != ESTNACT && ntries++ > MAXRETRY) break;
		sleep (RETRYI);
	}
	exit (c);
}

void cleanup(sig)
int sig;
{
	signal (sig, SIG_IGN);

	exit (USERR);
}

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s",
	  "[-h stage_host] [-P] [-p pool] [-U fun] [-u user] pathname\n");
}
