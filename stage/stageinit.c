/*
 * $Id: stageinit.c,v 1.7 1999/12/14 14:51:44 jdurand Exp $
 */

/*
 * Copyright (C) 1994-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageinit.c,v $ $Revision: 1.7 $ $Date: 1999/12/14 14:51:44 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <pwd.h>
#include <netinet/in.h>
#include "marshall.h"
#include "stage.h"
extern	char	*optarg;
extern	int	optind;

main(argc, argv)
int	argc;
char	**argv;
{
	int c, i;
	void cleanup();
	int errflg = 0;
	gid_t gid;
	int msglen;
	int ntries = 0;
	struct passwd *pw;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;

	uid = getuid();
	gid = getgid();
	while ((c = getopt (argc, argv, "Fh:")) != EOF) {
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
	if (argc > optind) {
		fprintf (stderr, STG16);
		errflg++;
	}
	if (errflg) {
		usage (argv[0]);
		exit (USERR);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEINIT);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	pw = getpwuid (uid);
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	marshall_WORD (sbp, gid);
	marshall_WORD (sbp, argc);
	for (i = 0; i < argc; i++)
		marshall_STRING (sbp, argv[i]);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	signal (SIGHUP, cleanup);
	signal (SIGINT, cleanup);
	signal (SIGQUIT, cleanup);
	signal (SIGTERM, cleanup);

	while (c = send2stgd (stghost, sendbuf, msglen, 1)) {
		if (c == 0 || c == USERR || c == CONFERR) break;
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
	fprintf (stderr, "usage: %s [-F] [-h stage_host]\n", cmd);
    return(0);
}
