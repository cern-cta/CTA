/*
 * $Id: stageshutdown.c,v 1.9 2002/03/05 14:44:04 jdurand Exp $
 */

/*
 * Copyright (C) 1994-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageshutdown.c,v $ $Revision: 1.9 $ $Date: 2002/03/05 14:44:04 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <signal.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#include <sys/types.h>
#include <pwd.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <errno.h>
#include "marshall.h"
#include "stage_api.h"
#include "Cpwd.h"
#include "Cgetopt.h"
#include "serrno.h"

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
void cleanup _PROTO((int));
void usage _PROTO((char *));

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
	void cleanup();
	int errflg = 0;
	gid_t gid;
 	int msglen;
	int ntries = 0;
	int nstg161 = 0;
	struct passwd *pw;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;
 	int force_shutdown;
	
	uid = getuid();
	gid = getgid();
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt (argc, argv, "Fh:")) != -1) {
		switch (c) {
		case 'F':
			force_shutdown = 1;
			break;
		case 'h':
			stghost = Coptarg;
			break;
		case '?':
			errflg++;
			break;
		default:
			errflg++;
			break;
		}
	}
	if (argc > Coptind) {
		fprintf (stderr, STG16);
		errflg++;
	}
	/* We force -h parameter to appear in the parameters */
	if (stghost == NULL) {
		errflg++;
	}

	if (errflg != 0) {
		usage (argv[0]);
		exit (USERR);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGESHUTDOWN);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = Cgetpwuid (uid)) == NULL) {
		char uidstr[8], *p;
		if (errno != ENOENT) fprintf (stderr, STG33, "Cgetpwuid", strerror(errno));
		sprintf (uidstr, "%d", uid);
		p = uidstr;
		fprintf (stderr, STG11, p);
		exit (SYERR);
	}
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	marshall_WORD (sbp, gid);
	marshall_WORD (sbp, argc);
	for (i = 0; i < argc; i++)
		marshall_STRING (sbp, argv[i]);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

#if !defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if !defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

	while (1) {
		c = send2stgd_cmd (stghost, sendbuf, msglen, 1, NULL, 0);
		if (c == 0 || serrno == EINVAL || serrno == CONFERR) break;
		if (serrno == ESTNACT && nstg161++ == 0) fprintf(stderr, STG161);
		if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
		sleep (RETRYI);
	}
	exit (c == 0 ? 0 : rc_castor2shift(serrno));
}

void cleanup(sig)
		 int sig;
{
	signal (sig, SIG_IGN);

	exit (USERR);
}

void usage(cmd)
		 char *cmd;
{
	fprintf (stderr, "usage: %s -h stagehost [-F]\n", cmd);
}
