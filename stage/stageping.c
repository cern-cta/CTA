/*
 * $Id: stageping.c,v 1.3 2002/03/05 14:44:04 jdurand Exp $
 */

/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageping.c,v $ $Revision: 1.3 $ $Date: 2002/03/05 14:44:04 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <fcntl.h>
#include "Cpwd.h"
#include "Cgrp.h"
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "stage_api.h"
#include "Cgetopt.h"
#include "serrno.h"

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
void usage _PROTO((char *));
void cleanup _PROTO((int));

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
	int errflg = 0;
	gid_t gid;
	struct group *gr;
	int msglen;
	int ntries = 0;
	int nstg161 = 0;
	char *p;
	struct passwd *pw;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	static struct Coptions longopts[] =
	{
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"verbose",            NO_ARGUMENT,        NULL,      'v'},
		{NULL,                 0,                  NULL,        0}
	};

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
	while ((c = Cgetopt_long (argc, argv, "h:v", longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			stghost = Coptarg;
			break;
		case 'v':
			break;
		case 0:
			/* Here are the long options */
			break;
		case '?':
			errflg++;
			break;
		default:
			errflg++;
			break;
		}
        if (errflg != 0) break;
	}
	if (argc > Coptind) {
		fprintf (stderr, STG16);
		errflg++;
	}

	if (errflg != 0) {
		usage (argv[0]);
		exit (USERR);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEPING);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = Cgetpwuid (uid)) == NULL) {
		char uidstr[8];
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
	
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
		exit (SYERR);
	}
#endif
	
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
		if (c == 0 || serrno == EINVAL) break;
		if (serrno == ESTNACT && nstg161++ == 0) fprintf(stderr, STG161);
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
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s", "[-h stage_host] [-v]\n");
}
