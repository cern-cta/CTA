/*
 * $Id: stageqry.c,v 1.12 2000/12/12 14:13:41 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageqry.c,v $ $Revision: 1.12 $ $Date: 2000/12/12 14:13:41 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cgetopt.h"

extern	char	*getconfent();

void usage _PROTO((char *));
void cleanup _PROTO((int));

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int Aflag = 0;
	int c, i;
	int errflg = 0;
	int Gflag = 0;
	gid_t gid;
	struct group *gr;
	int Mflag = 0;
	int msglen;
	int ntries = 0;
	int numvid;
	char *p;
	struct passwd *pw;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;
	char vid[MAXVSN][7];
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	numvid = 0;
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt (argc, argv, "A:afGh:I:LlM:Pp:q:Q:SsTuV:x")) != -1) {
		switch (c) {
		case 'A':
			Aflag = 1;
			break;
		case 'G':
			Gflag++;
			if ((gr = Cgetgrgid (gid)) == NULL) {
				fprintf (stderr, STG36, gid);
				exit (SYERR);
			}
			if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) {
				fprintf (stderr, STG10, gr->gr_name);
				errflg++;
			} else {
				if ((pw = Cgetpwnam (p)) == NULL) {
					fprintf (stderr, STG11, p);
					errflg++;
				}
			}
			break;
		case 'h':
			stghost = Coptarg;
			break;
		case 'M':
			Mflag = 1;
			break;
		case 'V':
			errflg += getlist_of_vid ("-V", vid, &numvid);
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (argc > Coptind) {
		fprintf (stderr, STG16);
		errflg++;
	}

	if (Aflag && Mflag)
		errflg++;

	if (errflg) {
		usage (argv[0]);
		exit (USERR);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEQRY);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = Cgetpwuid (uid)) == NULL) {
		char uidstr[8];
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
		c = send2stgd (stghost, sendbuf, msglen, 1, NULL, 0);
		if (c == 0 || serrno == EINVAL) break;
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
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
					 "[-A pattern | -M pattern] [-a] [-f] [-G] [-h stage_host] [-I external_filename]\n",
					 "[-L] [-l] [-P] [-p pool] [-q file_sequence_number(s)] [-Q file_sequence_range] [-S] [-s] [-T]\n",
					 "[-u] [-V visual_identifier(s)] [-x]\n");
}
