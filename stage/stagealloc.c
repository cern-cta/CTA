/*
 * $Id: stagealloc.c,v 1.30 2002/10/16 22:58:55 jdurand Exp $
 */

/*
 * Copyright (C) 1995-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stagealloc.c,v $ $Revision: 1.30 $ $Date: 2002/10/16 22:58:55 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "stage_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cgetopt.h"
#include "serrno.h"

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
extern	char	*getenv();
extern	char	*getconfent();
static gid_t gid;
static int pid;
static struct passwd *pw;
char *stghost;

void cleanup _PROTO((int));
void usage _PROTO((char *));

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
	char *dp;
	int errflg = 0;
	int fun = 0;
	int Gflag = 0;
	char Gname[15];
	uid_t Guid;
	struct group *gr;
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
	int mask;
#else
	mode_t mask;
#endif
	int msglen;
	int nargs;
	int ntries = 0;
	int nstg161 = 0;
	char *p, *q;
	char path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	int pflag = 0;
	char *pool_user = NULL;
	char *poolname = NULL;
	char *sbp;
	char sendbuf[REQBUFSZ];
	int uflag = 0;
	uid_t uid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	nargs = argc;
	uid = Guid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if ((uid < 0) || (uid >= CA_MAXUID) || (gid < 0) || (gid >= CA_MAXGID)) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt (argc, argv, "Gh:Pp:s:U:u:")) != -1) {
		switch (c) {
		case 'G':
			Gflag++;
			if ((gr = Cgetgrgid (gid)) == NULL) {
				if (errno != ENOENT) fprintf (stderr, STG33, "Cgetgrgid", strerror(errno));
				fprintf (stderr, STG36, gid);
				exit (SYERR);
			}
			if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) {
				fprintf (stderr, STG10, gr->gr_name);
				errflg++;
			} else {
				strcpy (Gname, p);
				if ((pw = Cgetpwnam (p)) == NULL) {
					if (errno != ENOENT) fprintf (stderr, STG33, "Cgetpwnam", strerror(errno));
					fprintf (stderr, STG11, p);
					errflg++;
				} else
					Guid = pw->pw_uid;
			}
			break;
		case 'h':
			stghost = Coptarg;
			break;
		case 'p':
			poolname = Coptarg;
			pflag++;
			break;
		case 'U':
			stage_strtoi(&fun, Coptarg, &dp, 10);
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
		default:
			break;
		}
	}
	if (Coptind >= argc && fun == 0) {
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
	if (pflag == 0 &&
			(poolname = getenv ("STAGE_POOL")) != NULL)
		nargs += 2;
	if (uflag == 0 &&
			(pool_user = getenv ("STAGE_USER")) != NULL)
		nargs += 2;
	if (pool_user &&
			((pw = Cgetpwnam (pool_user)) == NULL || pw->pw_gid != gid)) {
		if ((pw == NULL) && (errno != ENOENT)) fprintf (stderr, STG33, "Cgetpwnam", strerror(errno));
		fprintf (stderr, STG11, pool_user);
		errflg++;
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEALLOC);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = Cgetpwuid (uid)) == NULL) {
		char uidstr[8];
		if (errno != ENOENT) fprintf (stderr, STG33, "Cgetpwuid", strerror(errno));
		sprintf (uidstr, "%d", uid);
		fprintf (stderr, STG11, uidstr);
		exit (SYERR);
	}
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	if (Gflag) {
		marshall_STRING (sbp, Gname);
		marshall_WORD (sbp, Guid);
	} else {
		marshall_STRING (sbp, pw->pw_name);
		marshall_WORD (sbp, uid);
	}
	marshall_WORD (sbp, gid);
	umask (mask = umask(0));
	marshall_WORD (sbp, mask);
	pid = getpid();
	marshall_WORD (sbp, pid);

	marshall_WORD (sbp, nargs);
	for (i = 0; i < Coptind; i++)
		marshall_STRING (sbp, argv[i]);
	if (pflag == 0 && poolname) {
		marshall_STRING (sbp, "-p");
		marshall_STRING (sbp, poolname);
	}
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
	if (Coptind < argc) {
		if ((c = build_linkname (argv[i], path, sizeof(path), STAGEALLOC)) == SESYSERR) {
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
			} else
				marshall_STRING (sbp, path);
		}
	}
	if (fun) {
		if ((c = build_Upath (fun, path, sizeof(path), STAGEALLOC)) == SESYSERR) {
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

	while (1) {
		c = send2stgd_cmd (stghost, sendbuf, msglen, 1, NULL, 0);
		if (c == 0 || serrno == EINVAL || serrno == ESTCLEARED || serrno == ENOSPC || serrno == ESTKILLED) break;
		if (serrno == ESTLNKNSUP) {	/* symbolic links not supported on that platform */
			c = 0;
			break;
		}
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
	int c;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[64];

	signal (sig, SIG_IGN);

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEKILL);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	marshall_WORD (sbp, gid);
	marshall_WORD (sbp, pid);
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */
	c = send2stgd_cmd (stghost, sendbuf, msglen, 0, NULL, 0);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (USERR);
}

void usage(cmd)
		 char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s",
					 "[-G] [-h stage_host] [-P] [-p pool] [-s size] [-U fun] [-u user] pathname\n");
}
