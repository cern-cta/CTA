/*
 * $Id: stagealloc.c,v 1.8 2000/01/09 10:26:07 jdurand Exp $
 */

/*
 * Copyright (C) 1995-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stagealloc.c,v $ $Revision: 1.8 $ $Date: 2000/01/09 10:26:07 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
#define RFIO_KERNEL 1
#include "rfio.h"
#include "stage.h"
extern	char	*getenv();
extern	char	*getconfent();
extern	int	optind;
extern	char	*optarg;
static gid_t gid;
static int pid;
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
	int Gflag = 0;
	char Gname[15];
	uid_t Guid;
	struct group *gr;
#if (defined(sun) && !defined(SOLARIS)) || defined(ultrix) || defined(vms) || defined(_WIN32)
	int mask;
#else
	mode_t mask;
#endif
	int msglen;
	int nargs;
	int ntries = 0;
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
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	while ((c = getopt (argc, argv, "Gh:Pp:s:U:u:")) != EOF) {
		switch (c) {
		case 'G':
			Gflag++;
			if ((gr = getgrgid (gid)) == NULL) {
				fprintf (stderr, STG36, gid);
				exit (SYERR);
			}
			if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) {
				fprintf (stderr, STG10, gr->gr_name);
				errflg++;
			} else {
				strcpy (Gname, p);
				if ((pw = getpwnam (p)) == NULL) {
					fprintf (stderr, STG11, p);
					errflg++;
				} else
					Guid = pw->pw_uid;
			}
			break;
		case 'h':
			stghost = optarg;
			break;
		case 'p':
			poolname = optarg;
			pflag++;
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
		default:
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
	if (pflag == 0 &&
	   (poolname = getenv ("STAGE_POOL")) != NULL)
		nargs += 2;
	if (uflag == 0 &&
	   (pool_user = getenv ("STAGE_USER")) != NULL)
		nargs += 2;
	if (pool_user &&
	   ((pw = getpwnam (pool_user)) == NULL || pw->pw_gid != gid)) {
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

	if ((pw = getpwuid (uid)) == NULL) {
		char uidstr[8];
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
	for (i = 0; i < optind; i++)
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
	if (optind < argc) {
		if ((c = build_linkname (argv[i], path, sizeof(path), STAGEALLOC)) == SYERR) {
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
		if ((c = build_Upath (fun, path, sizeof(path), STAGEALLOC)) == SYERR) {
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

	while (c = send2stgd (stghost, sendbuf, msglen, 1)) {
		if (c == 0 || c == USERR || c == CLEARED || c == ENOSPC) break;
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
	c = send2stgd (stghost, sendbuf, msglen, 0);
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
	  "[-G] [-h stage_host] [-P] [-p pool] [-s size] [-U fun] [-u user] pathname\n");
    return(0);
}
