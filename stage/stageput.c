/*
 * $Id: stageput.c,v 1.10 2000/03/24 10:10:07 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageput.c,v $ $Revision: 1.10 $ $Date: 2000/03/24 10:10:07 $ CERN IT-PDP/DM Jean-Philippe Baud";
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

extern	char	*getconfent();
extern	int	optind;
extern	char	*optarg;
#if !defined(linux)
extern	char	*sys_errlist[];
#endif
static gid_t gid;
static int pid;
static struct passwd *pw;
char *stghost;

void usage _PROTO((char *));
void cleanup _PROTO((int));

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
	int Iflag = 0;
	int Mflag = 0;
	int msglen;
	int nargs;
	int ntries = 0;
	int numvid;
	char *p, *q;
	char path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	/* char repbuf[CA_MAXPATHLEN+1]; */

	nargs = argc;
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	numvid = 0;
	while ((c = getopt (argc, argv, "Gh:I:M:q:U:V:")) != EOF) {
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
		case 'I':
			Iflag = 1;
			break;
		case 'M':
			Mflag = 1;
			break;
		case 'U':
			fun = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				fprintf (stderr, STG06, "-U\n");
				errflg++;
			}
			break;
		case 'V':
			if ((int) strlen (optarg) <= 6)
				numvid = 1;
			else {
				fprintf (stderr, STG06, "V");
				errflg++;
			}
			break;
		case '?':
			errflg++;
			break;
		}
	}
	if (optind >= argc && fun == 0 && numvid == 0 && Iflag == 0 && Mflag == 0) {
		fprintf (stderr, STG46);
		errflg++;
	}

	if (Mflag && (argc > optind)) {
		fprintf (stderr, STG16);
		errflg++;
	}

	if (fun)
		nargs++;

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEPUT);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = getpwuid (uid)) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", uid);
		p = uidstr;
		fprintf (stderr, STG11, p);
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
		if ((c = build_linkname (argv[i], path, sizeof(path), STAGEPUT)) == SYERR) {
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
	if (fun) {
		if ((c = build_Upath (fun, path, sizeof(path), STAGEPUT)) == SYERR) {
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
		c = send2stgd (stghost, sendbuf, msglen, 1, NULL, 0);
		if (c == 0 || serrno == EINVAL || serrno == ERTLIMBYSZ || serrno == CLEARED ||
				serrno == ENOSPC) break;
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
	int c;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[64];
	/* char repbuf[CA_MAXPATHLEN+1]; */
	
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
	c = send2stgd (stghost, sendbuf, msglen, 0, NULL, 0);
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (USERR);
}

void usage(cmd)
		 char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s", "[-G] [-h stage_host] [-U fun] pathname(s)\n");
	fprintf (stderr, "       %s ", cmd);
	fprintf (stderr, "%s",
					 "[-G] [-h stage_host] [-q file_sequence_number(s)] -V visual_identifier\n");
}
