/*
 * $Id: stageupdc.c,v 1.26 2003/04/28 10:03:14 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageupdc.c,v $ $Revision: 1.26 $ $Date: 2003/04/28 10:03:14 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
#include "stage_api.h"
#include "Cpwd.h"
#include "Cgetopt.h"
#include "serrno.h"

#if !defined(linux)
extern	char	*sys_errlist[];
#endif

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
void usage _PROTO((char *));
void cleanup _PROTO((int));

#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
	void cleanup();
	char *dp;
	int errflg = 0;
	int fun = 0;
	gid_t gid;
	int key;
	int msglen;
	int nargs;
	int ntries = 0;
	int nstg161 = 0;
	char *p;
	char path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	struct passwd *pw;
	char *q;
	int reqid = 0;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char Zparm[CA_MAXHOSTNAMELEN + 1 + 14];
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

	nargs = argc;
	uid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if ((uid < 0) || (uid >= CA_MAXUID) || (gid < 0) || (gid >= CA_MAXGID)) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt (argc, argv, "b:D:F:f:h:I:L:q:R:s:T:U:W:Z:")) != -1) {
		switch (c) {
		case 'h':
			stghost = Coptarg;
			break;
		case 'U':
			stage_strtoi(&fun, Coptarg, &dp, 10);
			if (*dp != '\0') {
				fprintf (stderr, STG06, "-U\n");
				errflg++;
			}
			break;
		case 'Z':
			strcpy (Zparm, Coptarg);
			if ((p = strtok (Zparm, ".")) != NULL) {
				stage_strtoi(&reqid, p, &dp, 10);
				if (*dp != '\0' ||
						(p = strtok (NULL, "@")) == NULL) {
					fprintf (stderr, STG06, "-Z\n");
					errflg++;
				} else {
					stage_strtoi(&key, p, &dp, 10);
					if (*dp != '\0' ||
							(stghost = strtok (NULL, " ")) == NULL) {
						fprintf (stderr, STG06, "-Z\n");
						errflg++;
					}
				}
			} else {
				fprintf (stderr, STG06, "-Z\n");
				errflg++;
			}
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (reqid == 0 && Coptind >= argc && fun == 0) {
		fprintf (stderr, STG07);
		errflg++;
	}

	if (errflg) {
		usage (argv[0]);
		exit (USERR);
	}
	if (fun)
		nargs++;

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEUPDC);
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
	marshall_WORD (sbp, uid);
	marshall_WORD (sbp, gid);

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
		if ((c = build_linkname (argv[i], path, sizeof(path), STAGEUPDC)) == SESYSERR) {
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
		if ((c = build_Upath (fun, path, sizeof(path), STAGEUPDC)) == SESYSERR) {
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

	while (1) {
		c = send2stgd_cmd (stghost, sendbuf, msglen, 1, NULL, 0);
		if (c == 0 || serrno == EINVAL || serrno == ENOSPC) break;
		if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
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
	fprintf (stderr, "%s%s%s%s",
					 "[-b max_block_size] [-D device_name] [-F record_format] [-f file_id]\n",
					 "[-h stage_host] [-I network_interface] [-L record_length] [-q file_sequence_number]\n",
					 "[-R return_code] [-s size] [-T transfer_time] [-U fun] [-W waiting_time]\n",
					 "[-Z reqid.key@stagehost] pathname(s)\n");
}
