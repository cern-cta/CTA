/*
 * $Id: stagechng.c,v 1.13 2002/01/23 11:02:24 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stagechng.c,v $ $Revision: 1.13 $ $Date: 2002/01/23 11:02:24 $ CERN IT-PDP/DM Jean-Damien Durand";
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
#include "Cns_constants.h"
#include "serrno.h"

#if !defined(linux)
extern	char	*sys_errlist[];
#endif

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
void usage _PROTO((char *));
void cleanup _PROTO((int));
extern	char	*getconfent();

int mintime_beforemigr_flag = 0;
int reqid_flag = 0;
int retenp_on_disk_flag = 0;
int status_flag = 0;
int poolname_flag = 0;

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i;
	void cleanup();
	int errflg = 0;
	gid_t gid;
	int pid;
	int msglen;
	int nargs;
	int ntries = 0;
	int nstg161 = 0;
	char path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	struct passwd *pw;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	char *hsm_host;
	char hsm_path[2 + CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	int attached = 0;
	char *dummy;
	int Mflag = 0;
	int pflag = 0;
	int done_status = 0;
	int done_retenp_on_disk = 0;
	char *poolname = NULL;
	static struct Coptions longopts[] =
	{
		{"host",               REQUIRED_ARGUMENT,  NULL,                    'h'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,                    'M'},
		{"mintime_beforemigr", REQUIRED_ARGUMENT,  &mintime_beforemigr_flag,  1},
		{"poolname",           REQUIRED_ARGUMENT,  &poolname_flag,            1},
		{"reqid",              REQUIRED_ARGUMENT,  &reqid_flag,               1},
		{"retenp_on_disk",     REQUIRED_ARGUMENT,  &retenp_on_disk_flag,      1},
		{"status",             REQUIRED_ARGUMENT,  &status_flag,              1},
		{NULL,                 0,                  NULL,                      0}
	};

	mintime_beforemigr_flag = 0;
	reqid_flag = 0;
	retenp_on_disk_flag = 0;
	status_flag = 0;

	nargs = argc;
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
	while ((c = Cgetopt_long (nargs, argv, "h:M:p:", longopts, NULL)) != -1) {
		switch (c) {
		case 'h':
			stghost = Coptarg;
			break;
		case 'M':
			Mflag++;
			/* Check if the option -M is attached or not */
			if (strstr(argv[Coptind - 1],"-M") == argv[Coptind - 1]) {
				attached = 1;
			}
			/* We want to know if there is no ':' in the string or, if there is such a ':' */
			/* if there is no '/' before (then is will indicate a hostname)                */
			if (! ISCASTOR(Coptarg)) {
				/* We prepend HSM_HOST only for non CASTOR-like files */
				if ((dummy = strchr(Coptarg,':')) == NULL || (dummy != Coptarg && strrchr(dummy,'/') == NULL)) {
					if ((hsm_host = getenv("HSM_HOST")) != NULL) {
						if (hsm_host[0] != '\0') {
							if (attached != 0) {
								strcpy (hsm_path, "-M");
								strcat (hsm_path, hsm_host);
							} else {
								strcpy (hsm_path, hsm_host);
							}
							strcat (hsm_path, ":");
						}
						strcat (hsm_path, Coptarg);
						argv[Coptind - 1] = hsm_path;
					} else if ((hsm_host = getconfent("STG", "HSM_HOST",0)) != NULL) {
						if (hsm_host[0] != '\0') {
							if (attached != 0) {
								strcpy (hsm_path, "-M");
								strcat (hsm_path, hsm_host);
							} else {
								strcpy (hsm_path, hsm_host);
							}
							strcat (hsm_path, ":");
						}
						strcat (hsm_path, Coptarg);
						argv[Coptind - 1] = hsm_path;
					} else {
						fprintf (stderr, STG54);
						errflg++;
					}
				}
			}
			break;
		case 'p':
			poolname = Coptarg;
			pflag++;
		break;
		case 0:
			if (status_flag && ! done_status) { /* --status */
				if ((strcmp(Coptarg,"CAN_BE_MIGR") == 0) || (strcmp(Coptarg,"STAGEOUT|CAN_BE_MIGR") == 0)) {
					/* Per definition Coptarg has enough place to support to format 0x%x == STAGEOUT|CAN_BE_MIGR */
					sprintf(Coptarg, "0x%x", (STAGEOUT|CAN_BE_MIGR));
				}
				done_status = 1;
			}
			if (retenp_on_disk_flag && ! done_retenp_on_disk) { /* --retenp */
				if (strcmp(Coptarg,"AS_LONG_AS_POSSIBLE") == 0) {
					/* Per definition Coptarg has enough place to support to format 0x%x == AS_LONG_AS_POSSIBLE == 0x7FFFFFFF */
					sprintf(Coptarg, "0x%x", AS_LONG_AS_POSSIBLE);
				} else if (strcmp(Coptarg,"INFINITE_LIFETIME") == 0) {
					/* Per definition Coptarg has enough place to support to format 0x%x == INFINITE_LIFETIME == 0x7FFFFFFE */
					sprintf(Coptarg, "0x%x", INFINITE_LIFETIME);
				}
				done_retenp_on_disk = 1;
			}
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}

	if (errflg) {
		usage (argv[0]);
		exit (USERR);
	}

	if (pflag == 0 && (poolname = getenv ("STAGE_POOL")) != NULL)
		nargs += 2;

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEFILCHG);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 4 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = Cgetpwuid (uid)) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", uid);
		fprintf (stderr, STG11, uidstr);
		exit (SYERR);
	}
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	marshall_WORD (sbp, uid);
	marshall_WORD (sbp, gid);
	pid = getpid();
	marshall_WORD (sbp, pid);

	marshall_WORD (sbp, nargs);
	for (i = 0; i < Coptind; i++)
		marshall_STRING (sbp, argv[i]);
	if (pflag == 0 && poolname) {
		marshall_STRING (sbp, "-p");
		marshall_STRING (sbp, poolname);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
		exit (SYERR);
	}
#endif
	for (i = Coptind; i < argc; i++) {
		if ((c = build_linkname (argv[i], path, sizeof(path), STAGEUPDC)) == SYERR) {
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
	fprintf (stderr,
				"usage: %s [-h stagehost] pathname(s)\n"
				"\nor\n\n"
				"%s [-h stagehost] [-p poolname] -M filename"
				"[--mintime_beforemigr mintime_in_seconds]\n"
				"[--reqid request_id]\n"
				"[--retenp_on_disk retention_period[unit]]\n"
				"[--status status]\n",
				cmd, cmd);
}
