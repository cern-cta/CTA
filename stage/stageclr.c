/*
 * $Id: stageclr.c,v 1.37 2004/11/17 13:05:44 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageclr.c,v $ $Revision: 1.37 $ $Date: 2004/11/17 13:05:44 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <sys/types.h>
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
#include "rfio_api.h"

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
extern int getlist_of_vid _PROTO((char *, char[MAXVSN][7], int *));
extern	char	*getenv();
extern	char	*getconfent();

void cleanup _PROTO((int));
void usage _PROTO((char *));

int reqid_flag = 0;
int side_flag = 0;
int nodisk_flag = 0;
int dounlink_flag = 0;

int main(argc, argv)
		 int	argc;
		 char	**argv;
{
	int c, i, n;
	int errflg = 0;
	int Fflag = 0;
	int Gflag = 0;
	uid_t Guid;
	gid_t gid;
	struct group *gr;
	int Iflag = 0;
	char ibuf[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	int iflag = 0;
	int Lflag = 0;
	int Mflag = 0;
	int msglen;
	int ntries = 0;
	int nstg161 = 0;
	int numvid;
	int Pflag = 0;
	char *p;
	char path[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	struct passwd *pw;
	char *q;
	char *sbp;
	char *sbpp;
	char sendbuf[REQBUFSZ];
	char *stghost = NULL;
	uid_t uid;
	char vid[MAXVSN][7];
	char *hsm_host;
	char hsm_path[2 + CA_MAXHOSTNAMELEN + 1 + MAXPATH];
#if defined(_WIN32)
	WSADATA wsadata;
#endif
	int attached = 0;
	char *dummy;
	static struct Coptions longopts[] =
	{
		{"conditionnal",       NO_ARGUMENT,        NULL,      'c'},
		{"force",              NO_ARGUMENT,        NULL,      'F'},
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"input",              NO_ARGUMENT,        NULL,      'i'},
		{"link_name",          REQUIRED_ARGUMENT,  NULL,      'L'},
		{"label_type",         REQUIRED_ARGUMENT,  NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"minfree",            REQUIRED_ARGUMENT,  NULL,      'm'},
		{"pathname",           REQUIRED_ARGUMENT,  NULL,      'P'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"file_range",         REQUIRED_ARGUMENT,  NULL,      'Q'},
		{"nodisk",             NO_ARGUMENT,      &nodisk_flag,  1},
		{"dounlink",           NO_ARGUMENT,    &dounlink_flag,  1},
		{"r",                  REQUIRED_ARGUMENT,  NULL,      'r'},
		{"reqid",              REQUIRED_ARGUMENT, &reqid_flag,  1},
		{"side",               REQUIRED_ARGUMENT, &side_flag,   1},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{NULL,                 0,                  NULL,        0}
	};

	/* char repbuf[CA_MAXPATHLEN+1]; */

	uid = Guid = getuid();
	gid = getgid();
#if defined(_WIN32)
	if ((uid < 0) || (uid >= CA_MAXUID) || (gid < 0) || (gid >= CA_MAXGID)) {
		fprintf (stderr, STG52);
		exit (USERR);
	}
#endif
	numvid = 0;
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, STG51);
		exit (SYERR);
	}
#endif
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt_long (argc, argv, "cFGh:I:iL:l:M:m:P:p:q:Q:r:V:", longopts, NULL)) != -1) {
		switch (c) {
		case 'F':
			Fflag++;
			break;
		case 'G':
			Gflag++;
			if ((gr = Cgetgrgid (gid)) == NULL) {
				if (errno != ENOENT) fprintf (stderr, STG33, "Cgetgrgid", strerror(errno));
				fprintf (stderr, STG36, gid);
#if defined(_WIN32)
				WSACleanup();
#endif
				exit (SYERR);
			}
			if ((p = getconfent ("GRPUSER", gr->gr_name, 0)) == NULL) {
				fprintf (stderr, STG10, gr->gr_name);
				errflg++;
			} else {
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
		case 'I':
			Iflag++;
			break;
		case 'i':
			iflag++;
			break;
		case 'L':
			Lflag = Coptind - 1;
			if ((n = Coptarg - argv[Lflag]) != 0)
				strncpy (path, argv[Lflag], n);
			if ((c = build_linkname (Coptarg, path+n, sizeof(path)-n, STAGECLR)) == SESYSERR) {
#if defined(_WIN32)
				WSACleanup();
#endif
				exit (SYERR);
			} else if (c)
				errflg++;
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
		case 'P':
			Pflag = Coptind - 1;
			if ((n = Coptarg - argv[Pflag]) != 0)
				strncpy (path, argv[Pflag], n);
			if ((c = build_linkname (Coptarg, path+n, sizeof(path)-n, STAGECLR)) == SESYSERR) {
#if defined(_WIN32)
				WSACleanup();
#endif
				exit (SYERR);
			} else if (c)
				errflg++;
			break;
		case 'r':
			if (strcmp (Coptarg, "emove_from_hsm") != 0)
				errflg++;
			break;
		case 'V':
			errflg += getlist_of_vid ("-V", vid, &numvid);
			break;
		case 0:
			/* Long option without short option correspondance */
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
	if (numvid == 0 && Iflag == 0 && iflag == 0 && Lflag == 0 && Pflag == 0 && Mflag == 0 && reqid_flag == 0) {
		fprintf (stderr, STG46);
		errflg++;
	}
	if ((iflag != 0) + (Lflag != 0) + (Pflag != 0) + (Mflag != 0) > 1) {
		fprintf (stderr, STG35, "-i, -L, -M", "-P");
		errflg++;
	}

	if (errflg) {
		usage (argv[0]);
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (1);
	}

	/* If --dounlink, force --nodisk */
	if (dounlink_flag && ! nodisk_flag) {
	  nodisk_flag = 1;
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGECLR);
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
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (SYERR);
	}
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	if (Gflag) {
		marshall_WORD (sbp, Guid);
	} else {
		marshall_WORD (sbp, uid);
	}
	marshall_WORD (sbp, gid);

#if ! defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if ! defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

	if (iflag) {
		int rc = 0;
		marshall_WORD (sbp, argc+2);
		for (i = 0; i < argc; i++) {
		  if (strcmp(argv[i],"--dounlink") != 0) {
		    /* Ignore --dounlink */
		    marshall_STRING (sbp, argv[i]);
		  }
		}
		marshall_STRING (sbp, "-P");
		sbpp = sbp;
		while (fgets (ibuf, sizeof(ibuf), stdin) != NULL) {
			if ((p = strchr (ibuf, '\n')) != NULL) *p = '\0';
			if ((c = build_linkname (ibuf, path, sizeof(path), STAGECLR)) == SESYSERR) {
#if defined(_WIN32)
				WSACleanup();
#endif
				exit (SYERR);
			} else if (c) {
				if (! rc) rc = USERR;
				continue;
			}
			if (sbp + strlen (path) - sendbuf >= sizeof(sendbuf)) {
				fprintf (stderr, STG38);
				if (! rc) rc = USERR;
				continue;
			} else {
			  if (dounlink_flag) {
			    /* We clear ourself the file */
			    serrno = rfio_errno = 0;
			    if ((rfio_munlink(path) == 0) || (rfio_serrno() == ENOENT)) {
			      /* Deletion was successful or file absent */
			      marshall_STRING (sbp, path);
			    }
			  } else {
			    marshall_STRING (sbp, path);
			  }
			}

			msglen = sbp - sendbuf;
			sbp = q;
			marshall_LONG (sbp, msglen);	/* update length field */

			while (1) {
				c = send2stgd_cmd (stghost, sendbuf, msglen, 1, NULL, 0);
				if (c == 0 || serrno == EINVAL || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || serrno == EBUSY) break;
				if (serrno == ENOUGHF) break;
				if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
				if (serrno == ESTNACT && nstg161++ == 0) fprintf(stderr, STG161);
				if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
				sleep (RETRYI);
			}
			if (serrno == ENOUGHF) break;
			if (c && serrno != EBUSY && ! rc) rc = serrno;
			sbp = sbpp;
		}
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (rc == 0 ? 0 : rc_castor2shift(rc));
	} else {
		marshall_WORD (sbp, argc);
		for (i = 0; i < argc; i++)
			if ((Pflag && i == Pflag) || (Lflag && i == Lflag)) {
				marshall_STRING (sbp, path);
			} else {
			  if (strcmp(argv[i],"--dounlink") != 0) {
			    marshall_STRING (sbp, argv[i]);
			  }
			}

		msglen = sbp - sendbuf;
		marshall_LONG (q, msglen);	/* update length field */

		while (1) {
			c = send2stgd_cmd (stghost, sendbuf, msglen, 1, NULL, 0);
			if (c == 0 || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || serrno == EINVAL) break;
			if (serrno == ENOUGHF) {
				c = 0;
				break;
			}
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
	fprintf (stderr, "%s",
			 "[-c] [-h stage_host] [-F] [-G] [-I external_filename] [-i] [-L link]\n"
			 "[-l label_type] [-M hsmfile] [-m minfree] [-P path] [-p pool]\n"
			 "[-q file_sequence_number] [-Q file_sequence_range]\n"
			 "[--nodisk] [--dounlink]\n"
			 "[-remove_from_hsm] [--reqid reqid] [-V visual_identifier(s)] [--side sidenumber]\n");
}
