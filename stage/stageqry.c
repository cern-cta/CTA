/*
 * $Id: stageqry.c,v 1.24 2002/01/15 08:36:11 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stageqry.c,v $ $Revision: 1.24 $ $Date: 2002/01/15 08:36:11 $ CERN IT-PDP/DM Jean-Philippe Baud";
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
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "stage_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cgetopt.h"
#include "serrno.h"

extern	char	*getconfent();

EXTERN_C int  DLL_DECL  send2stgd_cmd _PROTO((char *, char *, int, int, char *, int));  /* Command-line version */
extern int getlist_of_vid _PROTO((char *, char[MAXVSN][7], int *));
void usage _PROTO((char *));
void cleanup _PROTO((int));
int noregexp_flag = 0;
int reqid_flag = 0;
int dump_flag = 0;
int migrator_flag = 0;
int class_flag = 0;
int queue_flag = 0;
int counters_flag = 0;
int retenp_flag = 0;
int mintime_flag = 0;
#ifdef STAGER_SIDE_CLIENT_SUPPORT
int side_flag = 0;
int force_side_format_flag = 0;
#endif

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
	int nstg161 = 0;
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
	static struct Coptions longopts[] =
	{
		{"allocated",          REQUIRED_ARGUMENT,  NULL,      'A'},
		{"allgroups",          NO_ARGUMENT,        NULL,      'a'},
		{"full",               NO_ARGUMENT,        NULL,      'f'},
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"link",               NO_ARGUMENT,        NULL,      'L'},
		{"long",               NO_ARGUMENT,        NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"path",               NO_ARGUMENT,        NULL,      'P'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"file_range",         REQUIRED_ARGUMENT,  NULL,      'Q'},
		{"sort",               NO_ARGUMENT,        NULL,      'S'},
		{"statistic",          NO_ARGUMENT,        NULL,      's'},
		{"tape_info",          NO_ARGUMENT,        NULL,      'T'},
		{"user",               NO_ARGUMENT,        NULL,      'u'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"extended",           NO_ARGUMENT,        NULL,      'x'},
		{"migration_rules",    NO_ARGUMENT,        NULL,      'X'},
		{"noregexp",           NO_ARGUMENT,  &noregexp_flag,    1},
		{"reqid",              REQUIRED_ARGUMENT,  &reqid_flag, 1},
		{"dump",               NO_ARGUMENT,  &dump_flag,        1},
		{"migrator",           NO_ARGUMENT,  &migrator_flag,    1},
		{"class",              NO_ARGUMENT,  &class_flag,       1},
		{"queue",              NO_ARGUMENT,  &queue_flag,       1},
		{"counters",           NO_ARGUMENT,  &counters_flag,    1},
		{"retenp",             NO_ARGUMENT,  &retenp_flag,      1},
#ifdef STAGER_SIDE_CLIENT_SUPPORT
		{"side",               REQUIRED_ARGUMENT, &side_flag,   1},
		{"force_side_format",  NO_ARGUMENT, &force_side_format_flag, 1},
#endif
		{"mintime",            NO_ARGUMENT,  &mintime_flag,     1},
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
	numvid = 0;
	Coptind = 1;
	Copterr = 1;
	while ((c = Cgetopt_long (argc, argv, "A:afGh:I:LlM:Pp:q:Q:SsTuV:x", longopts, NULL)) != -1) {
		switch (c) {
		case 'A':
			Aflag = 1;
			break;
		case 'a':
			break;
		case 'f':
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
		case 'I':
			break;
		case 'L':
			break;
		case 'l':
			break;
		case 'M':
			Mflag = 1;
			break;
		case 'P':
			break;
		case 'p':
			break;
		case 'q':
			break;
		case 'Q':
			break;
		case 'S':
			break;
		case 's':
			break;
		case 'T':
			break;
		case 'u':
			break;
		case 'V':
			errflg += getlist_of_vid ("-V", vid, &numvid);
			break;
		case 'x':
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

	if (Aflag && Mflag)
		errflg++;

	if (errflg != 0) {
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
#ifdef STAGER_SIDE_CLIENT_SUPPORT
	fprintf (stderr, "%s",
					 "[-A pattern | -M pattern] [-a] [-f] [-G] [-h stage_host] [-I external_filename]\n"
					 "[-L] [-l] [-P] [-p pool] [-q file_sequence_number(s)] [-Q file_sequence_range]\n"
					 "[-S] [-s] [-T] [-u] [-V visual_identifier(s)] [-x]\n"
					 "[--class] [--counters] [--dump] [--retenp] [--migrator] [--mintime] [--noregexp]\n"
					 "[--queue] [--reqid reqid]\n"
					 "[--force_side_format] [--side sidenumber]\n");
#else
	fprintf (stderr, "%s",
					 "[-A pattern | -M pattern] [-a] [-f] [-G] [-h stage_host] [-I external_filename]\n"
					 "[-L] [-l] [-P] [-p pool] [-q file_sequence_number(s)] [-Q file_sequence_range]\n"
					 "[-S] [-s] [-T] [-u] [-V visual_identifier(s)] [-x]\n"
					 "[--class] [--counters] [--dump] [--retenp] [--migrator] [--mintime] [--noregexp]\n"
					 "[--queue] [--reqid reqid]\n");
#endif
}
