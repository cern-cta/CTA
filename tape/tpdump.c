/*
 * Copyright (C) 1990-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tpdump.c,v $ $Revision: 1.20 $ $Date: 2000/03/06 07:49:03 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	tpdump - analyse the content of a tape */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <signal.h>
#include <varargs.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
extern	int	optind;
extern	char	*optarg;
int Ctape_kill_needed;
char *dvrname;
char infil[CA_MAXPATHLEN+1];
int reserve_done;

main(argc, argv)
int	argc;
char	**argv;
{
	static char aden[CA_MAXDENLEN+1] = "";
	int c;
	void cleanup();
	int code = 0;
	int count;
	char devtype[CA_MAXDVTLEN+1];
	static char dgn[CA_MAXDGNLEN+1] = "";
	struct dgn_rsv dgn_rsv;
	char *dp;
	char drive[CA_MAXUNMLEN+1];
	char driver_name[7];
	int errflg = 0;
	int flags = 0;
	int fromblock = -1;
	static char lbltype[CA_MAXLBLTYPLEN+1] = "";
	int maxblksize = -1;
	int maxbyte = -1;
	int maxfile = -1;
	char *p;
	char *tempnam();
	int toblock = -1;
	static char vid[CA_MAXVIDLEN+1] = "";
	static char vsn[CA_MAXVSNLEN+1] = "";

	while ((c = getopt (argc, argv, "B:b:C:d:E:F:g:N:S:T:V:v:")) != EOF) {
		switch (c) {
		case 'B':
			if (maxbyte < 0) {
				maxbyte = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-B");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-B");
				errflg++;
			}
			break;
		case 'b':
			if (maxblksize < 0) {
				maxblksize = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-b");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-b");
				errflg++;
			}
			break;
		case 'C':
			if (code == 0) {
				if (strcmp (optarg, "ascii") == 0)
					code = DMP_ASC;
				else if( strcmp (optarg, "ebcdic") == 0) {
					code = DMP_EBC;
				} else {
					fprintf (stderr, TP006, "-C");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-C");
				errflg++;
			}
			break;
		case 'd':
			if (aden[0]) {
				if (strlen (optarg) <= CA_MAXDENLEN) {
					strcpy (aden, optarg);
				} else {
					fprintf (stderr, TP006, "-d");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-d");
				errflg++;
			}
			break;
		case 'E':
			if (strcmp (optarg, "ignoreeoi") == 0)
				flags |= IGNOREEOI;
			else {
				fprintf (stderr, TP006, "-E");
				errflg++;
			}
			break;
		case 'F':
			if (maxfile < 0) {
				maxfile = strtol (optarg, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-F");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-F");
				errflg++;
			}
			break;
		case 'g':
			if (dgn[0]) {
				if (strlen (optarg) <= CA_MAXDGNLEN) {
					strcpy (dgn, optarg);
				} else {
					fprintf (stderr, TP006, "-g");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-g");
				errflg++;
			}
			break;
		case 'N':
			if (fromblock < 0) {
				if (p = strchr (optarg, ',')) {
					*p++ = '\0';
					fromblock = strtol (optarg, &dp, 10);
					if (*dp != '\0') {
						fprintf (stderr, TP006, "-N");
						errflg++;
					}
				} else {
					p = optarg;
					fromblock = 1;
				}
				toblock = strtol (p, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-N");
					errflg++;
				}
			}
			break;
		case 'S':
			break;
		case 'V':
			if (vid[0] == '\0') {
				if (strlen(optarg) <= CA_MAXVIDLEN) {
					strcpy (vid, optarg);
				} else {
					fprintf (stderr, TP006, "-V");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-V");
				errflg++;
			}
			break;
		case 'v':
			if (vsn[0] == '\0') {
				if (strlen(optarg) <= CA_MAXVSNLEN) {
					strcpy (vsn, optarg);
				} else {
					fprintf (stderr, TP006, "-v");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-v");
				errflg++;
			}
			break;
		case '?':
			errflg++;
			break;
		}
	}
	if (*vid == '\0' && *vsn == '\0') {
		fprintf (stderr, TP031);
		errflg++;
	}
	if (errflg) {
		usage (argv[0]);
		exit_prog (USERR);
	}

	if (code == 0) code = DMP_EBC;
	if (strcmp (dgn, "CT1") == 0) strcpy (dgn, "CART");

#if ! defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if ! defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

	/* Get defaults from TMS (if installed) */

#if TMS
	if (tmscheck (vid, vsn, dgn, aden, lbltype, WRITE_DISABLE, NULL))
		exit_prog (USERR);
#else
	if (*vsn == '\0')
		strcpy (vsn, vid);
	if (*dgn == '\0')
		strcpy (dgn, DEFDGN);
	if (strcmp (dgn, "TAPE") == 0 && *aden == '\0')
		strcpy (aden, "6250");
#endif
	strcpy (infil, tempnam (NULL, "tp"));

#if defined(_AIX) && defined(_IBMR2)
	if (getdvrnam (infil, driver_name) < 0)
		strcpy (driver_name, "tape");
	dvrname = driver_name;
#endif

	/* reserve resources */

	strcpy (dgn_rsv.name, dgn);
	dgn_rsv.num = 1;
	count = 1;
	if (Ctape_reserve (count, &dgn_rsv))
		exit_prog ((serrno == EINVAL || serrno == ETIDG) ? USERR : SYERR);
	reserve_done = 1;

	/* mount and position the input tape */

	Ctape_kill_needed = 1;
	while ((c = Ctape_mount (infil, vid, 0, dgn, NULL, NULL, WRITE_DISABLE,
	    NULL, "blp", 0)) && serrno == ETVBSY)
		sleep (VOLBSYRI);
	if (c)
		exit_prog ((serrno == EACCES || serrno == EINVAL) ? USERR : SYERR);
	if (Ctape_position (infil, TPPOSIT_FSEQ, 1, 1, 0, 0, 0, CHECK_FILE,
	    NULL, "U", 0, 0, 0, 0))
		exit_prog ((serrno == EINVAL) ? USERR : SYERR);
	Ctape_kill_needed = 0;
	if (Ctape_info (infil, NULL, NULL, aden, devtype, drive, NULL,
	    NULL, NULL, NULL))
		exit_prog (SYERR);

	if (Ctape_dmpinit (infil, vid, aden, drive, devtype, maxblksize,
	    fromblock, toblock, maxbyte, maxfile, code, flags))
		exit_prog (SYERR);

	while ((c = Ctape_dmpfil (infil, NULL, NULL, NULL, NULL, NULL, NULL,
	    NULL, NULL)) == 0) continue;
	exit_prog ((c < 0) ? USERR : 0);
}

void
cleanup(sig)
int sig;
{
	signal (sig, SIG_IGN);
	if (Ctape_kill_needed)
		(void) Ctape_kill (infil);
	else
		Ctape_dmpend();
	exit (USERR);
}

dmp_usrmsg(va_alist) va_dcl
{
	va_list args;
	char *msg;
	int msg_type;

	va_start (args);
	msg_type = va_arg (args, int);
	msg = va_arg (args, char *);
	if (msg_type == MSG_OUT)
		vprintf (msg, args);
	else {
		fflush (stdout);
		vfprintf (stderr, msg, args);
	}
}

exit_prog(exit_code)
int exit_code;
{
	Ctape_dmpend();
	if (reserve_done)
		(void) Ctape_rls (NULL, TPRLS_ALL);
	exit (exit_code);
}

usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
	    "[-B maxbyte] [-b max_block_size] [-C {ebcdic|ascii}]\n",
	    "[-d density] [-E ignoreeoi] [-F maxfile] [-g device_group_name]\n",
	    "[-N [fromblock,]toblock] [-v vsn] -V vid\n");
}
