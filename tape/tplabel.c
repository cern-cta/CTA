/*
 * Copyright (C) 1990-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: tplabel.c,v $ $Revision: 1.8 $ $Date: 2001/07/25 06:59:20 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	tplabel - prelabel al and sl tapes, write 2 tape marks for nl tapes */
#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <sys/types.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
extern	int	optind;
extern	char	*optarg;
int reserve_done;
char path[CA_MAXPATHLEN+1];

main(argc, argv)
int	argc;
char	**argv;
{
	char acctname[7];
	int c;
	void cleanup();
	int count;
	static char density[CA_MAXDENLEN+1] = "";
	static char dgn[CA_MAXDGNLEN+1] = "";
	struct dgn_rsv dgn_rsv;
	char *dp;
	static char drive[CA_MAXUNMLEN+1] = "";
	int errflg = 0;
	int flags = 0;
	char *getacct();
	static char lbltype[CA_MAXLBLTYPLEN+1] = "";
	int nbhdr = -1;
	char *p;
	int partition = 0;
	char *tempnam();
	static char vid[CA_MAXDENLEN+1] = "";
	static char vsn[CA_MAXDENLEN+1] = "";

	while ((c = getopt (argc, argv, "D:d:g:H:l:TV:v:")) != EOF) {
		switch (c) {
		case 'D':
			if (! drive[0]) {
				if (strlen (optarg) <= CA_MAXUNMLEN) {
					strcpy (drive, optarg);
				} else {
					fprintf (stderr, TP006, "-D");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-D");
				errflg++;
			}
			break;
		case 'd':
			if (! density[0]) {
				if (strlen (optarg) <= CA_MAXDENLEN) {
					strcpy (density, optarg);
				} else {
					fprintf (stderr, TP006, "-d");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-d");
				errflg++;
			}
			break;
		case 'g':
			if (! dgn[0]) {
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
		case 'H':
			if (nbhdr < 0) {
				nbhdr = strtol (optarg, &dp, 10);
				if (*dp != '\0' ||
				    nbhdr < 0 || nbhdr > 2) {
					fprintf (stderr, TP006, "-H");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-H");
				errflg++;
			}
			break;
		case 'l':
			if (! lbltype[0]) {
				if (strlen (optarg) <= CA_MAXLBLTYPLEN) {
					strcpy (lbltype, optarg);
				} else {
					fprintf (stderr, TP006, "-l");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-l");
				errflg++;
			}
			break;
		case 'T':
			flags = DOUBLETM;
			break;
		case 'V':
			if (! vid[0]) {
				if (strlen (optarg) <= CA_MAXVIDLEN) {
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
			if (! vsn[0]) {
				if (strlen (optarg) <= CA_MAXVSNLEN) {
					strcpy (vsn, optarg);
					if (! vid[0])
						strcpy (vid, optarg);
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
	if (! vid[0]) {
		fprintf (stderr, TP031);
		errflg++;
	}
	if (errflg) {
		usage (argv[0]);
		exit (USERR);
	}

	/* Set default values */

	if (nbhdr < 0) nbhdr = 1;
	strcpy (path, tempnam(NULL, "tp"));

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
	p = getacct();
	if (p == NULL) {
		fprintf (stderr, TP027);
		exit_prog (USERR);
	}
	strcpy (acctname, p);
	if (tmscheck (vid, vsn, dgn, density, lbltype, WRITE_ENABLE, acctname))
		exit_prog (USERR);
#endif

	/* reserve resources */

	strcpy (dgn_rsv.name, dgn);
	dgn_rsv.num = 1;
	count = 1;
	if (Ctape_reserve (count, &dgn_rsv))
		exit_prog ((serrno == EINVAL || serrno == ETIDG) ? USERR : SYERR);
	reserve_done = 1;

	/* mount the tape, check if blank tape and write the prelabel */

	if (Ctape_label (path, vid, partition, dgn, density, drive, vsn,
	    lbltype, nbhdr, flags, 0))
		exit_prog ((serrno == ETOPAB || serrno == ETWPROT || serrno == EINVAL) ? USERR : SYERR);
	exit_prog (0);
}

void cleanup(sig)
int sig;
{
	signal (sig, SIG_IGN);

	(void) Ctape_kill (path);
	exit (USERR);
}

exit_prog(exit_code)
int exit_code;
{
	if (reserve_done)
		(void) Ctape_rls (NULL, TPRLS_ALL);
	exit (exit_code);
}
usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
	    "[-D device_name] [-d density] [-g device_group_name]\n",
	    "[-H number_headers] [-l label_type] [-T] [-V visual_identifier]\n",	    "[-v volume_serial_number]\n");
}
