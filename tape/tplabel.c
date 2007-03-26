/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: tplabel.c,v $ $Revision: 1.12 $ $Date: 2007/03/26 12:14:53 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	tplabel - prelabel al and sl tapes, write 2 tape marks for nl tapes */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "Cgetopt.h"
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
#if VMGR
#include "vmgr_api.h"
#endif
int Ctape_kill_needed;
int reserve_done;
char path[CA_MAXPATHLEN+1];

int exit_prog(exit_code)
int exit_code;
{
	if (reserve_done)
		(void) Ctape_rls (NULL, TPRLS_ALL);
	exit (exit_code);
}

void usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
	    "[-D device_name] [-d density] [-g device_group_name]\n",
	    "[-H number_headers] [-l label_type] [-T] [-V visual_identifier]\n",
	    "[-v volume_serial_number] [--nbsides n]\n");
}

int main(argc, argv)
int	argc;
char	**argv;
{
#ifdef TMS
	char acctname[7];
	char *p;
#endif
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
	gid_t gid;
	static char lbltype[CA_MAXLBLTYPLEN+1] = "";
	static struct Coptions longopts[] = {
		{"nbsides", REQUIRED_ARGUMENT, 0, TPOPT_NBSIDES},
		{0, 0, 0, 0}
	};
	int nbhdr = -1;
	int nbsides = 1;
	int side = 0;
	char *tempnam();
	uid_t uid;
	static char vid[CA_MAXDENLEN+1] = "";
	static char vsn[CA_MAXDENLEN+1] = "";

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "D:d:g:H:l:TV:v:", longopts, NULL)) != EOF) {
		switch (c) {
		case 'D':
			if (! drive[0]) {
				if (strlen (Coptarg) <= CA_MAXUNMLEN) {
					strcpy (drive, Coptarg);
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
				if (strlen (Coptarg) <= CA_MAXDENLEN) {
					strcpy (density, Coptarg);
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
				if (strlen (Coptarg) <= CA_MAXDGNLEN) {
					strcpy (dgn, Coptarg);
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
				nbhdr = strtol (Coptarg, &dp, 10);
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
				if (strlen (Coptarg) <= CA_MAXLBLTYPLEN) {
					strcpy (lbltype, Coptarg);
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
				if (strlen (Coptarg) <= CA_MAXVIDLEN) {
					strcpy (vid, Coptarg);
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
				if (strlen (Coptarg) <= CA_MAXVSNLEN) {
					strcpy (vsn, Coptarg);
				} else {
					fprintf (stderr, TP006, "-v");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-v");
				errflg++;
			}
			break;
		case TPOPT_NBSIDES:
			if ((nbsides = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid number of sides %s\n", Coptarg);
				errflg++;
			}
			break;
		case '?':
			errflg++;
			break;
		}
	}
	if (! vid[0] && ! vsn[0]) {
		fprintf (stderr, TP031);
		errflg++;
	}
	if (errflg) {
		usage (argv[0]);
		exit (USERR);
	}

	/* Set default values */

	if (! vid[0])
		strcpy (vid, vsn);
	if (nbhdr < 0) nbhdr = 1;

#if ! defined(_WIN32)
	signal (SIGHUP, cleanup);
#endif
	signal (SIGINT, cleanup);
#if ! defined(_WIN32)
	signal (SIGQUIT, cleanup);
#endif
	signal (SIGTERM, cleanup);

        if (*dgn == '\0') {
#if TMS || VMGR

	/* If dgn not specified, get it from VMGR or TMS (if installed) */

#if VMGR
		uid = getuid();
		gid = getgid();
#if defined(_WIN32)
		if (uid < 0 || gid < 0) {
			fprintf (stderr, TP053);
			exit_prog (USERR);
		}
#endif
		if ((c = vmgrcheck (vid, vsn, dgn, density, lbltype, WRITE_ENABLE, uid, gid))) {
#if TMS
			if (c != ETVUNKN)
#endif
			{
				fprintf (stderr, "%s\n", sstrerror(c));
				exit_prog (USERR);
			}
#endif
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
#if VMGR
		}
#endif
#else
		strcpy (dgn, DEFDGN);
#endif
	}
	strcpy (path, tempnam(NULL, "tp"));

	/* reserve resources */

	strcpy (dgn_rsv.name, dgn);
	dgn_rsv.num = 1;
	count = 1;
	if (Ctape_reserve (count, &dgn_rsv))
		exit_prog ((serrno == EINVAL || serrno == ETIDG) ? USERR : SYERR);
	reserve_done = 1;

	/* mount the tape, check if blank tape and write the prelabel */

	Ctape_kill_needed = 1;
	for (side = 0; side < nbsides; side++) {
		if (Ctape_label (path, vid, side, dgn, density, drive, vsn,
		    lbltype, nbhdr, flags, 0))
			exit_prog ((serrno == ETOPAB || serrno == ETWPROT || serrno == EINVAL) ?
			    USERR : SYERR);
	}
	exit_prog (0);

        /* will never be reached, but makes the compiler happy */
        exit (0);        
}

void cleanup(sig)
int sig;
{
	signal (sig, SIG_IGN);

	if (Ctape_kill_needed)
		(void) Ctape_kill (path);
	exit (USERR);
}

