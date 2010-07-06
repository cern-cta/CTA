/*
 * Copyright (C) 1990-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

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
#include "vmgr_api.h"
int Ctape_kill_needed;
int reserve_done;
char path[CA_MAXPATHLEN+1];

int exit_prog(int exit_code)
{
	if (reserve_done)
		(void) Ctape_rls (NULL, TPRLS_ALL);
	exit (exit_code);
}

void usage(char *cmd)
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
	    "[-D device_name] [-d density] [-g device_group_name]\n",
	    "[-H number_headers] [-l label_type] [-T] [-V visual_identifier]\n",
	    "[-v volume_serial_number] [--nbsides n] [-f]\n");
}

int main(int	argc,
         char	**argv)
{
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
	while ((c = Cgetopt_long (argc, argv, "D:d:g:H:l:TV:v:f", longopts, NULL)) != EOF) {
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
			flags |= DOUBLETM;
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
		case 'f':          
			flags |= FORCEPRELBL;
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

	signal (SIGHUP, cleanup);
	signal (SIGINT, cleanup);
	signal (SIGQUIT, cleanup);
	signal (SIGTERM, cleanup);

        if (*dgn == '\0') {

	/* If dgn not specified, get it from VMGR */

		uid = getuid();
		gid = getgid();
		if ((c = vmgrcheck (vid, vsn, dgn, density, lbltype, WRITE_ENABLE, uid, gid))) {
			{
				fprintf (stderr, "%s\n", sstrerror(c));
				exit_prog (USERR);
			}
		}
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
                                 lbltype, nbhdr, flags, 0)) {
                        fprintf (stderr, "tplabel: tape not labeled.\n");
			exit_prog ((serrno == ETOPAB || serrno == ETWPROT || serrno == EINVAL) ?
                                   USERR : SYERR);
                }
	}
        fprintf (stderr, "tplabel: tape labeled.\n");
	exit_prog (0);

        /* will never be reached, but makes the compiler happy */
        exit (0);        
}

void cleanup(int sig)
{
	signal (sig, SIG_IGN);

	if (Ctape_kill_needed)
		(void) Ctape_kill (path);
	exit (USERR);
}

