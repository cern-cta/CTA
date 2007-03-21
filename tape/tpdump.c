/*
 * Copyright (C) 1990-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
/* static char sccsid[] = "@(#)$RCSfile: tpdump.c,v $ $Revision: 1.33 $ $Date: 2007/03/21 09:29:02 $ CERN IT-PDP/DM Jean-Philippe Baud"; */
#endif /* not lint */

/*	tpdump - analyse the content of a tape */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <unistd.h>
#include <signal.h>
#include <stdarg.h>
#include "Cgetopt.h"
#include "Ctape.h"
#include "Ctape_api.h"
#include "serrno.h"
#if VMGR
#include "vmgr_api.h"
#endif
int Ctape_kill_needed;
char *dvrname;
char infil[CA_MAXPATHLEN+1];
int reserve_done;

void tpdump_usrmsg(int msg_type, char *msg, ...)
{
	va_list args;

	va_start (args, msg);
	if (msg_type == MSG_OUT)
		vprintf (msg, args);
	else {
		fflush (stdout);
		vfprintf (stderr, msg, args);
	}
	va_end (args);
}

int exit_prog(exit_code)
int exit_code;
{
	Ctape_dmpend();
	if (reserve_done)
		(void) Ctape_rls (NULL, TPRLS_ALL|TPRLS_UNLOAD);
	exit (exit_code);
}

void usage(cmd)
char *cmd;
{
	fprintf (stderr, "usage: %s ", cmd);
	fprintf (stderr, "%s%s%s",
	    "[-B maxbyte] [-b max_block_size] [-C {ebcdic|ascii}]\n",
	    "[-d density] [-E ignoreeoi] [-F maxfile] [-g device_group_name]\n",
	    "[-N [fromblock,]toblock] [-q fromfile] [-v vsn] -V vid\n");
}

int main(argc, argv)
int	argc;
char	**argv;
{
	static char aden[CA_MAXDENLEN+1] = "";
	int c;
	void cleanup _PROTO((int));
	int code = 0;
	int count;
	char devtype[CA_MAXDVTLEN+1];
	static char dgn[CA_MAXDGNLEN+1] = "";
	struct dgn_rsv dgn_rsv;
	char *dp;
	char drive[CA_MAXUNMLEN+1];
#if defined(_AIX) && defined(_IBMR2)
	char driver_name[7];
#endif
	int errflg = 0;
	int flags = 0;
	int fromblock = -1;
	int fromfile = -1;
	static char lbltype[CA_MAXLBLTYPLEN+1] = "";
	static struct Coptions longopts[] = {
		{"side", REQUIRED_ARGUMENT, 0, TPOPT_SIDE},
		{0, 0, 0, 0}
	};
	int maxblksize = -1;
	int maxbyte = -1;
	int maxfile = -1;
	char *p;
	int side = 0;
        char *tempnam();
	int toblock = -1;
	static char vid[CA_MAXVIDLEN+1] = "";
	static char vsn[CA_MAXVSNLEN+1] = "";

	Copterr = 1;
	Coptind = 1;
	while ((c = Cgetopt_long (argc, argv, "B:b:C:d:E:F:g:N:q:S:T:V:v:", longopts, NULL)) != EOF) {
		switch (c) {
		case 'B':
			if (maxbyte < 0) {
				maxbyte = strtol (Coptarg, &dp, 10);
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
				maxblksize = strtol (Coptarg, &dp, 10);
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
				if (strcmp (Coptarg, "ascii") == 0)
					code = DMP_ASC;
				else if( strcmp (Coptarg, "ebcdic") == 0) {
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
			if (aden[0] == '\0') {
				if (strlen (Coptarg) <= CA_MAXDENLEN) {
					strcpy (aden, Coptarg);
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
			if (strcmp (Coptarg, "ignoreeoi") == 0)
				flags |= IGNOREEOI;
			else {
				fprintf (stderr, TP006, "-E");
				errflg++;
			}
			break;
		case 'F':
			if (maxfile < 0) {
				maxfile = strtol (Coptarg, &dp, 10);
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
			if (dgn[0] == '\0') {
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
		case 'N':
			if (fromblock < 0) {
				if ((p = strchr (Coptarg, ','))) {
					*p++ = '\0';
					fromblock = strtol (Coptarg, &dp, 10);
					if (*dp != '\0') {
						fprintf (stderr, TP006, "-N");
						errflg++;
					}
				} else {
					p = Coptarg;
					fromblock = 1;
				}
				toblock = strtol (p, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-N");
					errflg++;
				}
			}
			break;
		case 'q':
			if (fromfile < 0) {
				fromfile = strtol (Coptarg, &dp, 10);
				if (*dp != '\0') {
					fprintf (stderr, TP006, "-q");
					errflg++;
				}
			} else {
				fprintf (stderr, TP018, "-q");
				errflg++;
			}
			break;
		case 'S':
			break;
		case 'V':
			if (vid[0] == '\0') {
				if (strlen(Coptarg) <= CA_MAXVIDLEN) {
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
			if (vsn[0] == '\0') {
				if (strlen(Coptarg) <= CA_MAXVSNLEN) {
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
		case TPOPT_SIDE:
			if ((side = strtol (Coptarg, &dp, 10)) < 0 ||
			    *dp != '\0') {
				fprintf (stderr,
				    "invalid side number %s\n", Coptarg);
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
	if (*vid == '\0')
		strcpy (vid, vsn);

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
		if ((c = vmgrcheck (vid, vsn, dgn, aden, lbltype, WRITE_DISABLE, 0, 0))) {
#if TMS
			if (c != ETVUNKN)
#endif
			{
				fprintf (stderr, "%s\n", sstrerror(c));
				exit_prog (USERR);
			}
#endif
#if TMS
			if (tmscheck (vid, vsn, dgn, aden, lbltype, WRITE_DISABLE, NULL))
				exit_prog (USERR);
#endif
#if VMGR
		}
#endif
#else
		strcpy (dgn, DEFDGN);
#endif
	}
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
	while ((c = Ctape_mount (infil, vid, side, dgn, aden, NULL, WRITE_DISABLE,
	    NULL, "blp", 0)) && serrno == ETVBSY)
		sleep (VOLBSYRI);
	if (c)
		exit_prog ((serrno == ETOPAB || serrno == EINVAL) ? USERR : SYERR);
	if (Ctape_position (infil, TPPOSIT_FSEQ, 1, 1, 0, 0, 0, NOFILECHECK,
	    NULL, NULL, NULL, 0, 0, 0, 0))
		exit_prog (SYERR);
	Ctape_kill_needed = 0;
	if (Ctape_info (infil, NULL, NULL, aden, devtype, drive, NULL,
	    NULL, NULL, NULL))
		exit_prog (SYERR);

	Ctape_dmpmsg = (void (*) _PROTO((int, CONST char *, ...))) (&tpdump_usrmsg);
	if (Ctape_dmpinit (infil, vid, aden, drive, devtype, maxblksize,
	    fromblock, toblock, maxbyte, fromfile, maxfile, code, flags))
		exit_prog (SYERR);

	while ((c = Ctape_dmpfil (infil, NULL, NULL, NULL, NULL, NULL, NULL,
	    NULL, NULL)) == 0) continue;
	exit_prog ((c < 0) ? USERR : 0);

        /* 
           The return will actually never be called (due to exit_prog). 
           It's just inserted to make the compiler happy: Though
           the C standard allows void main( void ) and the value
           of exit() would be passed despite a void return value, the 
           compiler (gcc 3.2.3 in this case) complains about the return 
           type of void main() for not being int ...
        */
        return (0);
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

