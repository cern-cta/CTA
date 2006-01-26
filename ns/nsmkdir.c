/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nsmkdir.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:22 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	nsmkdir - make name server directory entries */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#include "statbits.h"
#define F_OK 0
#else
#include <unistd.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
extern	char	*getenv();
extern	char	*optarg;
extern	int	optind;
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char Cnsdir[CA_MAXPATHLEN+1];
	char *dir;
	char *dp;
	char *endp;
	int errflg = 0;
	int i;
	mode_t mask;
	int mflag = 0;
	mode_t mode;
	char *p;
	int pflag = 0;
	struct Cns_filestat statbuf;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	mode = 0777;
	while ((c = getopt (argc, argv, "m:p")) != EOF) {
		switch (c) {
		case 'm':
			mflag++;
			mode = strtol (optarg, &dp, 8);
			if (*dp != '\0') {
				fprintf (stderr, "invalid value for option -m\n");
				errflg++;
			}
			break;
		case 'p':
			pflag++;
			mask = Cns_umask (0);
			Cns_umask (mask);
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (optind >= argc) {
		errflg++;
	}
	if (errflg) {
		fprintf (stderr,
		    "usage: %s [-m absolute_mode] [-p] dirname...\n", argv[0]);
		exit (USERR);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	for (i = optind; i < argc; i++) {
		dir = argv[i];
		if (*dir != '/' && strstr (dir, ":/") == NULL) {
			if ((p = getenv (CNS_HOME_ENV)) == NULL ||
			    strlen (p) + strlen (dir) + 1 > CA_MAXPATHLEN) {
				fprintf (stderr, "cannot create %s: invalid path\n",
				dir);
				errflg++;
				continue;
			} else
				sprintf (Cnsdir, "%s/%s", p, dir);
		} else {
			if (strlen (dir) > CA_MAXPATHLEN) {
				fprintf (stderr, "cannot create %s: %s\n", dir,
				    sstrerror(SENAMETOOLONG));
				errflg++;
				continue;
			} else
				strcpy (Cnsdir, dir);
		}
		if (! pflag) {
			if ((c = Cns_mkdir (Cnsdir, mode)) < 0) {
				fprintf (stderr, "cannot create %s: %s\n", dir,
				    sstrerror(serrno));
				errflg++;
				continue;
			}
		} else {
			if (Cns_lstat (Cnsdir, &statbuf) == 0) {
				if ((statbuf.filemode & S_IFDIR) == 0) {
					fprintf (stderr, "cannot create %s: %s\n",
					    dir, strerror(EEXIST));
					errflg++;
				}
				continue;
			}
			endp = strrchr (Cnsdir, '/');
			p = endp;
			while (p > Cnsdir) {
				*p = '\0';
				if (Cns_access (Cnsdir, F_OK) == 0) break;
				p = strrchr (Cnsdir, '/');
			}
			/* make sure that mask allow write/execute for owner */
			if (mask & 0300)
				Cns_umask (mask & ~ 0300);
			while (p <= endp) {
				*p = '/';
				if (p == endp && mask & 0300)
					Cns_umask (mask);
				c = Cns_mkdir (Cnsdir, 0777);
				if (c < 0 && serrno != EEXIST) {
					fprintf (stderr, "cannot create %s: %s\n", dir,
					    sstrerror(serrno));
					errflg++;
					p = endp + 1;	/* exit from the loop */
					continue;
				}
				p += strlen (p);
			}
		}
		if (c == 0	/* directory successfully created */
		    && mflag) {
			/* must set requested mode + possible S_ISGID */
			if (Cns_stat (Cnsdir, &statbuf) < 0) {
				fprintf (stderr, "cannot stat %s: %s\n",
				    dir, sstrerror(serrno));
				errflg++;
				continue;
			}
			if (Cns_chmod (Cnsdir, (mode | statbuf.filemode & S_ISGID)) < 0) {
				fprintf (stderr, "cannot chmod %s: %s\n",
				    dir, sstrerror(serrno));
				errflg++;
				continue;
			}
		}
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	if (errflg)
		exit (USERR);
	exit (0);
}
