	/*
 * Copyright (C) 2003-2004 by CERN/IT/GD/CT
 * All rights reserved
 */
 
/*	nsln - make a symbolic link to a file or a directory */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
#include <getopt.h>

extern	char	*getenv();
extern	int	optind;
int sflag;
int main(argc, argv)
int argc;
char **argv;
{
	int c;
	int errflg = 0;
	char fullpath[CA_MAXPATHLEN+1];
	int i;
	char *lastarg;
	char *p;
	char path[CA_MAXPATHLEN+1];
	struct Cns_filestat statbuf;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	while ((c = getopt (argc, argv, "s")) != EOF) {
		switch (c) {
		case 's':
			sflag++;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (! sflag)
		errflg++;
	if (errflg || optind >= argc) {
		fprintf (stderr,
		    "usage: %s -s file [link]\n\t%s -s file...directory\n",
		    argv[0], argv[0]);
		exit (USERR);
	}
	if (argc - optind > 2) {
		if (Cns_stat (argv[argc-1], &statbuf) < 0 ||
		    (statbuf.filemode & S_IFDIR) == 0) {
			fprintf (stderr, "target %s must be a directory\n",
			    argv[argc-1]);
			exit (USERR);
		}
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	argc -= optind;
	argv += optind;
	if (argc == 1)
		if ((p = strrchr (argv[0], '/')))
			lastarg = p + 1;
		else
			lastarg = argv[0];
	else
		lastarg = argv[--argc];
	for (i = 0; i < argc; i++) {
		if (argc > 1)
			if ((p = strrchr (argv[i], '/')))
				sprintf (path, "%s/%s", lastarg, p + 1);
			else
				sprintf (path, "%s/%s", lastarg, argv[i]);
		else
			strcpy (path, lastarg);
		if (*path != '/' && strstr (path, ":/") == NULL) {
			if ((p = getenv (CNS_HOME_ENV)) == NULL ||
			    strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
				fprintf (stderr, "%s: invalid path\n", path);
				errflg++;
				continue;
			} else
				sprintf (fullpath, "%s/%s", p, path);
		} else {
			if (strlen (path) > CA_MAXPATHLEN) {
				fprintf (stderr, "%s: %s\n", path,
				    sstrerror(SENAMETOOLONG));
				errflg++;
				continue;
			} else
				strcpy (fullpath, path);
		}
		if (Cns_symlink (argv[i], fullpath)) {
			fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
			errflg++;
		}
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	if (errflg)
		exit (USERR);
	exit (0);
}
