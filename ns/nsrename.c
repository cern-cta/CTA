/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*	nsrename - rename entries in name server */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <stdlib.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
extern	char	*getenv();
int main(argc, argv)
int argc;
char **argv;
{
	char newpath[CA_MAXPATHLEN+1];
	char oldpath[CA_MAXPATHLEN+1];
	int errflg = 0;
	char *p;
	char *path;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	if (argc != 3) {
		fprintf (stderr,
		    "usage: %s oldname newname...\n", argv[0]);
		exit (USERR);
	}
	path = argv[1];
	if (*path != '/' && strstr (path, ":/") == NULL) {
		if ((p = getenv (CNS_HOME_ENV)) == NULL ||
		    strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
			fprintf (stderr, "%s: invalid path\n", path);
			errflg++;
		} else
			sprintf (oldpath, "%s/%s", p, path);
	} else {
		if (strlen (path) > CA_MAXPATHLEN) {
			fprintf (stderr, "%s: %s\n", path,
			    sstrerror(SENAMETOOLONG));
			errflg++;
		} else
			strcpy (oldpath, path);
	}
	path = argv[2];
	if (*path != '/' && strstr (path, ":/") == NULL) {
		if ((p = getenv (CNS_HOME_ENV)) == NULL ||
		    strlen (p) + strlen (path) + 1 > CA_MAXPATHLEN) {
			fprintf (stderr, "%s: invalid path\n", path);
			errflg++;
		} else
			sprintf (newpath, "%s/%s", p, path);
	} else {
		if (strlen (path) > CA_MAXPATHLEN) {
			fprintf (stderr, "%s: %s\n", path,
			    sstrerror(SENAMETOOLONG));
			errflg++;
		} else
			strcpy (newpath, path);
	}
	if (errflg)
		exit (USERR);
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	if (Cns_rename (oldpath, newpath) < 0) {
		fprintf (stderr, "cannot rename to %s: %s\n", path,
		    sstrerror(serrno));
#if defined(_WIN32)
		WSACleanup();
#endif
		exit (USERR);
	}
#if defined(_WIN32)
	WSACleanup();
#endif
	exit (0);
}
