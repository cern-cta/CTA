/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)nsrm.c,v 1.2 2006/01/26 15:36:23 CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	nsrm - remove name server directory/file entries */
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#if defined(_WIN32)
#define W_OK 2
#define S_IFLNK 0120000 // Symbolic link.
#include <winsock2.h>
#else
#include <unistd.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
extern	char	*getenv();
extern	int	optind;
#if sgi
extern char *strdup _PROTO((CONST char *));
#endif
int errflg;
int fflag;
int iflag;
int rflag;
main(argc, argv)
int argc;
char **argv;
{
	int c;
	char fullpath[CA_MAXPATHLEN+1];
	int i;
	char *p;
	char *path;
	struct Cns_filestat statbuf;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	while ((c = getopt (argc, argv, "fiRr")) != EOF) {
		switch (c) {
		case 'f':
			fflag++;
			break;
		case 'i':
			iflag++;
			break;
		case 'R':
		case 'r':
			rflag++;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (optind >= argc) {
		fprintf (stderr,
		    "usage: %s [-f] [-i] file...\n\t%s [-f] [-i] -r dirname...\n",
		    argv[0], argv[0]);
		exit (USERR);
	}
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	for (i = optind; i < argc; i++) {
		path = argv[i];
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
		if ((c = Cns_lstat (fullpath, &statbuf)) == 0) {
			if (statbuf.filemode & S_IFDIR) {
				if (rflag) {
					c = removedir (fullpath);
				} else {
					serrno = EISDIR;
					c = -1;
				}
			} else {
				if ((statbuf.filemode & S_IFLNK) != S_IFLNK &&
				    ! fflag && Cns_access (fullpath, W_OK) &&
				    isatty (fileno (stdin))) {
					printf ("override write protection for %s? ", fullpath);
					if (! isyes())
						continue;
				} else if (iflag) {
					printf ("remove %s? ", fullpath);
					if (! isyes())
						continue;
				}
				c = Cns_unlink (fullpath);
			}
		}
		if (c && (serrno != ENOENT || fflag == 0)) {
			fprintf (stderr, "%s: %s\n", path,
			    sstrerror(serrno));
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

isyes()
{
	int c;
	int fchar;

	fchar = c = getchar();
	while (c != '\n' && c != EOF)
		c = getchar();
	return (fchar == 'y');
}

removedir (dir)
char *dir;
{
	char curdir[CA_MAXPATHLEN+1];
	struct dirlist {
		char *d_name;
		struct dirlist *next;
	};
	Cns_DIR *dirp;
	struct dirlist *dlc;		/* pointer to current directory in the list */
	struct dirlist *dlf = NULL;	/* pointer to first directory in the list */
	struct dirlist *dll;		/* pointer to last directory in the list */
	struct Cns_direnstat *dxp;
	char fullpath[CA_MAXPATHLEN+1];

	if (! fflag && Cns_access (dir, W_OK) &&
	    isatty (fileno (stdin))) {
		printf ("override write protection for %s? ", dir);
		if (! isyes())
			return (0);
	} else if (iflag) {
		printf ("remove files in %s? ", dir);
		if (! isyes())
			return (0);
	}
	if ((dirp = Cns_opendir (dir)) == NULL)
		return (-1);

	if (Cns_chdir (dir) < 0)
		return (-1);
	while ((dxp = Cns_readdirx (dirp)) != NULL) {
		if (dxp->filemode & S_IFDIR) {
			if ((dlc = (struct dirlist *)
			    malloc (sizeof(struct dirlist))) == NULL ||
			    (dlc->d_name = strdup (dxp->d_name)) == NULL) {
				serrno = errno;
				return (-1);
			}
			dlc->next = 0;
			if (dlf == NULL)
				dlf = dlc;
			else
				dll->next = dlc;
			dll = dlc;
		} else {
			sprintf (fullpath, "%s/%s", dir, dxp->d_name);
			if ((dxp->filemode & S_IFLNK) != S_IFLNK &&
			    ! fflag && Cns_access (fullpath, W_OK) &&
			    isatty (fileno (stdin))) {
				printf ("override write protection for %s? ", fullpath);
				if (! isyes())
					continue;
			} else if (iflag) {
				printf ("remove %s? ", fullpath);
				if (! isyes())
					continue;
			}
			if (Cns_unlink (dxp->d_name)) {
				fprintf (stderr, "%s/%s: %s\n", dir,
				    dxp->d_name, sstrerror(serrno));
				errflg++;
			}
		}
	}
	(void) Cns_closedir (dirp);
	while (dlf) {
		sprintf (curdir, "%s/%s", dir, dlf->d_name);
		if (removedir (curdir) < 0)
			fprintf (stderr, "%s: %s\n", curdir, sstrerror(serrno));
		free (dlf->d_name);
		dlc = dlf;
		dlf = dlf->next;
		free (dlc);
	}
	if (Cns_chdir ("..") < 0)
		return (-1);
	if (iflag) {
		printf ("remove %s? ", dir);
		if (! isyes())
			return (0);
	}
	if (Cns_rmdir (dir)) {
		fprintf (stderr, "%s: %s\n", dir, (serrno == EEXIST) ?
		    "Directory not empty" : sstrerror(serrno));
		errflg++;
	}
	return (0);
}
