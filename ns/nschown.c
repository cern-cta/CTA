/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*	nschown - change directory/file ownership in name server */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <ctype.h>
#include <getopt.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
extern	char	*getenv();
extern	int	optind;
#if sgi
extern char *strdup _PROTO((CONST char *));
#endif
int hflag;
int Rflag;
int chowndir (char *dir,uid_t newuid,gid_t newgid);

int main(int argc,char **argv)
{
	int c;
	char *dp;
	int errflg = 0;
	char fullpath[CA_MAXPATHLEN+1];
	struct group *gr;
	int i;
	gid_t newgid;
	uid_t newuid;
	char *p;
	char *path;
	struct passwd *pwd;
	struct Cns_filestat statbuf;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	while ((c = getopt (argc, argv, "hR")) != EOF) {
		switch (c) {
		case 'h':
			hflag++;
			break;
		case 'R':
			Rflag++;
			break;
		case '?':
			errflg++;
			break;
		default:
			break;
		}
	}
	if (optind >= argc - 1) {
		fprintf (stderr,
		    "usage: %s [-h] [-R] owner[{.|:}group] file...\n", argv[0]);
		exit (USERR);
	}
	p = strtok (argv[optind], ":.");
	if (isdigit (*p)) {
		newuid = strtol (p, &dp, 10);
		if (*dp != '\0') {
			fprintf (stderr, "invalid user: %s\n", p);
			errflg++;
		}
	} else {
#ifdef VIRTUAL_ID
		if (strcmp (p, "root") == 0)
			newuid = 0;
		else if (Cns_getusrbynam (p, &newuid) < 0) {
#else
		if ((pwd = getpwnam (p)))
			newuid = pwd->pw_uid;
		else {
#endif
			fprintf (stderr, "invalid user: %s\n", p);
			errflg++;
		}
	}
    if ((p = strtok (NULL, ":."))) {
		if (isdigit (*p)) {
			newgid = strtol (p, &dp, 10);
			if (*dp != '\0') {
				fprintf (stderr, "invalid group: %s\n", p);
				errflg++;
			}
		} else {
#ifdef VIRTUAL_ID
			if (strcmp (p, "root") == 0)
				newgid = 0;
			else if (Cns_getgrpbynam (p, &newgid) < 0) {
#else
			if ((gr = getgrnam (p)))
				newgid = gr->gr_gid;
			else {
#endif
				fprintf (stderr, "invalid group: %s\n", p);
				errflg++;
			}
		}
	} else
		newgid = -1;
	if (errflg)
		exit (USERR);
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	for (i = optind+1; i < argc; i++) {
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
                if (Cns_lstat (fullpath, &statbuf) < 0) {
			fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
			errflg++;
			continue;
		}
		if (Rflag && (statbuf.filemode & S_IFDIR))
			if (chowndir (fullpath, newuid, newgid))
				errflg++;
		if (hflag) {
			if (Cns_lchown (fullpath, newuid, newgid)) {
				fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
				errflg++;
			}
		} else {
			if (Cns_chown (fullpath, newuid, newgid)) {
				fprintf (stderr, "%s: %s\n", path, sstrerror(serrno));
				errflg++;
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

int chowndir (char *dir,uid_t newuid,gid_t newgid)
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
	int errflg = 0;

	if ((dirp = Cns_opendir (dir)) == NULL) {
		fprintf (stderr, "%s: %s\n", dir, sstrerror(serrno));
		return (-1);
	}
	if (Cns_chdir (dir) < 0) {
		fprintf (stderr, "%s: %s\n", dir, sstrerror(serrno));
		return (-1);
	}
	while ((dxp = Cns_readdirx (dirp)) != NULL) {
		if (Rflag && (dxp->filemode & S_IFDIR)) {
			if ((dlc = (struct dirlist *)
			    malloc (sizeof(struct dirlist))) == NULL ||
			    (dlc->d_name = strdup (dxp->d_name)) == NULL) {
				fprintf (stderr, "%s: %s\n", dxp->d_name,
				    sstrerror(serrno));
				return (-1);
			}
			dlc->next = 0;
			if (dlf == NULL)
				dlf = dlc;
			else
				dll->next = dlc;
			dll = dlc;
		} else {
			if (Cns_chown (dxp->d_name, newuid, newgid) < 0) {
				fprintf (stderr, "%s: %s\n", dxp->d_name,
				    sstrerror(serrno));
				errflg++;
			}
		}
	}
	(void) Cns_closedir (dirp);
	while (dlf) {
		sprintf (curdir, "%s/%s", dir, dlf->d_name);
		if (chowndir (curdir, newuid, newgid) < 0)
			errflg++;
		if (Cns_chown (dlf->d_name, newuid, newgid) < 0) {
			fprintf (stderr, "%s: %s\n", dlf->d_name,
			    sstrerror(serrno));
			errflg++;
		}
		free (dlf->d_name);
		dlc = dlf;
		dlf = dlf->next;
		free (dlc);
	}
	if (Cns_chdir ("..") < 0)
		return (-1);
	return (errflg);
}
