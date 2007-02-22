/*
 * Copyright (C) 2000-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	nsfind - search for files in name server */
#include <errno.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#include "statbits.h"
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "Cregexp.h"
#include "serrno.h"
#include "u64subr.h"
#define ONEDAY (24*60*60)
#define SIXMONTHS (6*30*24*60*60)
extern	char	*getenv();
#if sgi
extern char *strdup _PROTO((CONST char *));
#endif
int procpath (char *dir);
char atimeflg;
int atimeval;
Cregexp_t *expstruct;
char ctimeflg;
int ctimeval;
int errflg;
u_signed64 fileid;
int lsflag;
char mtimeflg;
int mtimeval;
time_t current_time;


int main(argc, argv)
int argc;
char **argv;
{
	char fullpath[CA_MAXPATHLEN+1];
	int i;
	int lastpath = 0;
	char *p;
	char *path;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	for (i = 1; i < argc; i++) {
		if (*argv[i] != '-') {	/* path */
			lastpath = i;
		} else {
			if (strcmp (argv[i], "-atime") == 0) {
				if (i >= argc - 1) {
					errflg++;
					continue;
				}
				i++;
				atimeflg = *argv[i];
				if (atimeflg == '-' || atimeflg == '+')
					atimeval = atoi (argv[i]+1);
				else
					atimeval = atoi (argv[i]);
			} else if (strcmp (argv[i], "-ctime") == 0) {
				if (i >= argc - 1) {
					errflg++;
					continue;
				}
				i++;
				ctimeflg = *argv[i];
				if (ctimeflg == '-' || ctimeflg == '+')
					ctimeval = atoi (argv[i]+1);
				else
					ctimeval = atoi (argv[i]);
			} else if (strcmp (argv[i], "-inum") == 0) {
				if (i >= argc - 1) {
					errflg++;
					continue;
				}
				i++;
				fileid = strtou64 (argv[i]);
			} else if (strcmp (argv[i], "-ls") == 0) {
				lsflag = 1;
			} else if (strcmp (argv[i], "-mtime") == 0) {
				if (i >= argc - 1) {
					errflg++;
					continue;
				}
				i++;
				mtimeflg = *argv[i];
				if (mtimeflg == '-' || mtimeflg == '+')
					mtimeval = atoi (argv[i]+1);
				else
					mtimeval = atoi (argv[i]);
			} else if (strcmp (argv[i], "-name") == 0) {
				if (i >= argc - 1) {
					errflg++;
					continue;
				}
				i++;
				if ((expstruct = Cregexp_comp (argv[i])) == NULL)
					errflg++;
			} else
				errflg++;
		}
	}
	if (lastpath == 0 || errflg) {
		fprintf (stderr,
		    "usage: %s path-list [predicate-list]\n", argv[0]);
		exit (USERR);
	}
	(void) time (&current_time);
#if defined(_WIN32)
	if (WSAStartup (MAKEWORD (2, 0), &wsadata)) {
		fprintf (stderr, NS052);
		exit (SYERR);
	}
#endif
	for (i = 1; i <= lastpath; i++) {
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
		if (procpath (fullpath)) {
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

int listentry(dir, path, statbuf)
char *dir;
char *path;
struct Cns_filestat *statbuf;
{
	struct group *gr;
	char modestr[11];
	struct passwd *pw;
	static gid_t sav_gid = -1;
	static char sav_gidstr[7];
	time_t ltime;
	static uid_t sav_uid = -1;
	static char sav_uidstr[CA_MAXUSRNAMELEN+1];
	char timestr[13];
	struct tm *tm;
	char tmpbuf[21];

	if (statbuf->status == 'D')
		return (0);
	if (lsflag) {
		printf ("%s ", u64tostr (statbuf->fileid, tmpbuf, 20));
		if (statbuf->filemode & S_IFDIR)
			modestr[0] = 'd';
		else
			modestr[0] = statbuf->status;
		modestr[1] = (statbuf->filemode & S_IRUSR) ? 'r' : '-';
		modestr[2] = (statbuf->filemode & S_IWUSR) ? 'w' : '-';
		if (statbuf->filemode & S_IXUSR)
			if (statbuf->filemode & S_ISUID)
				modestr[3] = 's';
			else
				modestr[3] = 'x';
		else
			modestr[3] = '-';
		modestr[4] = (statbuf->filemode & S_IRGRP) ? 'r' : '-';
		modestr[5] = (statbuf->filemode & S_IWGRP) ? 'w' : '-';
		if (statbuf->filemode & S_IXGRP)
			if (statbuf->filemode & S_ISGID)
				modestr[6] = 's';
			else
				modestr[6] = 'x';
		else
			modestr[6] = '-';
		modestr[7] = (statbuf->filemode & S_IROTH) ? 'r' : '-';
		modestr[8] = (statbuf->filemode & S_IWOTH) ? 'w' : '-';
		if (statbuf->filemode & S_IXOTH)
			if (statbuf->filemode & S_ISVTX)
				modestr[9] = 't';
			else
				modestr[9] = 'x';
		else
			modestr[9] = '-';
		modestr[10] = '\0';
		if (statbuf->uid != sav_uid) {
			sav_uid = statbuf->uid;
			if ((pw = getpwuid (sav_uid)))
				strcpy (sav_uidstr, pw->pw_name);
			else
				sprintf (sav_uidstr, "%-8u", sav_uid);
		}
		if (statbuf->gid != sav_gid) {
			sav_gid = statbuf->gid;
			if ((gr = getgrgid (sav_gid)))
				strcpy (sav_gidstr, gr->gr_name);
			else
				sprintf (sav_gidstr, "%-6u", sav_gid);
		}
		ltime = statbuf->mtime;
		tm = localtime (&ltime);
		if (ltime < current_time - SIXMONTHS ||
		    ltime > current_time + 60)
			strftime (timestr, 13, "%b %d  %Y", tm);
		else
			strftime (timestr, 13, "%b %d %H:%M", tm);
		printf ("%s %3d %-8.8s %-6.6s %s %s ",
		    modestr, statbuf->nlink, sav_uidstr, sav_gidstr,
	    u64tostr (statbuf->filesize, tmpbuf, 20), timestr);
	}
	printf ("%s/%s", dir, path);
	printf ("\n");
	return (0);
}

int procpath (char *dir)
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
		}
		if (atimeflg) {
			if (atimeflg == '-') {
				 if ((current_time - dxp->atime) / ONEDAY > atimeval)
					continue;
			} else if (atimeflg == '+') {
				 if ((current_time - dxp->atime) / ONEDAY < atimeval)
					continue;
			} else
				 if ((current_time - dxp->atime) / ONEDAY != atimeval)
					continue;
		}
		if (ctimeflg) {
			if (ctimeflg == '-') {
				 if ((current_time - dxp->ctime) / ONEDAY > ctimeval)
					continue;
			} else if (ctimeflg == '+') {
				 if ((current_time - dxp->ctime) / ONEDAY < ctimeval)
					continue;
			} else
				 if ((current_time - dxp->ctime) / ONEDAY != ctimeval)
					continue;
		}
		if (fileid && dxp->fileid != fileid) continue;
		if (expstruct && Cregexp_exec (expstruct, dxp->d_name)) continue;
		if (mtimeflg) {
			if (mtimeflg == '-') {
				 if ((current_time - dxp->mtime) / ONEDAY > mtimeval)
					continue;
			} else if (mtimeflg == '+') {
				 if ((current_time - dxp->mtime) / ONEDAY < mtimeval)
					continue;
			} else
				 if ((current_time - dxp->mtime) / ONEDAY != mtimeval)
					continue;
		}
		listentry (dir, dxp->d_name, (struct Cns_filestat *)dxp);
	}
	(void) Cns_closedir (dirp);
	while (dlf) {
		sprintf (curdir, "%s/%s", dir, dlf->d_name);
		if (procpath (curdir) < 0) {
			fprintf (stderr, "%s: %s\n", curdir, sstrerror(serrno));
			errflg++;
		}
		free (dlf->d_name);
		dlc = dlf;
		dlf = dlf->next;
		free (dlc);
	}
	if (Cns_chdir ("..") < 0)
		return (-1);
	return (0);
}
