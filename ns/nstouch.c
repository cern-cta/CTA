/*
 * Copyright (C) 2001-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: nstouch.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:23 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	nstouch - set last access and modification times */
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#endif
#include "Cns.h"
#include "Cns_api.h"
#include "serrno.h"
static time_t cvt_datime();
extern	char	*getenv();
extern	char	*optarg;
extern	int	optind;
main(argc, argv)
int argc;
char **argv;
{
	int aflag = 0;
	int c;
	int cflag = 0;
	int errflg = 0;
	char fullpath[CA_MAXPATHLEN+1];
	int i;
	int mflag = 0;
	time_t newtime;
	char *p;
	char *path;
	struct Cns_filestat statbuf;
	int tflag = 0;
	struct utimbuf times;
#if defined(_WIN32)
	WSADATA wsadata;
#endif

	newtime = time (0);
	while ((c = getopt (argc, argv, "acmt:")) != EOF) {
		switch (c) {
		case 'a':
			aflag++;
			break;
		case 'c':
			cflag++;
			break;
		case 'm':
			mflag++;
			break;
		case 't':
			tflag++;
			newtime = cvt_datime (optarg);
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
		    "usage: %s [-a] [-c] [-m] [-t time] file...\n", argv[0]);
		exit (USERR);
	}
	if (! aflag && ! mflag) {
		aflag = 1;
		mflag = 1;
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
		if ((c = Cns_stat (fullpath, &statbuf)) == 0) {
			if (aflag)
				times.actime = newtime;
			else
				times.actime = statbuf.atime;
			if (mflag)
				times.modtime = newtime;
			else
				times.modtime = statbuf.mtime;
			c = Cns_utime (fullpath, tflag ? &times : NULL);
		} else if (serrno == ENOENT) {
			if (cflag) continue;
			c = Cns_creat (fullpath, 0666);
		}
		if (c) {
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

static time_t
cvt_datime(arg)
char *arg;
{
	int cc = 0;
	time_t curtime;
	char *dp;
	int l;
	static int lastd[12] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
	int n;
	char *p;
	struct tm tm;

	memset (&tm, 0, sizeof(struct tm));
        curtime = time (0);
        tm.tm_year = localtime(&curtime)->tm_year;
	if (p = strchr (arg, '.')) {
		*p = '\0';
		if ((n = strtol (p + 1, &dp, 10)) < 0 || *dp != '\0' || n > 59)
			return (-1);
		tm.tm_sec = n;
	}
	l = strlen (arg);
	switch (l) {
	case 8:
		n = sscanf(arg, "%2d%2d%2d%2d", &tm.tm_mon, &tm.tm_mday,
		    &tm.tm_hour, &tm.tm_min);
		break;
	case 10:
		n = sscanf(arg, "%2d%2d%2d%2d%2d", &tm.tm_year, &tm.tm_mon,
		    &tm.tm_mday, &tm.tm_hour, &tm.tm_min);
		break;
	case 12:
		n = sscanf(arg, "%2d%2d%2d%2d%2d%2d", &cc, &tm.tm_year,
		    &tm.tm_mon, &tm.tm_mday, &tm.tm_hour, &tm.tm_min);
		break;
	default:
		return (-1);
	}
	if (n != l/2)
		return (-1);

	if (tm.tm_min < 0 || tm.tm_min > 59)
		return (-1);
	if (tm.tm_hour < 0 || tm.tm_hour > 23)
		return (-1);
	if (tm.tm_mon < 1 || tm.tm_mon > 12)
		return (-1);
	if (cc == 0) {
		if (tm.tm_year < 69)
			tm.tm_year += 100;
	} else
		tm.tm_year += (cc - 19) * 100;

	if ((tm.tm_mon == 2) && (tm.tm_mday == 29) && (tm.tm_year % 4 != 0))
		return (-1);
	if ((tm.tm_mday < 1) || (tm.tm_mday > lastd[tm.tm_mon-1]))
		return (-1);
	tm.tm_mon--;

	tm.tm_isdst = -1;
	return (mktime (&tm));
}
