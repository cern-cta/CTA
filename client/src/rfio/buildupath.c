/*
 * $Id: buildupath.c,v 1.1 2004/10/18 21:46:44 jdurand Exp $
 */

/*
 * Copyright (C) 1994-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: buildupath.c,v $ $Revision: 1.1 $ $Date: 2004/10/18 21:46:44 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#endif
#include "osdep.h"
#include "Castor_limits.h"
#include "stage_constants.h"
#include "stage_messages.h"
#include "Cglobals.h"
#include "serrno.h"
#include "stage_api.h"

static int cwd_key = -1;
static int hostname_key = -1;
static int initialized_key = -1;
static int nfsroot_key = -1;

/*
static char cwd[256];
static char hostname[CA_MAXHOSTNAMELEN + 1];
static int initialized = 0;
static char nfsroot[MAXPATH];
*/

static int init_cwd_hostname _PROTO(());
static int resolvelinks _PROTO((char *, char *, int, int));

static int init_cwd_hostname()
{
	char *getconfent();
	char *getcwd();
	int n = 0;
	char *p;
	char *func = "init_cwd_hostname";
	char *cwd = NULL;
	char *hostname = NULL;
	int *initialized = NULL;
	char *nfsroot = NULL;

	Cglobals_get (&cwd_key, (void **)&cwd, (size_t) 256);
	Cglobals_get (&hostname_key, (void **)&hostname, (size_t) (CA_MAXHOSTNAMELEN + 1));
	Cglobals_get (&initialized_key, (void **)&initialized, sizeof(int));
	Cglobals_get (&nfsroot_key, (void **)&nfsroot, (size_t) (MAXPATH));

	if ((cwd == NULL) || (hostname == NULL) || (initialized == NULL) || (nfsroot == NULL)) return(SESYSERR);

	*initialized = 1;
	p = getconfent ("RFIO", "NFS_ROOT", 0);
#ifdef NFSROOT
	if (p == NULL) p = NFSROOT;
#endif

	if (p != NULL)
		strcpy (nfsroot, p);
	else
		nfsroot[0] = '\0';
	gethostname (hostname, CA_MAXHOSTNAMELEN + 1);
#if defined(_WIN32)
	p = getcwd (cwd, (size_t) 256);
#else
	while ((p = getcwd (cwd, (size_t) 256)) == NULL && errno == ETIMEDOUT) {
		if (n++ == 0)
			stage_errmsg(func, STG02, "", "getcwd", strerror(errno));
		stage_sleep (RETRYI);
	}
#endif
	if (p == NULL) {
		stage_errmsg(func, STG02, "", "getcwd", strerror(errno));
		return (SESYSERR);
	}
	return (0);
}

static int resolvelinks(argvi, buf, buflen, req_type)
		 char *argvi;
		 char *buf;
		 int buflen;
		 int req_type;
{
	char *dir;
	char dsksrvr[CA_MAXHOSTNAMELEN + 1];
	char *getcwd();
	char linkbuf[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *p, *q;
	char *func = "resolvelinks";
	char *cwd = NULL;
	char *hostname = NULL;
	char *nfsroot = NULL;

	Cglobals_get (&cwd_key, (void **)&cwd, (size_t) 256);
	Cglobals_get (&hostname_key, (void **)&hostname, (size_t) (CA_MAXHOSTNAMELEN + 1));
	Cglobals_get (&nfsroot_key, (void **)&nfsroot, (size_t) (MAXPATH));

	if ((cwd == NULL) || (hostname == NULL) || (nfsroot == NULL)) return(-1);

	if ((p = strstr (argvi, ":/")) != NULL) {
		strncpy (dsksrvr, argvi, p - argvi);
		dsksrvr[p - argvi] = '\0';
		dir = ++p;
		q = dir + strlen(nfsroot);
		if (! *nfsroot ||
				strncmp (dir, nfsroot, strlen (nfsroot)) || *q != '/')	
			/* server:/xxxx    where xxxx is not nfsroot */
			if (strcmp (dsksrvr, hostname)) {
				/* server is not the local machine */
				if (strlen (argvi) + 1 > buflen) return (-1);
				strcpy (buf, argvi);
				return (0);
			}
	} else
		dir = argvi;
	q = dir + strlen(nfsroot);
	if (*nfsroot &&
			strncmp (dir, nfsroot, strlen (nfsroot)) == 0 && *q == '/' &&
			(p = strchr (q + 1, '/')) != NULL) {
		/* /shift syntax */
		strncpy (dsksrvr, q + 1, p - q - 1);
		dsksrvr[p - q - 1] = '\0';
		if (strcmp (dsksrvr, hostname)) { /* not on local machine */
			if (strlen (dir) + 1 > buflen) return (-1);
			strcpy (buf, dir);
			return (0);
		}
	}
#if defined(_WIN32)
	strcpy (linkbuf, dir);	/* links are not supported */
#else
	if (chdir (dir) < 0) {
		stage_errmsg(func, STG02, dir, "chdir", strerror(errno));
		if (req_type == STAGECLR || req_type == STAGE_CLR) {	/* let stgdaemon try */
			if (strlen (dir) + 1 > buflen) return (-1);
			strcpy (buf, dir);
			return (0);
		} else
			return (-1);
	}
	if (getcwd (linkbuf, sizeof(linkbuf)) == NULL) {
		stage_errmsg(func, STG02, argvi, "getcwd", strerror(errno));
		return (-1);
	}
	if (chdir (cwd) < 0) {
		stage_errmsg(func, STG02, cwd, "chdir", strerror(errno));
		return (-1);
	}
#endif
	if ((! *nfsroot ||
			 strncmp (linkbuf, nfsroot, strlen (nfsroot)) ||
			 *(linkbuf + strlen(nfsroot)) != '/') /* not /shift syntax */
			&& (strncmp (linkbuf, "/afs/", 5) || (req_type == STAGEWRT || req_type == STAGE_WRT)))
		if ((int) strlen (hostname) + (int) strlen (linkbuf) + 2 > buflen)
			return (-1);
		else
			sprintf (buf, "%s:%s", hostname, linkbuf);
	else
		if ((int) strlen (linkbuf) + 1 > buflen)
			return (-1);
		else
			strcpy (buf, linkbuf);
	return (0);
}

int DLL_DECL build_linkname(argvi, path, size, req_type)
		 char *argvi;
		 char *path;
		 size_t size;
		 int req_type;
{
	char buf[CA_MAXPATHLEN+1];
	int c;
	char *p;
	char *func = "build_linkname";
	char *cwd = NULL;
	char *hostname = NULL;
	int *initialized = NULL;
	char *nfsroot = NULL;

	Cglobals_get (&cwd_key, (void **)&cwd, (size_t) 256);
	Cglobals_get (&hostname_key, (void **)&hostname, (size_t) (CA_MAXHOSTNAMELEN + 1));
	Cglobals_get (&initialized_key, (void **)&initialized, sizeof(int));
	Cglobals_get (&nfsroot_key, (void **)&nfsroot, (size_t) (MAXPATH));

	if ((cwd == NULL) || (hostname == NULL) || (initialized == NULL) || (nfsroot == NULL)) return(SESYSERR);

	if (! *initialized && (c = init_cwd_hostname())) return (c);
	if (*argvi != '/' && strstr (argvi, ":/") == NULL) {
		if ((! *nfsroot ||
				 strncmp (cwd, nfsroot, strlen (nfsroot)) ||
				 *(cwd + strlen(nfsroot)) != '/') /* not /shift syntax */
				&& (strncmp (cwd, "/afs/", 5) || (req_type == STAGEWRT || req_type == STAGE_WRT)))
			if ((int) strlen (hostname) + (int) strlen (cwd) +
					(int) strlen (argvi) + 3 > size) {
				stage_errmsg(func, STG08, argvi);
				return (EINVAL);
			} else
				sprintf (path, "%s:%s/%s", hostname, cwd, argvi);
		else
			if ((int) strlen (cwd) + (int) strlen (argvi) + 2 > size) {
				stage_errmsg(func, STG08, argvi);
				return (EINVAL);
			} else
				sprintf (path, "%s/%s", cwd, argvi);
	} else {
		if (strlen(argvi) > CA_MAXPATHLEN) {
			stage_errmsg(func, STG08, argvi);
			return (EINVAL);
		}
		strcpy (buf, argvi);
		if ((p = strrchr (buf, '/')) != NULL) {
			*p = '\0';
			if (resolvelinks (buf, path, size - strlen (p+1) - 1, req_type) < 0) {
				stage_errmsg(func, STG08, argvi);
				return (EINVAL);
			}
			*p = '/';
		} else {
			stage_errmsg(func, STG08, argvi);
			return (EINVAL);
		}
		if (((int) strlen (path) + (int) strlen (p) + 1) > size) {
			stage_errmsg(func, STG08, argvi);
			return (EINVAL);
		}
		strcat (path, p);
	}
	return (0);
}

int DLL_DECL build_Upath(fun, path, size, req_type)
		 int fun;
		 char *path;
		 size_t size;
		 int req_type;
{
#if !defined(_WIN32)
	char buf[16];
	int c;
	char *func = "build_Upath";
	char *cwd = NULL;
	char *hostname = NULL;
	int *initialized = NULL;
	char *nfsroot = NULL;

	Cglobals_get (&cwd_key, (void **)&cwd, (size_t) 256);
	Cglobals_get (&hostname_key, (void **)&hostname, (size_t) (CA_MAXHOSTNAMELEN + 1));
	Cglobals_get (&initialized_key, (void **)&initialized, sizeof(int));
	Cglobals_get (&nfsroot_key, (void **)&nfsroot, (size_t) (MAXPATH));

	if ((cwd == NULL) || (hostname == NULL) || (initialized == NULL) || (nfsroot == NULL)) return(SESYSERR);

	if (! *initialized && (c = init_cwd_hostname())) return (c);
#if _IBMESA
	sprintf (buf, "FILE.FT%d.F01", fun);
#else
#if hpux
	sprintf (buf, "ftn%02d", fun);
#else
	sprintf (buf, "fort.%d", fun);
#endif
#endif
	if ((! *nfsroot ||
			 strncmp (cwd, nfsroot, strlen (nfsroot)) ||
			 *(cwd + strlen(nfsroot)) != '/') /* not /shift syntax */
			&& (strncmp (cwd, "/afs/", 5) || (req_type == STAGEWRT || req_type == STAGE_WRT)))
		if ((int) strlen (hostname) + (int) strlen (cwd) +
				(int) strlen (buf) + 3 > size) {
			stage_errmsg(func, STG08, buf);
			return (EINVAL);
		} else
			sprintf (path, "%s:%s/%s", hostname, cwd, buf);
	else
		if ((int) strlen (cwd) + (int) strlen (buf) + 2 > size) {
			stage_errmsg(func, STG08, buf);
			return (EINVAL);
		} else
			sprintf (path, "%s/%s", cwd, buf);
#endif
	return (0);
}
