/*
 * Copyright (C) 1997-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)uxid.c	1.7 03/26/99 CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
#define _POSIX_
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <windows.h>
#include "grp.h"
#include "pwd.h"
#ifndef UXGRPFILE
#define UXGRPFILE "%SystemRoot%\\system32\\drivers\\etc\\group"
#endif
#ifndef UXPWDFILE
#define UXPWDFILE "%SystemRoot%\\system32\\drivers\\etc\\passwd"
#endif
FILE *fopen();
char *getenv();

static char getgrbuf[1024];
static char getpwbuf[1024];
static gid_t winux_gid = -1;
static uid_t winux_uid = -1;

char *
cuserid(bufp)
char *bufp;
{
	DWORD bufl = L_cuserid;
	char *p;
	static char winux_cuserid[L_cuserid];

	if (bufp == NULL)
		p = winux_cuserid;
	else
		p = bufp;
	if (! GetUserName (p, &bufl)) {
		if (bufp)
			*bufp = 0;
		return (NULL);
	}
	return (p);
}
 
struct group *
fillgrpent(buf)
char *buf;
{
	char *dp;
	static struct group gr;
	static char **group_members = NULL;
	int n;
	static nb_group_members = -1;
	char *p, *q;

	*(buf + strlen(buf) - 1) = '\0';
	gr.gr_name = buf;
	if ((p = strchr (buf, ':')) == NULL) return (NULL);
	*p = '\0';
	gr.gr_passwd = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	q = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	gr.gr_gid = strtol (q, &dp, 10);
	if (*dp != '\0') return (NULL);
	q = ++p;
	n = 0;
	if (*p != '\0') n = 1;
	while (p = strchr (p, ',')) {
		n++;
		p++;
	}
	if (n != nb_group_members) {
		if (group_members) free (group_members);
		group_members = (char **) malloc ((n + 1) * sizeof(char *));
		if (group_members == NULL) return (NULL);
		nb_group_members = n;
		gr.gr_mem = group_members;
		group_members[nb_group_members] = NULL;
	}
	if (nb_group_members == 0) return (&gr);
	n = 0;
	p = q;
	while (p = strchr (q, ',')) {
		group_members[n++] = q;
		*p = '\0';
		q = ++p;
	}
	group_members[n++] = q;
	return (&gr);
}
 
struct passwd *
fillpwent(buf)
char *buf;
{
	char *dp;
	char *p, *q;
	static struct passwd pw;

	pw.pw_name = buf;
	if ((p = strchr (buf, ':')) == NULL) return (NULL);
	*p = '\0';
	pw.pw_passwd = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	q = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw.pw_uid = strtol (q, &dp, 10);
	if (*dp != '\0') return (NULL);
	q = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw.pw_gid = strtol (q, &dp, 10);
	if (*dp != '\0') return (NULL);
	pw.pw_gecos = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw.pw_dir = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw.pw_shell = ++p;
	return (&pw);
}
gid_t
getgid()
{
	struct passwd *pw;

	if (winux_gid >= 0)
		return (winux_gid);
	if ((pw = getpwnam (cuserid (NULL))) == NULL)
		return (-1);
	winux_gid = pw->pw_gid;
	if (winux_uid < 0) winux_uid = pw->pw_uid;
	return (winux_gid);
}

struct group *
getgrgid(gid)
gid_t gid;
{
	char *dp;
	int found = 0;
	gid_t g;
	char *p, *p_gid;
	FILE *s;
	char uxgrpfile[256];

	if (gid < 0)
		return (NULL);
	if (strncmp (UXGRPFILE, "%SystemRoot%\\", 13) == 0 &&
	    (p = getenv ("SystemRoot")))
		sprintf (uxgrpfile, "%s%s", p, strchr (UXGRPFILE, '\\'));
	else
		strcpy (uxgrpfile, UXGRPFILE);
	if ((s = fopen (uxgrpfile, "r")) == NULL)
		return (NULL);
	while (fgets (getgrbuf, sizeof(getgrbuf), s) != NULL) {
		if (getgrbuf[0] == '+') break;	/* NIS is not supported */
		if ((p = strchr (getgrbuf, ':')) == NULL) break;
		if ((p = strchr (++p, ':')) == NULL) break;
		p_gid = ++p;
		if ((p = strchr (p_gid, ':')) == NULL) break;
		*p = '\0';
		g = strtol (p_gid, &dp, 10);
		if (*dp != '\0') break;
		if (g == gid) {
			*p = ':';
			found = 1;
			break;
		}
	}
	(void) fclose (s);
	if (! found) return (NULL);
	return (fillgrpent (getgrbuf));
}

struct group *
getgrnam(name)
char *name;
{
	int found = 0;
	char *p;
	FILE *s;
	char uxgrpfile[256];

	if (name == NULL || *name == '\0')
		return (NULL);
	if (strncmp (UXGRPFILE, "%SystemRoot%\\", 13) == 0 &&
	    (p = getenv ("SystemRoot")))
		sprintf (uxgrpfile, "%s%s", p, strchr (UXGRPFILE, '\\'));
	else
		strcpy (uxgrpfile, UXGRPFILE);
	if ((s = fopen (uxgrpfile, "r")) == NULL)
		return (NULL);
	while (fgets (getgrbuf, sizeof(getgrbuf), s) != NULL) {
		if (getgrbuf[0] == '+') break;	/* NIS is not supported */
		if ((p = strchr (getgrbuf, ':')) == NULL) break;
		*p = '\0';
		if (strcmp (getgrbuf, name) == 0) {
			*p = ':';
			found = 1;
			break;
		}
	}
	(void) fclose (s);
	if (! found) return (NULL);
	return (fillgrpent (getgrbuf));
}

struct passwd *
getpwnam(name)
char    *name;
{
	int found = 0;
	char *p;
	FILE *s;
	char uxpwdfile[256];

	if (name == NULL || *name == '\0')
		return (NULL);
	if (strncmp (UXPWDFILE, "%SystemRoot%\\", 13) == 0 &&
	    (p = getenv ("SystemRoot")))
		sprintf (uxpwdfile, "%s%s", p, strchr (UXPWDFILE, '\\'));
	else
		strcpy (uxpwdfile, UXPWDFILE);
	if ((s = fopen (uxpwdfile, "r")) == NULL)
		return (NULL);
	while (fgets (getpwbuf, sizeof(getpwbuf), s) != NULL) {
		if (getpwbuf[0] == '+') break;	/* NIS is not supported */
		if ((p = strchr (getpwbuf, ':')) == NULL) break;
		*p = '\0';
		if (strcmp (getpwbuf, name) == 0) {
			*p = ':';
			found = 1;
			break;
		}
	}
	(void) fclose (s);
	if (! found) return (NULL);
	return (fillpwent (getpwbuf));
}

struct passwd *
getpwuid(uid)
uid_t uid;
{
	char *dp;
	int found = 0;
	uid_t u;
	char *p, *p_uid;
	FILE *s;
	char uxpwdfile[256];

	if (uid < 0)
		return (NULL);
	if (strncmp (UXPWDFILE, "%SystemRoot%\\", 13) == 0 &&
	    (p = getenv ("SystemRoot")))
		sprintf (uxpwdfile, "%s%s", p, strchr (UXPWDFILE, '\\'));
	else
		strcpy (uxpwdfile, UXPWDFILE);
	if ((s = fopen (uxpwdfile, "r")) == NULL)
		return (NULL);
	while (fgets (getpwbuf, sizeof(getpwbuf), s) != NULL) {
		if (getpwbuf[0] == '+') break;	/* NIS is not supported */
		if ((p = strchr (getpwbuf, ':')) == NULL) break;
		if ((p = strchr (++p, ':')) == NULL) break;
		p_uid = ++p;
		if ((p = strchr (p_uid, ':')) == NULL) break;
		*p = '\0';
		u = strtol (p_uid, &dp, 10);
		if (*dp != '\0') break;
		if (u == uid) {
			*p = ':';
			found = 1;
			break;
		}
	}
	(void) fclose (s);
	if (! found) return (NULL);
	return (fillpwent (getpwbuf));
}

uid_t
getuid()
{
	struct passwd *pw;

	if (winux_uid >= 0)
		return (winux_uid);
	if ((pw = getpwnam (cuserid (NULL))) == NULL)
		return (-1);
	if (winux_gid < 0) winux_gid = pw->pw_gid;
	winux_uid = pw->pw_uid;
	return (winux_uid);
}

gid_t
getegid()
{
	return (getgid());	/* for now */
}

uid_t
geteuid()
{
	return (getuid());	/* for now */
}

int setuid( uid_t uid )
{
   return 0;
}

int setgid( gid_t gid )
{
   return 0;
}
