/*
 * $Id: uxid.c,v 1.3 2000/06/15 14:21:15 jdurand Exp $
 */

/*
 * Copyright (C) 1997-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: uxid.c,v $ $Revision: 1.3 $ $Date: 2000/06/15 14:21:15 $ CERN/IT/PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
#define _POSIX_
#include "Cthread_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
//#include <windows.h>
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

static int winux_uid_key = 0;
static int winux_gid_key = 0;
static int winux_cuserid_key = 0;
static int gr_key = 0;
static int group_members_key = 0;
static int nb_group_members_key = 0;
static int pw_key = 0;

char * DLL_DECL
cuserid(bufp)
char *bufp;
{
#define DWORD int
	DWORD bufl = L_cuserid;
    char *winux_cuserid;
	char *p;

    if (Cthread_getspecific(&winux_cuserid_key,(void **) &winux_cuserid) != 0) {
      return(NULL);
    }
    if (winux_cuserid == NULL) {
      if ((winux_cuserid = (char *) calloc(1,bufl)) == NULL) {
        return(NULL);
      }
      if (Cthread_setspecific(&winux_cuserid_key,winux_cuserid) != 0) {
        return(NULL);
      }
    }

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
 
struct group * DLL_DECL
fillgrpent(buf)
char *buf;
{
	char *dp;
	struct group *gr;
	char ***group_members;
	int n;
	int *nb_group_members;
	char *p, *q;

    if (Cthread_getspecific(&gr_key,(void **) &gr) != 0) {
      return(NULL);
    }
    if (gr == NULL) {
      if ((gr = (struct group *) calloc(1,sizeof(struct group))) == NULL) {
        return(NULL);
      }
      if (Cthread_setspecific(&gr_key,gr) != 0) {
        return(NULL);
      }
    }
    if (Cthread_getspecific(&group_members_key,(void **) &group_members) != 0) {
      return(NULL);
    }
    if (group_members == NULL) {
      if ((group_members = (char ***) calloc(1,sizeof(char **))) == NULL) {
        return(NULL);
      }
      if (Cthread_setspecific(&group_members_key,group_members) != 0) {
        return(NULL);
      }
    }
    if (Cthread_getspecific(&nb_group_members_key,(void **) &nb_group_members) != 0) {
      return(NULL);
    }
    if (nb_group_members == NULL) {
      if ((nb_group_members = (int *) calloc(1,sizeof(int))) == NULL) {
        return(NULL);
      }
      if (Cthread_setspecific(&nb_group_members_key,nb_group_members) != 0) {
        return(NULL);
      }
    }

	*(buf + strlen(buf) - 1) = '\0';
	gr->gr_name = buf;
	if ((p = strchr (buf, ':')) == NULL) return (NULL);
	*p = '\0';
	gr->gr_passwd = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	q = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	gr->gr_gid = strtol (q, &dp, 10);
	if (*dp != '\0') return (NULL);
	q = ++p;
	n = 0;
	if (*p != '\0') n = 1;
	while ((p = strchr (p, ',')) != NULL) {
		n++;
		p++;
	}
	if (n != *nb_group_members) {
		if (*group_members) free (*group_members);
		*group_members = (char **) malloc ((n + 1) * sizeof(char *));
		if (*group_members == NULL) return (NULL);
		*nb_group_members = n;
		gr->gr_mem = *group_members;
		(*group_members)[*nb_group_members] = NULL;
	}
	if (*nb_group_members == 0) return (gr);
	n = 0;
	p = q;
	while ((p = strchr (q, ',')) != NULL) {
		(*group_members)[n++] = q;
		*p = '\0';
		q = ++p;
	}
	(*group_members)[n++] = q;
	return (gr);
}
 
struct passwd * DLL_DECL
fillpwent(buf)
char *buf;
{
	char *dp;
	char *p, *q;
	struct passwd *pw;

    if (Cthread_getspecific(&pw_key,(void **) &pw) != 0) {
      return(NULL);
    }
    if (pw == NULL) {
      if ((pw = (struct passwd *) calloc(1,sizeof(struct passwd))) == NULL) {
        return(NULL);
      }
      if (Cthread_setspecific(&pw_key,pw) != 0) {
        return(NULL);
      }
    }

	pw->pw_name = buf;
	if ((p = strchr (buf, ':')) == NULL) return (NULL);
	*p = '\0';
	pw->pw_passwd = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	q = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw->pw_uid = strtol (q, &dp, 10);
	if (*dp != '\0') return (NULL);
	q = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw->pw_gid = strtol (q, &dp, 10);
	if (*dp != '\0') return (NULL);
	pw->pw_gecos = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw->pw_dir = ++p;
	if ((p = strchr (p, ':')) == NULL) return (NULL);
	*p = '\0';
	pw->pw_shell = ++p;
	return (pw);
}
gid_t DLL_DECL
getgid()
{
	struct passwd *pw;
    gid_t *winux_gid;
    uid_t *winux_uid;

    if (Cthread_getspecific(&winux_gid_key,(void **) &winux_gid) != 0) {
      return(-1);
    }
    if (winux_gid == NULL) {
      if ((winux_gid = (gid_t *) malloc(sizeof(gid_t))) == NULL) {
        return(-1);
      }
      *winux_gid = -1;
      if (Cthread_setspecific(&winux_gid_key,winux_gid) != 0) {
        return(-1);
      }
    }

    if (Cthread_getspecific(&winux_uid_key,(void **) &winux_uid) != 0) {
      return(-1);
    }
    if (winux_uid == NULL) {
      if ((winux_uid = (uid_t *) malloc(sizeof(uid_t))) == NULL) {
        return(-1);
      }
      *winux_uid = -1;
      if (Cthread_setspecific(&winux_uid_key,winux_uid) != 0) {
        return(-1);
      }
    }

	if (*winux_gid >= 0)
		return (*winux_gid);
	if ((pw = getpwnam (cuserid (NULL))) == NULL)
		return (-1);
	*winux_gid = pw->pw_gid;
	if (*winux_uid < 0) *winux_uid = pw->pw_uid;
	return (*winux_gid);
}

struct group * DLL_DECL
getgrgid(gid)
gid_t gid;
{
	char *dp;
	int found = 0;
	gid_t g;
	char *p, *p_gid;
	FILE *s;
	char uxgrpfile[256];
    char getgrbuf[1024];

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

struct group * DLL_DECL
getgrnam(name)
char *name;
{
	int found = 0;
	char *p;
	FILE *s;
	char uxgrpfile[256];
    char getgrbuf[1024];

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

struct passwd * DLL_DECL
getpwnam(name)
char    *name;
{
	int found = 0;
	char *p;
	FILE *s;
	char uxpwdfile[256];
    char getpwbuf[1024];

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

struct passwd * DLL_DECL
getpwuid(uid)
uid_t uid;
{
	char *dp;
	int found = 0;
	uid_t u;
	char *p, *p_uid;
	FILE *s;
	char uxpwdfile[256];
    char getpwbuf[1024];

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
    gid_t *winux_gid;
    uid_t *winux_uid;

    if (Cthread_getspecific(&winux_gid_key,(void **) &winux_gid) != 0) {
      return(-1);
    }
    if (winux_gid == NULL) {
      if ((winux_gid = (gid_t *) malloc(sizeof(gid_t))) == NULL) {
        return(-1);
      }
      *winux_gid = -1;
      if (Cthread_setspecific(&winux_gid_key,winux_gid) != 0) {
        return(-1);
      }
    }

    if (Cthread_getspecific(&winux_uid_key,(void **) &winux_uid) != 0) {
      return(-1);
    }
    if (winux_uid == NULL) {
      if ((winux_uid = (uid_t *) malloc(sizeof(uid_t))) == NULL) {
        return(-1);
      }
      *winux_uid = -1;
      if (Cthread_setspecific(&winux_uid_key,winux_uid) != 0) {
        return(-1);
      }
    }

	if (*winux_uid >= 0)
		return (*winux_uid);
	if ((pw = getpwnam (cuserid (NULL))) == NULL)
		return (-1);
	if (*winux_gid < 0) *winux_gid = pw->pw_gid;
	*winux_uid = pw->pw_uid;
	return (*winux_uid);
}

gid_t DLL_DECL
getegid()
{
	return (getgid());	/* for now */
}

uid_t DLL_DECL
geteuid()
{
	return (getuid());	/* for now */
}

int DLL_DECL
setuid(uid)
uid_t uid;
{
   return 0;
}

int DLL_DECL
setgid(gid)
gid_t gid;
{
   return 0;
}
