/*
 * $Id: uxid.c,v 1.10 2005/10/19 13:44:45 jdurand Exp $
 */

/*
 * Copyright (C) 1997-2001 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: uxid.c,v $ $Revision: 1.10 $ $Date: 2005/10/19 13:44:45 $ CERN/IT/PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
#define _POSIX_
#include "Cthread_api.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
/* #include <windows.h> */
#include "grp.h"
#include "pwd.h"
#include "Castor_limits.h"

static int winux_uid_key = 0;
static int winux_gid_key = 0;
static int winux_cuserid_key = 0;
static int gr_key = 0;
static int group_members_key = 0;
static int nb_group_members_key = 0;
static int pw_key = 0;
static int grbuf_key = 0;
static int pwbuf_key = 0;
static int grlen_key = 0;
static int pwlen_key = 0;

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
  char *dp, *grbuf;
  struct group *gr;
  char ***group_members;
  int n;
  int *nb_group_members;
  char *p, *q;
  size_t *grlen;

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
    *nb_group_members = -1;
  }

  if (Cthread_getspecific(&grlen_key,(void **) &grlen) != 0) {
    return(NULL);
  }
  if (grlen == NULL) {
    if ((grlen = (size_t *) calloc(1,sizeof(size_t))) == NULL) {
      return(NULL);
    }
    if (Cthread_setspecific(&grlen_key,grlen) != 0) {
      return(NULL);
    }
  }

  if (Cthread_getspecific(&grbuf_key,(void **) &grbuf) != 0) {
    return(NULL);
  }
  if (grbuf == NULL) {
    if ((grbuf = (char *) malloc(strlen(buf) + 1)) == NULL) {
      return(NULL);
    }
    if (Cthread_setspecific(&grbuf_key,grbuf) != 0) {
      return(NULL);
    }
    *grlen = strlen(buf) + 1;
  } else if (*grlen < (strlen(buf) + 1)) {
    /* We need to extend the buffer */
    char *dummy;
    if ((dummy = (char *) realloc(grbuf, strlen(buf) + 1)) == NULL) {
      return(NULL);
    }
    grbuf = dummy;
    if (Cthread_setspecific(&grbuf_key,grbuf) != 0) {
      return(NULL);
    }
    *grlen = strlen(buf) + 1;
  }

  *(buf + strlen(buf) - 1) = '\0';
  strcpy(grbuf,buf);
  gr->gr_name = grbuf;
  if ((p = strchr (grbuf, ':')) == NULL) return (NULL);
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
  /* Because of initialization of *nb_group_members to -1 this code ensures */
  /* that *group_members is always allocated, thus gr_mem is never NULL */
  if (n != *nb_group_members) {
    if (*group_members != NULL) free (*group_members);
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
  char *dp, *pwbuf;
  char *p, *q;
  struct passwd *pw;
  size_t *pwlen;

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

  if (Cthread_getspecific(&pwlen_key,(void **) &pwlen) != 0) {
    return(NULL);
  }
  if (pwlen == NULL) {
    if ((pwlen = (size_t *) calloc(1,sizeof(size_t))) == NULL) {
      return(NULL);
    }
    if (Cthread_setspecific(&pwlen_key,pwlen) != 0) {
      return(NULL);
    }
  }

  if (Cthread_getspecific(&pwbuf_key,(void **) &pwbuf) != 0) {
    return(NULL);
  }
  if (pwbuf == NULL) {
    if ((pwbuf = (char *) calloc(1,strlen(buf) + 1)) == NULL) {
      return(NULL);
    }
    if (Cthread_setspecific(&pwbuf_key,pwbuf) != 0) {
      return(NULL);
    }
    *pwlen = strlen(buf) + 1;
  } else if (*pwlen < (strlen(buf) + 1)) {
    /* We need to extend the buffer */
    char *dummy;
    if ((dummy = (char *) realloc(pwbuf, strlen(buf) + 1)) == NULL) {
      return(NULL);
    }
    pwbuf = dummy;
    if (Cthread_setspecific(&pwbuf_key,pwbuf) != 0) {
      return(NULL);
    }
    *pwlen = strlen(buf) + 1;
  }

  strcpy(pwbuf,buf);
  pw->pw_name = pwbuf;
  if ((p = strchr (pwbuf, ':')) == NULL) return (NULL);
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

/* For conform to standard, saying that getgid() NEVER fails */
/* we return the maximum gid instead of -1. */

gid_t DLL_DECL
getgid()
{
  struct passwd *pw;
  gid_t *winux_gid;
  uid_t *winux_uid;

  if (Cthread_getspecific(&winux_gid_key,(void **) &winux_gid) != 0) {
    return(CA_MAXGID);
  }
  if (winux_gid == NULL) {
    if ((winux_gid = (gid_t *) malloc(sizeof(gid_t))) == NULL) {
      return(CA_MAXGID);
    }
    *winux_gid = CA_MAXGID;
    if (Cthread_setspecific(&winux_gid_key,winux_gid) != 0) {
      return(CA_MAXGID);
    }
  }

  if (Cthread_getspecific(&winux_uid_key,(void **) &winux_uid) != 0) {
    return(CA_MAXGID);
  }
  if (winux_uid == NULL) {
    if ((winux_uid = (uid_t *) malloc(sizeof(uid_t))) == NULL) {
      return(CA_MAXGID);
    }
    *winux_uid = CA_MAXUID;
    if (Cthread_setspecific(&winux_uid_key,winux_uid) != 0) {
      return(CA_MAXGID);
    }
  }

  if (*winux_gid >= 0 && *winux_gid < CA_MAXGID)
    return (*winux_gid);
  if ((pw = getpwnam (cuserid (NULL))) == NULL)
    return (CA_MAXGID);
  *winux_gid = pw->pw_gid;
  if (*winux_gid < 0 || *winux_gid > CA_MAXGID)
    *winux_gid = CA_MAXGID;
  if (*winux_uid < 0 || *winux_uid >= CA_MAXUID) *winux_uid = pw->pw_uid;
  if (*winux_uid < 0 || *winux_uid > CA_MAXUID)
    *winux_uid = CA_MAXUID;
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
  OSVERSIONINFO osvi;

  if (gid < 0)
    return (NULL);
  memset (&osvi, 0, sizeof(osvi));
  osvi.dwOSVersionInfoSize = sizeof(osvi);
  GetVersionEx (&osvi);
  if (osvi.dwMajorVersion >= 5) {
    if (strncmp (W2000MAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxgrpfile, "%s%s\\group", p, strchr (W2000MAPDIR, '\\'));
    else
      sprintf (uxgrpfile, "%s\\group", W2000MAPDIR);
  } else {
    if (strncmp (WNTMAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxgrpfile, "%s%s\\group", p, strchr (WNTMAPDIR, '\\'));
    else
      sprintf (uxgrpfile, "%s\\group", WNTMAPDIR);
  }
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
  OSVERSIONINFO osvi;

  if (name == NULL || *name == '\0')
    return (NULL);
  memset (&osvi, 0, sizeof(osvi));
  osvi.dwOSVersionInfoSize = sizeof(osvi);
  GetVersionEx (&osvi);
  if (osvi.dwMajorVersion >= 5) {
    if (strncmp (W2000MAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxgrpfile, "%s%s\\group", p, strchr (W2000MAPDIR, '\\'));
    else
      sprintf (uxgrpfile, "%s\\group", W2000MAPDIR);
  } else {
    if (strncmp (WNTMAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxgrpfile, "%s%s\\group", p, strchr (WNTMAPDIR, '\\'));
    else
      sprintf (uxgrpfile, "%s\\group", WNTMAPDIR);
  }
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
  OSVERSIONINFO osvi;

  if (name == NULL || *name == '\0')
    return (NULL);
  memset (&osvi, 0, sizeof(osvi));
  osvi.dwOSVersionInfoSize = sizeof(osvi);
  GetVersionEx (&osvi);
  if (osvi.dwMajorVersion >= 5) {
    if (strncmp (W2000MAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxpwdfile, "%s%s\\passwd", p, strchr (W2000MAPDIR, '\\'));
    else
      sprintf (uxpwdfile, "%s\\passwd", W2000MAPDIR);
  } else {
    if (strncmp (WNTMAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxpwdfile, "%s%s\\passwd", p, strchr (WNTMAPDIR, '\\'));
    else
      sprintf (uxpwdfile, "%s\\passwd", WNTMAPDIR);
  }
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
  OSVERSIONINFO osvi;

  if (uid < 0)
    return (NULL);
  memset (&osvi, 0, sizeof(osvi));
  osvi.dwOSVersionInfoSize = sizeof(osvi);
  GetVersionEx (&osvi);
  if (osvi.dwMajorVersion >= 5) {
    if (strncmp (W2000MAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxpwdfile, "%s%s\\passwd", p, strchr (W2000MAPDIR, '\\'));
    else
      sprintf (uxpwdfile, "%s\\passwd", W2000MAPDIR);
  } else {
    if (strncmp (WNTMAPDIR, "%SystemRoot%\\", 13) == 0 &&
        (p = getenv ("SystemRoot")))
      sprintf (uxpwdfile, "%s%s\\passwd", p, strchr (WNTMAPDIR, '\\'));
    else
      sprintf (uxpwdfile, "%s\\passwd", WNTMAPDIR);
  }
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

/* For conform to standard, saying that getuid() NEVER fails */
/* we return the maximum uid instead of -1. */

uid_t
getuid()
{
  struct passwd *pw;
  gid_t *winux_gid;
  uid_t *winux_uid;

  if (Cthread_getspecific(&winux_gid_key,(void **) &winux_gid) != 0) {
    return(CA_MAXUID);
  }
  if (winux_gid == NULL) {
    if ((winux_gid = (gid_t *) malloc(sizeof(gid_t))) == NULL) {
      return(CA_MAXUID);
    }
    *winux_gid = CA_MAXGID;
    if (Cthread_setspecific(&winux_gid_key,winux_gid) != 0) {
      return(CA_MAXUID);
    }
  }

  if (Cthread_getspecific(&winux_uid_key,(void **) &winux_uid) != 0) {
    return(CA_MAXUID);
  }
  if (winux_uid == NULL) {
    if ((winux_uid = (uid_t *) malloc(sizeof(uid_t))) == NULL) {
      return(CA_MAXUID);
    }
    *winux_uid = CA_MAXUID;
    if (Cthread_setspecific(&winux_uid_key,winux_uid) != 0) {
      return(CA_MAXUID);
    }
  }

  if (*winux_uid >= 0 && *winux_uid < CA_MAXUID)
    return (*winux_uid);
  if ((pw = getpwnam (cuserid (NULL))) == NULL)
    return (CA_MAXUID);
  *winux_uid = pw->pw_uid;
  if (*winux_uid < 0 || *winux_uid > CA_MAXUID)
    *winux_uid = CA_MAXUID;
  if (*winux_gid < 0 || *winux_gid >= CA_MAXGID) *winux_gid = pw->pw_gid;
  if (*winux_gid < 0 || *winux_gid > CA_MAXGID)
    *winux_gid = CA_MAXGID;
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
