/*
 * stage_updc.c,v 1.3 2003/10/31 07:03:57 jdurand Exp
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)stage_updc.c,v 1.3 2003/10/31 07:03:57 CERN IT-PDP/DM Jean-Damien Durand Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "u64subr.h"
#include "Cpwd.h"
#include "stage_api.h"

#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif /* _WIN32 */

int DLL_DECL stage_updc_filcp(stageid, subreqid, copyrc, ifce, size, waiting_time, transfer_time, blksize, drive, fid, fseq, lrecl, recfm, path)
     char *stageid;
     int subreqid;
     int copyrc;
     char *ifce;
     u_signed64 size;
     int waiting_time;
     int transfer_time;
     int blksize;
     char *drive;
     char *fid;
     int fseq;
     int lrecl;
     char *recfm;
     char *path;
{
  int c;
  char *dp;
  int errflg = 0;
  gid_t egid;
  int key;
  int msglen;
  int nargs;
  int ntries = 0;
  int nstg161 = 0;
  char *p;
  struct passwd *pw;
  char *q, *q2;
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  int reqid;
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  char *stghost = NULL;
  char tmpbuf[21];
  uid_t euid;
  char Zparm[CA_MAXSTGRIDLEN+1];
  char *func = "stage_updc_filcp";
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

  euid = (getenv("STAGE_EUID") != NULL) ? atoi(getenv("STAGE_EUID")) : geteuid();
  egid = (getenv("STAGE_EGID") != NULL) ? atoi(getenv("STAGE_EGID")) : getegid();
#if defined(_WIN32)
  if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif
  if (! stageid) {
    serrno = EFAULT;
    return (-1);
  }
  if (stageid[0] == '\0') {
    serrno = EFAULT;
    return (-1);
  }

  /* Init repbuf to null */
  repbuf[0] = '\0';

  /* check stager request id */

  if (strlen (stageid) <= CA_MAXSTGRIDLEN) {
    strcpy (Zparm, stageid);
    if ((p = strtok (Zparm, ".")) != NULL) {
      stage_strtoi(&reqid, p, &dp, 10);
      if (*dp != '\0' || reqid <= 0 ||
          (p = strtok (NULL, "@")) == NULL) {
        errflg++;
      } else {
        stage_strtoi(&key, p, &dp, 10);
        if (*dp != '\0' ||
            (stghost = strtok (NULL, " ")) == NULL) {
          errflg++;
        }
      }
    } else {
      errflg++;
    }
  } else
    errflg++;
  if (errflg) {
    serrno = EINVAL;
    return (-1);
  }

  if ((pw = Cgetpwuid (euid)) == NULL) {
    if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* Check how many bytes we need */
  sendbuf_size = 3 * LONGSIZE;                       /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;           /* Login name */
  sendbuf_size += 3 * WORDSIZE;                      /* euid, egid, nargs */
  sendbuf_size += strlen(func) + 1;               /* Func name */
  sendbuf_size += strlen("-Z") + strlen(stageid) + 2; /* -Z option and value */
  if (subreqid >= 0) {
    sprintf (tmpbuf, "%d", subreqid);
    sendbuf_size += strlen("-i") + strlen(tmpbuf) + 2; /* -i option and value */
  }
  if (blksize > 0) {
    sprintf (tmpbuf, "%d", blksize);
    sendbuf_size += strlen("-b") + strlen(tmpbuf) + 2; /* -b option and value */
  }
  if (drive) {
    if (drive[0] != '\0') {
      sendbuf_size += strlen("-D") + strlen(drive) + 2; /* -D option and value */
    }
  }
  if (recfm) {
    if (recfm[0] != '\0') {
      sendbuf_size += strlen("-F") + strlen(recfm) + 2; /* -F option and value */
    }
  }
  if (fid) {
    if (fid[0] != '\0') {
      sendbuf_size += strlen("-f") + strlen(fid) + 2; /* -f option and value */
    }
  }
int DLL_DECL stage_updc_user(stghost,hsmstruct)
     char *stghost;
     stage_hsm_t *hsmstruct;
{
  int c;
  gid_t egid;
  int msglen;
  int nargs;
  int ntries = 0;
  int nstg161 = 0;
  struct passwd *pw;
  char *q, *q2;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  uid_t euid;
  stage_hsm_t *hsm;
  char *func = "stage_updc_user";
  int nupath;

  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  euid = (getenv("STAGE_EUID") != NULL) ? atoi(getenv("STAGE_EUID")) : geteuid();
  egid = (getenv("STAGE_EGID") != NULL) ? atoi(getenv("STAGE_EGID")) : getegid();
#if defined(_WIN32)
  if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Init repbuf to null */
  repbuf[0] = '\0';

  if ((pw = Cgetpwuid (euid)) == NULL) {
    if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;                     /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;         /* Login name */
  sendbuf_size += 3 * WORDSIZE;                    /* euid, egid and nargs */
  sendbuf_size += strlen(func) + 1;                /* Func name */

  /* Count the number of link files */
  hsm = hsmstruct;
  nupath = 0;
  while (hsm != NULL) {
    if (hsm->upath != NULL && hsm->upath[0] != '\0') {
      sendbuf_size += strlen(hsm->upath) + 1; /* User path */
      nupath++;
    }
    hsm = hsm->next;
  }

  if (nupath == 0) {
    serrno = EFAULT;
    return(-1);
  }

  /* Allocate memory */
  if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
    serrno = SEINTERNAL;
    return(-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, STGMAGIC);
  marshall_LONG (sbp, STAGEUPDC);
  q = sbp;	/* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_STRING (sbp, pw->pw_name);	/* login name */
  marshall_WORD (sbp, euid);
  marshall_WORD (sbp, egid);
  q2 = sbp;	/* save pointer. The next field will be updated */
  nargs = 1;
  marshall_WORD (sbp, nargs);
  marshall_STRING (sbp, func);

  hsm = hsmstruct;
  while (hsm != NULL) {
    marshall_STRING (sbp, hsm->upath);
    nargs += 1;
    hsm = hsm->next;
  }

  marshall_WORD (q2, nargs);	/* update nargs */

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while (1) {
    c = send2stgd_compat (stghost, sendbuf, msglen, 1, repbuf, (int) sizeof(repbuf));
    if ((c == 0) || (serrno == EINVAL) || (serrno == ENOSPC) || (serrno == ENOENT) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SENAMETOOLONG)) break;
	if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
	if (serrno == ESTNACT && nstg161++ == 0) stage_errmsg(func, STG161);
    if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
    stage_sleep (RETRYI);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}

int DLL_DECL stage_updc_filchg(stghost,hsmstruct)
     char *stghost;
     stage_hsm_t *hsmstruct;
{
  int c;
  gid_t egid;
  int msglen;
  int nargs;
  int ntries = 0;
  int nstg161 = 0;
  struct passwd *pw;
  char *q, *q2;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  uid_t euid;
  stage_hsm_t *hsm;
  char *func = "stage_updc_filchg";
  int pid;
  int nupath;

  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  euid = (getenv("STAGE_EUID") != NULL) ? atoi(getenv("STAGE_EUID")) : geteuid();
  egid = (getenv("STAGE_EGID") != NULL) ? atoi(getenv("STAGE_EGID")) : getegid();
#if defined(_WIN32)
  if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Init repbuf to null */
  repbuf[0] = '\0';

  if ((pw = Cgetpwuid (euid)) == NULL) {
    if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;                     /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;         /* Login name */
  sendbuf_size += 4 * WORDSIZE;                    /* euid, egid, pid and nargs */
  sendbuf_size += strlen(func) + 1;                /* Func name */

  /* Count the number of link files */
  hsm = hsmstruct;
  nupath = 0;
  while (hsm != NULL) {
    if (hsm->upath != NULL && hsm->upath[0] != '\0') {
      sendbuf_size += strlen(hsm->upath) + 1; /* User path */
      nupath++;
    }
    hsm = hsm->next;
  }

  if (nupath == 0) {
    serrno = EFAULT;
    return(-1);
  }

  /* Allocate memory */
  if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
    serrno = SEINTERNAL;
    return(-1);
  }

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, STGMAGIC);
  marshall_LONG (sbp, STAGEFILCHG);
  q = sbp;	/* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  marshall_STRING (sbp, pw->pw_name);	/* login name */
  marshall_WORD (sbp, euid);
  marshall_WORD (sbp, egid);
  pid = getpid();
  marshall_WORD (sbp, pid);
  q2 = sbp;	/* save pointer. The next field will be updated */
  nargs = 1;
  marshall_WORD (sbp, nargs);
  marshall_STRING (sbp, func);

  hsm = hsmstruct;
  while (hsm != NULL) {
    marshall_STRING (sbp, hsm->upath);
    nargs += 1;
    hsm = hsm->next;
  }

  marshall_WORD (q2, nargs);	/* update nargs */

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while (1) {
    c = send2stgd_compat (stghost, sendbuf, msglen, 1, repbuf, (int) sizeof(repbuf));
    if ((c == 0) || (serrno == EINVAL) || (serrno == ENOSPC) || (serrno == ENOENT) || (serrno == EACCES) || (serrno == EPERM) || (serrno == ESTGROUP) || (serrno == ESTUSER) || (serrno == SEUSERUNKN) || (serrno == SEGROUPUNKN) || (serrno == SENAMETOOLONG)) break;
	if (serrno == SHIFT_ESTNACT) serrno = ESTNACT; /* Stager daemon bug */
	if (serrno == ESTNACT && nstg161++ == 0) stage_errmsg(func, STG161);
    if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
    stage_sleep (RETRYI);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}

