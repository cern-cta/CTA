/*
 * $Id: stage_updc.c,v 1.17 2001/09/18 21:10:54 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_updc.c,v $ $Revision: 1.17 $ $Date: 2001/09/18 21:10:54 $ CERN IT-PDP/DM Jean-Damien Durand Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
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
#include "stage_api.h"
#include "stage.h"
#include "u64subr.h"
#include "Cpwd.h"

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
  char *p;
  struct passwd *pw;
  char *q, *q2;
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  int reqid;
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  char *stghost;
  char tmpbuf[21];
  uid_t euid;
  char Zparm[CA_MAXSTGRIDLEN+1];
  char *command = "stage_updc_filcp";
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

  euid = geteuid();
  egid = getegid();
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
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* Check how many bytes we need */
  sendbuf_size = 3 * LONGSIZE;                       /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;           /* Login name */
  sendbuf_size += 3 * WORDSIZE;                      /* euid, egid, nargs */
  sendbuf_size += strlen(command) + 1;               /* Command name */
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
  if (ifce) {
    if (ifce[0] != '\0') {
      sendbuf_size += strlen("-I") + strlen(ifce) + 2; /* -I option and value */
    }
  }
  if (lrecl > 0) {
    sprintf (tmpbuf, "%d", lrecl);
    sendbuf_size += strlen("-L") + strlen(tmpbuf) + 2; /* -L option and value */
  }
  if (size > 0) {
    u64tostr((u_signed64) size, tmpbuf, 0);
    sendbuf_size += strlen("-s") + strlen(tmpbuf) + 2; /* -s option and value */
  }
  if (copyrc >= 0) {
    sprintf (tmpbuf, "%d", rc_castor2shift(copyrc));
    sendbuf_size += strlen("-R") + strlen(tmpbuf) + 2; /* -R option and value */
  }
  if (transfer_time > 0) {
    sprintf (tmpbuf, "%d", transfer_time);
    sendbuf_size += strlen("-T") + strlen(tmpbuf) + 2; /* -T option and value */
  }
  if (waiting_time > 0) {
    sprintf (tmpbuf, "%d", waiting_time);
    sendbuf_size += strlen("-W") + strlen(tmpbuf) + 2; /* -W option and value */
  }
  if (fseq > 0) {
    sprintf (tmpbuf, "%d", fseq);
    sendbuf_size += strlen("-q") + strlen(tmpbuf) + 2; /* -q option and value */
  }

  /* Allocate memory */
  if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
    serrno = SEINTERNAL;
    return (-1);
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
  marshall_STRING (sbp, command);

  marshall_STRING (sbp, "-Z");
  marshall_STRING (sbp, stageid);
  nargs += 2;
  if (blksize > 0) {
    sprintf (tmpbuf, "%d", blksize);
    marshall_STRING (sbp,"-b");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (subreqid >= 0) {
    sprintf (tmpbuf, "%d", subreqid);
    marshall_STRING (sbp,"-i");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (drive) {
    if (drive[0] != '\0') {
      marshall_STRING (sbp,"-D");
      marshall_STRING (sbp, drive);
      nargs += 2;
    }
  }
  if (recfm) {
    if (recfm[0] != '\0') {
      marshall_STRING (sbp,"-F");
      marshall_STRING (sbp, recfm);
      nargs += 2;
    }
  }
  if (fid) {
    if (fid[0] != '\0') {
      marshall_STRING (sbp,"-f");
      marshall_STRING (sbp, fid);
      nargs += 2;
    }
  }
  if (ifce) {
    if (ifce[0] != '\0') {
      marshall_STRING (sbp,"-I");
      marshall_STRING (sbp, ifce);
      nargs += 2;
    }
  }
  if (lrecl > 0) {
    sprintf (tmpbuf, "%d", lrecl);
    marshall_STRING (sbp,"-L");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (size > 0) {
    u64tostr((u_signed64) size, tmpbuf, 0);
    marshall_STRING (sbp,"-s");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (copyrc >= 0) {
    sprintf (tmpbuf, "%d", rc_castor2shift(copyrc));
    marshall_STRING (sbp, "-R");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (transfer_time > 0) {
    sprintf (tmpbuf, "%d", transfer_time);
    marshall_STRING (sbp, "-T");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (waiting_time > 0) {
    sprintf (tmpbuf, "%d", waiting_time);
    marshall_STRING (sbp, "-W");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (fseq > 0) {
    sprintf (tmpbuf, "%d", fseq);
    marshall_STRING (sbp,"-q");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  marshall_WORD (q2, nargs);	/* update nargs */

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while (1) {
    c = send2stgd_compat (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if ((c == 0) || (serrno == EINVAL) || (serrno == ENOSPC)) break;
    if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
    stage_sleep (RETRYI);
  }
  if (c == 0 && path != NULL && repbuf[0] != '\0') {
    rbp = repbuf;
    unmarshall_STRING (rbp, path);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}

int DLL_DECL stage_updc_tppos(stageid, subreqid, status, blksize, drive, fid, fseq, lrecl, recfm, path)
     char *stageid;
     int subreqid;
     int status;
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
  int n;
  int nargs;
  int ntries = 0;
  char *p;
  struct passwd *pw;
  char *q, *q2;
  char *rbp;
  char repbuf[CA_MAXPATHLEN+1];
  int reqid;
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  char *stghost;
  char tmpbuf[21];
  uid_t euid;
  char Zparm[CA_MAXSTGRIDLEN+1];
  char *command = "stage_updc_tppos";
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

  euid = geteuid();
  egid = getegid();
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
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* Check how many bytes we need */
  sendbuf_size = 3 * LONGSIZE;                        /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;            /* Login name */
  sendbuf_size += 3 * LONGSIZE;                       /* euid, egid, nargs */
  sendbuf_size += strlen(command) + 1;                /* Command name */
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
  if (lrecl > 0) {
    sprintf (tmpbuf, "%d", lrecl);
    sendbuf_size += strlen("-L") + strlen(tmpbuf) + 2; /* -L option and value */
  }
  if (fseq > 0) {
    sprintf (tmpbuf, "%d", fseq);
    sendbuf_size += strlen("-q") + strlen(tmpbuf) + 2; /* -q option and value */
  }
  if (status == ETFSQ) {
    n = 211;	/* emulate old (SHIFT) ETFSQ */
    sprintf (tmpbuf, "%d", n);
    sendbuf_size += strlen("-R") + strlen(tmpbuf) + 2; /* -R option and value */
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
  marshall_STRING (sbp, command);

  marshall_STRING (sbp, "-Z");
  marshall_STRING (sbp, stageid);
  nargs += 2;
  if (subreqid >= 0) {
    sprintf (tmpbuf, "%d", subreqid);
    marshall_STRING (sbp,"-i");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (blksize > 0) {
    sprintf (tmpbuf, "%d", blksize);
    marshall_STRING (sbp,"-b");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (drive) {
    if (drive[0] != '\0') {
      marshall_STRING (sbp,"-D");
      marshall_STRING (sbp, drive);
      nargs += 2;
    }
  }
  if (recfm) {
    if (recfm[0] != '\0') {
      marshall_STRING (sbp,"-F");
      marshall_STRING (sbp, recfm);
      nargs += 2;
    }
  }
  if (fid) {
    if (fid[0] != '\0') {
      marshall_STRING (sbp,"-f");
      marshall_STRING (sbp, fid);
      nargs += 2;
    }
  }
  if (lrecl > 0) {
    sprintf (tmpbuf, "%d", lrecl);
    marshall_STRING (sbp,"-L");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (fseq > 0) {
    sprintf (tmpbuf, "%d", fseq);
    marshall_STRING (sbp,"-q");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  if (status == ETFSQ) {
    n = 211;	/* emulate old (SHIFT) ETFSQ */
    sprintf (tmpbuf, "%d", n);
    marshall_STRING (sbp, "-R");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  marshall_WORD (q2, nargs);	/* update nargs */

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while (1) {
    c = send2stgd_compat (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if ((c == 0) || (serrno == EINVAL) || (serrno == ENOSPC)) break;
    if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
    stage_sleep (RETRYI);
  }
  if (c == 0 && path != NULL && repbuf[0] != '\0') {
    rbp = repbuf;
    unmarshall_STRING (rbp, path);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
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
  struct passwd *pw;
  char *q, *q2;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  uid_t euid;
  stage_hsm_t *hsm;
  char *command = "stage_updc_user";
  int nupath;

  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  euid = geteuid();
  egid = getegid();
#if defined(_WIN32)
  if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Init repbuf to null */
  repbuf[0] = '\0';

  if ((pw = Cgetpwuid (euid)) == NULL) {
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;                     /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;         /* Login name */
  sendbuf_size += 3 * WORDSIZE;                    /* euid, egid and nargs */
  sendbuf_size += strlen(command) + 1;                /* Command name */

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
  marshall_STRING (sbp, command);

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
    c = send2stgd_compat (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if ((c == 0) || (serrno == EINVAL) || (serrno == ENOSPC)) break;
    if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
    stage_sleep (RETRYI);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}

int DLL_DECL stage_updc_error(stghost,copyrc,hsmstruct)
     char *stghost;
     int copyrc;
     stage_hsm_t *hsmstruct;
{
  int c;
  gid_t egid;
  int msglen;
  int nargs;
  int ntries = 0;
  struct passwd *pw;
  char *q, *q2;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  uid_t euid;
  stage_hsm_t *hsm;
  char *command = "stage_updc_error";
  int nupath;
  char tmpbuf[21];

  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  euid = geteuid();
  egid = getegid();
#if defined(_WIN32)
  if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Init repbuf to null */
  repbuf[0] = '\0';

  if ((pw = Cgetpwuid (euid)) == NULL) {
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;                     /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;         /* Login name */
  sendbuf_size += 3 * WORDSIZE;                    /* euid, egid and nargs */
  sendbuf_size += strlen(command) + 1;                /* Command name */

  if (copyrc >= 0) {
    sprintf (tmpbuf, "%d", rc_castor2shift(copyrc));
    sendbuf_size += strlen("-R") + strlen(tmpbuf) + 2; /* -R option and value */
  }

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
  marshall_STRING (sbp, command);

  if (copyrc >= 0) {
    sprintf (tmpbuf, "%d", rc_castor2shift(copyrc));
    marshall_STRING (sbp, "-R");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }

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
    c = send2stgd_compat (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if ((c == 0) || (serrno == EINVAL) || (serrno == ENOSPC)) break;
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
  struct passwd *pw;
  char *q, *q2;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  uid_t euid;
  stage_hsm_t *hsm;
  char *command = "stage_updc_filchg";
  int pid;
  int nupath;

  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  euid = geteuid();
  egid = getegid();
#if defined(_WIN32)
  if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Init repbuf to null */
  repbuf[0] = '\0';

  if ((pw = Cgetpwuid (euid)) == NULL) {
    serrno = SEUSERUNKN;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;                     /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;         /* Login name */
  sendbuf_size += 4 * WORDSIZE;                    /* euid, egid, pid and nargs */
  sendbuf_size += strlen(command) + 1;                /* Command name */

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
  marshall_STRING (sbp, command);

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
    c = send2stgd_compat (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if ((c == 0) || (serrno == EINVAL) || (serrno == ENOSPC)) break;
    if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
    stage_sleep (RETRYI);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}
