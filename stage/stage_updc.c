/*
 * $Id: stage_updc.c,v 1.9 2000/06/23 07:21:17 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_updc.c,v $ $Revision: 1.9 $ $Date: 2000/06/23 07:21:17 $ CERN IT-PDP/DM Jean-Damien Durand Jean-Philippe Baud";
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

int copyrc_shift2castor _PROTO((int));

#ifndef _WIN32
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */
#endif /* _WIN32 */

int copyrc_castor2shift(copyrc)
     int copyrc;
{
  /* Input  is a CASTOR return code */
  /* Output is a SHIFT  return code */
  switch (copyrc) {
  case ERTBLKSKPD:
    return(BLKSKPD);
  case ERTTPE_LSZ:
    return(TPE_LSZ);
  case ERTMNYPARY:
  case ETPARIT:
    return(MNYPARI);
  case ERTLIMBYSZ:
    return(LIMBYSZ);
  default:
    return(copyrc);
  }
}

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
  gid_t gid;
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
  uid_t uid;
  char Zparm[CA_MAXSTGRIDLEN+1];
  char *command = "stage_updc_filcp";
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

  uid = getuid();
  gid = getgid();
#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
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
      reqid = strtol (p, &dp, 10);
      if (*dp != '\0' || reqid <= 0 ||
          (p = strtok (NULL, "@")) == NULL) {
        errflg++;
      } else {
        key = strtol (p, &dp, 10);
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

  if ((pw = Cgetpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }

  /* Check how many bytes we need */
  sendbuf_size = 3 * LONGSIZE;                       /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;           /* Login name */
  sendbuf_size += 3 * WORDSIZE;                      /* uid, gid, nargs */
  sendbuf_size += strlen(command) + 1;               /* Command name */
  sendbuf_size += strlen("-Z") + strlen(stageid) + 2; /* -Z option and value */
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
    sprintf (tmpbuf, "%d", copyrc_castor2shift(copyrc));
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
  marshall_WORD (sbp, uid);
  marshall_WORD (sbp, gid);
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
    sprintf (tmpbuf, "%d", copyrc_castor2shift(copyrc));
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
    c = send2stgd (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if (c == 0) break;
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
    sleep (RETRYI);
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
  gid_t gid;
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
  uid_t uid;
  char Zparm[CA_MAXSTGRIDLEN+1];
  char *command = "stage_updc_tppos";
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
  char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

  uid = getuid();
  gid = getgid();
#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
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
      reqid = strtol (p, &dp, 10);
      if (*dp != '\0' || reqid <= 0 ||
          (p = strtok (NULL, "@")) == NULL) {
        errflg++;
      } else {
        key = strtol (p, &dp, 10);
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

  if ((pw = Cgetpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }

  /* Check how many bytes we need */
  sendbuf_size = 3 * LONGSIZE;                        /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;            /* Login name */
  sendbuf_size += 3 * LONGSIZE;                       /* uid, gid, nargs */
  sendbuf_size += strlen(command) + 1;                /* Command name */
  sendbuf_size += strlen("-Z") + strlen(stageid) + 2; /* -Z option and value */
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
  marshall_WORD (sbp, uid);
  marshall_WORD (sbp, gid);
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
    c = send2stgd (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if (c == 0) break;
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
    sleep (RETRYI);
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
  gid_t gid;
  int msglen;
  int nargs;
  int ntries = 0;
  struct passwd *pw;
  char *q, *q2;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  uid_t uid;
  stage_hsm_t *hsm;
  char *command = "stage_updc_user";

  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  uid = getuid();
  gid = getgid();
#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Init repbuf to null */
  repbuf[0] = '\0';

  if ((pw = Cgetpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;                     /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;         /* Login name */
  sendbuf_size += 3 * WORDSIZE;                    /* uid, gid and nargs */
  sendbuf_size += strlen(command) + 1;                /* Command name */
  hsm = hsmstruct;
  while (hsm != NULL) {
    if (hsm->upath == NULL) {
      serrno = EFAULT;
      return (-1);
    }
    if (hsm->upath[0] == '\0') {
      serrno = EFAULT;
      return (-1);
    }
    sendbuf_size += strlen(hsm->upath) + 1;        /* user path */
    hsm = hsm->next;
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
  marshall_WORD (sbp, uid);
  marshall_WORD (sbp, gid);
  q2 = sbp;	/* save pointer. The next field will be updated */
  nargs = 1;
  marshall_WORD (sbp, nargs);
  marshall_STRING (sbp, command);

  /* Build link files arguments */
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
    c = send2stgd (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if (c == 0) break;
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
    sleep (RETRYI);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}

int DLL_DECL stage_updc_filchg(stghost,hsmstruct)
     char *stghost;
     stage_hsm_t *hsmstruct;
{
  int c;
  gid_t gid;
  int msglen;
  int nargs;
  int ntries = 0;
  struct passwd *pw;
  char *q, *q2;
  char repbuf[CA_MAXPATHLEN+1];
  char *sbp;
  char *sendbuf;
  size_t sendbuf_size;
  uid_t uid;
  stage_hsm_t *hsm;
  char *command = "stage_updc_filchg";
  int pid;

  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  uid = getuid();
  gid = getgid();
#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif

  /* Init repbuf to null */
  repbuf[0] = '\0';

  if ((pw = Cgetpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;                     /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1;         /* Login name */
  sendbuf_size += 4 * WORDSIZE;                    /* uid, gid, pid and nargs */
  sendbuf_size += strlen(command) + 1;                /* Command name */
  hsm = hsmstruct;
  while (hsm != NULL) {
    if (hsm->upath == NULL) {
      serrno = EFAULT;
      return (-1);
    }
    if (hsm->upath[0] == '\0') {
      serrno = EFAULT;
      return (-1);
    }
    sendbuf_size += strlen(hsm->upath) + 1;        /* user path */
    hsm = hsm->next;
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
  marshall_WORD (sbp, uid);
  marshall_WORD (sbp, gid);
  pid = getpid();
  marshall_WORD (sbp, pid);
  q2 = sbp;	/* save pointer. The next field will be updated */
  nargs = 1;
  marshall_WORD (sbp, nargs);
  marshall_STRING (sbp, command);

  /* Build link files arguments */
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
    c = send2stgd (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if (c == 0) break;
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
    sleep (RETRYI);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}
