/*
 * $Id: stage_xxx.c,v 1.10 2000/12/12 08:56:15 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_xxx.c,v $ $Revision: 1.10 $ $Date: 2000/12/12 08:56:15 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <pwd.h>
#include <stdlib.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "osdep.h"
#include "stage_api.h"
#include "stage.h"
#include "u64subr.h"
#include "Cpwd.h"
#include "Castor_limits.h"

int _stage_xxx_hsm _PROTO((int, char *, char *, char *, int, stage_hsm_t *));

int DLL_DECL stage_out_hsm(stghost,stgpool,stguser,Kopt,hsmstruct)
     char *stghost;
     char *stgpool;
     char *stguser;
     int Kopt;
     stage_hsm_t *hsmstruct;
{
  char stgpool_ok[CA_MAXPOOLNAMELEN + 1];
  char stguser_ok[CA_MAXUSRNAMELEN + 1];
  char *p;

  if (stgpool == NULL) {
    if ((p = getenv("STAGE_POOL")) != NULL) {
      strncpy(stgpool_ok,p,CA_MAXPOOLNAMELEN);
      stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
    } else {
      stgpool_ok[0] = '\0';
    }
  } else {
    strncpy(stgpool_ok,stgpool,CA_MAXPOOLNAMELEN);
    stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
  }

  if (stguser == NULL) {
    if ((p = getenv("STAGE_USER")) != NULL) {
      strncpy(stguser_ok,p,CA_MAXUSRNAMELEN);
      stguser_ok[CA_MAXUSRNAMELEN] = '\0';
    } else {
      stguser_ok[0] = '\0';
    }
  } else {
    strncpy(stguser_ok,stguser,CA_MAXUSRNAMELEN);
    stguser_ok[CA_MAXUSRNAMELEN] = '\0';
  }

  return(_stage_xxx_hsm(STAGEOUT,stghost,stgpool_ok,stguser_ok,Kopt,hsmstruct));
}

int DLL_DECL stage_in_hsm(stghost,stgpool,stguser,hsmstruct)
     char *stghost;
     char *stgpool;
     char *stguser;
     stage_hsm_t *hsmstruct;
{
  char stgpool_ok[CA_MAXPOOLNAMELEN + 1];
  char stguser_ok[CA_MAXUSRNAMELEN + 1];
  char *p;

  if (stgpool == NULL) {
    if ((p = getenv("STAGE_POOL")) != NULL) {
      strncpy(stgpool_ok,p,CA_MAXPOOLNAMELEN);
      stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
    } else {
      stgpool_ok[0] = '\0';
    }
  } else {
    strncpy(stgpool_ok,stgpool,CA_MAXPOOLNAMELEN);
    stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
  }

  if (stguser == NULL) {
    if ((p = getenv("STAGE_USER")) != NULL) {
      strncpy(stguser_ok,p,CA_MAXUSRNAMELEN);
      stguser_ok[CA_MAXUSRNAMELEN] = '\0';
    } else {
      stguser_ok[0] = '\0';
    }
  } else {
    strncpy(stguser_ok,stguser,CA_MAXUSRNAMELEN);
    stguser_ok[CA_MAXUSRNAMELEN] = '\0';
  }

  return(_stage_xxx_hsm(STAGEIN,stghost,stgpool_ok,stguser_ok,0,hsmstruct));
}

int DLL_DECL stage_wrt_hsm(stghost,stgpool,Kopt,hsmstruct)
     char *stghost;
     char *stgpool;
     int Kopt;
     stage_hsm_t *hsmstruct;
{
  char stgpool_ok[CA_MAXPOOLNAMELEN + 1];
  char *p;

  if (stgpool == NULL) {
    if ((p = getenv("STAGE_POOL")) != NULL) {
      strncpy(stgpool_ok,p,CA_MAXPOOLNAMELEN);
      stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
    } else {
      stgpool_ok[0] = '\0';
    }
  } else {
    strncpy(stgpool_ok,stgpool,CA_MAXPOOLNAMELEN);
    stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
  }

  return(_stage_xxx_hsm(STAGEWRT,stghost,stgpool_ok,NULL,Kopt,hsmstruct));
}

int DLL_DECL stage_cat_hsm(stghost,stgpool,hsmstruct)
     char *stghost;
     char *stgpool;
     stage_hsm_t *hsmstruct;
{
  char stgpool_ok[CA_MAXPOOLNAMELEN + 1];
  char *p;

  if (stgpool == NULL) {
    if ((p = getenv("STAGE_POOL")) != NULL) {
      strncpy(stgpool_ok,p,CA_MAXPOOLNAMELEN);
      stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
    } else {
      stgpool_ok[0] = '\0';
    }
  } else {
    strncpy(stgpool_ok,stgpool,CA_MAXPOOLNAMELEN);
    stgpool_ok[CA_MAXPOOLNAMELEN] = '\0';
  }

  return(_stage_xxx_hsm(STAGECAT,stghost,stgpool_ok,NULL,0,hsmstruct));
}

int _stage_xxx_hsm(command,stghost,stgpool,stguser,Kopt,hsmstruct)
     int command;
     char *stghost;
     char *stgpool;
     char *stguser;
     int Kopt;
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
  char tmpbuf[21];
  uid_t uid;
  stage_hsm_t *hsm;
  int isize;
#if (defined(sun) && !defined(SOLARIS)) || defined(_WIN32)
  int mask;
#else
  mode_t mask;
#endif
  int pid;
  int nxfile;
  int nupath;

  uid = geteuid();
  gid = getegid();
#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif
  switch (command) {
  case STAGEOUT:
  case STAGEIN:
  case STAGEWRT:
  case STAGECAT:
    break;
  default:
    serrno = EFAULT;
    return (-1);
  }
  
  if (hsmstruct == NULL) {
    serrno = EFAULT;
    return (-1);
  }
  
  if ((pw = Cgetpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;             /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1; /* Login name */
  sendbuf_size += strlen(pw->pw_name) + 1; /* User name */
  sendbuf_size += 5 * WORDSIZE;            /* uid, gid, mask, pid and nargs */
  switch (command) {
  case STAGEOUT:
    sendbuf_size += strlen("stage_out_hsm") + 1; /* Command name */
    break;
  case STAGEIN:
    sendbuf_size += strlen("stage_in_hsm") + 1; /* Command name */
    break;
  case STAGEWRT:
    sendbuf_size += strlen("stage_wrt_hsm") + 1; /* Command name */
    break;
  case STAGECAT:
    sendbuf_size += strlen("stage_cat_hsm") + 1; /* Command name */
    break;
  }
  switch (command) {
  case STAGEOUT:
  case STAGEWRT:
    if (Kopt) {
      sendbuf_size += strlen("-K") + 1; /* -K option */
    }
    break;
  default:
    break;
  }
  if (stgpool != NULL) {
    if (stgpool[0] != '\0') {
      sendbuf_size += strlen("-p") + strlen(stgpool) + 2; /* -p option and value */
    }
  }
  if (stguser != NULL) {
    if (stguser[0] != '\0') {
      sendbuf_size += strlen("-u") + strlen(stguser) + 2; /* -u option and value */
    }
  }
  hsm = hsmstruct;
  isize = 0;
  while (hsm != NULL) {
    int correct_size_MB;

    if (hsm->size > 0) {
      correct_size_MB = (int) ((hsm->size > ONE_MB) ? (((hsm->size - ((hsm->size / ONE_MB) * ONE_MB)) == 0) ? (hsm->size / ONE_MB) : ((hsm->size / ONE_MB) + 1)) : 1);
      if (isize++ == 0) {
        sendbuf_size += strlen("-s") + 1; /* -s option */
      }
      u64tostr((u_signed64) correct_size_MB, tmpbuf, 0);
      sendbuf_size += strlen(tmpbuf) + 1; /* -s value */
    }
    hsm = hsm->next;
  }
  hsm = hsmstruct;
  nxfile = 0;
  while (hsm != NULL) {
    if (hsm->xfile == NULL) {
      serrno = EFAULT;
      return (-1);
    }
    if (hsm->xfile[0] == '\0') {
      serrno = EFAULT;
      return (-1);
    }
    sendbuf_size += strlen("-M") + strlen(hsm->xfile) + 2; /* -M option and value */
    hsm = hsm->next;
    nxfile++;
  }
  hsm = hsmstruct;
  nupath = 0;
  while (hsm != NULL) {
    if (hsm->upath != NULL && hsm->upath[0] != '\0') {
      sendbuf_size += strlen(hsm->upath) + 1; /* User path */
      nupath++;
    }
    hsm = hsm->next;
  }

  /* We accept either that nupath is zero or equal to the number of hsm files */
  if (nupath != 0 && nupath != nxfile) {
    serrno = EINVAL;
    return (-1);
  }

  /* Allocate memory */
  if ((sendbuf = (char *) malloc(sendbuf_size)) == NULL) {
    serrno = SEINTERNAL;
    return(-1);
  }

  /* Init repbuf to null */
  repbuf[0] = '\0';
  
  /* Build request header */
  
  sbp = sendbuf;
  marshall_LONG (sbp, STGMAGIC);
  marshall_LONG (sbp, command);
  q = sbp;	/* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);
  
  /* Build request body */
  
  marshall_STRING (sbp, pw->pw_name);	/* login name */
  marshall_STRING (sbp, pw->pw_name);	/* user */
  marshall_WORD (sbp, uid);
  marshall_WORD (sbp, gid);
  umask (mask = umask(0));
  marshall_WORD (sbp, mask);
  pid = getpid();
  marshall_WORD (sbp, pid);
  
  q2 = sbp;	/* save pointer. The next field will be updated */
  nargs = 1;
  marshall_WORD (sbp, nargs);
  switch (command) {
  case STAGEOUT:
    marshall_STRING (sbp, "stage_out_hsm");
    break;
  case STAGEIN:
    marshall_STRING (sbp, "stage_in_hsm");
    break;
  case STAGEWRT:
    marshall_STRING (sbp, "stage_wrt_hsm");
    break;
  case STAGECAT:
    marshall_STRING (sbp, "stage_cat_hsm");
    break;
  }
  
  switch (command) {
  case STAGEOUT:
  case STAGEWRT:
    if (Kopt) {
      marshall_STRING (sbp, "-K");
      nargs += 1;
    }
    break;
  default:
    break;
  }

  if (stgpool != NULL) {
    if (stgpool[0] != '\0') {
      marshall_STRING (sbp, "-p");
      marshall_STRING (sbp, stgpool);
      nargs += 2;
    }
  }
  if (stguser != NULL) {
    if (stguser[0] != '\0') {
      marshall_STRING (sbp, "-u");
      marshall_STRING (sbp, stguser);
      nargs += 2;
    }
  }

  /* Build -s option */
  hsm = hsmstruct;
  isize = 0;
  while (hsm != NULL) {
    int correct_size_MB;

    if (hsm->size > 0) {
      correct_size_MB = (int) ((hsm->size > ONE_MB) ? (((hsm->size - ((hsm->size / ONE_MB) * ONE_MB)) == 0) ? (hsm->size / ONE_MB) : ((hsm->size / ONE_MB) + 1)) : 1);
      if (isize++ == 0) {
        marshall_STRING (sbp,"-s");
        nargs += 2;
      }
      u64tostr((u_signed64) correct_size_MB, tmpbuf, 0);
      marshall_STRING (sbp, tmpbuf);
      if (isize > 1) {
        /* We remove the '\0' character between us and our previous... */
        /* and we replace it by the expect ':' character ... */
        *(sbp - (strlen(tmpbuf) + 1) - 1) = ':';
      }
    }
    hsm = hsm->next;
  }

  /* Build -M option(s) */
  hsm = hsmstruct;
  while (hsm != NULL) {
    marshall_STRING (sbp,"-M");
    marshall_STRING (sbp, hsm->xfile);
    nargs += 2;
    hsm = hsm->next;
  }

  /* Build link files arguments */
  hsm = hsmstruct;
  while (hsm != NULL) {
    if (hsm->upath != NULL && hsm->upath[0] != '\0') {
      marshall_STRING (sbp, hsm->upath);
      nargs += 1;
    }
    hsm = hsm->next;
  }

  marshall_WORD (q2, nargs);	/* update nargs */
  
  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */
  
  while (1) {
    c = send2stgd (NULL, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if (c == 0) break;
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
    sleep (RETRYI);
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}
