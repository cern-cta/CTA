/*
 * $Id: stage_xxx.c,v 1.1 2000/05/08 10:56:37 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_xxx.c,v $ $Revision: 1.1 $ $Date: 2000/05/08 10:56:37 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <pwd.h>
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

int DLL_DECL stage_xxx_hsm(command,stghost,Kopt,diskpool,hsmstruct)
     int command;
     char *stghost;
     int Kopt;
     char *diskpool;
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
  char sendbuf[REQBUFSZ];
  char tmpbuf[21];
  uid_t uid;
  stage_hsm_t *hsm;
  int isize = 0;
#if (defined(sun) && !defined(SOLARIS)) || defined(ultrix) || defined(_WIN32)
  int mask;
#else
  mode_t mask;
#endif
  int pid;
  
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
  
  if ((pw = getpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }
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

  if (diskpool != NULL) {
    if (diskpool[0] != '\0') {
      marshall_STRING (sbp, "-p");
      marshall_STRING (sbp, diskpool);
      nargs += 2;
    }
  }

  /* Build -s option */
  hsm = hsmstruct;
  while (hsm != NULL) {
    if (hsm->size > 0) {
      if (isize++ == 0) {
        marshall_STRING (sbp,"-s");
        nargs += 2;
      }
      u64tostr((u_signed64) hsm->size / ONE_MB, tmpbuf, 0);
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
    if (hsm->xfile == NULL) {
      serrno = EFAULT;
      return (-1);
    }
    if (hsm->xfile[0] == '\0') {
      serrno = EFAULT;
      return (-1);
    }
    marshall_STRING (sbp,"-M");
    marshall_STRING (sbp, hsm->xfile);
    nargs += 2;
    hsm = hsm->next;
  }

  /* Build link files arguments */
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
    marshall_STRING (sbp, hsm->upath);
    nargs += 1;
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
  return (c == 0 ? 0 : -1);
}
