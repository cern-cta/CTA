/*
 * $Id: stage_qry.c,v 1.1 2000/11/17 07:46:33 jdurand Exp $
 */

/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_qry.c,v $ $Revision: 1.1 $ $Date: 2000/11/17 07:46:33 $ CERN IT-PDP/DM Jean-Damien Durand";
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

int _stage_qry_hsm_ipath _PROTO((char *, char *, char *, char *));

int DLL_DECL stage_qry_hsm_ipath(stghost,stgpool,hsmpath,ipath)
     char *stghost;
     char *stgpool;
     char *hsmpath;
     char *ipath;
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

  return(_stage_qry_hsm_ipath(stghost,stgpool_ok,hsmpath,ipath));
}

int _stage_qry_hsm_ipath(stghost,stgpool,hsmpath,ipath)
     char *stghost;
     char *stgpool;
     char *hsmpath;
     char *ipath;
{
  int c;
  gid_t gid;
  int msglen;
  int nargs;
  int ntries = 0;
  struct passwd *pw;
  char *q, *q2;
  char *rbp;
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
  char *command = "stage_qry_hsm_ipath";

  uid = geteuid();
  gid = getegid();
#if defined(_WIN32)
  if (uid < 0 || gid < 0) {
    serrno = SENOMAPFND;
    return (-1);
  }
#endif
  
  if (hsmpath == NULL || hsmpath[0] == '\0') {
    serrno = EFAULT;
    return (-1);
  }

  if ((pw = Cgetpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }

  /* How many bytes do we need ? */
  sendbuf_size = 3 * LONGSIZE;             /* Request header */
  sendbuf_size += strlen(pw->pw_name) + 1; /* User name */
  sendbuf_size += 2 * WORDSIZE;            /* gid, nargs */
  sendbuf_size += strlen(command) + 1; /* Command name */
  if (stgpool != NULL) {
    if (stgpool[0] != '\0') {
      sendbuf_size += strlen("-p") + strlen(stgpool) + 2; /* -p option and value */
    }
  }
  sendbuf_size += strlen("-M") + strlen(hsmpath) + 2; /* -M option and value */
  sendbuf_size += strlen("-P") + 1;                   /* -P option */

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
  marshall_LONG (sbp, STAGEQRY);
  q = sbp;	/* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);
  
  /* Build request body */
  
  marshall_STRING (sbp, pw->pw_name);	/* login name */
  marshall_WORD (sbp, gid);
  q2 = sbp;	/* save pointer. The next field will be updated */
  nargs = 1;
  marshall_WORD (sbp, nargs);
  marshall_STRING (sbp, "stage_qry_hsm_ipath");

  if (stgpool != NULL) {
    if (stgpool[0] != '\0') {
      marshall_STRING (sbp, "-p");
      marshall_STRING (sbp, stgpool);
      nargs += 2;
    }
  }

  marshall_STRING (sbp,"-M");
  marshall_STRING (sbp, hsmpath);
  nargs += 2;
  marshall_STRING (sbp,"-P");
  nargs ++;

  marshall_WORD (q2, nargs);	/* update nargs */
  
  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */
  
  while (1) {
    c = send2stgd (stghost, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
    if (c == 0) break;
    if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
    sleep (RETRYI);
  }
  if (c == 0) {
    if (repbuf[0] != '\0' && repbuf[0] != '\n') {
      if (ipath != NULL) {
        rbp = repbuf;
        unmarshall_STRING (rbp, ipath);
        /* We also remove a fishy newline */
        if (ipath[strlen(ipath) - 1] == '\n') {
          ipath[strlen(ipath) - 1] = '\0';
        }
      }
    } else {
      c = -1;
      serrno = ENOENT;
    }
  }
  free(sendbuf);
  return (c == 0 ? 0 : -1);
}
