/*
 * $Id: stage_updc_filcp.c,v 1.3 2000/02/27 10:42:41 baud Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_updc_filcp.c,v $ $Revision: 1.3 $ $Date: 2000/02/27 10:42:41 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <fcntl.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "serrno.h"
#include "stage.h"
#include "u64subr.h"

int DLL_DECL stage_updc_filcp(stageid, copyrc, ifce, size, waiting_time, transfer_time, blksize, drive, fid, fseq, lrecl, recfm, path, want_reply)
     char *stageid;
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
     int want_reply;
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
  char sendbuf[REQBUFSZ];
  char *stghost;
  char tmpbuf[21];
  uid_t uid;
  char Zparm[CA_MAXSTGRIDLEN+1];

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

  /* Build request header */

  sbp = sendbuf;
  marshall_LONG (sbp, STGMAGIC);
  marshall_LONG (sbp, STAGEUPDC);
  q = sbp;	/* save pointer. The next field will be updated */
  msglen = 3 * LONGSIZE;
  marshall_LONG (sbp, msglen);

  /* Build request body */

  if ((pw = getpwuid (uid)) == NULL) {
    serrno = SENOMAPFND;
    return (-1);
  }
  marshall_STRING (sbp, pw->pw_name);	/* login name */
  marshall_WORD (sbp, uid);
  marshall_WORD (sbp, gid);
  q2 = sbp;	/* save pointer. The next field will be updated */
  nargs = 1;
  marshall_WORD (sbp, nargs);
  marshall_STRING (sbp, "stage_updc_filcp");

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
    marshall_STRING (sbp,"-D");
    marshall_STRING (sbp, drive);
    nargs += 2;
  }
  if (recfm) {
    marshall_STRING (sbp,"-F");
    marshall_STRING (sbp, recfm);
    nargs += 2;
  }
  if (fid) {
    marshall_STRING (sbp,"-f");
    marshall_STRING (sbp, fid);
    nargs += 2;
  }
  if (ifce) {
    marshall_STRING (sbp,"-I");
    marshall_STRING (sbp, ifce);
    nargs += 2;
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
  if (size > 0) {
    u64tostr((u_signed64) size, tmpbuf, 0);
    marshall_STRING (sbp,"-s");
    marshall_STRING (sbp, tmpbuf);
    nargs += 2;
  }
  sprintf (tmpbuf, "%d", copyrc);
  marshall_STRING (sbp, "-R");
  marshall_STRING (sbp, tmpbuf);
  nargs += 2;
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
  marshall_WORD (q2, nargs);	/* update nargs */

  msglen = sbp - sendbuf;
  marshall_LONG (q, msglen);	/* update length field */

  while (c = send2stgd (stghost, sendbuf, msglen, want_reply, repbuf, sizeof(repbuf))) {
    if (c != ESTNACT || ntries++ > MAXRETRY) break;
    sleep (RETRYI);
  }
  if (want_reply) {
    if (c = 0) {
      rbp = repbuf;
      unmarshall_STRING (rbp, path);
    }
  }
  return (c);
}
