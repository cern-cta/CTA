/*
 * $Id: stage_put.c,v 1.1 2000/05/08 10:58:55 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_put.c,v $ $Revision: 1.1 $ $Date: 2000/05/08 10:58:55 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <fcntl.h>
#include <grp.h>
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

extern char *getconfent();

int DLL_DECL stage_put_hsm(stghost,Migrationflag,hsmstruct)
     char *stghost;
     int Migrationflag;
     stage_hsm_t *hsmstruct;
{
	uid_t uid;
	gid_t gid;
	char *sbp;
	struct passwd *pw;
	char *p, *q, *q2;
	int nargs;
	char sendbuf[REQBUFSZ];
	int msglen;
	int c;
	int ntries = 0;
    char *stghost_ok = NULL;
	char Gname[15];
	uid_t Guid;
	struct group *gr;
    int pid;
	char repbuf[CA_MAXPATHLEN+1];
    stage_hsm_t *hsm;

	uid = geteuid();
	gid = getegid();
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
      serrno = SENOMAPFND;
      return (-1);
	}
#endif
    
    if (hsmstruct == NULL) {
      serrno = EFAULT;
      return (-1);
    }

    /* Makes sure stghost is valid if any */
    if (stghost != NULL) {
      if (stghost[0] != '\0') {
        stghost_ok = stghost;
      }
    }

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEPUT);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	if ((pw = getpwuid (uid)) == NULL) {
		serrno = SENOMAPFND;
		return (-1);
	}
	marshall_STRING (sbp, pw->pw_name);	/* login name */
    marshall_STRING (sbp, pw->pw_name);
    marshall_WORD (sbp, uid);
	marshall_WORD (sbp, gid);
	pid = getpid();
	marshall_WORD (sbp, pid);

	q2 = sbp;	/* save pointer. The next field will be updated */
	nargs = 1;
	marshall_WORD (sbp, nargs);
	marshall_STRING (sbp, "stage_put_hsm");

    if (Migrationflag) {
		marshall_STRING (sbp, "-m");
		nargs++;
    }

    hsm = hsmstruct;
    while (hsm != NULL) {
      if (hsm->xfile[0] == '\0') {
        serrno = EFAULT;
        return (-1);
      }
      marshall_STRING (sbp,"-M");
      marshall_STRING (sbp, hsm->xfile);
      nargs += 2;
      hsm = hsm->next;
	}

	marshall_WORD (q2, nargs);	/* update nargs */

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	/* Init repbuf to null */
	repbuf[0] = '\0';

	while (1) {
		c = send2stgd (stghost_ok, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
		if (c == 0) break;
		if (serrno != ESTNACT || ntries++ > MAXRETRY) break;
		sleep (RETRYI);
	}
	return (c == 0 ? 0 : -1);
}
