/*
 * $Id: stage_put.c,v 1.17 2002/03/05 14:44:04 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stage_put.c,v $ $Revision: 1.17 $ $Date: 2002/03/05 14:44:04 $ CERN IT-PDP/DM Jean-Damien Durand";
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
#include "Cpwd.h"
#include "Castor_limits.h"
#include "stage_api.h"

extern char *getconfent();

int DLL_DECL stage_put_hsm(stghost,migratorflag,hsmstruct)
	char *stghost;
	int migratorflag;
	stage_hsm_t *hsmstruct;
{
	uid_t euid;
	gid_t egid;
	char *sbp;
	struct passwd *pw;
	char *q, *q2;
	int nargs;
	char *sendbuf = NULL;
	size_t sendbuf_size = 0;
	int msglen;
	int c;
	int ntries = 0;
	int nstg161 = 0;
	char *stghost_ok = NULL;
	int pid;
	char repbuf[CA_MAXPATHLEN+1];
	stage_hsm_t *hsm;
	char *func = "stage_put_hsm";

	euid = geteuid();
	egid = getegid();
#if defined(_WIN32)
	if ((euid < 0) || (euid >= CA_MAXUID) || (egid < 0) || (egid >= CA_MAXGID)) {
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

	if ((pw = Cgetpwuid (euid)) == NULL) {
        if (errno != ENOENT) stage_errmsg(func, STG33, "Cgetpwuid", strerror(errno));
		serrno = SEUSERUNKN;
		return (-1);
	}

	/* We calculate the amount of buffer we need */
	sendbuf_size = 3 * LONGSIZE;                   /* Header */
	sendbuf_size += strlen(pw->pw_name) + 1;       /* Username */
	sendbuf_size += strlen(pw->pw_name) + 1;       /* Username under which put is done */
	sendbuf_size += WORDSIZE;                      /* Uid */
	sendbuf_size += WORDSIZE;                      /* Gid */
	sendbuf_size += WORDSIZE;                      /* Pid */
	sendbuf_size += WORDSIZE;                      /* Narg */
	sendbuf_size += strlen(func) + 1;              /* func */
	if (migratorflag != 0) {
		sendbuf_size += strlen("-m") + 1;            /* Migrator flag */
	}
	hsm = hsmstruct;
	while (hsm != NULL) {
		if (hsm->xfile[0] == '\0') {
			serrno = EFAULT;
			return (-1);
		}
		sendbuf_size += strlen("-M") + 1;          /* HSM or CASTOR file */
		sendbuf_size += strlen(hsm->xfile) + 1;
		hsm = hsm->next;
	}

	/* We request for this amount of space */
	if ((sendbuf = (char *) malloc((size_t) sendbuf_size)) == NULL) {
		serrno = SEINTERNAL;
		return (-1);
	}

	/* Build request header */
	sbp = sendbuf;
	marshall_LONG (sbp, STGMAGIC);
	marshall_LONG (sbp, STAGEPUT);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
	marshall_STRING (sbp, pw->pw_name);	/* login name */
	marshall_STRING (sbp, pw->pw_name);
	marshall_WORD (sbp, euid);
	marshall_WORD (sbp, egid);
	pid = getpid();
	marshall_WORD (sbp, pid);

	q2 = sbp;	/* save pointer. The next field will be updated */
	nargs = 1;
	marshall_WORD (sbp, nargs);
	marshall_STRING (sbp, func);

	if (migratorflag != 0) {
		marshall_STRING (sbp,"-m");
		nargs += 1;
	}

	hsm = hsmstruct;
	while (hsm != NULL) {
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
		c = send2stgd_compat (stghost_ok, sendbuf, msglen, 1, repbuf, sizeof(repbuf));
		if ((c == 0) || (serrno == EINVAL) || (serrno == ERTLIMBYSZ) || (serrno == ESTCLEARED) || (serrno == ENOSPC) || (serrno == ESTKILLED)) break;
		if (serrno == ESTNACT && nstg161++ == 0) stage_errmsg(func, STG161);
		if (serrno != ESTNACT && ntries++ > MAXRETRY) break;
		stage_sleep (RETRYI);
	}
	free(sendbuf);
	return (c == 0 ? 0 : -1);
}
