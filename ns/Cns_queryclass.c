/*
 * Copyright (C) 2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_queryclass.c,v $ $Revision: 1.3 $ $Date: 2006/01/26 15:36:19 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_queryclass - query about a fileclass */

#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "Cns_api.h"
#include "Cns.h"
#include "serrno.h"

int DLL_DECL
Cns_queryclass(char *server, int classid, char *class_name, struct Cns_fileclass *Cns_fileclass)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	int nbtppools;
	char *p;
	char *q;
	char *rbp;
	char repbuf[LISTBUFSZ];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;

	strcpy (func, "Cns_queryclass");
	if (Cns_apiinit (&thip))
		return (-1);
	Cns_getid(&uid, &gid);
	
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Cns_errmsg (func, NS053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	if (! Cns_fileclass) {
		serrno = EFAULT;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_QRYCLASS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, classid);
	if (class_name) {
		marshall_STRING (sbp, class_name);
	} else {
		marshall_STRING (sbp, "");
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	if (c == 0) {
		rbp = repbuf;
		unmarshall_LONG (rbp, Cns_fileclass->classid);
		unmarshall_STRING (rbp, Cns_fileclass->name);
		unmarshall_LONG (rbp, Cns_fileclass->uid);
		unmarshall_LONG (rbp, Cns_fileclass->gid);
		unmarshall_LONG (rbp, Cns_fileclass->min_filesize);
		unmarshall_LONG (rbp, Cns_fileclass->max_filesize);
		unmarshall_LONG (rbp, Cns_fileclass->flags);
		unmarshall_LONG (rbp, Cns_fileclass->maxdrives);
		unmarshall_LONG (rbp, Cns_fileclass->max_segsize);
		unmarshall_LONG (rbp, Cns_fileclass->migr_time_interval);
		unmarshall_LONG (rbp, Cns_fileclass->mintime_beforemigr);
		unmarshall_LONG (rbp, Cns_fileclass->nbcopies);
		unmarshall_LONG (rbp, Cns_fileclass->retenp_on_disk);
		unmarshall_LONG (rbp, Cns_fileclass->nbtppools);
		nbtppools = Cns_fileclass->nbtppools;
		if ((p = calloc (nbtppools, CA_MAXPOOLNAMELEN+1)) == NULL) {
			serrno = ENOMEM;
			return (-1);
		}
		Cns_fileclass->tppools = p;
		while (nbtppools--) {
			unmarshall_STRING (rbp, p);
			p += (CA_MAXPOOLNAMELEN+1);
		}
	}
	return (c);
}
