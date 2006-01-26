/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_getidmap.c,v $ $Revision: 1.2 $ $Date: 2006/01/26 15:36:18 $ CERN IT-GD/SC Jean-Philippe Baud";
#endif /* not lint */

/*      Cns_getidmap - get uid/gids associated with a given dn/roles */

#include <errno.h>
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
Cns_getidmap (const char *username, int nbgroups, const char **groupnames, uid_t *userid, gid_t *gids)
{
	int c;
	char func[16];
	int i;
	int msglen;
	int n;
	char *q;
	char *rbp;
	char repbuf[REPBUFSZ];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;

	strcpy (func, "Cns_getidmap");
	if (Cns_apiinit (&thip))
		return (-1);

	if (! username || ! userid || ! gids) {
		serrno = EFAULT;
		return (-1);
	}
	if (nbgroups < 0) {
		serrno = EINVAL;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_GETIDMAP);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_STRING (sbp, username);
	marshall_LONG (sbp, nbgroups);
	if (groupnames) {
		for (i = 0; i < nbgroups; i++) {
			marshall_STRING (sbp, groupnames[i]);
		}
	} else {
		marshall_STRING (sbp, "");
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, NULL, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_LONG (rbp, n);
		*userid = n;
		if (nbgroups == 0)
			nbgroups = 1;
		for (i = 0; i < nbgroups; i++) {
			unmarshall_LONG (rbp, n);
			*(gids+i) = n;
		}
	}
	return (c);
}
