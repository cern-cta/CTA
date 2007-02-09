/*
 * Copyright (C) 2003 by CERN/IT/ADC/CA
 * All rights reserved
 */

/*	Cns_getacl - get the Access Control List for a file/directory */

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
Cns_getacl(const char *path, int nentries, struct Cns_acl *acl)
{
	char *actual_path;
	int c;
	char func[16];
	gid_t gid;
	int i;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[REPBUFSZ];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
 
	strcpy (func, "Cns_getacl");
	if (Cns_apiinit (&thip))
		return (-1);
	Cns_getrealid(&uid, &gid);
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Cns_errmsg (func, NS053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	if (! path || (! acl && nentries > 0)) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (path) > CA_MAXPATHLEN) {
		serrno = ENAMETOOLONG;
		return (-1);
	}
	if (nentries < 0) {
		serrno = EINVAL;
		return (-1);
	}

	if (Cns_selectsrvr (path, thip->server, server, &actual_path))
		return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_GETACL);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_HYPER (sbp, thip->cwd);
	marshall_STRING (sbp, actual_path);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_WORD (rbp, c);
		if (nentries == 0)
			return (c);
		if (c > nentries) {
			serrno = ENOSPC;
			return (-1);
		}
		for (i = 0; i < c; i++) {
			unmarshall_BYTE (rbp, acl->a_type);
			unmarshall_LONG (rbp, acl->a_id);
			unmarshall_BYTE (rbp, acl->a_perm);
			acl++;
		}
	}
	if (c < 0 && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}
