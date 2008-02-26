/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

/*	Cns_getlinks - get the link entries associated with a given file */

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
Cns_getlinks(const char *path, const char *guid, int *nbentries, struct Cns_linkinfo **linkinfos)
{
	char *actual_path;
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char repbuf[1];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;

	strcpy (func, "Cns_getlinks");
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

	if ((! path && ! guid) || ! nbentries || ! linkinfos) {
		serrno = EFAULT;
		return (-1);
	}

	if (path && strlen (path) > CA_MAXPATHLEN) {
		serrno = ENAMETOOLONG;
		return (-1);
	}
	if (guid && strlen (guid) > CA_MAXGUIDLEN) {
		serrno = EINVAL;
		return (-1);
	}

	if (path && Cns_selectsrvr (path, thip->server, server, &actual_path))
		return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_GETLINKS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_HYPER (sbp, thip->cwd);
	if (path) {
		marshall_STRING (sbp, actual_path);
	} else {
		marshall_STRING (sbp, "");
	}
	if (guid) {
		marshall_STRING (sbp, guid);
	} else {
		marshall_STRING (sbp, "");
	}
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2nsdx (NULL, path ? server : NULL, sendbuf,
		       msglen, repbuf, sizeof(repbuf), 
		       (void **)linkinfos, nbentries);
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}
