/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Cns_opendir - open a directory entry */

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

Cns_DIR DLL_DECL *
Cns_opendirxg(char *host, const char *path, const char *guid)
{
	char *actual_path;
	char *actual_server;
	int c;
	Cns_DIR *dirp = NULL;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[8];
	int s = -1;
	char *sbp;
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Cns_opendir");
	if (Cns_apiinit (&thip))
		return (NULL);
	Cns_getid(&uid, &gid);
	
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Cns_errmsg (func, NS053);
		serrno = SENOMAPFND;
		return (NULL);
	}
#endif

	if (! path && ! guid) {
		serrno = EFAULT;
		return (NULL);
	}

	if (path && strlen (path) > CA_MAXPATHLEN) {
		serrno = ENAMETOOLONG;
		return (NULL);
	}
	if (guid && strlen (guid) > CA_MAXGUIDLEN) {
		serrno = EINVAL;
		return (NULL);
	}

	if ((dirp = (Cns_DIR *) malloc (sizeof(Cns_DIR) + DIRBUFSZ)) == NULL) {
		serrno = ENOMEM;
		return (NULL);
	}

	if (path && Cns_selectsrvr (path, thip->server, server, &actual_path))
		return (NULL);
	if (path && actual_path != path)
		actual_server = server;
	else if (host)
		actual_server = host;
	else if (path)
		actual_server = server;
	else
		actual_server = NULL;

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, guid ? CNS_MAGIC2 : CNS_MAGIC);
	marshall_LONG (sbp, CNS_OPENDIR);
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

	while ((c = send2nsd (&s, actual_server, sendbuf, msglen, repbuf,
	    sizeof(repbuf))) && serrno == ENSNACT)
		sleep (RETRYI);
	if (c < 0) {
		if (serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
		free (dirp);
		return (NULL);
	}
	memset (dirp, 0, sizeof(Cns_DIR));
	dirp->dd_fd = s;
	rbp = repbuf;
	unmarshall_HYPER (rbp, dirp->fileid);
	dirp->bod = 1;
	dirp->dd_buf = (char *) dirp + sizeof(Cns_DIR);
	return (dirp);
}

Cns_DIR DLL_DECL *
Cns_opendirg(const char *path, const char *guid)
{
	return (Cns_opendirxg (NULL, path, guid));
}

Cns_DIR DLL_DECL *
Cns_opendir(const char *path)
{
	return (Cns_opendirg (path, NULL));
}
