/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_setfsize.c,v $ $Revision: 1.3 $ $Date: 2006/01/26 15:36:20 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_setfsize - set file size and last modification date */

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
Cns_setfsize(const char *path, struct Cns_fileid *file_uniqueid, u_signed64 filesize)
{
	char *actual_path;
	int c, n;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	u_signed64 zero = 0;
 
	strcpy (func, "Cns_setfsize");
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

	if (! path && ! file_uniqueid) {
		serrno = EFAULT;
		return (-1);
	}

	if (path && strlen (path) > CA_MAXPATHLEN) {
		serrno = ENAMETOOLONG;
		return (-1);
	}

	if (file_uniqueid && *file_uniqueid->server)
		strcpy (server, file_uniqueid->server);
	else
		if (Cns_selectsrvr (path, thip->server, server, &actual_path))
			return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_SETFSIZE);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_HYPER (sbp, thip->cwd);
	if (file_uniqueid && *file_uniqueid->server) {
		marshall_HYPER (sbp, file_uniqueid->fileid);
		marshall_STRING (sbp, "");
	} else {
		marshall_HYPER (sbp, zero);
		marshall_STRING (sbp, actual_path);
	}
	marshall_HYPER (sbp, filesize);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0)) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}

int DLL_DECL
Cns_setfsizeg(const char *guid, u_signed64 filesize, const char *csumtype, char *csumvalue)
{
	int c, n;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	u_signed64 zero = 0;
 
	strcpy (func, "Cns_setfsizeg");
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

	if (! guid) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (guid) > CA_MAXGUIDLEN ||
	    (csumtype && strlen (csumtype) > 2) ||
	    (csumvalue && strlen (csumvalue) > 32)) {
		serrno = EINVAL;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_SETFSIZEG);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, guid);
	marshall_HYPER (sbp, filesize);
	if (csumtype) {
		marshall_STRING (sbp, csumtype);
	} else {
		marshall_STRING (sbp, "");
	}
	if (csumvalue) {
		marshall_STRING (sbp, csumvalue);
	} else {
		marshall_STRING (sbp, "");
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, NULL, sendbuf, msglen, NULL, 0)) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	return (c);
}
