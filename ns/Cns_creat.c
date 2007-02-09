/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Cns_creatx - create a new file entry and return unique fileid */

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
Cns_creatc(const char *path, const char *guid, mode_t mode, struct Cns_fileid *file_uniqueid)
{
	char *actual_path;
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[8];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
 
	strcpy (func, "Cns_creat");
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

	if (! path || ! file_uniqueid) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (path) > CA_MAXPATHLEN) {
		serrno = ENAMETOOLONG;
		return (-1);
	}
	if (guid && strlen (guid) > CA_MAXGUIDLEN) {
		serrno = EINVAL;
		return (-1);
	}

	if (Cns_selectsrvr (path, thip->server, server, &actual_path))
		return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, guid ? CNS_MAGIC2 : CNS_MAGIC);
	marshall_LONG (sbp, CNS_CREAT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_WORD (sbp, thip->mask);
	marshall_HYPER (sbp, thip->cwd);
	marshall_STRING (sbp, actual_path);
	marshall_LONG (sbp, mode);
	if (guid)
		marshall_STRING (sbp, guid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		strcpy (file_uniqueid->server, server);
		unmarshall_HYPER (rbp, file_uniqueid->fileid);
	}
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}

/*	Cns_creat - create a new file entry */

int DLL_DECL
Cns_creat(const char *path, mode_t mode)
{
	struct Cns_fileid file_uniqueid;

	return (Cns_creatc (path, NULL, mode, &file_uniqueid));
}

int DLL_DECL
Cns_creatg(const char *path, const char *guid, mode_t mode)
{
	struct Cns_fileid file_uniqueid;

	return (Cns_creatc (path, guid, mode, &file_uniqueid));
}

int DLL_DECL
Cns_creatx(const char *path, mode_t mode, struct Cns_fileid *file_uniqueid)
{
	return (Cns_creatc (path, NULL, mode, file_uniqueid));
}
