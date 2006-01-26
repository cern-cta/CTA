/*
 * Copyright (C) 1999-2005 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_stat.c,v $ $Revision: 1.3 $ $Date: 2006/01/26 15:36:21 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_statx - get information about a file or a directory and
	return unique fileid */

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

/*	Cns_lstat - get information about a symbolic link */

int DLL_DECL
Cns_lstat(const char *path, struct Cns_filestat *statbuf)
{
	char *actual_path;
	int c, n;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[57];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	u_signed64 zero = 0;
 
	strcpy (func, "Cns_lstat");
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

	if (! path || ! statbuf) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (path) > CA_MAXPATHLEN) {
		serrno = ENAMETOOLONG;
		return (-1);
	}

	if (Cns_selectsrvr (path, thip->server, server, &actual_path))
		return (-1);

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC2);
	marshall_LONG (sbp, CNS_LSTAT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_HYPER (sbp, thip->cwd);
	marshall_HYPER (sbp, zero);
	marshall_STRING (sbp, actual_path);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_HYPER (rbp, statbuf->fileid);
		unmarshall_WORD (rbp, statbuf->filemode);
		unmarshall_LONG (rbp, statbuf->nlink);
		unmarshall_LONG (rbp, statbuf->uid);
		unmarshall_LONG (rbp, statbuf->gid);
		unmarshall_HYPER (rbp, statbuf->filesize);
		unmarshall_TIME_T (rbp, statbuf->atime);
		unmarshall_TIME_T (rbp, statbuf->mtime);
		unmarshall_TIME_T (rbp, statbuf->ctime);
		unmarshall_WORD (rbp, statbuf->fileclass);
		unmarshall_BYTE (rbp, statbuf->status);
	}
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}

int DLL_DECL
Cns_statx(const char *path, struct Cns_fileid *file_uniqueid, struct Cns_filestat *statbuf)
{
	char *actual_path;
	int c, n;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[57];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	u_signed64 zero = 0;
 
	strcpy (func, "Cns_stat");
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

	if (! path || ! statbuf || ! file_uniqueid) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (path) > CA_MAXPATHLEN) {
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
	marshall_LONG (sbp, CNS_STAT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_HYPER (sbp, thip->cwd);
	if (*file_uniqueid->server) {
		marshall_HYPER (sbp, file_uniqueid->fileid);
		marshall_STRING (sbp, "");
	} else {
		marshall_HYPER (sbp, zero);
		marshall_STRING (sbp, actual_path);
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_HYPER (rbp, statbuf->fileid);
		unmarshall_WORD (rbp, statbuf->filemode);
		unmarshall_LONG (rbp, statbuf->nlink);
		unmarshall_LONG (rbp, statbuf->uid);
		unmarshall_LONG (rbp, statbuf->gid);
		unmarshall_HYPER (rbp, statbuf->filesize);
		unmarshall_TIME_T (rbp, statbuf->atime);
		unmarshall_TIME_T (rbp, statbuf->mtime);
		unmarshall_TIME_T (rbp, statbuf->ctime);
		unmarshall_WORD (rbp, statbuf->fileclass);
		unmarshall_BYTE (rbp, statbuf->status);

		strcpy (file_uniqueid->server, server);
		file_uniqueid->fileid = statbuf->fileid;
	}
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}

/*	Cns_stat - get information about a file or a directory */

int DLL_DECL
Cns_stat(const char *path, struct Cns_filestat *statbuf)
{
	struct Cns_fileid file_uniqueid;

	memset ((void *) &file_uniqueid, 0, sizeof(struct Cns_fileid));
	return (Cns_statx (path, &file_uniqueid, statbuf));
}

int DLL_DECL
Cns_statg(const char *path, const char *guid, struct Cns_filestatg *statbuf)
{
	char *actual_path;
	int c, n;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[130];
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
 
	strcpy (func, "Cns_statg");
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

	if ((! path && ! guid) || ! statbuf) {
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
	marshall_LONG (sbp, CNS_STATG);
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

	while ((c = send2nsd (NULL, path ? server : NULL, sendbuf, msglen,
	    repbuf, sizeof(repbuf))) && serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_HYPER (rbp, statbuf->fileid);
		unmarshall_STRING (rbp, statbuf->guid);
		unmarshall_WORD (rbp, statbuf->filemode);
		unmarshall_LONG (rbp, statbuf->nlink);
		unmarshall_LONG (rbp, statbuf->uid);
		unmarshall_LONG (rbp, statbuf->gid);
		unmarshall_HYPER (rbp, statbuf->filesize);
		unmarshall_TIME_T (rbp, statbuf->atime);
		unmarshall_TIME_T (rbp, statbuf->mtime);
		unmarshall_TIME_T (rbp, statbuf->ctime);
		unmarshall_WORD (rbp, statbuf->fileclass);
		unmarshall_BYTE (rbp, statbuf->status);
		unmarshall_STRING (rbp, statbuf->csumtype);
		unmarshall_STRING (rbp, statbuf->csumvalue);
	}
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}

int DLL_DECL
Cns_statr(const char *sfn, struct Cns_filestatg *statbuf)
{
	int c, n;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[94];
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;
 
	strcpy (func, "Cns_statr");
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

	if (! sfn || ! statbuf) {
		serrno = EFAULT;
		return (-1);
	}

	if (strlen (sfn) > CA_MAXSFNLEN) {
		serrno = ENAMETOOLONG;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_STATR);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, sfn);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, NULL, sendbuf, msglen, repbuf,
	    sizeof(repbuf))) && serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_HYPER (rbp, statbuf->fileid);
		unmarshall_STRING (rbp, statbuf->guid);
		unmarshall_WORD (rbp, statbuf->filemode);
		unmarshall_LONG (rbp, statbuf->nlink);
		unmarshall_LONG (rbp, statbuf->uid);
		unmarshall_LONG (rbp, statbuf->gid);
		unmarshall_HYPER (rbp, statbuf->filesize);
		unmarshall_TIME_T (rbp, statbuf->atime);
		unmarshall_TIME_T (rbp, statbuf->mtime);
		unmarshall_TIME_T (rbp, statbuf->ctime);
		unmarshall_WORD (rbp, statbuf->fileclass);
		unmarshall_BYTE (rbp, statbuf->status);
		unmarshall_STRING (rbp, statbuf->csumtype);
		unmarshall_STRING (rbp, statbuf->csumvalue);
	}
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}
