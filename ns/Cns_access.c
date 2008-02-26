/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Cns_access - check accessibility of a file/directory */

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

static int 
Cns_access_internal(const char* func, const char *path, int amode, uid_t uid, gid_t gid)
{
	char *actual_path;
	int c;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
 
	if (Cns_apiinit (&thip))
		return (-1);
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Cns_errmsg (func, NS053);
		serrno = SENOMAPFND;
		return (-1);
	}
#endif

	if (! path) {
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
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_ACCESS);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_HYPER (sbp, thip->cwd);
	marshall_STRING (sbp, actual_path);
	marshall_LONG (sbp, amode);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0);
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}


int DLL_DECL
Cns_access(const char *path, int amode)
{
  gid_t gid;
  uid_t uid;
  char* func = "Cns_access";
  Cns_getrealid(&uid, &gid);

  return Cns_access_internal(func, path, amode, uid, gid);
}


int DLL_DECL
Cns_accessUser(const char *path, int amode, uid_t uid, gid_t gid)
{
  char* func = "Cns_accessUser";

  return Cns_access_internal(func, path, amode, uid, gid);
}


int DLL_DECL
Cns_accessr(const char *sfn, int amode)
{
	int c;
	char func[16];
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	gid_t gid; 

	strcpy (func, "Cns_accessr");
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

	if (! sfn) {
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
	marshall_LONG (sbp, CNS_ACCESSR);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, sfn);
	marshall_LONG (sbp, amode);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2nsd (NULL, NULL, sendbuf, msglen, NULL, 0);
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}
