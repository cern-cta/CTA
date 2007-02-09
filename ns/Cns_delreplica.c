/*
 * Copyright (C) 2004-2005 by CERN/IT/GD/CT
 * All rights reserved
 */

/*	Cns_delreplica - delete a replica for a given file */

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
Cns_delreplica(const char *guid, struct Cns_fileid *file_uniqueid, const char *sfn)
{
	int c;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	u_signed64 zero = 0;
 
	strcpy (func, "Cns_delreplica");
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

	if (! sfn) {
		serrno = EFAULT;
		return (-1);
	}

	if (guid && strlen (guid) > CA_MAXGUIDLEN) {
		serrno = EINVAL;
		return (-1);
	}
	if (strlen (sfn) > CA_MAXSFNLEN) {
		serrno = ENAMETOOLONG;
		return (-1);
	}

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC);
	marshall_LONG (sbp, CNS_DELREPLICA);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);
 
	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	if (file_uniqueid && file_uniqueid->fileid) {
		marshall_HYPER (sbp, file_uniqueid->fileid);
		marshall_STRING (sbp, "");
	} else if (guid) {
		marshall_HYPER (sbp, zero);
		marshall_STRING (sbp, guid);
	} else {
		marshall_HYPER (sbp, zero);
		marshall_STRING (sbp, "");
	}
	marshall_STRING (sbp, sfn);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL,
	    (file_uniqueid && *file_uniqueid->server) ? file_uniqueid->server : NULL,
	    sendbuf, msglen, NULL, 0)) && serrno == ENSNACT)
		sleep (RETRYI);
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}
