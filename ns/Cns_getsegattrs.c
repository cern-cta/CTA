/*
 * Copyright (C) 1999-2004 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)Cns_getsegattrs.c,v 1.16 2004/03/03 08:51:30 CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
/*      Cns_getsegattrs - get file segments attributes */

#include <errno.h>
#include <stdlib.h>
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
Cns_getsegattrs(const char *path, struct Cns_fileid *file_uniqueid, int *nbseg, struct Cns_segattrs **segattrs)
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
	struct Cns_segattrs *segments = NULL;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	u_signed64 zero = 0;

        strcpy (func, "Cns_getsegattrs");
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

	if ((! path && ! file_uniqueid) || ! nbseg || ! segattrs) {
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
	marshall_LONG (sbp, CNS_MAGIC4);
	marshall_LONG (sbp, CNS_GETSEGAT);
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
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, repbuf,
	    sizeof(repbuf))) && serrno == ENSNACT)
		sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_WORD (rbp, *nbseg);
		if (*nbseg == 0) {
			*segattrs = NULL;
			return (0);
		}
		segments = malloc (*nbseg * sizeof(struct Cns_segattrs));
		if (segments == NULL) {
			serrno = ENOMEM;
			return (-1);
		}
		for (i = 0; i < *nbseg; i++) {
			unmarshall_WORD (rbp, segments[i].copyno);
			unmarshall_WORD (rbp, segments[i].fsec);
			unmarshall_HYPER (rbp, segments[i].segsize);
			unmarshall_LONG (rbp, segments[i].compression);
			unmarshall_BYTE (rbp, segments[i].s_status);
			unmarshall_STRING (rbp, segments[i].vid);
			unmarshall_WORD (rbp, segments[i].side);
			unmarshall_LONG (rbp, segments[i].fseq);
			unmarshall_OPAQUE (rbp, segments[i].blockid, 4);
			unmarshall_STRINGN (rbp, segments[i].checksum_name,
						CA_MAXCKSUMNAMELEN);
			unmarshall_LONG (rbp, segments[i].checksum);
		}
		*segattrs = segments;
	}
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}
