/*
 * Copyright (C) 1999-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
/*      Cns_setsegattrs - set file segments attributes */

#include <errno.h>
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
Cns_setsegattrs(const char *path, struct Cns_fileid *file_uniqueid, int nbseg, struct Cns_segattrs *segattrs)
{
	char *actual_path;
	int c;
	char func[16];
	gid_t gid;
	int i;
	int msglen;
	char *q;
	char *sbp;
	char sendbuf[REQBUFSZ];
	char server[CA_MAXHOSTNAMELEN+1];
	struct Cns_api_thread_info *thip;
	uid_t uid;
	u_signed64 zero = 0;

        strcpy (func, "Cns_setsegattrs");
        if (Cns_apiinit (&thip))
                return (-1);
        Cns_getid(&uid, &gid);
        
#        
#if defined(_WIN32)
        if (uid < 0 || gid < 0) {
                Cns_errmsg (func, NS053);
                serrno = SENOMAPFND;
                return (-1);
        }
#endif

	if ((! path && ! file_uniqueid) || ! segattrs) {
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
	marshall_LONG (sbp, CNS_SETSEGAT);
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
	marshall_WORD (sbp, nbseg);
	for (i = 0; i < nbseg; i++) {
		marshall_WORD (sbp, (segattrs+i)->copyno);
		marshall_WORD (sbp, (segattrs+i)->fsec);
		marshall_HYPER (sbp, (segattrs+i)->segsize);
		marshall_LONG (sbp, (segattrs+i)->compression);
		marshall_BYTE (sbp, (segattrs+i)->s_status);
		marshall_STRING (sbp, (segattrs+i)->vid);
		marshall_WORD (sbp, (segattrs+i)->side);
		marshall_LONG (sbp, (segattrs+i)->fseq);
		marshall_OPAQUE (sbp, (segattrs+i)->blockid, 4);
        marshall_STRING (sbp, (segattrs+i)->checksum_name);
        marshall_LONG (sbp, (segattrs+i)->checksum);
	}
 
	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	while ((c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0)) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	if (c && serrno == SENAMETOOLONG) serrno = ENAMETOOLONG;
	return (c);
}
