/*
 * Copyright (C) 2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_replacetapecopy.c,v $ $Revision: 1.1 $ $Date: 2006/05/12 08:11:58 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */
 
/*      Cns_replaceseg - replace file segment (used by repack) */

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
Cns_replacetapecopy(struct Cns_fileid *file_uniqueid, const char* oldvid, const char* newvid, int nbseg, struct Cns_segattrs *newsegattrs)
{
	int c,i;
	int msglen;
	char func[19];

	gid_t gid;
	uid_t uid;

	char *q;
	char *sbp;
	char *actual_path;
	char server[CA_MAXHOSTNAMELEN+1];
	char sendbuf[REQBUFSZ];
	struct Cns_api_thread_info *thip;


        strcpy (func, "Cns_replacetapecopy");
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

	if (! newsegattrs || !newvid || !oldvid || !file_uniqueid ) {
		serrno = EFAULT;
		return (-1);
	}

	/* set the nameserver */
	if (file_uniqueid && *file_uniqueid->server)
		strcpy (server, file_uniqueid->server);
	else{
		if (Cns_selectsrvr (NULL, thip->server, server, &actual_path))
			return (-1);
	}
			
	/* Build request header */	
	sbp = sendbuf;
	marshall_LONG (sbp, CNS_MAGIC4);
	marshall_LONG (sbp, CNS_REPLACETAPECOPY);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_HYPER (sbp, file_uniqueid->fileid);
	marshall_STRING (sbp, newvid);
	marshall_STRING (sbp, oldvid);
	
	/* normaly we have only one segment, but for common compatibility we 
	   implement this functionality for nbsegs>1 */
	marshall_WORD (sbp, nbseg);
	for (i = 0; i < nbseg; i++) {
		marshall_WORD (sbp, (newsegattrs+i)->copyno);
		marshall_WORD (sbp, (newsegattrs+i)->fsec);
		marshall_HYPER (sbp, (newsegattrs+i)->segsize);
		marshall_LONG (sbp, (newsegattrs+i)->compression);
		marshall_BYTE (sbp, (newsegattrs+i)->s_status);
		marshall_STRING (sbp, (newsegattrs+i)->vid);
		marshall_WORD (sbp, (newsegattrs+i)->side);
		marshall_LONG (sbp, (newsegattrs+i)->fseq);
		marshall_OPAQUE (sbp, (newsegattrs+i)->blockid, 4);
		marshall_STRING (sbp, (newsegattrs+i)->checksum_name);
		marshall_LONG (sbp, (newsegattrs+i)->checksum);
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */


	while ((c = send2nsd (NULL, server, sendbuf, msglen, NULL, 0)) &&
	    serrno == ENSNACT)
		sleep (RETRYI);
	return (c);
}
