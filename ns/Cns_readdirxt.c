/*
 * Copyright (C) 2000-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cns_readdirxt.c,v $ $Revision: 1.2 $ $Date: 2004/11/03 09:49:50 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

/*	Cns_readdirxt - read a directory entry including tape file attributes */

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

struct Cns_direntape DLL_DECL *
Cns_readdirxt(Cns_DIR *dirp)
{
	int c;
	int direntsz;
	struct Cns_direntape *dp;
	char func[16];
	int getattr = 2;
	gid_t gid;
	int msglen;
	int n;
	int nbentries;
	char *q;
	char *rbp;
	char repbuf[DIRBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Cns_readdirxt");
	Cns_getid(&uid, &gid);
	
#if defined(_WIN32)
	if (uid < 0 || gid < 0) {
		Cns_errmsg (func, NS053);
		serrno = SENOMAPFND;
		return (NULL);
	}
#endif

	if (! dirp) {
		serrno = EFAULT;
		return (NULL);
	}
	/* compute size of client machine Cns_direntape structure excluding d_name */

	dp = (struct Cns_direntape *) dirp->dd_buf;
	direntsz = &dp->d_name[0] - (char *) dp;

	if (dirp->dd_size == 0) {	/* no data in the cache */
		if (dirp->eod)
			return (NULL);

		/* Build request header */

		sbp = sendbuf;
		marshall_LONG (sbp, CNS_MAGIC4);
		marshall_LONG (sbp, CNS_READDIR);
		q = sbp;        /* save pointer. The next field will be updated */
		msglen = 3 * LONGSIZE;
		marshall_LONG (sbp, msglen);
	 
		/* Build request body */

		marshall_LONG (sbp, uid);
		marshall_LONG (sbp, gid);
		marshall_WORD (sbp, getattr);
		marshall_WORD (sbp, direntsz);
		marshall_HYPER (sbp, dirp->fileid);
		marshall_WORD (sbp, dirp->bod);

		msglen = sbp - sendbuf;
		marshall_LONG (q, msglen);	/* update length field */

		while ((c = send2nsd (&dirp->dd_fd, NULL, sendbuf, msglen,
		    repbuf, sizeof(repbuf))) && serrno == ENSNACT)
			sleep (RETRYI);
		if (c < 0)
			return (NULL);
		rbp = repbuf;
		unmarshall_WORD (rbp, nbentries);
		if (nbentries == 0)
			return (NULL);		/* end of directory */

		/* unmarshall reply into the Cns_direntape structures */

		dp = (struct Cns_direntape *) dirp->dd_buf;
		while (nbentries--) {
			unmarshall_HYPER (rbp, dp->parent_fileid);
			unmarshall_HYPER (rbp, dp->fileid);
			unmarshall_WORD (rbp, dp->copyno);
			unmarshall_WORD (rbp, dp->fsec);
			unmarshall_HYPER (rbp, dp->segsize);
			unmarshall_LONG (rbp, dp->compression);
			unmarshall_BYTE (rbp, dp->s_status);
			unmarshall_STRING (rbp, dp->vid);
            unmarshall_STRINGN (rbp, dp->checksum_name,
                                CA_MAXCKSUMNAMELEN);
            unmarshall_LONG (rbp, dp->checksum);
			unmarshall_WORD (rbp, dp->side);
			unmarshall_LONG (rbp, dp->fseq);
			unmarshall_OPAQUE (rbp, dp->blockid, 4);
			unmarshall_STRING (rbp, dp->d_name);
			dp->d_reclen = ((direntsz + strlen (dp->d_name) + 8) / 8) * 8;
			dp = (struct Cns_direntape *) ((char *) dp + dp->d_reclen);
		}
		dirp->bod = 0;
		unmarshall_WORD (rbp, dirp->eod);
		dirp->dd_size = (char *) dp - dirp->dd_buf;
	}
	dp = (struct Cns_direntape *) (dirp->dd_buf + dirp->dd_loc);
	dirp->dd_loc += dp->d_reclen;
	if (dirp->dd_loc >= dirp->dd_size) {	/* must refill next time */
		dirp->dd_loc = 0;
		dirp->dd_size = 0;
	}
	return (dp);
}
