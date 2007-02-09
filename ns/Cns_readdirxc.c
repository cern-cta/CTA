/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Cns_readdirxc - read a directory entry including file attributes  and comments */

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

struct Cns_direnstatc DLL_DECL *
Cns_readdirxc(Cns_DIR *dirp)
{
	int c;
	int direntsz;
	struct Cns_direnstatc *dp;
	char func[16];
	int getattr = 4;
	gid_t gid;
	int msglen;
	int nbentries;
	char *q;
	char *rbp;
	char repbuf[DIRBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Cns_readdirx");
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
	/* compute size of client machine Cns_direnstatc structure excluding d_name */

	dp = (struct Cns_direnstatc *) dirp->dd_buf;
	direntsz = &dp->d_name[0] - (char *) dp;

	if (dirp->dd_size == 0) {	/* no data in the cache */
		if (dirp->eod)
			return (NULL);

		/* Build request header */

		sbp = sendbuf;
		marshall_LONG (sbp, CNS_MAGIC);
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

		/* unmarshall reply into the Cns_direnstatc structures */

		dp = (struct Cns_direnstatc *) dirp->dd_buf;
		while (nbentries--) {
			unmarshall_HYPER (rbp, dp->fileid);
			unmarshall_WORD (rbp, dp->filemode);
			unmarshall_LONG (rbp, dp->nlink);
			unmarshall_LONG (rbp, dp->uid);
			unmarshall_LONG (rbp, dp->gid);
			unmarshall_HYPER (rbp, dp->filesize);
			unmarshall_TIME_T (rbp, dp->atime);
			unmarshall_TIME_T (rbp, dp->mtime);
			unmarshall_TIME_T (rbp, dp->ctime);
			unmarshall_WORD (rbp, dp->fileclass);
			unmarshall_BYTE (rbp, dp->status);
			unmarshall_STRING (rbp, dp->d_name);
			dp->d_reclen = direntsz + strlen (dp->d_name) + 1;
			dp->comment = (char *) dp + dp->d_reclen;
			unmarshall_STRING (rbp, dp->comment);
			dp->d_reclen = ((dp->d_reclen + strlen (dp->comment) + 8) / 8) * 8;
			dp = (struct Cns_direnstatc *) ((char *) dp + dp->d_reclen);
		}
		dirp->bod = 0;
		unmarshall_WORD (rbp, dirp->eod);
		dirp->dd_size = (char *) dp - dirp->dd_buf;
	}
	dp = (struct Cns_direnstatc *) (dirp->dd_buf + dirp->dd_loc);
	dirp->dd_loc += dp->d_reclen;
	if (dirp->dd_loc >= dirp->dd_size) {	/* must refill next time */
		dirp->dd_loc = 0;
		dirp->dd_size = 0;
	}
	return (dp);
}
