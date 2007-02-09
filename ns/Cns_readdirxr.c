/*
 * Copyright (C) 2005 by CERN/IT/GD/SC
 * All rights reserved
 */

/*	Cns_readdirxr - read a directory entry including replica information */

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

struct Cns_direnrep DLL_DECL *
Cns_readdirxr(Cns_DIR *dirp, char *se)
{
	int c;
	int direntsz;
	struct Cns_direnrep *dp;
	char func[16];
	int getattr = 5;
	gid_t gid;
	int i;
	struct Cns_rep_info *ir;
	int msglen;
	int nbentries;
	char *q;
	char *rbp;
	struct Cns_rep_info *rep_entries;
	char repbuf[DIRBUFSZ+4];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;
 
	strcpy (func, "Cns_readdirxr");
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
	if (se && strlen (se) > CA_MAXHOSTNAMELEN) {
		serrno = EINVAL;
		return (NULL);
	}
	/* compute size of client machine Cns_direnrep structure excluding d_name */

	dp = (struct Cns_direnrep *) dirp->dd_buf;
	direntsz = &dp->d_name[0] - (char *) dp;

	if (dirp->dd_size == 0) {	/* no data in the cache */
		if (dirp->replicas) {	/* free previous replica information */
			ir = (struct Cns_rep_info *) dirp->replicas;
			for (i = 0; i < dirp->nbreplicas; i++) {
				free (ir->host);
				free (ir->sfn);
				ir++;
			}
			free (dirp->replicas);
			dirp->nbreplicas = 0;
			dirp->replicas = NULL;
		}
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
		if (se) {
			marshall_STRING (sbp, se);
		} else {
			marshall_STRING (sbp, "");
		}

		msglen = sbp - sendbuf;
		marshall_LONG (q, msglen);	/* update length field */

		while ((c = send2nsdx (&dirp->dd_fd, NULL, sendbuf, msglen,
		    repbuf, sizeof(repbuf), (void **) &dirp->replicas,
		    &dirp->nbreplicas)) && serrno == ENSNACT)
			sleep (RETRYI);
		if (c < 0)
			return (NULL);
		rbp = repbuf;
		unmarshall_WORD (rbp, nbentries);
		if (nbentries == 0)
			return (NULL);		/* end of directory */

		/* unmarshall reply into the Cns_direnrep structures */

		dp = (struct Cns_direnrep *) dirp->dd_buf;
		i = 0;
		rep_entries = (struct Cns_rep_info *) dirp->replicas;
		while (nbentries--) {
			unmarshall_HYPER (rbp, dp->fileid);
			unmarshall_STRING (rbp, dp->guid);
			unmarshall_WORD (rbp, dp->filemode);
			unmarshall_HYPER (rbp, dp->filesize);
			dp->nbreplicas = 0;
			dp->rep = NULL;
			for ( ; i < dirp->nbreplicas; i++) {
				if (dp->fileid != (rep_entries+i)->fileid) break;
				dp->nbreplicas++;
				if (dp->nbreplicas == 1)
					dp->rep = rep_entries + i;
			}
			unmarshall_STRING (rbp, dp->d_name);
			dp->d_reclen = ((direntsz + strlen (dp->d_name) + 8) / 8) * 8;
			dp = (struct Cns_direnrep *) ((char *) dp + dp->d_reclen);
		}
		dirp->bod = 0;
		unmarshall_WORD (rbp, dirp->eod);
		dirp->dd_size = (char *) dp - dirp->dd_buf;
	}
	dp = (struct Cns_direnrep *) (dirp->dd_buf + dirp->dd_loc);
	dirp->dd_loc += dp->d_reclen;
	if (dirp->dd_loc >= dirp->dd_size) {	/* must refill next time */
		dirp->dd_loc = 0;
		dirp->dd_size = 0;
	}
	return (dp);
}
