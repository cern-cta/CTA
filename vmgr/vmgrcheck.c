/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vmgrcheck.c,v $ $Revision: 1.1 $ $Date: 2002/03/05 16:43:03 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */
 
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include "serrno.h"
#include "vmgr_api.h"
#if VMGR
vmgrcheck(vid, vsn, dgn, den, lbl, mode, uid, gid)
char *vid;
char *vsn;
char *dgn;
char *den;
char *lbl;
int mode;
uid_t uid;
gid_t gid;
{
	int errflg = 0;
	char func[16];
	gid_t pool_gid;
	uid_t pool_uid;
	struct vmgr_tape_info tape_info;
	char vmgrdgn[CA_MAXDGNLEN+1];

	strcpy (func, "vmgrcheck");
	while (vmgr_querytape (vid, 0, &tape_info, vmgrdgn) < 0) {
		if (serrno == ENOENT)
			return (ETVUNKN);
		sleep (60);
	}
	if (tape_info.status & TAPE_RDONLY) {
		if (mode == WRITE_ENABLE)
			return (ETWPROT);
	} else if (tape_info.status & EXPORTED)
		return (ETABSENT);
	else if (tape_info.status & DISABLED)
		return (ETHELD);
	else if (tape_info.status & ARCHIVED)
		return (ETARCH);
	if (mode == WRITE_ENABLE) {
		while (vmgr_querypool (tape_info.poolname, &pool_uid, &pool_gid,
		    NULL, NULL) < 0) {
			if (serrno == ENOENT)
				return (EINVAL);
			sleep (60);
		}
		if ((pool_uid && pool_uid != uid && uid != 0) ||
		    (pool_gid && pool_gid != gid && gid != 0))
			return (EACCES);
	}
	if (*vsn) {
		if (strcmp (vsn, tape_info.vsn)) {
			vmgr_errmsg (func, VMG64, vid, vsn, tape_info.vsn);
			errflg++;
		}
	} else {
		strcpy (vsn, tape_info.vsn);
	}

	if (*dgn) {
		if (strcmp (dgn, vmgrdgn) != 0) {
			vmgr_errmsg (func, VMG64, vid, dgn, vmgrdgn);
			errflg++;
		}
	} else {
		strcpy (dgn, vmgrdgn);
	}

	if (*den) {
		if (strcmp (den, tape_info.density) != 0) {
			vmgr_errmsg (func, VMG64, vid, den, tape_info.density);
			errflg++;
		}
	} else {
		strcpy (den, tape_info.density);
	}

	if (*lbl) {
		if (strcmp (lbl, "blp") && strcmp (lbl, tape_info.lbltype)) {
			vmgr_errmsg (func, VMG64, vid, lbl, tape_info.lbltype);
			errflg++;
		}
	} else {
		strcpy (lbl, tape_info.lbltype);
	}
	return (errflg ? EINVAL : 0);
}
#endif
