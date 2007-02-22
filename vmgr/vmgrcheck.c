/*
 * Copyright (C) 2001-2003 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#include <errno.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include "Cupv_api.h"
#include "serrno.h"
#include "vmgr.h"
#include "vmgr_api.h"
int vmgrcheck(char *vid, char *vsn, char *dgn, char *den, char *lbl, int mode, uid_t uid, gid_t gid)
{
	return (vmgrchecki (vid, vsn, dgn, den, lbl, mode, uid, gid, NULL));
}

int vmgrchecki(char *vid, char *vsn, char *dgn, char *den, char *lbl, int mode, uid_t uid, gid_t gid, char *clienthost)
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
	if (tape_info.status & EXPORTED) {
		return (ETABSENT);
	}
	if (tape_info.status & ARCHIVED) {
		return (ETARCH);
	}
	if (tape_info.status & TAPE_RDONLY) {
		if (mode) {	/* WRITE_ENABLE */
			return (ETWPROT);
		}
	}
	if (tape_info.status & DISABLED) {
		if (mode ||
		    (Cupv_check (uid, gid, clienthost, "TAPE_SERVERS", P_TAPE_OPERATOR) &&
		    Cupv_check (uid, gid, clienthost, NULL, P_TAPE_OPERATOR)))
			return (ETHELD);
	}

	if (mode) {	/* WRITE_ENABLE */
		while (vmgr_querypool (tape_info.poolname, &pool_uid, &pool_gid,
		    NULL, NULL) < 0) {
			if (serrno == ENOENT)
				return (EINVAL);
			sleep (60);
		}
		if (((pool_uid && pool_uid != uid && uid != 0) ||
		    (pool_gid && pool_gid != gid && gid != 0)) &&
		    (Cupv_check (uid, gid, clienthost, "TAPE_SERVERS", P_ADMIN) &&
		    Cupv_check (uid, gid, clienthost, NULL, P_ADMIN)))
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
