/*
 * Copyright (C) 2001-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */
 
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/stat.h>
#include <time.h>
#include <sys/types.h>
#include <netinet/in.h>
#include "h/Cupv_api.h"
#include "h/marshall.h"
#include "h/serrno.h"
#include "h/rmc_constants.h"
#include "h/rmc_logit.h"
#include "h/rmc_logreq.h"
#include "h/rmc_marshall_element.h"
#include "h/rmc_procreq.h"
#include "h/rmc_smcsubr.h"
#include "h/rmc_smcsubr2.h"
#include "h/rmc_sendrep.h"
#include "h/tplogger_api.h"
#include <string.h>
#include <Ctape_api.h>
extern struct extended_robot_info extended_robot_info;
 
/*	rmc_srv_export - export/eject a cartridge from the robot */

int rmc_srv_export(struct rmc_srv_rqst_context *const rqst_context) {
	int c;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+8];
	char *rbp;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strncpy (func, "rmc_srv_export", 16);
	rbp = rqst_context->req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	rmc_logit (func, RMC92, "export", uid, gid, rqst_context->clienthost);
	/* Unmarshall and ignore the loader field as it is no longer used */
	{
		char smc_ldr[CA_MAXRBTNAMELEN+1];
		if (unmarshall_STRINGN (rbp, smc_ldr, CA_MAXRBTNAMELEN+1)) {
			rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06,
				"loader");
			rmc_logit (func, "returns %d\n", ERMCUNREC);
			return ERMCUNREC;
		}
	}
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "vid");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	snprintf (logbuf, CA_MAXVIDLEN+8, "export %s", vid);
	rmc_logreq (func, logbuf);

	if (Cupv_check (uid, gid, rqst_context->clienthost,
		rqst_context->localhost, P_TAPE_OPERATOR)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, "%s\n",
			sstrerror(serrno));
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	c = smc_export (rqst_context->rpfd, extended_robot_info.smc_fd,
          extended_robot_info.smc_ldr, &extended_robot_info.robot_info, vid);
	if (c) c += ERMCRBTERR;
	rmc_logit (func, "returns %d\n", c);
	return c;
}

/*	rmc_srv_findcart - find cartridge(s) */

int rmc_srv_findcart(struct rmc_srv_rqst_context *const rqst_context) {
	int c;
	struct smc_element_info *element_info;
	struct smc_element_info *elemp;
	char func[17];
	gid_t gid;
	int i;
	char logbuf[CA_MAXVIDLEN+15];
	char *msgaddr;
	int nbelem;
	char *rbp;
	char *repbuf;
	char *sbp;
	struct smc_status smc_status;
	int startaddr;
	char template[40];
	int type;
	uid_t uid;

	strncpy (func, "rmc_srv_findcart", 17);
	rbp = rqst_context->req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	rmc_logit (func, RMC92, "findcart", uid, gid, rqst_context->clienthost);
	/* Unmarshall and ignore the loader fiel as it is no longer used */
	{
		char smc_ldr[CA_MAXRBTNAMELEN+1];
		if (unmarshall_STRINGN (rbp, smc_ldr, CA_MAXRBTNAMELEN+1)) {
			rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06,
				"loader");
			rmc_logit (func, "returns %d\n", ERMCUNREC);
			return ERMCUNREC;
		}
	}
	if (unmarshall_STRINGN (rbp, template, 40)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "template");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	unmarshall_LONG (rbp, type);
	unmarshall_LONG (rbp, startaddr);
	unmarshall_LONG (rbp, nbelem);
	snprintf (logbuf, CA_MAXVIDLEN+15, "findcart %s %d", template, nbelem);
	rmc_logreq (func, logbuf);

	if (nbelem < 1) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "nbelem");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC05);
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	if (extended_robot_info.smc_support_voltag)
		c = smc_find_cartridge (extended_robot_info.smc_fd,
		    extended_robot_info.smc_ldr, template, type, startaddr,
		    nbelem, element_info);
	else
		c = smc_find_cartridge2 (extended_robot_info.smc_fd,
		    extended_robot_info.smc_ldr, template, type, startaddr,
		    nbelem, element_info);
	if (c < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		free (element_info);
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC02,
			"smc_find_cartridge", msgaddr);
		c += ERMCRBTERR;
		rmc_logit (func, "returns %d\n", c);
		return c;
	}
	if ((repbuf = malloc (c * 18 + 4)) == NULL) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC05);
		free (element_info);
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	sbp = repbuf;
	marshall_LONG (sbp, c);
	for (i = 0, elemp = element_info; i < c; i++, elemp++)
		rmc_marshall_element (&sbp, elemp);
	free (element_info);
	rmc_sendrep (rqst_context->rpfd, MSG_DATA, sbp - repbuf, repbuf);
	free (repbuf);
	rmc_logit (func, "returns %d\n", 0);
	return 0;
}

/*	rmc_srv_getgeom - get the robot geometry */

int rmc_srv_getgeom(struct rmc_srv_rqst_context *const rqst_context) {
	char func[16];
	gid_t gid;
	char logbuf[8];
	char *rbp;
	char repbuf[64];
	char *sbp;
	uid_t uid;

	strncpy (func, "rmc_srv_getgeom", 16);
	rbp = rqst_context->req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	rmc_logit (func, RMC92, "getgeom", uid, gid, rqst_context->clienthost);
	/* Unmarshall and ignore the loader field as it is no longer used */
	{
		char smc_ldr[CA_MAXRBTNAMELEN+1];
		if (unmarshall_STRINGN (rbp, smc_ldr, CA_MAXRBTNAMELEN+1)) {
			rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06,
				"loader");
			rmc_logit (func, "returns %d\n", ERMCUNREC);
			return ERMCUNREC;
		}
	}
	snprintf (logbuf, 8, "getgeom");
	rmc_logreq (func, logbuf);

	sbp = repbuf;
	marshall_STRING (sbp, extended_robot_info.robot_info.inquiry);
	marshall_LONG (sbp, extended_robot_info.robot_info.transport_start);
	marshall_LONG (sbp, extended_robot_info.robot_info.transport_count);
	marshall_LONG (sbp, extended_robot_info.robot_info.slot_start);
	marshall_LONG (sbp, extended_robot_info.robot_info.slot_count);
	marshall_LONG (sbp, extended_robot_info.robot_info.port_start);
	marshall_LONG (sbp, extended_robot_info.robot_info.port_count);
	marshall_LONG (sbp, extended_robot_info.robot_info.device_start);
	marshall_LONG (sbp, extended_robot_info.robot_info.device_count);
	rmc_sendrep (rqst_context->rpfd, MSG_DATA, sbp - repbuf, repbuf);
	rmc_logit (func, "returns %d\n", 0);
	return 0;
}

/*	rmc_srv_import - import/inject a cartridge into the robot */

int rmc_srv_import(struct rmc_srv_rqst_context *const rqst_context) {
	int c;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+8];
	char *rbp;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strncpy (func, "rmc_srv_import", 16);
	rbp = rqst_context->req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	rmc_logit (func, RMC92, "import", uid, gid, rqst_context->clienthost);
	/* Unmarshall and ignore the loader field as it is no longer used */
	{
		char smc_ldr[CA_MAXRBTNAMELEN+1];
		if (unmarshall_STRINGN (rbp, smc_ldr, CA_MAXRBTNAMELEN+1)) {
			rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06,
				"loader");
			rmc_logit (func, "returns %d\n", ERMCUNREC);
			return ERMCUNREC;
		}
	}
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "vid");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	snprintf (logbuf, CA_MAXVIDLEN+8, "import %s", vid);
	rmc_logreq (func, logbuf);

	if (Cupv_check (uid, gid, rqst_context->clienthost,
		rqst_context->localhost, P_TAPE_OPERATOR)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, "%s\n",
			sstrerror(serrno));
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	c = smc_import (rqst_context->rpfd, extended_robot_info.smc_fd,
	  extended_robot_info.smc_ldr, &extended_robot_info.robot_info, vid);
	if (c) c += ERMCRBTERR;
	rmc_logit (func, "returns %d\n", c);
	return c;
}

/*	rmc_srv_mount - mount a cartridge on a drive */

int rmc_srv_mount(struct rmc_srv_rqst_context *const rqst_context) {
	int c;
	int drvord;
	char func[16];
	gid_t gid;
	int invert;
	char logbuf[CA_MAXVIDLEN+64];
	char *rbp;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strncpy (func, "rmc_srv_mount", 16);
	rbp = rqst_context->req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	rmc_logit (func, RMC92, "mount", uid, gid, rqst_context->clienthost);
	/* Unmarshall and ignore the loader field as it is no longer used */
	{
		char smc_ldr[CA_MAXRBTNAMELEN+1];
		if (unmarshall_STRINGN (rbp, smc_ldr, CA_MAXRBTNAMELEN+1)) {
			rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06,
				"loader");
			rmc_logit (func, "returns %d\n", ERMCUNREC);
			return ERMCUNREC;
		}
	}
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "vid");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	unmarshall_WORD (rbp, invert);
	unmarshall_WORD (rbp, drvord);
	snprintf (logbuf, CA_MAXVIDLEN+64, "mount %s/%d on drive %d", vid, invert, drvord);
	rmc_logreq (func, logbuf);

	if (Cupv_check (uid, gid, rqst_context->clienthost,
		rqst_context->localhost, P_TAPE_SYSTEM)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, "%s\n",
			sstrerror(serrno));
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	c = smc_mount (rqst_context->rpfd, extended_robot_info.smc_fd,
	  extended_robot_info.smc_ldr, &extended_robot_info.robot_info, drvord,
	  vid, invert);
	if (c) c += ERMCRBTERR;
	rmc_logit (func, "returns %d\n", c);
	return c;
}

/*	rmc_srv_readelem - read element status */

int rmc_srv_readelem(struct rmc_srv_rqst_context *const rqst_context) {
	int c;
	struct smc_element_info *element_info;
	struct smc_element_info *elemp;
	char func[17];
	gid_t gid;
	int i;
	char logbuf[21];
	char *msgaddr;
	int nbelem;
	char *rbp;
	char *repbuf;
	char *sbp;
	struct smc_status smc_status;
	int startaddr;
	int type;
	uid_t uid;

	strncpy (func, "rmc_srv_readelem", 17);
	rbp = rqst_context->req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	rmc_logit (func, RMC92, "readelem", uid, gid, rqst_context->clienthost);
	/* Unmarshall and ignore the loader field as it is no longer used */
	{
		char smc_ldr[CA_MAXRBTNAMELEN+1];
		if (unmarshall_STRINGN (rbp, smc_ldr, CA_MAXRBTNAMELEN+1)) {
			rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06,
				"loader");
			rmc_logit (func, "returns %d\n", ERMCUNREC);
			return ERMCUNREC;
		}
	}
	unmarshall_LONG (rbp, type);
	unmarshall_LONG (rbp, startaddr);
	unmarshall_LONG (rbp, nbelem);
	snprintf (logbuf, 21, "readelem %d %d", startaddr, nbelem);
	rmc_logreq (func, logbuf);

	if (type < 0 || type > 4) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "type");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	if (nbelem < 1) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "nbelem");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	if ((element_info = malloc (nbelem * sizeof(struct smc_element_info))) == NULL) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC05);
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	if ((c = smc_read_elem_status (extended_robot_info.smc_fd,
	    extended_robot_info.smc_ldr, type, startaddr, nbelem,
	    element_info)) < 0) {
		c = smc_lasterror (&smc_status, &msgaddr);
		free (element_info);
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC02,
			"smc_read_elem_status", msgaddr);
		c += ERMCRBTERR;
		rmc_logit (func, "returns %d\n", c);
		return c;
	}
	if ((repbuf = malloc (c * 18 + 4)) == NULL) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC05);
		free (element_info);
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	sbp = repbuf;
	marshall_LONG (sbp, c);
	for (i = 0, elemp = element_info; i < c; i++, elemp++)
		rmc_marshall_element (&sbp, elemp);
	free (element_info);
	rmc_sendrep (rqst_context->rpfd, MSG_DATA, sbp - repbuf, repbuf);
	free (repbuf);
	rmc_logit (func, "returns %d\n", 0);
	return 0;
}

/*	rmc_srv_unmount - dismount a cartridge from a drive */

int rmc_srv_unmount(struct rmc_srv_rqst_context *const rqst_context) {
	int c;
	int drvord;
	int force;
	char func[16];
	gid_t gid;
	char logbuf[CA_MAXVIDLEN+64];
	char *rbp;
	uid_t uid;
	char vid[CA_MAXVIDLEN+1];

	strncpy (func, "rmc_srv_unmount", 16);
	rbp = rqst_context->req_data;
	unmarshall_LONG (rbp, uid);
	unmarshall_LONG (rbp, gid);
	rmc_logit (func, RMC92, "unmount", uid, gid, rqst_context->clienthost);
	/* Unmarshall and ignore the loader field as it is no longer used */
	{
		char smc_ldr[CA_MAXRBTNAMELEN+1];
		if (unmarshall_STRINGN (rbp, smc_ldr, CA_MAXRBTNAMELEN+1)) {
			rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06,
				"loader");
			rmc_logit (func, "returns %d\n", ERMCUNREC);
			return ERMCUNREC;
		}
	}
	if (unmarshall_STRINGN (rbp, vid, CA_MAXVIDLEN+1)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, RMC06, "vid");
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	unmarshall_WORD (rbp, drvord);
	unmarshall_WORD (rbp, force);
	snprintf (logbuf, CA_MAXVIDLEN+64, "unmount %s %d %d", vid, drvord, force);
	rmc_logreq (func, logbuf);

	if (Cupv_check (uid, gid, rqst_context->clienthost,
		rqst_context->localhost, P_TAPE_SYSTEM)) {
		rmc_sendrep (rqst_context->rpfd, MSG_ERR, "%s\n",
			sstrerror(serrno));
		rmc_logit (func, "returns %d\n", ERMCUNREC);
		return ERMCUNREC;
	}
	c = smc_dismount (rqst_context->rpfd, extended_robot_info.smc_fd,
	  extended_robot_info.smc_ldr, &extended_robot_info.robot_info, drvord,
	  force == 0 ? vid : "");
	if (c) c += ERMCRBTERR;
	rmc_logit (func, "returns %d\n", c);
	return c;
}

int rmc_srv_genericmount(struct rmc_srv_rqst_context *const rqst_context) {
	return 0;
}

int rmc_srv_genericunmount(struct rmc_srv_rqst_context *const rqst_context) {
	return 0;
}
