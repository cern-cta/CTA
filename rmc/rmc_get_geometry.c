/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/*      rmc_get_geometry - get the remote SCSI robot geometry */

#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "h/marshall.h"
#include "h/rmc_api.h"
#include "h/rmc_constants.h"
#include "h/serrno.h"
int rmc_get_geometry(
	const char *const server,
	struct robot_info *const robot_info)
{
	int c;
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[64];
	char *sbp;
	char sendbuf[RMC_REQBUFSZ];
	uid_t uid;

	uid = getuid();
	gid = getgid();

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, RMC_MAGIC);
	marshall_LONG (sbp, RMC_GETGEOM);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, ""); /* loader field is no longer used */

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

        while ((c = send2rmc (server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
            serrno == ERMCNACT)
                sleep (RMC_RETRYI);
	if (c == 0) {
		rbp = repbuf;
        	unmarshall_STRING (rbp, robot_info->inquiry);
        	unmarshall_LONG (rbp, robot_info->transport_start);
        	unmarshall_LONG (rbp, robot_info->transport_count);
        	unmarshall_LONG (rbp, robot_info->slot_start);
        	unmarshall_LONG (rbp, robot_info->slot_count);
        	unmarshall_LONG (rbp, robot_info->port_start);
        	unmarshall_LONG (rbp, robot_info->port_count);
        	unmarshall_LONG (rbp, robot_info->device_start);
        	unmarshall_LONG (rbp, robot_info->device_count);
	}
	return (c);
}
