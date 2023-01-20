/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2002-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

/*      rmc_get_geometry - get the remote SCSI robot geometry */

#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <netinet/in.h>
#include "mediachanger/castorrmc/h/marshall.h"
#include "mediachanger/castorrmc/h/rmc_api.h"
#include "mediachanger/castorrmc/h/rmc_constants.h"
#include "mediachanger/castorrmc/h/serrno.h"
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
