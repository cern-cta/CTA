/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

/*      rmc_get_geometry - get the remote SCSI robot geometry */

#include <stdlib.h>
#include <sys/types.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <unistd.h>
#include <netinet/in.h>
#endif
#include "marshall.h"
#include "rmc.h"
#include "rmc_api.h"
#include "serrno.h"
int rmc_get_geometry(server, smc_ldr, robot_info)
char *server;
char *smc_ldr;
struct robot_info *robot_info;
{
	int c;
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[64];
	char *sbp;
	char sendbuf[REQBUFSZ];
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
	marshall_STRING (sbp, smc_ldr);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

        while ((c = send2rmc (server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
            serrno == ERMCNACT)
                sleep (RETRYI);
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
