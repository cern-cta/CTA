/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rmc_dismount.c,v $ $Revision: 1.1 $ $Date: 2002/11/29 08:51:47 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/*      rmc_dismount - dismount a cartridge from a drive */

#include <stdlib.h>
#include <sys/types.h>
#include "marshall.h"
#include "rmc.h"
#include "rmc_api.h"
#include "serrno.h"
rmc_dismount(server, smc_ldr, vid, drvord, force)
char *server;
char *smc_ldr;
char *vid;
int drvord;
int force;
{
	int c;
	gid_t gid;
	int msglen;
	char *q;
	char repbuf[1];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;

	uid = getuid();
	gid = getgid();

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, RMC_MAGIC);
	marshall_LONG (sbp, RMC_UNMOUNT);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, smc_ldr);
	marshall_STRING (sbp, vid);
	marshall_WORD (sbp, drvord);
	marshall_WORD (sbp, force);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

        while ((c = send2rmc (server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
            serrno == ERMCNACT)
                sleep (RETRYI);
	return (c);
}
