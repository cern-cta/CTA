/*
 * Copyright (C) 2002 by CERN/IT/DS/HSM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rmc_find_cartridge.c,v $ $Revision: 1.1 $ $Date: 2002/11/29 08:51:47 $ CERN IT-DS/HSM Jean-Philippe Baud";
#endif /* not lint */

/*      rmc_find_cartridge - find cartridge(s) in a remote SCSI robot */

#include <stdlib.h>
#include <sys/types.h>
#include "marshall.h"
#include "rmc.h"
#include "rmc_api.h"
#include "serrno.h"
rmc_find_cartridge(server, smc_ldr, template, type, startaddr, nbelem, element_info)
char *server;
char *smc_ldr;
char *template;
int type;
int startaddr;
int nbelem;
struct smc_element_info *element_info;
{
	int c;
	struct smc_element_info *elemp;
	gid_t gid;
	int i;
	int msglen;
	int nbavail;
	char *q;
	char *rbp;
	char repbuf[REPBUFSZ];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;

	uid = getuid();
	gid = getgid();

	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, RMC_MAGIC);
	marshall_LONG (sbp, RMC_FINDCART);
	q = sbp;	/* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */

	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_STRING (sbp, smc_ldr);
	marshall_STRING (sbp, template);
	marshall_LONG (sbp, type);
	marshall_LONG (sbp, startaddr);
	marshall_LONG (sbp, nbelem);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

        while ((c = send2rmc (server, sendbuf, msglen, repbuf, sizeof(repbuf))) &&
            serrno == ERMCNACT)
                sleep (RETRYI);
	if (c == 0) {
		rbp = repbuf;
		unmarshall_LONG (rbp, c);
		for (i = 0, elemp = element_info; i < c; i++, elemp++) {
			unmarshall_WORD (rbp, elemp->element_address);
			unmarshall_BYTE (rbp, elemp->element_type);
			unmarshall_BYTE (rbp, elemp->state);
			unmarshall_BYTE (rbp, elemp->asc);
			unmarshall_BYTE (rbp, elemp->ascq);
			unmarshall_BYTE (rbp, elemp->flags);
			unmarshall_WORD (rbp, elemp->source_address);
			unmarshall_STRING (rbp, elemp->name);
		}
	}
	return (c);
}
