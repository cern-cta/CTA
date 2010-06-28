/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Ctape_rstatus - get resource reservation status */

#include <stdio.h>
#include <sys/types.h>
#include <netinet/in.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "marshall.h"
#include "serrno.h"
#include <unistd.h>
#include <sys/types.h>

int Ctape_rstatus(host, rsv_status, nbentries, nbdgp)
char *host;
struct rsv_status rsv_status[];
int nbentries;
int nbdgp;
{
	int c, i, j, n;
	char func[16];
	gid_t gid;
	int msglen;
	char *q;
	char *rbp;
	char repbuf[REPBUFSZ];
	char *sbp;
	char sendbuf[REQBUFSZ];
	uid_t uid;

	strcpy (func, "Ctape_rstatus");
	uid = getuid();
	gid = getgid();
 
	/* Build request header */

	sbp = sendbuf;
	marshall_LONG (sbp, TPMAGIC);
	marshall_LONG (sbp, TPRSTAT);
	q = sbp;        /* save pointer. The next field will be updated */
	msglen = 3 * LONGSIZE;
	marshall_LONG (sbp, msglen);

	/* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);      /* update length field */
 
	c = send2tpd (host, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c)
		return (c);
	rbp = repbuf;
	unmarshall_WORD (rbp, n);
	for (i = 0; i < n; i++) {
		if (i >= nbentries) {
			serrno = SEUBUF2SMALL;
			return (-1);
		}
		unmarshall_LONG (rbp, rsv_status[i].uid);
		unmarshall_LONG (rbp, rsv_status[i].jid);
		unmarshall_WORD (rbp, rsv_status[i].count);
		for (j = 0; j < rsv_status[i].count; j++) {
			if (j >= nbdgp) {
				serrno = SEUBUF2SMALL;
				return (-1);
			}
			unmarshall_STRING (rbp, rsv_status[i].dg[j].name);
			unmarshall_WORD (rbp, rsv_status[i].dg[j].rsvd);
			unmarshall_WORD (rbp, rsv_status[i].dg[j].used);
		}
	}
	return (i);
}
