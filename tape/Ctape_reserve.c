/*
 * Copyright (C) 1999-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*	Ctape_reserve - reserve tape resources */

#include <sys/types.h>
#include <netinet/in.h>
#include <stddef.h>
#include "Ctape.h"
#include "Ctape_api.h"
#include "marshall.h"
#include "serrno.h"
#include <unistd.h>
#include <sys/types.h>

int Ctape_reserve(int count,
                  struct dgn_rsv dgn_rsv[])
{
	int c;
	char func[16];
	gid_t gid;
	int i;
	int jid;
	int msglen;
	char *q;
	char repbuf[1];
	char *sbp;
	char sendbuf[REQBUFSZ];
	int totrsvd = 0;
	uid_t uid;

	strcpy (func, "Ctape_reserve");
	uid = getuid();
	gid = getgid();
	jid = findpgrp();
 
        /* Build request header */
 
        sbp = sendbuf;
        marshall_LONG (sbp, TPMAGIC);
        marshall_LONG (sbp, TPRSV);
        q = sbp;        /* save pointer. The next field will be updated */
        msglen = 3 * LONGSIZE;
        marshall_LONG (sbp, msglen);
 
        /* Build request body */
 
	marshall_LONG (sbp, uid);
	marshall_LONG (sbp, gid);
	marshall_LONG (sbp, jid);
	marshall_WORD (sbp, count);
	for (i = 0; i < count; i++) {
		marshall_STRING (sbp, dgn_rsv[i].name);
		marshall_WORD (sbp, dgn_rsv[i].num);
		totrsvd += dgn_rsv[i].num;
	}

	msglen = sbp - sendbuf;
	marshall_LONG (q, msglen);	/* update length field */

	c = send2tpd (NULL, sendbuf, msglen, repbuf, sizeof(repbuf));
	if (c == 0)
		c = initlabelroutines (totrsvd);
	return (c);
}
