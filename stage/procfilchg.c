/*
 * $Id: procfilchg.c,v 1.5 2001/01/31 18:59:51 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procfilchg.c,v $ $Revision: 1.5 $ $Date: 2001/01/31 18:59:51 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <sys/stat.h>
#include "marshall.h"
#define local_unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#if SACCT
#include "../h/sacct.h"
#endif
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include <serrno.h>
#include "osdep.h"
#include "Cgetopt.h"

void procfilchgreq _PROTO((char *, char *));

extern char func[16];
extern int reqid;
extern int rpfd;
extern int req2argv _PROTO((char *, char ***));
extern int upd_staged _PROTO((char *));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *));

void
procfilchgreq(req_data, clienthost)
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	int c, i;
	gid_t gid;
	int nargs;
	uid_t uid;
	char *user;
	char *diskpool = NULL;
	char *rbp;
	int clientpid;

	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	unmarshall_WORD (rbp, clientpid);
	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
						 reqid, STAGEFILCHG, 0, 0, NULL, "");
#endif

	Coptind = 1;
	Copterr = 0;
	while ((c = Cgetopt (nargs, argv, "p:")) != -1) {
		switch (c) {
		case 'p':
			diskpool = Coptarg;
			break;
		}
	}
	if (nargs > Coptind) {
		for  (i = Coptind; i < nargs; i++)
			if ((c = upd_staged (argv[i])) != 0)
				goto reply;
	}
 reply:
	free (argv);
	sendrep (rpfd, STAGERC, STAGEFILCHG, c);
}
