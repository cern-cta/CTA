/*
 * $Id: procping.c,v 1.5 2002/03/04 10:34:50 jdurand Exp $
 */

/*
 * Copyright (C) 2001 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procping.c,v $ $Revision: 1.5 $ $Date: 2002/03/04 10:34:50 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

#include <sys/types.h>
#include <stdlib.h>
#include <errno.h>
#include "osdep.h"
#include "stage_constants.h"
#include "stage_messages.h"
#include "Cgrp.h"
#include "Castor_limits.h"
#include "stage_api.h"
#include "Cgetopt.h"
#include "marshall.h"
#ifndef _WIN32
#include <unistd.h>
#endif
#define local_unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#if SACCT
#include "../h/sacct.h"
#endif
#include "patchlevel.h"
#define __BASEVERSION__ "?"
#define __PATCHLEVEL__ 0

void procpingreq _PROTO((int, int, char *, char *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern char *stglogflags _PROTO((char *, char *, u_signed64));

extern char func[16];
extern int rpfd;
extern int reqid;

void procpingreq(req_type, magic, req_data, clienthost)
		 int req_type;
		 int magic;
		 char *req_data;
		 char *clienthost;
{
	int c;
	int rc = 0;
	char *rbp;
	char *user;
	gid_t gid;
	struct group *gr;
	extern char localhost[CA_MAXHOSTNAMELEN+1];
	extern time_t started_time;
	char timestr[64] ;   /* Time in its ASCII format             */
	static struct Coptions longopts[] =
	{
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"verbose",            NO_ARGUMENT,        NULL,      'v'},
		{NULL,                 0,                  NULL,        0}
	};
	u_signed64 flags;
	int nargs;
	int errflg = 0;
	char **argv = NULL;
#if defined(HPSSCLIENT)
	char hpss_aware = 'H';
#else
	char hpss_aware = ' ';
#endif
	int verbose_flag = 0;

	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	if (req_type > STAGE_00) {
		unmarshall_LONG (rbp, gid);
		unmarshall_HYPER (rbp, flags);
		stglogit (func, "STG92 - %s request by %s (,%d) from %s\n", "stage_ping", user, gid, clienthost);
    } else {
		unmarshall_WORD (rbp, gid);
		stglogit (func, "STG92 - %s request by %s (,%d) from %s\n", "stageping", user, gid, clienthost);
		nargs = req2argv (rbp, &argv);
	}
#if SACCT
	stageacct (STGCMDR, -1, gid, clienthost,
						 reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif
	
	if ((gr = Cgetgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (rpfd, MSG_ERR, STG36, gid);
		rc = SYERR;
		goto reply;
	}

	if (req_type > STAGE_00) {
		if ((flags & STAGE_VERBOSE) == STAGE_VERBOSE) {
			verbose_flag++;
		}
		/* Print the flags */
		stglogflags("stage_ping",LOGFILE,(u_signed64) flags);
	} else {
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt_long (nargs, argv, "h:v", longopts, NULL)) != -1) {
			switch (c) {
			case 'h':
				break;
			case 'v':
				verbose_flag++;
				break;
			case 0:
				/* These are the long options */
				break;
			default:
				errflg++;
				break;
			}
			if (errflg != 0) break;
		}
	}

	if (errflg != 0) {
		rc = USERR;
		goto reply;
	}

	/* We send util information */
	/* The list of object linked to produce stgdaemon is: */
	/* stgdaemon.o packfseq.o poolmgr.o procalloc.o procclr.o  stgdb_Cdb_ifce.o Cstage_db.o Cstage_ifce.o  procio.o procping.o procqry.o procupd.o procfilchg.o sendrep.o stglogit.o stageacct.o */

	if (verbose_flag) {
		stage_util_time(started_time,timestr);
		sendrep (rpfd, MSG_OUT, "Stager daemon - CASTOR %s.%d%c\n", BASEVERSION, PATCHLEVEL, hpss_aware);
		sendrep (rpfd, MSG_OUT, "Generated %s around %s\n", __DATE__, __TIME__);
		sendrep (rpfd, MSG_OUT, "Running since %s, pid=%d\n", timestr, (int) getpid());
	}
	reply:
	sendrep (rpfd, STAGERC, STAGEPING, rc);
	return;
}
