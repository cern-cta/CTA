/*
 * $Id: procalloc.c,v 1.20 2000/10/27 14:04:31 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procalloc.c,v $ $Revision: 1.20 $ $Date: 2000/10/27 14:04:31 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include <grp.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
#else
#include <netinet/in.h>
#include <sys/time.h>
#endif
#include "marshall.h"
#undef  unmarshall_STRING
#define unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#if SACCT
#include "../h/sacct.h"
#endif
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include <serrno.h>
#include "osdep.h"
#include "Cgrp.h"

extern char *optarg;
extern int optind;
extern char defpoolname[CA_MAXPOOLNAMELEN + 1];
extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, char *, char *));
extern struct stgcat_entry *newreq _PROTO(());
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif

void procallocreq _PROTO((char *, char *));
void procgetreq _PROTO((char *, char *));
extern int updfreespace _PROTO((char *, char *, signed64));

void procallocreq(req_data, clienthost)
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	int c, i;
	int clientpid;
	char *dp;
	int errflg = 0;
	struct group *gr;
	char *name;
	int nargs;
	int nbdskf;
	int Pflag = 0;
	char *pool_user = NULL;
	char *rbp;
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
	int Uflag = 0;
	int Upluspath = 0;
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;
	struct waitf *wfp;
	struct waitq *wqp;

	memset ((char *)&stgreq, 0, sizeof(stgreq));
	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	strcpy (stgreq.user, user);
	unmarshall_STRING (rbp, name);
	unmarshall_WORD (rbp, stgreq.uid);
	unmarshall_WORD (rbp, stgreq.gid);
	unmarshall_WORD (rbp, stgreq.mask);
	unmarshall_WORD (rbp, clientpid);

	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, stgreq.uid, stgreq.gid, clienthost,
						 reqid, STAGEALLOC, 0, 0, NULL, "");
#endif

	wqp = NULL;
	if ((gr = Cgetgrgid (stgreq.gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, stgreq.gid);
		c = SYERR;
		goto reply;
	}
	strcpy (stgreq.group, gr->gr_name);
#ifdef linux
	optind = 0;
#else
	optind = 1;
#endif
	while ((c = getopt (nargs, argv, "Gh:Pp:s:U:u:")) != EOF) {
		switch (c) {
		case 'G':
			break;
		case 'h':
			break;
		case 'P':
			Pflag++;
			break;
		case 'p':
			if (isvalidpool (optarg)) {
				strcpy (stgreq.poolname, optarg);
			} else {
				sendrep (rpfd, MSG_ERR, STG32, optarg);
				errflg++;
			}
			break;
		case 's':
			stgreq.size = strtol (optarg, &dp, 10);
			if (*dp != '\0' || stgreq.size > 2047 || stgreq.size <= 0) {
				sendrep (rpfd, MSG_ERR, STG06, "-s");
				errflg++;
			}
			break;
		case 'u':
			pool_user = optarg;
			break;
		case 'U':
			Uflag++;
			break;
		}
	}
	if (errflg) {
		c = USERR;
		goto reply;
	}

	/* setting defaults */

	if (*stgreq.poolname == '\0')
		strcpy (stgreq.poolname, defpoolname);
	if (pool_user == NULL)
		pool_user = "stage";

	nbdskf = nargs - optind;
	if (Uflag && nbdskf == 2)
		Upluspath = 1;

	/* building catalog entry */

	stgreq.t_or_d = 'a';
	strcpy (stgreq.u1.d.xfile, argv[optind]);
	strcpy (upath, argv[optind]);
	stcp = newreq ();
	memcpy (stcp, &stgreq, sizeof(stgreq));
	stcp->reqid = reqid;
	stcp->status = STAGEALLOC;
	stcp->c_time = time (0);
	stcp->a_time = stcp->c_time;
	stcp->nbaccesses++;
	if ((c = build_ipath (upath, stcp, pool_user)) < 0) {
		stcp->status |= WAITING_SPC;
		if (!wqp) wqp = add2wq (clienthost, user,
								stcp->uid, stcp->gid, clientpid,
								Upluspath, reqid, STAGEALLOC, nbdskf, &wfp, NULL, NULL);
		wqp->Pflag = Pflag;
		wfp->subreqid = stcp->reqid;
		strcpy (wfp->upath, upath);
		wqp->nbdskf++;
		wfp++;
		if (Upluspath) {
			wfp->subreqid = stcp->reqid;
			strcpy (wfp->upath, argv[optind+1]);
		}
		strcpy (wqp->pool_user, pool_user);
		strcpy (wqp->waiting_pool, stcp->poolname);
		wqp->nb_clnreq++;
		cleanpool (stcp->poolname);
	} else if (c) {
		updfreespace (stcp->poolname, stcp->ipath,
									(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
		delreq (stcp,1);
		goto reply;
	} else {
		if (Pflag)
			sendrep (rpfd, MSG_OUT, "%s\n", stcp->ipath);
		if (*upath && strcmp (stcp->ipath, upath))
			create_link (stcp, upath);
		if (Upluspath &&
				strcmp (stcp->ipath, argv[optind+1]))
			create_link (stcp, argv[optind+1]);
	}
#ifdef USECDB
	if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
		stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif
	savepath ();
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	free (argv);
	return;
 reply:
	free (argv);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
						 reqid, STAGEALLOC, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, STAGEALLOC, c);
	if (c && wqp) {
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
			if (! wfp->waiting_on_req)
				updfreespace (stcp->poolname, stcp->ipath,
											(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
			delreq (stcp,0);
		}
		rmfromwq (wqp);
	}
}

void procgetreq(req_data, clienthost)
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	char *basename;
	int c;
	int errflg = 0;
	int found;
	gid_t gid;
	struct group *gr;
	int nargs;
	int nbdskf;
	int Pflag = 0;
	char *p, *q;
	char *pool_user = NULL;
	char poolname[CA_MAXPOOLNAMELEN + 1];
	char *rbp;
	struct stgcat_entry *stcp;
	int Uflag = 0;
	int Upluspath = 0;
	uid_t uid;
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;

	poolname[0] = '\0';
	rbp = req_data;
	unmarshall_STRING (rbp, user);  /* login name */
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);

	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
						 reqid, STAGEGET, 0, 0, NULL, "");
#endif

	if ((gr = Cgetgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
		goto reply;
	}
#ifdef linux
	optind = 0;
#else
	optind = 1;
#endif
	while ((c = getopt (nargs, argv, "Gh:Pp:U:u:")) != EOF) {
		switch (c) {
		case 'G':
			break;
		case 'h':
			break;
		case 'P':
			Pflag++;
			break;
		case 'p':
			if (isvalidpool (optarg)) {
				strcpy (poolname, optarg);
			} else {
				sendrep (rpfd, MSG_ERR, STG32, optarg);
				errflg++;
			}
			break;
		case 'U':
			Uflag++;
			break;
		case 'u':
			pool_user = optarg;
			break;
		}
	}
	if (errflg) {
		c = USERR;
		goto reply;
	}

	/* setting defaults */

	if (pool_user == NULL)
		pool_user = "stage";

	nbdskf = nargs - optind;
	if (Uflag && nbdskf == 2)
		Upluspath = 1;
	strcpy (upath, argv[optind]);
	if ((basename = strrchr (upath, '/')) == NULL)
		basename = upath;
	else
		basename++;
	found = 0;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if (stcp->t_or_d != 'a') continue;
		if (*poolname && strcmp (poolname, stcp->poolname)) continue;
		if ((p = strrchr (stcp->u1.d.xfile, '/')) == NULL)
			p = stcp->u1.d.xfile;
		else
			p++;
		if (strcmp (p, basename)) continue;
		if ((p = strrchr (stcp->ipath, '/')) != NULL) {
			*p = '\0';
			if ((q = strrchr (stcp->ipath, '/')) != NULL) {
				if (strcmp (q+1, pool_user) == 0) {
					*p = '/';
					found = 1;
					break;
				}
			}
			*p = '/';
		}
	}
	if (found == 0 ||
			stcp->status != (STAGEALLOC|STAGED)) {
		sendrep (rpfd, MSG_ERR, STG22);
		c = USERR;
		goto reply;
	}
	stcp->a_time = time (0);
	stcp->nbaccesses++;
#ifdef USECDB
	if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
		stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif
	if (Pflag)
		sendrep (rpfd, MSG_OUT, "%s\n", stcp->ipath);
	if (*upath && strcmp (stcp->ipath, upath))
		create_link (stcp, upath);
	if (Upluspath &&
			strcmp (stcp->ipath, argv[optind+1]))
		create_link (stcp, argv[optind+1]);
	savereqs ();
	c = 0;
 reply:
	free (argv);
#if SACCT
	stageacct (STGCMDC, uid, gid, clienthost,
						 reqid, STAGEGET, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, STAGEGET, c);
}
