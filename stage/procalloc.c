/*
 * $Id: procalloc.c,v 1.50 2002/09/17 11:29:11 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procalloc.c,v $ $Revision: 1.50 $ $Date: 2002/09/17 11:29:11 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
#else
#include <netinet/in.h>
#include <sys/time.h>
#endif
#include "marshall.h"
#define local_unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#include "stage_api.h"
#if SACCT
#include "../h/sacct.h"
#endif
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include <errno.h>
#include "serrno.h"
#include "osdep.h"
#include "Cgrp.h"
#include "Cgetopt.h"
#include "u64subr.h"

extern char defpoolname[CA_MAXPOOLNAMELEN + 1];
extern char func[16];
extern int reqid;
extern int rpfd;
extern u_signed64 stage_uniqueid;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, int **, char *, char *, int));
extern struct stgcat_entry *newreq _PROTO((int));
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif

void procallocreq _PROTO((int, int, char *, char *));
void procgetreq _PROTO((int, int, char *, char *));
extern int updfreespace _PROTO((char *, char *, int, u_signed64 *, signed64));
extern int req2argv _PROTO((char *, char ***));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int *, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());
extern char *stglogflags _PROTO((char *, char *, u_signed64));
extern int isvalidpool _PROTO((char *));
extern int build_ipath _PROTO((char *, struct stgcat_entry *, char *, int, int, mode_t));
extern int cleanpool _PROTO((char *));
extern void delreq _PROTO((struct stgcat_entry *, int));
extern void create_link _PROTO((struct stgcat_entry *, char *));
extern int savepath _PROTO(());
extern int savereqs _PROTO(());
extern void rmfromwq _PROTO((struct waitq *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern void bestnextpool_out _PROTO((char *, int));
extern void rwcountersfs _PROTO((char *, char *, int, int));

void procallocreq(req_type, magic, req_data, clienthost)
	int req_type;
	int magic;
	char *req_data;
	char *clienthost;
{
	char **argv = NULL;
	int c, i;
	int clientpid;
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
	struct waitq *wqp = NULL;
	int checkrc;
	int api_out = 0;
	struct stgcat_entry stcp_input;
	u_signed64  flags = 0;
	mode_t openmode = 0;
	int noretry_flag = 0;

	memset ((char *)&stgreq, 0, sizeof(stgreq));
	memset ((char *)&stcp_input, 0, sizeof(stcp_input));
	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name - this is not yet a local copy */
	if (strlen(user) > CA_MAXUSRNAMELEN) {
		serrno = SENAMETOOLONG;
		sendrep (&rpfd, MSG_ERR, STG33, "user name", sstrerror(serrno));
		c = EINVAL;
		goto reply;
	}
	strcpy (stgreq.user, user);
	if (req_type > STAGE_00) {
		/* API method */
		unmarshall_LONG (rbp, stgreq.uid);
		unmarshall_LONG (rbp, stgreq.gid);
		unmarshall_LONG (rbp, stgreq.mask);
		unmarshall_LONG (rbp, clientpid);
		unmarshall_HYPER (rbp, stage_uniqueid);
	} else {
		/* Command-line method */
		local_unmarshall_STRING (rbp, name); /* Not used after - this can overbound */
		unmarshall_WORD (rbp, stgreq.uid);
		unmarshall_WORD (rbp, stgreq.gid);
		unmarshall_WORD (rbp, stgreq.mask);
		unmarshall_WORD (rbp, clientpid);
	}
		
	stglogit (func, STG92, req_type == STAGEALLOC ? "stagealloc" : "stage_alloc", stgreq.user, stgreq.uid, stgreq.gid, clienthost);
#if SACCT
	stageacct (STGCMDR, stgreq.uid, stgreq.gid, clienthost,
			   reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif

	if ((gr = Cgetgrgid (stgreq.gid)) == NULL) {
		if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (&rpfd, MSG_ERR, STG36, stgreq.gid);
		c = (errno == ENOENT) ? ESTGROUP : SESYSERR;
		goto reply;
	}
	strncpy (stgreq.group, gr->gr_name, CA_MAXGRPNAMELEN);
	stgreq.group[CA_MAXGRPNAMELEN] = '\0';

	if (req_type > STAGE_00) {
		char logit[BUFSIZ + 1];
		char *User;
		char t_or_d;
		int  nstcp_input, struct_status;

		/* This is coming from the API */
		api_out = 1;
		local_unmarshall_STRING(rbp, User);
		if (User[0] != '\0') pool_user = User;
		unmarshall_HYPER(rbp, flags);
		if ((flags & STAGE_PATHNAME) == STAGE_PATHNAME) Pflag++;
		if ((flags & STAGE_UFUN) == STAGE_UFUN) Uflag++;
		if ((flags & STAGE_NORETRY) == STAGE_NORETRY) noretry_flag = 1;
		unmarshall_LONG(rbp, openmode);
		{
			char tmpbyte;
			unmarshall_BYTE(rbp, tmpbyte);
			t_or_d = tmpbyte;
		}
		if (t_or_d != 'a') {
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid structure identifier ('%c')\n", t_or_d);
			c = EINVAL;
			goto reply;
		}
		unmarshall_LONG(rbp, nstcp_input);
		if (nstcp_input != 1) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid number of input structure (%d)\n", nstcp_input);
			c = EINVAL;
			goto reply;
		}
		struct_status = 0;
		unmarshall_STAGE_CAT(magic, STGDAEMON_LEVEL,STAGE_INPUT_MODE, struct_status, rbp, &stcp_input);
		if (struct_status != 0) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Bad input\n");
			c = SEINTERNAL;
			goto reply;
		}
		logit[0] = '\0';
		stcp_input.reqid = 0; /* Disable explicitely eventual reqid in the protocol */
		if ((stage_stcp2buf(logit,BUFSIZ,&stcp_input) == 0) || (serrno == SEUMSG2LONG)) {
			logit[BUFSIZ] = '\0';
			stglogit(func,"stcp[1/1] :%s\n",logit);
		}
		if (stcp_input.poolname[0] != '\0') {
			/* User specified a poolname */
			if (! isvalidpool (stcp_input.poolname)) {
				sendrep (&rpfd, MSG_ERR, STG32, stcp_input.poolname);
				c = EINVAL;
				goto reply;
			}
		}
		/* Print the general flags */
		stglogflags(func,LOGFILE,flags);
		nargs = Coptind = 1;
	} else {
		nargs = req2argv (rbp, &argv);
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt (nargs, argv, "Gh:Pp:s:U:u:")) != -1) {
			switch (c) {
			case 'G':
				break;
			case 'h':
				break;
			case 'P':
				Pflag++;
				break;
			case 'p':
				if (isvalidpool (Coptarg)) {
					strcpy (stgreq.poolname, Coptarg);
				} else {
					sendrep (&rpfd, MSG_ERR, STG32, Coptarg);
					errflg++;
				}
				break;
			case 's':
				if ((checkrc = stage_util_check_for_strutou64(Coptarg)) < 0) {
					sendrep (&rpfd, MSG_ERR, STG06, "-s");
					errflg++;
				} else {
					stgreq.size = strutou64(Coptarg);
					if (stgreq.size <= 0) {
						sendrep (&rpfd, MSG_ERR, STG06, "-s");
						errflg++;
					} else {
						if (checkrc == 0) stgreq.size *= ONE_MB; /* Not unit : default MB */
					}
				}
				break;
			case 'u':
				pool_user = Coptarg;
				break;
			case 'U':
				Uflag++;
				break;
			}
		}
	}
	if (errflg) {
		c = EINVAL;
		goto reply;
	}

	/* setting defaults */

	if (*stgreq.poolname == '\0')
		bestnextpool_out(stgreq.poolname,WRITE_MODE);
	if (pool_user == NULL)
		pool_user = STAGERSUPERUSER;

	nbdskf = nargs - Coptind;
	if (Uflag && nbdskf == 2)
		Upluspath = 1;

	/* building catalog entry */

	stgreq.t_or_d = 'a';
	if (req_type < STAGE_00) {
		strcpy (stgreq.u1.d.xfile, argv[Coptind]);
		strcpy (upath, argv[Coptind]);
	} else {
		strcpy (stgreq.u1.d.xfile, stcp_input.u1.d.xfile);
		strcpy (upath, stgreq.u1.d.xfile);
		strcpy (stgreq.poolname, stcp_input.poolname);
		stgreq.size = stcp_input.size;
	}
	stcp = newreq((int) 'a');
	memcpy (stcp, &stgreq, sizeof(stgreq));
	stcp->reqid = reqid;
	stcp->status = STAGEALLOC;
	stcp->c_time = time(NULL);
	stcp->a_time = stcp->c_time;
	stcp->nbaccesses++;
	if ((c = build_ipath (upath, stcp, pool_user, 0, api_out, (mode_t) openmode)) < 0) {
		if (noretry_flag) {
			c = ENOSPC;
			sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(c));
			goto noretry;
		}
		stcp->status |= WAITING_SPC;
		if (!wqp) wqp = add2wq (clienthost,
								user, stcp->uid, stcp->gid,
								user, stcp->group, stcp->uid, stcp->gid,
								clientpid,
								Upluspath, reqid, STAGEALLOC, nbdskf, &wfp, NULL, NULL, NULL, 0);
		wqp->api_out = api_out;
		wqp->magic = magic;
		wqp->Pflag = Pflag;
		wqp->openmode = openmode;
		wqp->uniqueid = stage_uniqueid;
		wqp->flags = flags;
		wfp->subreqid = stcp->reqid;
		strcpy (wfp->upath, upath);
		wqp->nbdskf++;
		wfp++;
		if (Upluspath) {
			/* Never happen in API mode (argv == NULL there, btw) */
			wfp->subreqid = stcp->reqid;
			strcpy (wfp->upath, argv[Coptind+1]);
		}
		strcpy (wqp->pool_user, pool_user);
		strcpy (wqp->waiting_pool, stcp->poolname);
		wqp->nb_clnreq++;
		cleanpool (stcp->poolname);
	} else if (c) {
	  noretry:
		delreq (stcp,1);
		goto reply;
	} else {
		if (Pflag) {
			sendrep (&rpfd, MSG_OUT, "%s\n", stcp->ipath);
			if (api_out != 0) sendrep(&rpfd, API_STCP_OUT, stcp, magic);
		}
		if (*upath && strcmp (stcp->ipath, upath)) {
			create_link (stcp, upath);
		}
		if (Upluspath) {
			/* Never happen in API mode (argv == NULL there, btw) */
			if ((argv[Coptind+1][0] != '\0') && strcmp (stcp->ipath, argv[Coptind+1])) {
				create_link (stcp, argv[Coptind+1]);
			}
		}
		rwcountersfs(stcp->poolname, stcp->ipath, STAGEALLOC, STAGEALLOC);
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
	if (argv != NULL) free (argv);
	return;
 reply:
	if (argv != NULL) free (argv);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
						 reqid, req_type, 0, c, NULL, "", (char) 0);
#endif
	sendrep (&rpfd, STAGERC, req_type, magic, c);
	if (c && wqp) {
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
			if (! wfp->waiting_on_req)
				updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			delreq (stcp,0);
		}
		rmfromwq (wqp);
	}
}

void procgetreq(req_type, magic, req_data, clienthost)
	int req_type;
	int magic;
	char *req_data;
	char *clienthost;
{
	char **argv = NULL;
	char *basename;
	int c;
	int errflg = 0;
	int found;
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
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;
	struct stgcat_entry stgreq;
	struct stgcat_entry stcp_input;
	int clientpid;
	u_signed64  flags = 0;
	int api_out = 0;

	poolname[0] = '\0';
	memset ((char *)&stgreq, 0, sizeof(stgreq));
	memset ((char *)&stcp_input, 0, sizeof(stcp_input));
	rbp = req_data;
	local_unmarshall_STRING (rbp, user);  /* login name - not user after - this can overbound */
	if (strlen(user) > CA_MAXUSRNAMELEN) {
		serrno = SENAMETOOLONG;
		sendrep (&rpfd, MSG_ERR, STG33, "user name", sstrerror(serrno));
		c = EINVAL;
		goto reply;
	}
	strcpy (stgreq.user, user);
	if (req_type > STAGE_00) {
		/* API method */
		unmarshall_LONG (rbp, stgreq.uid);
		unmarshall_LONG (rbp, stgreq.gid);
		unmarshall_LONG (rbp, stgreq.mask);
		unmarshall_LONG (rbp, clientpid);
		unmarshall_HYPER (rbp, stage_uniqueid);
	} else {
		/* Command-line method */
		unmarshall_WORD (rbp, stgreq.uid);
		unmarshall_WORD (rbp, stgreq.gid);
	}

	stglogit (func, STG92, req_type == STAGEGET ? "stageget" : "stage_get", stgreq.user, stgreq.uid, stgreq.gid, clienthost);
#if SACCT
	stageacct (STGCMDR, stgreq.uid, stgreq.gid, clienthost,
			   reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif

	if ((gr = Cgetgrgid (stgreq.gid)) == NULL) {
		if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (&rpfd, MSG_ERR, STG36, stgreq.gid);
		c = (errno == ENOENT) ? ESTGROUP : SESYSERR;
		goto reply;
	}
	if (req_type > STAGE_00) {
		char logit[BUFSIZ + 1];
		char *User;
		char t_or_d;
		int  nstcp_input, struct_status;

		/* This is coming from the API */
		local_unmarshall_STRING(rbp, User);
		if (User[0] != '\0') pool_user = User;
		unmarshall_HYPER(rbp, flags);
		if ((flags & STAGE_PATHNAME) == STAGE_PATHNAME) Pflag++;
		if ((flags & STAGE_UFUN) == STAGE_UFUN) Uflag++;
		{
			char tmpbyte;
			unmarshall_BYTE(rbp, tmpbyte);
			t_or_d = tmpbyte;
		}
		if (t_or_d != 'a') {
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid structure identifier ('%c')\n", t_or_d);
			c = EINVAL;
			goto reply;
		}
		unmarshall_LONG(rbp, nstcp_input);
		if (nstcp_input != 1) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid number of input structure (%d)\n", nstcp_input);
			c = EINVAL;
			goto reply;
		}
		struct_status = 0;
		unmarshall_STAGE_CAT(magic, STGDAEMON_LEVEL,STAGE_INPUT_MODE, struct_status, rbp, &stcp_input);
		if (struct_status != 0) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Bad input\n");
			c = SEINTERNAL;
			goto reply;
		}
		logit[0] = '\0';
		stcp_input.reqid = 0; /* Disable explicitely eventual reqid in the protocol */
		if ((stage_stcp2buf(logit,BUFSIZ,&stcp_input) == 0) || (serrno == SEUMSG2LONG)) {
			logit[BUFSIZ] = '\0';
			stglogit(func,"stcp[1/1] :%s\n",logit);
		}
		if (stcp_input.poolname[0] != '\0') {
			/* User specified a poolname */
			if (! isvalidpool (stcp_input.poolname)) {
				sendrep (&rpfd, MSG_ERR, STG32, stcp_input.poolname);
				c = EINVAL;
				goto reply;
			}
			strcpy(poolname,stcp_input.poolname);
		}
		/* Print the general flags */
		stglogflags(func,LOGFILE,flags);
		nargs = Coptind = 1;
	} else {
		nargs = req2argv (rbp, &argv);
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt (nargs, argv, "Gh:Pp:U:u:")) != -1) {
			switch (c) {
			case 'G':
				break;
			case 'h':
				break;
			case 'P':
				Pflag++;
				break;
			case 'p':
				if (isvalidpool (Coptarg)) {
					strcpy (poolname, Coptarg);
				} else {
					sendrep (&rpfd, MSG_ERR, STG32, Coptarg);
					errflg++;
				}
				break;
			case 'U':
				Uflag++;
				break;
			case 'u':
				pool_user = Coptarg;
				break;
			}
		}
	}
	if (errflg) {
		c = EINVAL;
		goto reply;
	}

	/* setting defaults */

	if (pool_user == NULL)
		pool_user = STAGERSUPERUSER;

	nbdskf = nargs - Coptind;
	if (Uflag && nbdskf == 2)
		Upluspath = 1;

	if (req_type < STAGE_00) {
		strcpy (upath, argv[Coptind]);
	} else {
		strcpy (upath, stcp_input.u1.d.xfile);
		strcpy (poolname, stcp_input.poolname);
	}

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
	if ((found == 0) || (stcp->status != (STAGEALLOC|STAGED))) {
		sendrep (&rpfd, MSG_ERR, STG22);
		c = ENOENT;
		goto reply;
	}
	stcp->a_time = time(NULL);
	stcp->nbaccesses++;
#ifdef USECDB
	if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
		stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif
	if (Pflag) {
		sendrep (&rpfd, MSG_OUT, "%s\n", stcp->ipath);
		if (api_out != 0) sendrep(&rpfd, API_STCP_OUT, stcp, magic);
	}
	if (*upath && strcmp (stcp->ipath, upath))
		create_link (stcp, upath);
	if (Upluspath && (argv[Coptind+1][0] != '\0') &&
			strcmp (stcp->ipath, argv[Coptind+1]))
		create_link (stcp, argv[Coptind+1]);
	savereqs ();
	c = 0;
 reply:
	if (argv != NULL) free (argv);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
						 reqid, req_type, 0, c, NULL, "", (char) 0);
#endif
	sendrep (&rpfd, STAGERC, req_type, magic, c);
}
