/*
 * $Id: procalloc.c,v 1.3 1999/07/20 17:29:17 jdurand Exp $
 *
 * $Log: procalloc.c,v $
 * Revision 1.3  1999/07/20 17:29:17  jdurand
 * Added Id and Log CVS's directives
 *
 */

/*
 * Copyright (C) 1993-1997 by CERN/CN/PDP/DH
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)procalloc.c	1.8 09/05/97 CERN CN-PDP/DH Jean-Philippe Baud";
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
#ifdef DB
#include <Cdb_api.h>
#include <errno.h>
#include "wrapdb.h"
#endif

extern char *optarg;
extern int optind;
extern char defpoolname[MAXPOOLNAMELEN];
extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
#ifdef DB
extern db_fd **db_stgcat;       /* DB connection on STGCAT catalog */
#endif
struct waitq *add2wq();

procallocreq(req_data, clienthost)
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
	struct stgcat_entry *newreq();
	int Pflag = 0;
	char *pool_user = NULL;
	char *rbp;
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
	int Uflag = 0;
	int Upluspath = 0;
	char upath[MAXHOSTNAMELEN + MAXPATH];
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
	if ((gr = getgrgid (stgreq.gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, stgreq.gid);
		c = SYERR;
		goto reply;
	}
	strcpy (stgreq.group, gr->gr_name);
	optind = 1;
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
			if (*dp != '\0' || stgreq.size > 2047) {
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
	c = build_ipath (upath, stcp, pool_user);
#ifdef DB
	if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
		sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
		goto reply;
	}
#endif /* DB */
	if (c < 0) {
		stcp->status |= WAITING_SPC;
#ifdef DB
		if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
			sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
			goto reply;
		}
#endif /* DB */
		if (!wqp) wqp = add2wq (clienthost, user,
			stcp->uid, stcp->gid, clientpid,
			Upluspath, reqid, STAGEALLOC, nbdskf, &wfp);
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
			stcp->size*1024*1024);
		delreq (stcp);
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
	savepath ();
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	free (argv);
#ifdef DB
	if (stcp != NULL)
		free(stcp);
#endif /* DB */
	return;
reply:
#ifdef DB
	if (stcp != NULL)
		free(stcp);
#endif /* DB */
	free (argv);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
		reqid, STAGEALLOC, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, STAGEALLOC, c);
	if (c && wqp) {
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
#ifdef DB
			if (wrapCdb_fetch(db_stgcat,wfp->subreqid,(void **) &stcp, NULL, 0) != 0) {
					sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
				if (stcp != NULL)
					free(stcp);
				return;
			}
#else /* DB */
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
#endif /* DB */
			if (! wfp->waiting_on_req)
				updfreespace (stcp->poolname, stcp->ipath,
					stcp->size*1024*1024);
			delreq (stcp);
		}
		rmfromwq (wqp);
	}
#ifdef DB
	if (stcp != NULL)
		free(stcp);
#endif /* DB */
}

procgetreq(req_data, clienthost)
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
	char poolname[MAXPOOLNAMELEN];
	char *rbp;
	struct stgcat_entry *stcp;
	int Uflag = 0;
	int Upluspath = 0;
	uid_t uid;
	char upath[MAXHOSTNAMELEN + MAXPATH];
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

	if ((gr = getgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
		goto reply;
	}
	optind = 1;
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
#ifdef DB
	if (Cdb_altkey_rewind(db_stgcat, "u1.d.xfile") == 0) {
		size_t db_size;
		char *db_key = NULL;
		char *db_data = NULL;
		while (Cdb_altkey_nextrec(db_stgcat, "u1.d.xfile", &db_key, (void **) &db_data, &db_size) != 0) {
			char *ptr, *ptrmax;
			ptrmax = ptr = db_data;
			ptrmax += db_size;
			do {
				size_t stcp_size;
				int reqid;

				reqid = atoi(ptr);
				/* In case of a string, sizeof() == strlen() + 1 */
				/* ptr points to the reqid (as a string, e.g. a master key) */
				if (wrapCdb_fetch(db_stgcat,reqid,(void **) &stcp, NULL, 0) == 0) {
					if (stcp != NULL) {
						if (stcp->t_or_d != 'a') goto docontinue;
						if (*poolname && strcmp (poolname, stcp->poolname)) goto docontinue;
						if ((p = strrchr (stcp->u1.d.xfile, '/')) == NULL)
							p = stcp->u1.d.xfile;
						else
							p++;
						if (strcmp (p, basename)) goto docontinue;
						p = strrchr (stcp->ipath, '/');
						*p = '\0';
						q = strrchr (stcp->ipath, '/');
						if (strcmp (q+1, pool_user) == 0) {
							*p = '/';
							found = 1;
							break;
						}
						*p = '/';
						wrapCdb_store(db_stgcat,stcp->reqid,stcp,sizeof(struct stgcat_entry),DB_ALWAYS,0);
					}
				}
			docontinue:
				ptr += (strlen(ptr) + 1);
			} while (ptr < ptrmax);
		}
		if (db_key != NULL)
			free(db_key);
		if (db_data != NULL)
			free(db_data);
		if (stcp != NULL)
			free(stcp);
	}
#else /* DB */
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if (stcp->t_or_d != 'a') continue;
		if (*poolname && strcmp (poolname, stcp->poolname)) continue;
		if ((p = strrchr (stcp->u1.d.xfile, '/')) == NULL)
			p = stcp->u1.d.xfile;
		else
			p++;
		if (strcmp (p, basename)) continue;
		p = strrchr (stcp->ipath, '/');
		*p = '\0';
		q = strrchr (stcp->ipath, '/');
		if (strcmp (q+1, pool_user) == 0) {
			*p = '/';
			found = 1;
			break;
		}
		*p = '/';
	}
#endif /* DB */
	if (found == 0 ||
	    stcp->status != (STAGEALLOC|STAGED)) {
		sendrep (rpfd, MSG_ERR, STG22);
		c = USERR;
		goto reply;
	}
	stcp->a_time = time (0);
	stcp->nbaccesses++;
#ifdef DB
	if (wrapCdb_store(db_stgcat, stcp->reqid, stcp, sizeof(struct stgcat_entry), DB_ALWAYS, 0) != 0) {
		sendrep (rpfd, MSG_ERR, STG102, __FILE__, __LINE__, errno, db_strerror(errno));
		c = SYERR;
		goto reply;
	}
#endif /* DB */
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
