/*
 * $Id: procupd.c,v 1.64 2001/03/16 09:07:39 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procupd.c,v $ $Revision: 1.64 $ $Date: 2001/03/16 09:07:39 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include <sys/stat.h>
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
#include "rfio_api.h"
#include "Cns_api.h"
#include "Cgetopt.h"
#include "u64subr.h"
#include "Cgrp.h"

void procupdreq _PROTO((char *, char *));
void update_hsm_a_time _PROTO((struct stgcat_entry *));

extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct waitq *waitqp;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
extern struct fileclass *fileclasses;
extern u_signed64 stage_uniqueid;

extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *));
extern struct waitf *add2wf _PROTO((struct waitq *));
extern int add2otherwf _PROTO((struct waitq *, char *, struct waitf *, struct waitf *));
extern int extend_waitf _PROTO((struct waitf *, struct waitf *));
extern int check_waiting_on_req _PROTO((int, int));
extern int check_coff_waiting_on_req _PROTO((int, int));
extern struct stgcat_entry *newreq _PROTO(());
extern int update_migpool _PROTO((struct stgcat_entry **, int, int));
extern int updfreespace _PROTO((char *, char *, signed64));
extern int req2argv _PROTO((char *, char ***));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());
extern int nextreqid _PROTO(());
extern int savereqs _PROTO(());
extern int build_ipath _PROTO((char *, struct stgcat_entry *, char *));
extern int cleanpool _PROTO((char *));
extern void delreq _PROTO((struct stgcat_entry *, int));
extern int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int, int));
extern void sendinfo2cptape _PROTO((int, struct stgcat_entry *));
extern void create_link _PROTO((struct stgcat_entry *, char *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *));
extern int retenp_on_disk _PROTO((int));
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *));
extern void rwcountersfs _PROTO((char *, char *, int, int));
extern struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, int **, char *, char *, int));

#define IS_RC_OK(rc) (rc == 0)
#define IS_RC_WARNING(rc) (rc == LIMBYSZ || rc == BLKSKPD || rc == TPE_LSZ || (rc == MNYPARI && (stcp->u1.t.E_Tflags & KEEPFILE)))

/* offset can be undef sometimes */
#ifndef offsetof
#define offsetof(s_name, s_member) \
        ((size_t)((char *)&((s_name *)NULL)->s_member - (char *)NULL))
#endif

#ifdef HAVE_SENSIBLE_STCP
#undef HAVE_SENSIBLE_STCP
#endif
/* This macro will check sensible part of the CASTOR HSM union */
#define HAVE_SENSIBLE_STCP(stcp) (((stcp)->u1.h.xfile[0] != '\0') && ((stcp)->u1.h.server[0] != '\0') && ((stcp)->u1.h.fileid != 0) && ((stcp)->u1.h.fileclass != 0))

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

void
procupdreq(req_data, clienthost)
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	int blksize = -1;
	int c, i, n;
	struct stgcat_entry *cur;
	char *dp;
	char dsksrvr[CA_MAXHOSTNAMELEN + 1];
	char *dvn;
	int errflg = 0;
	char *fid = NULL;
	int found, index_found;
	char *fseq = NULL;
	gid_t gid;
	char *ifce;
	int j;
	int key;
	int lrecl = -1;
	int nargs;
	char *p_cmd;
	char *p_stat;
	char *p, *q;
	char prevfseq[CA_MAXFSEQLEN + 1];
	char *rbp;
	int rc = -1;
	char *recfm = NULL;
	static char s_cmd[5][9] = {"", "stagein", "stageout", "stagewrt", "stageput"};
	int size = 0;
	struct stat st;
	struct stgcat_entry *stcp;
	int subreqid;
	int transfer_time = 0;
	uid_t uid;
	static char unknown[2] = {"*"};
	int upd_reqid;
	int upd_rpfd;
	char *user;
	int waiting_time = 0;
	struct waitf *wfp;
	struct waitq *wqp;
	int Zflag = 0;
	int callback_index = -1;
	int found_wfp = 0;
	size_t u1h_sizeof;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

	u1h_sizeof = sizeof(struct stgcat_entry) - offsetof(struct stgcat_entry,u1.h.xfile);

	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost, reqid, STAGEUPDC, 0, 0, NULL, "");
#endif

	dvn = unknown;
	ifce = unknown;
	upd_reqid = reqid;
	upd_rpfd = rpfd;
	Coptind = 1;
	Copterr = 0;
	while ((c = Cgetopt (nargs, argv, "b:D:F:f:h:i:I:L:q:R:s:T:U:W:Z:")) != -1) {
		switch (c) {
		case 'b':
			blksize = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-b");
				errflg++;
			}
			break;
		case 'D':
			dvn = Coptarg;
			break;
		case 'F':
			recfm = Coptarg;
			break;
		case 'f':
			fid = Coptarg;
			break;
		case 'h':
			break;
		case 'i':
			if ((callback_index = atoi(Coptarg)) < 0) {
				sendrep (rpfd, MSG_ERR, STG06, "-i");
				errflg++;
			}
			break;
		case 'I':
			ifce = Coptarg;
			break;
		case 'L':
			lrecl = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-L");
				errflg++;
			}
			break;
		case 'q':
			fseq = Coptarg;
			break;
		case 'R':
			rc = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-R");
				errflg++;
			}
			break;
		case 's':
			size = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-s");
				errflg++;
			}
			break;
		case 'T':
			transfer_time = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-T");
				errflg++;
			}
			break;
		case 'W':
			waiting_time = strtol (Coptarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-W");
				errflg++;
			}
			break;
		case 'Z':
			Zflag++;
			if ((p = strtok (Coptarg, ".")) != NULL) {
				reqid = strtol (p, &dp, 10);
				if ((p = strtok (NULL, "@")) != NULL) {
					key = strtol (p, &dp, 10);
				}
			}
			break;
		}
	}
	if (Zflag && nargs > Coptind) {
		sendrep (rpfd, MSG_ERR, STG16);
		errflg++;
	}
	if (errflg) {
		c = USERR;
		goto reply;
	}
	if (nargs > Coptind) {
		for  (i = Coptind; i < nargs; i++) {
			if ((c = upd_stageout(STAGEUPDC, argv[i], &subreqid, 1, NULL)) != 0) {
				if (c != CLEARED) {
					goto reply;
				} else {
					/* This is not formally an error to do an updc on a zero-length file */
					c = 0;
				}
			}
		}
		goto reply;
	}
	found = 0;
	wqp = waitqp;
	while (wqp) {
		if (wqp->reqid == reqid &&
			wqp->key == key &&
			wqp->rtcp_uid == uid &&
			wqp->rtcp_gid == gid
		) {
			found = 1;
			break;
		}
		wqp = wqp->next;
	}
	if (! found) {
		sendrep (rpfd, MSG_ERR, STG22);
		/* We log information because this might be a bug */
		wqp = waitqp;
		while (wqp) {
			stglogit(func,"Compared (reqid,key,uid,gid)=(%d,%d,%d,%d) with wqp's (reqid,key,rtcp_uid,rtcp_gid)=(%d,%d,%d,%d)\n",
					(int) reqid,
					(int) key,
					(int) uid,
					(int) gid,
					(int) wqp->reqid,
					(int) wqp->key,
					(int) wqp->rtcp_uid,
					(int) wqp->rtcp_gid);
			wqp = wqp->next;
		}
		c = USERR;
		goto reply;
	}
	c = 0;
	if (callback_index < 0) {
		/* We got no indication of index - this is a sequential callback */
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			if (! wfp->waiting_on_req) {
				found_wfp = 1;
				break;
			}
		}
	} else {
		if (callback_index >= wqp->save_nbsubreqid) {
			/* False value */
			sendrep (rpfd, MSG_ERR, STG22);
			c = USERR;
			goto reply;
		}
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
#ifdef STAGER_DEBUG
			sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] Comparing wqp->save_subreqid[%d]=%d with wfp->subreqid=%d [i=%d]\n", callback_index, wqp->save_subreqid[callback_index], wfp->subreqid, i);
#endif
			if (wfp->subreqid == wqp->save_subreqid[callback_index] && ! wfp->waiting_on_req) {
				found_wfp = 1;
				break;
			}
		}
	}
	if (! found_wfp) {
		/* False value */
		sendrep (rpfd, MSG_ERR, STG22);
		c = USERR;
		goto reply;
	}
	subreqid = wfp->subreqid;

	found = index_found = 0;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if (stcp->reqid == subreqid) {
			found = 1;
			break;
		}
		index_found++;
	}
	if (! found) {
		sendrep (rpfd, MSG_ERR, STG22);
		c = USERR;
		goto reply;
	}
#ifdef STAGER_DEBUG
	sendrep(rpfd, MSG_ERR, "[DEBUG] procupd : rc=%d, concat_off_fseq=%d, i=%d, nbdskf=%d\n", rc, wqp->concat_off_fseq, i, wqp->nbdskf);
#endif

	if ((IS_RC_OK(rc) || IS_RC_WARNING(rc)) &&
		wqp->concat_off_fseq > 0 && i == (wqp->nbdskf - 1) && stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-') {
		struct stgcat_entry *stcp_ok;
		int save_nextreqid;
		struct waitf *wfp_ok;

		/* ==> Recall that "i" is the index of the found wf... <== */

		/* If this is a tape file copy callback from concat_off and for the last entry */
		/* and IF this last entry has a trailing, we prepare the next round */

		/* Note that is is possible that the last does not have a trailing because this */
		/* request was, for example, waiting on another one that failed because fseq did */
		/* not exist. In such a case the entry with the trailing '-' is removed from the */
		/* waitf of this waitq. */

		/* We are extending the number of files in this waiting member of the waitq */

		if ((wfp_ok = add2wf(wqp)) == NULL) {
			c = SYERR;
			goto reply;
		}
		wfp = &(wqp->wf[i]); /* We restore the correct found wf (mem might have been realloced) */
		stcp_ok = newreq();
		/* The memory may have been realloced, so we follow this eventual reallocation */
		/* by reassigning stcp pointer using the found index */
		stcp = &(stcs[index_found]);
		save_nextreqid = nextreqid();
		memcpy(stcp_ok,stcp,sizeof(struct stgcat_entry));
		stcp_ok->reqid = save_nextreqid;
		/* Current stcp is "<X>-", new one is a "<X+1>- -c off" */
		sprintf(stcp_ok->u1.t.fseq,"%d",atoi(fseq) + 1);
		stcp_ok->filler[0] = 'c';
		strcat(stcp_ok->u1.t.fseq,"-");
		/* And it is a deffered allocation */
		stcp_ok->ipath[0] = '\0';
		wfp_ok = &(wqp->wf[wqp->nbdskf - 1]);
		memset((char *) wfp_ok,0,sizeof(struct waitf));
		wfp_ok->subreqid = stcp_ok->reqid;
		/* Current stcp become "<X>" */
		sprintf(stcp->u1.t.fseq,"%d",atoi(fseq));
		/* If it has the filler[0] == 'c' (it has to have it!), we remove this hidden flag */
		if (stcp->filler[0] != '\0') stcp->filler[0] = '\0';
#ifdef USECDB
		if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
			stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
		}
		if (stgdb_ins_stgcat(&dbfd,stcp_ok) != 0) {
			stglogit(func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		savereqs ();
		stglogit(func, "STG33 - Extended wfp for eventual tape_fseq %s\n",stcp_ok->u1.t.fseq);

		/* We extend the others that are also waiting on wfp */
		if (add2otherwf(wqp,fseq,wfp,wfp_ok) != 0) {
			c = SYERR;
			goto reply;
		}
		/* The memory may have been realloced, so we follow this eventual reallocation */
		/* by reassigning stcp pointer using the found index */
		stcp = &(stcs[index_found]);
	}
	if ( rc < 0) {	/* -R not specified, i.e. tape mounted and positionned */
		int has_been_modified = 0;

		/* deferred allocation (STAGEIN) */
		if (stcp->ipath[0] == '\0') {
			int has_trailing = 0;
			if (wqp->concat_off_fseq > 0 && i == (wqp->nbdskf - 1)) {
				/* We remove the trailing '-' */
				/* It can happen that the tppos on the first file is for a waitf */
				/* that does not have a trailing when the request is splitted because */
				/* of previous one(s) on the same VID */
				if (stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-') {
					has_trailing = 1;
					stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] = '\0';
				}
			}
			c = build_ipath (NULL, stcp, wqp->pool_user);
			if (has_trailing != 0) {
				/* We restore the trailing '-' */
				strcat(stcp->u1.t.fseq,"-");
			}
			if (c > 0) {
				sendrep (rpfd, MSG_ERR, "STG02 - build_ipath error\n");
				goto reply;
            }
			if (c < 0) {
				sendrep (rpfd, MSG_ERR, "STG02 - build_ipath reaching out of space\n");
				wqp->clnreq_reqid = upd_reqid;
				wqp->clnreq_rpfd = rpfd;
				stcp->status |= WAITING_SPC;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				strcpy (wqp->waiting_pool, stcp->poolname);
				if ((c = cleanpool (stcp->poolname)) != 0) goto reply;
				return;
			}
			has_been_modified = 1;
		}
		if (fseq && stcp->t_or_d == 't') {
			if (*stcp->poolname &&
					stcp->u1.t.fseq[0] == 'u') {
				p = strrchr (stcp->ipath, '/') + 1;
				sprintf (p, "%s.%s.%s",
								 stcp->u1.t.vid[0], fseq, stcp->u1.t.lbl);
				has_been_modified = 1;
				savereqs ();
			}
		}
		if (has_been_modified != 0) {
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
		}
		sendrep (rpfd, MSG_OUT, "%s", stcp->ipath);
		if (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)) {
			if (wqp->last_rwcounterfs_vs_R != LAST_RWCOUNTERSFS_TPPOS) {
				/* Should be either 0 (from initialization) or LAST_RWCOUNTERSFS_FILCP */
				rwcountersfs(stcp->poolname, stcp->ipath, STAGEWRT, STAGEWRT); /* Could be STAGEPUT */
				wqp->last_rwcounterfs_vs_R = LAST_RWCOUNTERSFS_TPPOS;
			}
		}
		goto reply;
	}
	if (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)) {
		if (wqp->last_rwcounterfs_vs_R == LAST_RWCOUNTERSFS_TPPOS) {
			/* Should be LAST_RWCOUNTERSFS_TPPOS */
			rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, STAGEUPDC);
			wqp->last_rwcounterfs_vs_R = LAST_RWCOUNTERSFS_FILCP;
		}
	}
	if (stcp->ipath[0] != '\0') {
		if ((p = strchr (stcp->ipath, ':')) != NULL) {
			q = stcp->ipath;
		} else {
			q = strchr (stcp->ipath + 1, '/') + 1;
			p = strchr (q, '/');
		}
		strncpy (dsksrvr, q, p - q);
		dsksrvr[p - q] = '\0';
		if (stcp->status == STAGEIN) {
			char tmpbuf[21];

			if (rfio_stat (stcp->ipath, &st) == 0) {
				stcp->actual_size = (u_signed64) st.st_size;
				wfp->nb_segments++;
				wfp->size_yet_recalled += (u_signed64) st.st_size;
				if (wfp->size_yet_recalled >= wfp->size_to_recall) {
					/* Final message - Note that wfp->size_to_recall can be zero */
					/* (always the case it it is not a recall of CASTOR file) */
					if (wfp->nb_segments == 1) {
						/* Done in one segment */
						stglogit(func, STG97,
								dsksrvr, strrchr (stcp->ipath, '/')+1,
								stcp->user, stcp->group,
								clienthost, dvn, ifce,
								u64tostr((u_signed64) size, tmpbuf, 0),
								waiting_time, transfer_time, rc);
					} else {
						/* Done in more than one segment */
						stglogit(func, STG108,
							dsksrvr, strrchr (stcp->ipath, '/')+1,
							wfp->nb_segments, stcp->user, stcp->group,
							u64tostr(stcp->actual_size, tmpbuf, 0),
							size,
							rc);
					}
				} else if (wfp->size_to_recall > 0) {
					/* Not everything has yet been recalled */
					/* Done for this segment */
					stglogit(func, STG107,
						dsksrvr, strrchr (stcp->ipath, '/')+1,
						wfp->nb_segments, stcp->user, stcp->group,
						clienthost, dvn, ifce,
						u64tostr((u_signed64) size, tmpbuf, 0),
						waiting_time, transfer_time, rc);
				}
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
			} else {
				stglogit (func, STG02, stcp->ipath, "rfio_stat", rfio_serror());
			}
		} else {
			char tmpbuf[21];

			stglogit(func, STG97,
					dsksrvr, strrchr (stcp->ipath, '/')+1,
					stcp->user, stcp->group,
					clienthost, dvn, ifce,
					u64tostr((u_signed64) size, tmpbuf, 0),
					waiting_time, transfer_time, rc);
		}
	}
#if SACCT
	stageacct (STGFILS, wqp->req_uid, wqp->req_gid, wqp->clienthost,
						 wqp->reqid, wqp->req_type, wqp->nretry, rc, stcp, clienthost);
#endif
	if (rc == 211) {	/* no more file on tape */
		/* flag previous file as last file on tape */
		cur = stcp;
		sprintf (prevfseq, "%d", atoi(stcp->u1.t.fseq) - 1);
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (stcp->t_or_d != 't') continue;
			if (strcmp (cur->poolname, stcp->poolname)) continue;
			if ((stcp->status & 0xF0) != STAGED) continue;
			for (j = 0; j < MAXVSN; j++)
				if (strcmp (cur->u1.t.vid[j], stcp->u1.t.vid[j])) break;
			if (j < MAXVSN) continue;
			for (j = 0; j < MAXVSN; j++)
				if (strcmp (cur->u1.t.vsn[j], stcp->u1.t.vsn[j])) break;
			if (j < MAXVSN) continue;
			if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) continue;
			if (strcmp (prevfseq, stcp->u1.t.fseq)) continue;
			stcp->status |= LAST_TPFILE;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (wqp->api_out) sendrep(rpfd, API_STCP_OUT, stcp);
			break;
		}
		wqp->nb_subreqs = i;
		for ( ; i < wqp->nbdskf; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
			if (wfp->waiting_on_req) {
				delreq (stcp,0);
				continue;
			}
			if (delfile (stcp, 1, 0, 1, "no more file", uid, gid, 0, 0) < 0)
				sendrep (wqp->rpfd, MSG_ERR, STG02, stcp->ipath,
								 "rfio_unlink", rfio_serror());
			check_coff_waiting_on_req (wfp->subreqid, LAST_TPFILE);
			check_waiting_on_req (wfp->subreqid, STG_FAILED);
		}
		wqp->nbdskf = wqp->nb_subreqs;
		goto reply;
	}
	if (wfp->size_yet_recalled >= wfp->size_to_recall) {
		/* Note : wfp->size_to_recall is zero if it not a recall of CASTOR file(s) */
		/* (always the case it it is not a recall of CASTOR file) */
		p_cmd = s_cmd[stcp->status & 0xF];
		if (IS_RC_OK(rc))
			p_stat = "succeeded";
		else if (IS_RC_WARNING(rc))
			p_stat = "warning";
		else
			p_stat = "failed";
		if (stcp->t_or_d == 't')
			sendrep (wqp->rpfd, RTCOPY_OUT, STG41, p_cmd, p_stat,
							 fseq ? fseq : stcp->u1.t.fseq, stcp->u1.t.vid[0], rc);
		else if (stcp->t_or_d == 'd')
			sendrep (wqp->rpfd, RTCOPY_OUT, STG42, p_cmd, p_stat, stcp->u1.d.xfile, rc);
		else if (stcp->t_or_d == 'm')
			sendrep (wqp->rpfd, RTCOPY_OUT, STG42, p_cmd, p_stat, stcp->u1.m.xfile, rc);
		else if (stcp->t_or_d == 'h')
			sendrep (wqp->rpfd, RTCOPY_OUT, STG42, p_cmd, p_stat, stcp->u1.h.xfile, rc);
	}
	if (((rc != 0) && (rc != LIMBYSZ) &&
			 (rc != BLKSKPD) && (rc != TPE_LSZ) && (rc != MNYPARI)) ||
			((rc == MNYPARI) && ((stcp->u1.t.E_Tflags & KEEPFILE) == 0))) {
		wqp->status = rc;
		if (rc != ENOSPC) {
			goto reply;
		}
		if ((*stcp->poolname == '\0') ||
				ISSTAGEWRT(stcp) ||
				ISSTAGEPUT(stcp) ||
				wqp->nb_clnreq++ > MAXRETRY) {
			c = ENOSPC;
			goto reply;
		}
		if (delfile (stcp, 1, 0, 0, "nospace retry", uid, gid, 0, 0) < 0)
			sendrep (wqp->rpfd, MSG_ERR, STG02, stcp->ipath,
							 "rfio_unlink", rfio_serror());
		wqp->status = 0;
		wqp->clnreq_reqid = upd_reqid;
		wqp->clnreq_rpfd = rpfd;
		stcp->status |= WAITING_SPC;
#ifdef USECDB
		if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
			stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		strcpy (wqp->waiting_pool, stcp->poolname);
		if ((c = cleanpool (stcp->poolname)) != 0) goto reply;
		return;
	}
	if ((stcp->t_or_d != 'm') && (stcp->t_or_d != 'h')) {
		int has_been_updated = 0;

		if (blksize > 0) {
			stcp->blksize = blksize;
			has_been_updated = 1;
		}
		if (lrecl > 0) {
			stcp->lrecl = lrecl;
			has_been_updated = 1;
		}
		if (recfm) {
			strncpy (stcp->recfm, recfm, 3);
			has_been_updated = 1;
		}
		if (stcp->recfm[0] == 'U') {
			stcp->lrecl = 0;
			has_been_updated = 1;
		} else if (stcp->lrecl == 0) {
			stcp->lrecl = stcp->blksize;
			has_been_updated = 1;
		}
		if (fid) {
			strcpy (stcp->u1.t.fid, fid);
			has_been_updated = 1;
		}
		if (fseq &&
				(stcp->u1.t.fseq[0] == 'u' || stcp->u1.t.fseq[0] == 'n')) {
			strcpy (stcp->u1.t.fseq, fseq);
			has_been_updated = 1;
		}
		if (has_been_updated != 0) {
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
		}
	}
	if ((wfp->size_yet_recalled >= wfp->size_to_recall) ||
		((rc == 0) && (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)))) {
		/* Note : wfp->size_to_recall is zero if it not a recall of CASTOR file(s) */
		/* (always the case it it is not a recall of CASTOR file) */
		wqp->nb_subreqs--;
		wqp->nbdskf--;
		rpfd = wqp->rpfd;
		if (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)) {
			n = 1;
			if (wqp->nb_subreqs == 0 && wqp->nbdskf > 0) {
				n += wqp->nbdskf;
				wqp->nbdskf = 0;
			}
			for (i = 0; i < n; i++, wfp++) {
				int ifileclass;

				if (stcp->t_or_d == 'h')
					ifileclass = upd_fileclass(NULL,stcp);
				else
					ifileclass = -1;
				for (stcp = stcs; stcp < stce; stcp++)
					if (stcp->reqid == wfp->subreqid) break;
				if (wqp->copytape)
					sendinfo2cptape (rpfd, stcp);
				if (! stcp->keep && stcp->nbaccesses <= 1) {
					/* No -K option and only one access */
					struct stgcat_entry *stcp_search, *stcp_found;
					if (ISSTAGEWRT(stcp) &&
						(stcp->poolname[0] == '\0')) {
						if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
							update_migpool(&stcp,-1,0);
						}
						delreq (stcp,0);
					} else {
						struct stgcat_entry *save_stcp = stcp;
						int save_status = stcp->status;
						if ((save_stcp->t_or_d == 'h') && (ISSTAGEWRT(save_stcp) || ISSTAGEPUT(save_stcp)) && HAVE_SENSIBLE_STCP(save_stcp)) {
							/* If this entry is a CASTOR file and has tppool[0] != '\0' this must/might be */
							/* a corresponding stcp entry with the stageout matching STAGEOUT|CAN_BE_MIGR|BEING_MIGR */
							stcp_found = NULL;
							for (stcp_search = stcs; stcp_search < stce; stcp_search++) {
								if (stcp_search->reqid == 0) break;
								if (! ISCASTORBEINGMIG(stcp_search)) continue;
								if (memcmp(stcp_search->u1.h.xfile,save_stcp->u1.h.xfile,u1h_sizeof) == 0) {
									if (stcp_found == NULL) {
										stcp_found = stcp_search;
										break;
									}
								}
							}
							if (stcp_found == NULL) {
								/* This is acceptable only if the request is an explicit STAGEPUT */
								/*
								if (stcp->status != STAGEPUT) {
									sendrep (rpfd, MSG_ERR, STG22);
									continue;
								} else {
									stcp_found = stcp;
								}
								*/
								stcp_found = stcp;
							}
							if ((stcp_found->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
								update_migpool(&stcp_found,-1,0);
								stcp_found->status &= ~CAN_BE_MIGR;
							}
							stcp_found->u1.h.tppool[0] = '\0'; /* We reset the poolname */
							stcp_found->status |= STAGED;
							update_hsm_a_time(stcp_found);
#ifdef USECDB
							if (stgdb_upd_stgcat(&dbfd,stcp_found) != 0) {
								stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
							}
#endif
							if (wqp->api_out) sendrep(rpfd, API_STCP_OUT, stcp_found);
						} else {
							stcp_found = stcp;
							goto delete_entry;
						}
						if (stcp_found != NULL) {
							if ((stcp_found != save_stcp) || ((stcp_found == save_stcp) && (save_status & STAGEPUT) == STAGEPUT)) {
								int continue_flag = 0;
								/* The found element is not the STAGEWRT itself, or is the STAGEPUT */
								/* this means it was a migration request */
								if (ifileclass >= 0) {
									time_t thistime = time(NULL);
									int thisretenp = retenp_on_disk(ifileclass);

									/* Fileclass's retention period on disk larger than current lifetime ? */
									if ((thisretenp == INFINITE_LIFETIME) || (thisretenp == AS_LONG_AS_POSSIBLE)) {
										stglogit(func, STG142, stcp_found->u1.h.xfile, (thisretenp == INFINITE_LIFETIME) ? "INFINITE_LIFETIME" : "AS_LONG_AS_POSSIBLE");
										continue_flag = 1;
									} else if (((int) (thistime - stcp_found->a_time)) < thisretenp) {
										stglogit(func, STG131, stcp_found->u1.h.xfile, thisretenp, (thistime - stcp_found->a_time));
										continue_flag = 1;
									} else {
										stglogit (func, STG133, stcp_found->u1.h.xfile, fileclasses[ifileclass].Cnsfileclass.name, stcp_found->u1.h.server, fileclasses[ifileclass].Cnsfileclass.classid, thisretenp, (int) (thistime - stcp_found->a_time));
									}
								}
								if ((save_stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
									/* save_stcp is not a real valid pointer but we don't care because */
									/* this can change only if update_migpool() calls newreq(), e.g. when */
									/* the second argument is 1 */
									update_migpool(&save_stcp,-1,0);
								}
								if (! ((stcp_found == save_stcp) && (save_status & STAGEPUT) == STAGEPUT)) delreq(save_stcp,0);
								if (continue_flag != 0) {
									continue;
								} else {
									goto delete_entry;
								}
							} else {
							delete_entry:
								/* We can delete this entry */
								if (delfile (stcp_found, 0, 1, 1, ((save_status & 0xF) == STAGEWRT) ? "stagewrt ok" : "stageput ok", uid, gid, 0, ((save_status & 0xF) == STAGEWRT) ? 1 : 0) < 0) {
									sendrep (rpfd, MSG_ERR, STG02, stcp_found->ipath,
												 "rfio_unlink", rfio_serror());
								}
							}
						}
					}
				} else {
					if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
						update_migpool(&stcp,-1,0);
						stcp->status &= ~CAN_BE_MIGR;
					}
					stcp->status |= STAGED;
					update_hsm_a_time(stcp);
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					if (wqp->api_out) sendrep(rpfd, API_STCP_OUT, stcp);
				}
			}
			i = 0;		/* reset these variables changed by the above loop */
			wfp = wqp->wf;	/* and needed in the loop below */
		} else {	/* STAGEIN */
			stcp->status |= STAGED;
			if (rc == BLKSKPD || rc == TPE_LSZ || rc == MNYPARI)
				stcp->status |= STAGED_TPE;
			if (rc == LIMBYSZ || rc == TPE_LSZ)
				stcp->status |= STAGED_LSZ;
			stcp->a_time = time(NULL);
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (wqp->api_out) sendrep(rpfd, API_STCP_OUT, stcp);
			if (wqp->copytape)
				sendinfo2cptape (rpfd, stcp);
			if (*(wfp->upath) && strcmp (stcp->ipath, wfp->upath))
				create_link (stcp, wfp->upath);
			if (wqp->Upluspath && *((wfp+1)->upath) &&
					strcmp (stcp->ipath, (wfp+1)->upath))
				create_link (stcp, (wfp+1)->upath);
			updfreespace (stcp->poolname, stcp->ipath,
										(signed64) (((signed64) stcp->size * (signed64) ONE_MB) - (signed64) stcp->actual_size));
			check_waiting_on_req (subreqid, STAGED);
		}
		savereqs();
		for ( ; i < wqp->nbdskf; i++, wfp++) {
			memcpy(wfp,wfp+1,sizeof(struct waitf));
		}
	}
	if (rc == MNYPARI) wqp->status = rc;
#ifdef STAGER_DEBUG
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : Waitq->wf : \n");
		for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				char tmpbuf1[21];
				char tmpbuf2[21];

				if (wfp->subreqid == stcp->reqid) {
					switch (stcp->t_or_d) {
					case 't':
						sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : No %3d    : VID.FSEQ=%s.%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.t.vid[0], stcp->u1.t.fseq,wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'd':
					case 'a':
						sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : No %3d    : u1.d.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.d.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'm':
						sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : No %3d    : u1.m.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.m.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'h':
						sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : No %3d    : u1.h.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s, size_to_recall=%s, size_yet_recalled=%s\n",i,stcp->u1.h.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath, u64tostr((u_signed64) wfp->size_to_recall, tmpbuf1, 0), u64tostr((u_signed64) wfp->size_yet_recalled, tmpbuf2, 0));
						break;
					default:
						sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : No %3d    : <unknown to_or_d=%c>\n",i,stcp->t_or_d);
						break;
					}
					break;
				}
			}
		}
	}
#endif
 reply:
	free (argv);
	reqid = upd_reqid;
	rpfd = upd_rpfd;
	sendrep (rpfd, STAGERC, STAGEUPDC, c);
}

void update_hsm_a_time(stcp)
	struct stgcat_entry *stcp;
{
	struct stat statbuf;
	struct Cns_fileid Cnsfileid;
	struct Cns_filestat Cstatbuf;

	switch (stcp->t_or_d) {
	case 'm':
		/* HPSS */
		if (rfio_stat(stcp->u1.m.xfile, &statbuf) == 0) {
			stcp->a_time = statbuf.st_mtime;
		} else {
			stglogit (func, STG02, stcp->u1.m.xfile, "rfio_stat", rfio_serror());
		}
		break;
	case 'h':
		/* CASTOR */
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
		if (Cns_statx(stcp->u1.h.xfile, &Cnsfileid, &Cstatbuf) == 0) {
			stcp->a_time = Cstatbuf.mtime;
		} else {
			stglogit (func, STG02, stcp->u1.h.xfile, "Cns_statx", sstrerror(serrno));
		}
		break;
	default:
		break;
	}
}





