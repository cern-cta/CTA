/*
 * $Id: procupd.c,v 1.34 2000/10/16 11:11:16 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procupd.c,v $ $Revision: 1.34 $ $Date: 2000/10/16 11:11:16 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

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

void procupdreq _PROTO((char *, char *));

extern char *optarg;
extern int optind;
extern char *rfio_serror();
extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct waitq *waitqp;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *));
extern struct waitf *add2wf _PROTO((struct waitq *));
extern int add2otherwf _PROTO((struct waitq *, char *, struct waitf *, struct waitf *));
extern int extend_waitf _PROTO((struct waitf *, struct waitf *));
extern int check_waiting_on_req _PROTO((int, int));
extern int check_coff_waiting_on_req _PROTO((int, int));
extern struct stgcat_entry *newreq _PROTO(());
extern void update_migpool _PROTO((struct stgcat_entry *, int));
extern int updfreespace _PROTO((char *, char *, int));

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
	extern struct passwd *stpasswd;             /* Generic uid/gid stage:st */

	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
						 reqid, STAGEUPDC, 0, 0, NULL, "");
#endif

	dvn = unknown;
	ifce = unknown;
	upd_reqid = reqid;
	upd_rpfd = rpfd;
#ifdef linux
	optind = 0;
#else
	optind = 1;
#endif
	while ((c = getopt (nargs, argv, "b:D:F:f:h:I:L:q:R:s:T:U:W:Z:")) != EOF) {
		switch (c) {
		case 'b':
			blksize = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-b");
				errflg++;
			}
			break;
		case 'D':
			dvn = optarg;
			break;
		case 'F':
			recfm = optarg;
			break;
		case 'f':
			fid = optarg;
			break;
		case 'h':
			break;
		case 'I':
			ifce = optarg;
			break;
		case 'L':
			lrecl = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-L");
				errflg++;
			}
			break;
		case 'q':
			fseq = optarg;
			break;
		case 'R':
			rc = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-R");
				errflg++;
			}
			break;
		case 's':
			size = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-s");
				errflg++;
			}
			break;
		case 'T':
			transfer_time = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-T");
				errflg++;
			}
			break;
		case 'W':
			waiting_time = strtol (optarg, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-W");
				errflg++;
			}
			break;
		case 'Z':
			Zflag++;
			if ((p = strtok (optarg, ".")) != NULL) {
				reqid = strtol (p, &dp, 10);
				if ((p = strtok (NULL, "@")) != NULL) {
					key = strtol (p, &dp, 10);
				}
			}
			break;
		}
	}
	if (Zflag && nargs > optind) {
		sendrep (rpfd, MSG_ERR, STG16);
		errflg++;
	}
	if (errflg) {
		c = USERR;
		goto reply;
	}
	if (nargs > optind) {
		for  (i = optind; i < nargs; i++)
			if (c = upd_stageout (STAGEUPDC, argv[i], &subreqid, 1, NULL))
				goto reply;
		goto reply;
	}
	found = 0;
	wqp = waitqp;
	while (wqp) {
		if (wqp->reqid == reqid &&
			wqp->key == key &&
			(wqp->StageIDflag != 0 ?
				(stpasswd->pw_uid == uid && stpasswd->pw_gid == gid) :
				(wqp->uid         == uid && wqp->gid         == gid)
			)
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
			stglogit(func,"Compared (reqid,key,uid,gid)=(%d,%d,%d,%d) with wqp's (reqid,key,uid,gid)=(%d,%d,%d,%d)\n",
					(int) reqid,
					(int) key,
					(int) uid,
					(int) gid,
					(int) wqp->reqid,
					(int) wqp->key,
					(int) (wqp->StageIDflag != 0 ? stpasswd->pw_uid : wqp->uid),
					(int) (wqp->StageIDflag != 0 ? stpasswd->pw_gid : wqp->gid)
			);
			wqp = wqp->next;
		}
		c = USERR;
		goto reply;
	}
	c = 0;
	for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++)
		if (! wfp->waiting_on_req) break;
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

	if (rc == 0 && wqp->concat_off_fseq > 0 && i == (wqp->nbdskf - 1) && stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-') {
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

		/* deferred allocation */
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
				if (c = cleanpool (stcp->poolname)) goto reply;
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
		sendrep (rpfd, MSG_OUT, stcp->ipath);
		goto reply;
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
		stglogit (func, STG97, dsksrvr, strrchr (stcp->ipath, '/')+1,
						stcp->user, stcp->group,
						clienthost, dvn, ifce, size, waiting_time, transfer_time, rc);
		if (stcp->status == STAGEIN && rfio_stat (stcp->ipath, &st) == 0) {
			stcp->actual_size = st.st_size;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
		}
	}
#if SACCT
	stageacct (STGFILS, wqp->uid, wqp->gid, wqp->clienthost,
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
			if (delfile (stcp, 1, 0, 1, "no more file", uid, gid, 0) < 0)
				sendrep (wqp->rpfd, MSG_ERR, STG02, stcp->ipath,
								 "rfio_unlink", rfio_serror());
			check_coff_waiting_on_req (wfp->subreqid, LAST_TPFILE);
			check_waiting_on_req (wfp->subreqid, STG_FAILED);
		}
		wqp->nbdskf = wqp->nb_subreqs;
		goto reply;
	}
	p_cmd = s_cmd[stcp->status & 0xF];
	if (rc == 0)
		p_stat = "succeeded";
	else if (rc == LIMBYSZ || rc == BLKSKPD || rc == TPE_LSZ ||
					 (rc == MNYPARI && (stcp->u1.t.E_Tflags & KEEPFILE)))
		p_stat = "warning";
	else
		p_stat = "failed";
	if (stcp->t_or_d == 't')
		sendrep (wqp->rpfd, MSG_ERR, STG41, p_cmd, p_stat,
						 fseq ? fseq : stcp->u1.t.fseq, stcp->u1.t.vid[0], rc);
	else if (stcp->t_or_d == 'd')
		sendrep (wqp->rpfd, MSG_ERR, STG42, p_cmd, p_stat, stcp->u1.d.xfile, rc);
	else if (stcp->t_or_d == 'm')
		sendrep (wqp->rpfd, MSG_ERR, STG42, p_cmd, p_stat, stcp->u1.m.xfile, rc);
	else if (stcp->t_or_d == 'h')
		sendrep (wqp->rpfd, MSG_ERR, STG42, p_cmd, p_stat, stcp->u1.h.xfile, rc);
	if ((rc != 0 && rc != LIMBYSZ &&
			 rc != BLKSKPD && rc != TPE_LSZ && rc != MNYPARI) ||
			(rc == MNYPARI && (stcp->u1.t.E_Tflags & KEEPFILE) == 0)) {
		wqp->status = rc;
		if (rc != ENOSPC) goto reply;
		if (*stcp->poolname == '\0' ||
				stcp->status == STAGEWRT || ((stcp->status & 0xF) == STAGEPUT) ||
				wqp->nb_clnreq++ > MAXRETRY) {
			c = ENOSPC;
			goto reply;
		}
		if (delfile (stcp, 1, 0, 0, "nospace retry", uid, gid, 0) < 0)
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
		if (c = cleanpool (stcp->poolname)) goto reply;
		return;
	}
	if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') {
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
	wqp->nb_subreqs--;
	wqp->nbdskf--;
	rpfd = wqp->rpfd;
	if (stcp->status == STAGEWRT || ((stcp->status & 0xF) == STAGEPUT)) {
		n = 1;
		if (wqp->nb_subreqs == 0 && wqp->nbdskf > 0) {
			n += wqp->nbdskf;
			wqp->nbdskf = 0;
		}
		for (i = 0; i < n; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++)
				if (stcp->reqid == wfp->subreqid) break;
			if (wqp->copytape)
				sendinfo2cptape (rpfd, stcp);
			if (! stcp->keep && stcp->nbaccesses <= 1) {
				if (stcp->status == STAGEWRT && stcp->poolname[0] == '\0')
					delreq (stcp,0);
				else if (delfile (stcp, 0, 1, 1, stcp->status == STAGEWRT ? "stagewrt ok" : (((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) ? "migration stageput ok" : "stageput ok"), uid, gid, 0) < 0) {
					sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
									 "rfio_unlink", rfio_serror());
					if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) stcp->status &= ~CAN_BE_MIGR;
					stcp->status |= STAGED;
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				}
			} else {
				if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
					stcp->status &= ~CAN_BE_MIGR;
					update_migpool(stcp,-1);
				}
				stcp->status |= STAGED;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
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
#ifdef USECDB
		if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
			stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		if (wqp->copytape)
			sendinfo2cptape (rpfd, stcp);
		if (*(wfp->upath) && strcmp (stcp->ipath, wfp->upath))
			create_link (stcp, wfp->upath);
		if (wqp->Upluspath &&
				strcmp (stcp->ipath, (wfp+1)->upath))
			create_link (stcp, (wfp+1)->upath);
		updfreespace (stcp->poolname, stcp->ipath,
									stcp->size * ONE_MB - (int)stcp->actual_size);
		check_waiting_on_req (subreqid, STAGED);
	}
	savereqs  ();
	for ( ; i < wqp->nbdskf; i++, wfp++) {
		wfp->subreqid = (wfp+1)->subreqid;
		wfp->waiting_on_req = (wfp+1)->waiting_on_req;
		strcpy (wfp->upath, (wfp+1)->upath);
	}
	if (rc == MNYPARI) wqp->status = rc;
#ifdef STAGER_DEBUG
	for (wqp = waitqp; wqp; wqp = wqp->next) {
      sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : Waitq->wf : \n");
      for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
        for (stcp = stcs; stcp < stce; stcp++) {
          if (wfp->subreqid == stcp->reqid) {
            sendrep(wqp->rpfd, MSG_ERR, "[DEBUG] procupd : No %3d    : VID.FSEQ=%s.%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.t.vid[0], stcp->u1.t.fseq,wfp->subreqid, wfp->waiting_on_req, wfp->upath);
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
