/*
 * procupd.c,v 1.121 2002/10/30 16:26:10 jdurand Exp
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)procupd.c,v 1.121 2002/10/30 16:26:10 CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <stdlib.h>
#include <errno.h>
#include <stdio.h>
#include <sys/types.h>
#ifndef _WIN32
#include <unistd.h>
#endif
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
#define local_unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#include "stage_api.h"
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

void procupdreq _PROTO((int, int, char *, char *));
int update_hsm_a_time _PROTO((struct stgcat_entry *));

extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgpath_entry *stpe;	/* end of stage path catalog */
extern struct stgpath_entry *stps;	/* start of stage path catalog */
extern struct waitq *waitqp;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
extern struct fileclass *fileclasses;
extern u_signed64 stage_uniqueid;

extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *, int, int));
extern struct waitf *add2wf _PROTO((struct waitq *));
extern int add2otherwf _PROTO((struct waitq *, char *, struct waitf *, struct waitf *));
extern int extend_waitf _PROTO((struct waitf *, struct waitf *));
extern int check_waiting_on_req _PROTO((int, int));
extern int check_coff_waiting_on_req _PROTO((int, int));
extern struct stgcat_entry *newreq _PROTO((int));
extern int update_migpool _PROTO((struct stgcat_entry **, int, int));
extern int updfreespace _PROTO((char *, char *, int, u_signed64 *, signed64));
extern int req2argv _PROTO((char *, char ***));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int *, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());
extern char *stglogflags _PROTO((char *, char *, u_signed64));
extern int nextreqid _PROTO(());
extern int savereqs _PROTO(());
extern int build_ipath _PROTO((char *, struct stgcat_entry *, char *, int, int, mode_t));
extern int cleanpool _PROTO((char *));
extern void delreq _PROTO((struct stgcat_entry *, int));
extern int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int, int, int));
extern void sendinfo2cptape _PROTO((int *, struct stgcat_entry *));
extern void create_link _PROTO((struct stgcat_entry *, char *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern int retenp_on_disk _PROTO((int));
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *, int, int, int));
extern void rwcountersfs _PROTO((char *, char *, int, int));
extern struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, int **, char *, char *, int));
extern char *findpoolname _PROTO((char *));
extern u_signed64 findblocksize _PROTO((char *));
extern int findfs _PROTO((char *, char **, char **));
extern int rc_shift2castor _PROTO((int,int));
extern int ask_stageout _PROTO((int, char *, struct stgcat_entry **));

#define IS_RC_OK(rc) (rc == 0)
#define IS_RC_WARNING(rc) (rc == LIMBYSZ || rc == BLKSKPD || rc == TPE_LSZ || (rc == MNYPARI && (stcp->u1.t.E_Tflags & KEEPFILE)))

#ifdef HAVE_SENSIBLE_STCP
#undef HAVE_SENSIBLE_STCP
#endif
/* This macro will check sensible part of the CASTOR HSM union */
#define HAVE_SENSIBLE_STCP(stcp) (((stcp)->u1.h.xfile[0] != '\0') && ((stcp)->u1.h.server[0] != '\0') && ((stcp)->u1.h.fileid != 0) && ((stcp)->u1.h.fileclass != 0))

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

void
procupdreq(req_type, magic, req_data, clienthost)
		 int req_type;
		 int magic;
		 char *req_data;
		 char *clienthost;
{
	char **argv = NULL;
	int blksize = -1;
	int c = 0, i, n;
	struct stgcat_entry *cur;
	char *dp;
	char dsksrvr[CA_MAXHOSTNAMELEN + 1];
	char *dvn = NULL;
	int errflg = 0;
	char *fid = NULL;
	int found, index_found;
	char *fseq = NULL;
	gid_t gid;
	char *ifce = NULL;
	int j;
	int key;
	int lrecl = -1;
	int nargs = 0;
	char *p_cmd;
	char *p_stat;
	char *p, *q;
	char prevfseq[CA_MAXFSEQLEN + 1];
	char *rbp;
	int rc = -1;
	char *recfm = NULL;
	static char s_cmd[5][9] = {"", "stagein", "stageout", "stagewrt", "stageput"};
	u_signed64 size = 0;
	struct stat64 st;
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
	struct waitq *wqp = NULL;
	int Zflag = 0;
	int cflag = 0;
	int oflag = 0;
	int Cflag = 0;
	int Oflag = 0;
	int callback_index = -1;
	int found_wfp = 0;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
	u_signed64 actual_size_block = 0;
	int api_out = 0;
#if defined(_WIN32)
	int mask;
#else
	mode_t mask;
#endif
	int clientpid;
	u_signed64  flags;
	int  nstpp_input, path_status;
	struct stgpath_entry *stpp_input = NULL;
	struct group *gr;
	char save_group[CA_MAXGRPNAMELEN+1];
	char *pool_user = NULL;
	char *User;
	char *name;
	int checkrc;
	extern time_t last_upd_fileclasses; /* == 0 only at the initial startup or when not wanted */
	time_t save_last_upd_fileclasses;

	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	if (req_type > STAGE_00) {
		char tmpbuf[21];

		upd_reqid = reqid;
		upd_rpfd = rpfd;

		/* Note that we already checked in stgdaemon.c that magic >= STGMAGIC2 */
		local_unmarshall_STRING (rbp, name);
		unmarshall_LONG(rbp, uid);
		unmarshall_LONG(rbp, gid);
		unmarshall_LONG(rbp, mask);
		unmarshall_LONG(rbp, clientpid);
		unmarshall_HYPER(rbp, stage_uniqueid);
		local_unmarshall_STRING(rbp, User);
		pool_user = NULL;
		if (User[0] != '\0') pool_user = User;
		unmarshall_HYPER(rbp, flags);
		unmarshall_STRING(rbp, tmpbuf);
		rc = atoi(tmpbuf);
		api_out = 1;
		unmarshall_LONG(rbp, nstpp_input);
#ifndef __INSURE__
		stglogit (func, STG92, "stage_updc", user, uid, gid, clienthost);
#endif
		if (nstpp_input != 1) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid number of input structure (%d stgpath) - one only is supported\n", nstpp_input);
			c = EINVAL;
			goto reply;
		}
		if ((stpp_input = (struct stgpath_entry *) calloc(nstpp_input,sizeof(struct stgpath_entry))) == NULL) {
			sendrep(&rpfd, MSG_ERR, "STG02 - memory allocation error (%s)\n", strerror(errno));
			c = SESYSERR;
			goto reply;
		}
		path_status = 0;
		unmarshall_STAGE_PATH(magic, STGDAEMON_LEVEL, STAGE_INPUT_MODE, path_status, rbp, &(stpp_input[0]));
		if ((path_status != 0) || (stpp_input[0].upath[0] == '\0')) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Bad input (path input structure\n");
			c = EINVAL;
			goto reply;
		}
		stglogit(func,"stpp[1/1] : %s\n",stpp_input[0].upath);
		if (rc >= 0) stglogit(func,"-R %d\n", rc);
		if ((flags & STAGE_FILE_ROPEN) == STAGE_FILE_ROPEN) oflag = 1;
		if ((flags & STAGE_FILE_RCLOSE) == STAGE_FILE_RCLOSE) cflag = 1;
		if ((flags & STAGE_FILE_WOPEN) == STAGE_FILE_ROPEN) Oflag = 1;
		if ((flags & STAGE_FILE_WCLOSE) == STAGE_FILE_RCLOSE) Cflag = 1;
		/* Print the flags */
		stglogflags(func,LOGFILE,(u_signed64) flags);

#if SACCT
		stageacct (STGCMDR, uid, gid, clienthost, reqid, STAGE_UPDC, 0, 0, NULL, "", (char) 0);
#endif

	} else {
		unmarshall_WORD (rbp, uid);
		unmarshall_WORD (rbp, gid);

#ifndef __INSURE__
		stglogit (func, STG92, "stageupdc", user, uid, gid, clienthost);
#endif

		nargs = req2argv (rbp, &argv);

#if SACCT
		stageacct (STGCMDR, uid, gid, clienthost, reqid, STAGEUPDC, 0, 0, NULL, "", (char) 0);
#endif

		dvn = unknown;
		ifce = unknown;
		upd_reqid = reqid;
		upd_rpfd = rpfd;
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt (nargs, argv, "b:cCD:F:f:h:i:I:L:oOq:R:s:T:U:W:Z:")) != -1) {
			switch (c) {
			case 'b':
				stage_strtoi(&blksize, Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-b");
					errflg++;
				}
				break;
			case 'c':
				cflag++;
				break;
			case 'C':
				Cflag++;
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
					sendrep (&rpfd, MSG_ERR, STG06, "-i");
					errflg++;
				}
				break;
			case 'I':
				ifce = Coptarg;
				break;
			case 'L':
				stage_strtoi(&lrecl, Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-L");
					errflg++;
				}
				break;
			case 'o':
				oflag++;
				break;
			case 'O':
				Oflag++;
				break;
			case 'q':
				fseq = Coptarg;
				break;
			case 'R':
				stage_strtoi(&rc, Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-R");
					errflg++;
				}
				break;
			case 's':
				if ((checkrc = stage_util_check_for_strutou64(Coptarg)) < 0) {
					sendrep (&rpfd, MSG_ERR, STG06, "-s");
					errflg++;
				} else {
					size = strutou64(Coptarg); /* Default unit is byte there */
				}
				break;
			case 'T':
				stage_strtoi(&transfer_time, Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-T");
					errflg++;
				}
				break;
			case 'W':
				stage_strtoi(&waiting_time, Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-W");
					errflg++;
				}
				break;
			case 'Z':
				Zflag++;
				if ((p = strtok (Coptarg, ".")) != NULL) {
					stage_strtoi(&reqid, p, &dp, 10);
					if ((p = strtok (NULL, "@")) != NULL) {
						stage_strtoi(&key, p, &dp, 10);
					}
				}
				break;
			}
		}
		if (Zflag && nargs > Coptind) {
			sendrep (&rpfd, MSG_ERR, STG16);
			errflg++;
		}
		if (errflg) {
			c = EINVAL;
			goto reply;
		}
	}

    /* Protect agains specifying open and close at the same time */
	if ((oflag + cflag + Oflag + Cflag) > 1) {
		sendrep (&rpfd, MSG_ERR, STG35, "-c [or -C]", "-o [or -O]");
		c = EINVAL;
		goto reply;
	}

	if ((gr = Cgetgrgid (gid)) == NULL) {
		if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (&rpfd, MSG_ERR, STG36, gid);
		c = (api_out != 0) ? ESTGROUP : SESYSERR;
		goto reply;
	}
	strncpy (save_group, gr->gr_name, CA_MAXGRPNAMELEN);
	save_group[CA_MAXGRPNAMELEN] = '\0';

	if ((req_type == STAGE_UPDC) || ((req_type == STAGEUPDC) && (nargs > Coptind))) {
		char *argv_i = NULL;
		/* Please note that STAGE_UPDC supports ONLY the following actions */
		if (req_type == STAGE_UPDC) {
			/* Makes sure that the loop below will also work with STAGE_UPDC */
			nargs = nstpp_input;
			Coptind = 0;
			c = 0;
		}
		for  (i = Coptind; i < nargs; i++) {
			argv_i = ((req_type == STAGE_UPDC) ? stpp_input[i].upath : argv[i]);
			/* We need to restrict the following action to STAGE_UPDC (e.g. API) because we NEED */
			/* to have more info, like pid of the client - that only the API sends */
			if ((req_type == STAGE_UPDC) && (rc == ENOSPC)) {
				/* We are going here to mimic a STAGEOUT that would fail because of WAITING_SPC */
				/* This means that we have to do in one go: creation of an entry in the waitq */
				/* followed by the same processing as with a stage_updc_tppos -R 28 */
				struct stgpath_entry *stpp;
				struct stgcat_entry stgreq;
				char save_fseq[CA_MAXFSEQLEN+1];
				char *fseq = NULL;
				char *found_server;
				char *found_dirpath;
				char *new_server;
				char *new_dirpath;

				found = 0;
				for (stpp = stps; stpp < stpe; stpp++) {
					if (stpp->reqid == 0) break;
					if (strcmp (argv_i, stpp->upath)) continue;
					found = 1;
					break;
				}
				if (found != 0) {
					found = 0;
					for (stcp = stcs; stcp < stce; stcp++) {
						if (stcp->reqid == 0) break;
						if ((stcp->status != STAGEOUT) && (stcp->status != STAGEALLOC)) continue;
						if (stpp->reqid != stcp->reqid) continue;
						found = 1;
						break;
					}
				} else {
					for (stcp = stcs; stcp < stce; stcp++) {
						if (stcp->reqid == 0) break;
						if ((stcp->status != STAGEOUT) && (stcp->status != STAGEALLOC)) continue;
						if (strcmp (argv_i, stcp->ipath)) continue;
						found = 1;
						break;
					}
				}
				if (found != 1) {
					sendrep (&rpfd, MSG_ERR, STG22);
					c = ENOENT;
					goto reply;
				}
				/* We have found a STAGEOUT or STAGEALLOC entry matching argv_i */
				/* So we now end up in the case where we simulate a stageout on */
				/* an entry that is already in stageout */

				/* We look if this entry is known to the stagein pool */
				if ((findfs(stcp->ipath,&found_server,&found_dirpath) != 0) ||
					(found_server == NULL) || (found_dirpath == NULL)) {
					/* Strange - seems to not be in a known fs or a known pool - this is not legal */
					sendrep (&rpfd, MSG_ERR, STG166, stcp->ipath);
					c = EINVAL;
					goto reply;
				}

				/* We decrement the r/w counter on this file system */
				rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, STAGEUPDC);

				memset((char *) &stgreq, 0, sizeof(stgreq));
				stgreq = *stcp;
				strncpy (stgreq.user, user, CA_MAXUSRNAMELEN);
				stgreq.user[CA_MAXUSRNAMELEN] = '\0';
				stgreq.uid = uid;
				stgreq.gid = gid;
				stgreq.mask = stcp->mask;
				strcpy(stgreq.poolname,stcp->poolname);
				if (stcp->t_or_d == 't') {
					strcpy(save_fseq,stcp->u1.t.fseq);
					fseq = save_fseq;
				} else {
					save_fseq[0] = '\0';
				}
				if (strcmp (user, stcp->user)) {
					sendrep (&rpfd, MSG_ERR, STG37);
					c = EINVAL;
					goto reply;
				}
				if (delfile (stcp, 1, 1, 1, "no more space", uid, gid, 0, 0, 0) < 0) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
					c = SESYSERR;
					goto reply;
				}
				c = 0;
			procupd_launch_gc:
				stcp = newreq((int) stgreq.t_or_d);
				memcpy (stcp, &stgreq, sizeof(stgreq));
				if (i > 0)
					stcp->reqid = nextreqid();
				else
					stcp->reqid = reqid;
				stcp->status = STAGEOUT;
				stcp->c_time = time(NULL);
				stcp->a_time = stcp->c_time;
				stcp->nbaccesses++;
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs();
				/* We try to find another entry immediately */
				if (pool_user == NULL)
					pool_user = STAGERSUPERUSER;
				if (c == 0) {
					/* This is how we distinguish from a first pass and a goto */
					c = build_ipath (NULL, stcp, pool_user, 0, 0, (mode_t) 0);
				}
				if (c < 0) {
					stcp->status |= WAITING_SPC;
					/* But add this request to the waiting queue */
					if (!wqp) {
						wqp = add2wq (clienthost,
										user, stcp->uid, stcp->gid,
										user, save_group, stcp->uid, stcp->gid,
										clientpid,    /* Client pid unknown with old protocol, Aie... */
										0,    /* No Upluspath */
										reqid, STAGEOUT, 1, &wfp, NULL, 
										stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq, 0);
						wqp->api_out = api_out;
						wqp->magic = magic;
						wqp->openflags = 0;
						wqp->openmode = 0;
						wqp->uniqueid = stage_uniqueid;
						wqp->silent = 0;
					}
					wfp->subreqid = stcp->reqid;
					wfp->upath[0] = '\0';
					wqp->nbdskf++;
					wfp++;
					strcpy (wqp->pool_user, pool_user);
					strcpy (wqp->waiting_pool, stcp->poolname);
					/* This is for the return here at the end of the routine */
					c = 0;
				} else if (c) {
					delreq(stcp,0);
					goto reply;
				} else {
					/* Space successfully allocated - Here c == 0 per construction */

					/* We neverthless check if the selected filesystem is the same as on which there was an ENOSPC */
					/* In such a case we prefer to go back to usual processing */
					if ((findfs(stcp->ipath,&new_server,&new_dirpath) != 0) ||
						(new_server == NULL) || (new_dirpath == NULL)) {
						/* Strange - seems to not be in a known fs or a known pool - this is impossible */
						sendrep (&rpfd, MSG_ERR, STG166, stcp->ipath);
						if (delfile (stcp, 1, 1, 1, "unknown filesystem", uid, gid, 0, 0, 0) < 0) {
							sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
						}
						c = SESYSERR;
						goto reply;
					}

					/* We check if we ever selected again the same filesystem... */
					/* There is a time-window that give you the right to say: */
					/* Within the time ENOSPC was reached and the time stgdaemon */
					/* processes the ENOSPC, some data could have been freed on the */
					/* filesystem. Yes. I think neverthless this time is small */
					/* enough so that not taking it into account gives good behaviour */
					/* in > 95% of the cases - at least in the CASTOR framework */
					if ((strcmp(found_server,new_server) == 0) && (strcmp(found_dirpath,new_dirpath) == 0)) {
						/* Same server - same filesystem */
						/* sendrep (&rpfd, MSG_ERR, STG167, stcp->ipath); */
						if (delfile (stcp, 1, 1, 1, "same [server,fs] selected", uid, gid, 0, 0, 0) < 0) {
							sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
							c = SESYSERR;
							goto reply;
						}
						c = -1;
						goto procupd_launch_gc;
					}
					if (api_out) sendrep(&rpfd, API_STCP_OUT, stcp, magic);
					sendrep (&rpfd, MSG_OUT, "%s", stcp->ipath);
					/* We increment r/w counter on this new file system */
					rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, stcp->status);
				}
			} else if ((req_type == STAGE_UPDC) && (cflag || oflag || Cflag || Oflag)) {
				struct stgpath_entry *stpp;

				found = 0;
				for (stpp = stps; stpp < stpe; stpp++) {
					if (stpp->reqid == 0) break;
					if (strcmp (argv_i, stpp->upath)) continue;
					found = 1;
					break;
				}
				if (found != 0) {
					found = 0;
					for (stcp = stcs; stcp < stce; stcp++) {
						if (stcp->reqid == 0) break;
						if (stpp->reqid != stcp->reqid) continue;
						found = 1;
						break;
					}
				} else {
					for (stcp = stcs; stcp < stce; stcp++) {
						if (stcp->reqid == 0) break;
						if (strcmp (argv_i, stcp->ipath)) continue;
						found = 1;
						break;
					}
				}
				if (found != 1) {
					sendrep (&rpfd, MSG_ERR, STG22);
					c = ENOENT;
					goto reply;
				}

				if (oflag) /* Open on disk in read mode (disk -> client) */
					rwcountersfs(stcp->poolname, stcp->ipath, STAGEWRT, STAGEWRT); /* Could be STAGEPUT */
				else if (cflag) /* Corresponding close */
					rwcountersfs(stcp->poolname, stcp->ipath, STAGEWRT, STAGEUPDC);
				else if (Oflag) /* Open on disk in write mode (client -> disk) */
					rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT); /* Could be STAGEALLOC */
				else if (Cflag) /* Corresponding close */
					rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEUPDC);

				c = 0;
				goto reply;

			} else {
				if ((c = upd_stageout(req_type, argv_i, &subreqid, 1, NULL, 0, 0)) != 0) {
					if ((c != CLEARED) && (c != ESTCLEARED)) {
						struct stgcat_entry *found_stcp;
						int save_rpfd;

						save_rpfd = rpfd;
						rpfd = -1;
						/* Check in silent mode */
						if (ask_stageout (req_type, argv_i, &found_stcp) == 0) {
							if ((found_stcp->t_or_d == 'h') && (found_stcp->status == STAGEOUT)) {
								/* Force a Cns_statx call for a STAGEOUT file */
								/* upd_fileclass() will move to PUT_FAILED if necessary */
								save_last_upd_fileclasses = last_upd_fileclasses;
								last_upd_fileclasses = 0;
								/* Take action not in silent mode */
								rpfd = save_rpfd;
								if (upd_fileclass(NULL,found_stcp,2,0,0) < 0) {
									/* Try to fetch fileclass only unless record was cleared */
									if (serrno == ESTCLEARED) {
										last_upd_fileclasses = save_last_upd_fileclasses;
										continue;
									}
								}
								last_upd_fileclasses = save_last_upd_fileclasses;
							}
						}
						rpfd = save_rpfd;
						goto reply;
					} else {
						/* This is not formally an error to do an updc on a zero-length file */
						c = 0;
					}
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
		sendrep (&rpfd, MSG_ERR, STG22);
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
		c = ENOENT;
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
			sendrep (&rpfd, MSG_ERR, STG22);
			c = ENOENT;
			goto reply;
		}
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
#ifdef STAGER_DEBUG
			sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] Comparing wqp->save_subreqid[%d]=%d with wfp->subreqid=%d [i=%d]\n", callback_index, wqp->save_subreqid[callback_index], wfp->subreqid, i);
#endif
			if ((wfp->subreqid == wqp->save_subreqid[callback_index]) && (! wfp->waiting_on_req)) {
				found_wfp = 1;
				break;
			}
		}
	}
	if (! found_wfp) {
		/* False value */
		sendrep (&rpfd, MSG_ERR, STG22);
		c = ENOENT;
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
		sendrep (&rpfd, MSG_ERR, STG22);
		c = ENOENT;
		goto reply;
	}
#ifdef STAGER_DEBUG
	sendrep(&rpfd, MSG_ERR, "[DEBUG] procupd : rc=%d, concat_off_fseq=%d, i=%d, nbdskf=%d\n", rc, wqp->concat_off_fseq, i, wqp->nbdskf);
#endif

	/* Here stcp points to the found entry */
	/* and index_found points to the index starting at stcs */
	if (oflag) {
		/* The processing of oflag is an end point doing nothing else but updating r/w counters */
		if (wqp->last_rwcounterfs_vs_R != LAST_RWCOUNTERSFS_TPPOS) {
			/* Should be either 0 (from initialization) or LAST_RWCOUNTERSFS_FILCP */
			rwcountersfs(stcp->poolname, stcp->ipath, STAGEWRT, STAGEWRT); /* Could be STAGEPUT */
			wqp->last_rwcounterfs_vs_R = LAST_RWCOUNTERSFS_TPPOS;
		}
		c = 0;
		goto reply;
	}

	if (cflag) {
		/* The processing of cflag is an end point doing nothing else but updating r/w counters */
		if (wqp->last_rwcounterfs_vs_R == LAST_RWCOUNTERSFS_TPPOS) {
			/* Should be LAST_RWCOUNTERSFS_TPPOS */
			rwcountersfs(stcp->poolname, stcp->ipath, STAGEWRT, STAGEUPDC);
			wqp->last_rwcounterfs_vs_R = LAST_RWCOUNTERSFS_FILCP;
		}
		c = 0;
		goto reply;
	}

	if (Oflag) {
		/* The processing of Oflag is an end point doing nothing else but updating r/w counters */
		if (wqp->last_rwcounterfs_vs_R != LAST_RWCOUNTERSFS_TPPOS) {
			/* Should be either 0 (from initialization) or LAST_RWCOUNTERSFS_FILCP */
			rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT); /* Could be STAGEALLOC */
			wqp->last_rwcounterfs_vs_R = LAST_RWCOUNTERSFS_TPPOS;
		}
		c = 0;
		goto reply;
	}

	if (Cflag) {
		/* The processing of Cflag is an end point doing nothing else but updating r/w counters */
		if (wqp->last_rwcounterfs_vs_R == LAST_RWCOUNTERSFS_TPPOS) {
			/* Should be LAST_RWCOUNTERSFS_TPPOS */
			rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEUPDC);
			wqp->last_rwcounterfs_vs_R = LAST_RWCOUNTERSFS_FILCP;
		}
		c = 0;
		goto reply;
	}

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
			c = SESYSERR;
			goto reply;
		}
		wfp = &(wqp->wf[i]); /* We restore the correct found wf (mem might have been realloced) */
		stcp_ok = newreq((int) 't');
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
			c = SESYSERR;
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
			if ((wqp->concat_off_fseq > 0) && (i == (wqp->nbdskf - 1))) {
				/* We remove the trailing '-' */
				/* It can happen that the tppos on the first file is for a waitf */
				/* that does not have a trailing when the request is splitted because */
				/* of previous one(s) on the same VID */
				if (stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-') {
					has_trailing = 1;
					stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] = '\0';
				}
			}
			c = build_ipath (NULL, stcp, wqp->pool_user, 0, 0, (mode_t) 0);
			if (has_trailing != 0) {
				/* We restore the trailing '-' */
				strcat(stcp->u1.t.fseq,"-");
			}
			if (c > 0) {
				sendrep (&rpfd, MSG_ERR, "STG02 - build_ipath error\n");
				goto reply;
            }
			if (c < 0) {
				sendrep (&rpfd, MSG_ERR, "STG02 - build_ipath reaching out of space\n");
				wqp->clnreq_reqid = upd_reqid;
				wqp->clnreq_rpfd = rpfd;
				wqp->clnreq_waitingreqid = stcp->reqid;
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
			stcp->c_time = time(NULL);
			stcp->a_time = stcp->c_time;
			stcp->nbaccesses = 1;
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
		sendrep (&rpfd, MSG_OUT, "%s", stcp->ipath);
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
		if (((p = strstr (stcp->ipath, ":/")) != NULL) && (strchr(stcp->ipath, '/') > p)) {
			q = stcp->ipath;
		} else {
			/* Assumes RFIO syntax */
			q = strchr (stcp->ipath + 1, '/') + 1;
			p = strchr (q, '/');
		}
		strncpy (dsksrvr, q, p - q);
		dsksrvr[p - q] = '\0';
		if (stcp->status == STAGEIN) {
			char tmpbuf[21];
			char tmpbuf2[21];

			PRE_RFIO;
			if (RFIO_STAT64(stcp->ipath, &st) == 0) {
			  rfio_stat_retry_ok:
				stcp->actual_size = (u_signed64) st.st_size;
				wfp->size_yet_recalled = (u_signed64) st.st_size;
				if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
					actual_size_block = stcp->actual_size;
				}
			rfio_stat_continue:
				wfp->nb_segments++;
				if (wfp->size_yet_recalled >= wfp->size_to_recall) {
					/* Final message - Note that wfp->size_to_recall can be zero */
					/* (always the case it it is not a recall of CASTOR file) */
					if (wfp->nb_segments == 1) {
						/* Done in one segment */
						stglogit(func, STG97,
								dsksrvr, strrchr (stcp->ipath, '/')+1,
								stcp->user, stcp->group,
								clienthost, dvn, ifce,
								u64tostr(size, tmpbuf, 0),
								waiting_time, transfer_time, rc);
					} else {
						/* Done in more than one segment */
						stglogit(func, STG108,
							dsksrvr, strrchr (stcp->ipath, '/')+1,
							wfp->nb_segments, stcp->user, stcp->group,
							u64tostr(stcp->actual_size, tmpbuf, 0),
							u64tostr(size,tmpbuf2,0),
							rc);
					}
				} else if (wfp->size_to_recall > 0) {
					/* Not everything has yet been recalled */
					/* Done for this segment */
					stglogit(func, STG107,
						dsksrvr, strrchr (stcp->ipath, '/')+1,
						wfp->nb_segments, stcp->user, stcp->group,
						clienthost, dvn, ifce,
						u64tostr(size, tmpbuf, 0),
						waiting_time, transfer_time, rc);
				}
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
			} else {
				stglogit (func, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
				if (rfio_serrno() == EACCES) {
					extern struct passwd start_passwd;             /* Start uid/gid stage:st */

					/* We got permission denied - perhaps root is not trusted on remote machine ? */
					stglogit (func, "STG02 - %s : try under %s (%d,%d)\n", stcp->ipath, user, (int) uid, (int) gid);
					setegid(gid);
					seteuid(uid);
					PRE_RFIO;
					if (RFIO_STAT64(stcp->ipath, &st) == 0) {
						/* Worked! */
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						goto rfio_stat_retry_ok;
					}
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					stglogit (func, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
				}
				stglogit (func, "STG02 - %s : Incrementing actual_size with -s option value\n", stcp->ipath);
				/* No block information - assume mismatch with actual_size will be acceptable */
				stcp->actual_size += size;
				wfp->size_yet_recalled += size;
				actual_size_block = stcp->actual_size;
				goto rfio_stat_continue;
			}
		} else {
			char tmpbuf[21];

			stglogit(func, STG97,
					dsksrvr, strrchr (stcp->ipath, '/')+1,
					stcp->user, stcp->group,
					clienthost, dvn, ifce,
					u64tostr(size, tmpbuf, 0),
					waiting_time, transfer_time, rc);
		}
	}
#if SACCT
	stageacct (STGFILS, wqp->req_uid, wqp->req_gid, wqp->clienthost,
						 wqp->reqid, wqp->req_type, wqp->nretry, rc, stcp, clienthost, (char) 0);
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
			if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
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
			if (delfile (stcp, 1, 0, 1, "no more file", uid, gid, 0, 0, 0) < 0)
				sendrep (&(wqp->rpfd), MSG_ERR, STG02, stcp->ipath,
								 RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
			check_coff_waiting_on_req (wfp->subreqid, LAST_TPFILE);
			check_waiting_on_req (wfp->subreqid, STG_FAILED);
		}
		wqp->nbdskf = wqp->nb_subreqs;
		goto reply;
	}
	if (wfp->size_yet_recalled >= wfp->size_to_recall) {
		/* Note : wfp->size_to_recall is zero if it not a recall of CASTOR file(s) */
		/* (always the case it it is not a recall of CASTOR file) */
		/* Case of CASTOR files : if size recalled is exactly size (uncompress) on tape we have LIMBYSZ because */
		/* the mover do no readahead, e.g. the mover do not know it is the end of the file - only us do know about */
		/* that */
		/* Note that wfp->hsmsize is set only for recall of HSM files, CASTOR in particular */
		if ((wfp->hsmsize) > 0 && (wfp->size_yet_recalled >= wfp->hsmsize) && (rc == LIMBYSZ)) {
			stglogit(func, "Forcing rc=0\n");
			rc = 0;
		}
		p_cmd = s_cmd[stcp->status & 0xF];
		if (IS_RC_OK(rc))
			p_stat = "succeeded";
		else if (IS_RC_WARNING(rc))
			p_stat = "warning";
		else
			p_stat = "failed";
		if (stcp->t_or_d == 't')
			sendrep (&(wqp->rpfd), RTCOPY_OUT, STG41, p_cmd, p_stat,
							 fseq ? fseq : stcp->u1.t.fseq, stcp->u1.t.vid[0], rc);
		else if (stcp->t_or_d == 'd')
			sendrep (&(wqp->rpfd), RTCOPY_OUT, STG42, p_cmd, p_stat, stcp->u1.d.xfile, rc);
		else if (stcp->t_or_d == 'm')
			sendrep (&(wqp->rpfd), RTCOPY_OUT, STG42, p_cmd, p_stat, stcp->u1.m.xfile, rc);
		else if (stcp->t_or_d == 'h')
			sendrep (&(wqp->rpfd), RTCOPY_OUT, STG42, p_cmd, p_stat, stcp->u1.h.xfile, rc);
	}
	if (((rc != 0) && (rc != LIMBYSZ) &&
			 (rc != BLKSKPD) && (rc != TPE_LSZ) && (rc != MNYPARI)) ||
			((rc == MNYPARI) && ((stcp->u1.t.E_Tflags & KEEPFILE) == 0))) {
		char *found_server;
		char *found_dirpath;
		char *new_server;
		char *new_dirpath;

		wqp->status = rc_shift2castor(STGMAGIC,rc);
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
		/* We look if this entry is known to the stagein pool */
		if ((findfs(stcp->ipath,&found_server,&found_dirpath) != 0) ||
			(found_server == NULL) || (found_dirpath == NULL)) {
			/* Strange - seems to not be in a known fs or a known pool - this is not legal */
			sendrep (&rpfd, MSG_ERR, STG166, stcp->ipath);
			c = EINVAL;
			goto reply;
		}

		if (delfile (stcp, 1, 0, 0, "nospace retry", uid, gid, 0, 0, 0) < 0)
			sendrep (&(wqp->rpfd), MSG_ERR, STG02, stcp->ipath,
							 RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
		c = 0;
	procupd_launch_gc2:
		/* Make sure stcp->ipath[] is reset */
		stcp->ipath[0] = '\0';

		/* Instead of asking immediately for a garbage collector, we give it another immediate try */
		/* by asking for another filesystem */
		if (c == 0) {
			int has_trailing;
			/* This is how we distinguish from a first pass and a goto */

			has_trailing = 0;
			if ((wqp->concat_off_fseq > 0) && (i == (wqp->nbdskf - 1))) {
				/* We remove the trailing '-' */
				/* It can happen that the tppos on the first file is for a waitf */
				/* that does not have a trailing when the request is splitted because */
				/* of previous one(s) on the same VID */
				if (stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-') {
					has_trailing = 1;
					stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] = '\0';
				}
			}
			c = build_ipath (wfp->upath, stcp, wqp->pool_user, 0, 0, (mode_t) 0);
			if (has_trailing != 0) {
				/* We restore the trailing '-' */
				strcat(stcp->u1.t.fseq,"-");
			}
		}
		if (c < 0) {
			wqp->status = 0;
			wqp->clnreq_reqid = upd_reqid;
			wqp->clnreq_rpfd = rpfd;
			wqp->clnreq_waitingreqid = stcp->reqid;
			stcp->status |= WAITING_SPC;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			strcpy (wqp->waiting_pool, stcp->poolname);
			if ((c = cleanpool (stcp->poolname)) != 0) goto reply;
			return;
		} else if (c) {
			delreq(stcp,0);
			wqp->status = c;
			goto reply;
		} else {
			/* Space successfully allocated */

			/* We neverthless check if the selected filesystem is the same as on which there was an ENOSPC */
			/* In such a case we prefer to go back to usual processing */
			if ((findfs(stcp->ipath,&new_server,&new_dirpath) != 0) ||
				(new_server == NULL) || (new_dirpath == NULL)) {
				/* Strange - seems to not be in a known fs or a known pool - this is impossible */
				sendrep (&rpfd, MSG_ERR, STG166, stcp->ipath);
				if (delfile (stcp, 1, 0, 0, "unknown filesystem", uid, gid, 0, 0, 0) < 0) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
				}
				c = SESYSERR;
				goto reply;
			}

			/* We check if we ever selected again the same filesystem... */
			/* There is a time-window that give you the right to say: */
			/* Within the time ENOSPC was reached and the time stgdaemon */
			/* processes the ENOSPC, some data could have been freed on the */
			/* filesystem. Yes. I think neverthless this time is small */
			/* enough so that not taking it into account gives good behaviour */
			/* in > 95% of the cases - at least in the CASTOR framework */
			if ((strcmp(found_server,new_server) == 0) && (strcmp(found_dirpath,new_dirpath) == 0)) {
				/* Same server - same filesystem */
				/* sendrep (&rpfd, MSG_ERR, STG167, stcp->ipath); */
				if (delfile (stcp, 1, 0, 0, "same [server,fs] selected", uid, gid, 0, 0, 0) < 0) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
				}
				c = -1;
				goto procupd_launch_gc2;
			}

			if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
			sendrep (&rpfd, MSG_OUT, "%s", stcp->ipath);
			wqp->status = 0;
			goto reply;
		}
		/* In any case, here, we should have returned */
	}
	if ((stcp->t_or_d != 'm') && (stcp->t_or_d != 'h')) {
		int has_been_updated = 0;

		if ((blksize > 0) && (stcp->blksize != blksize)) {
			stcp->blksize = blksize;
			has_been_updated = 1;
		}
		if ((lrecl > 0) && (stcp->lrecl != lrecl)) {
			stcp->lrecl = lrecl;
			has_been_updated = 1;
		}
		if ((recfm != NULL) && (strcmp(stcp->recfm,recfm) != 0)) {
			strncpy (stcp->recfm, recfm, CA_MAXRECFMLEN);
			stcp->recfm[CA_MAXRECFMLEN] = '\0';
			has_been_updated = 1;
		}
		if (stcp->recfm[0] == 'U') {
			if (stcp->lrecl != 0) {
				stcp->lrecl = 0;
				has_been_updated = 1;
			}
		} else if (stcp->lrecl == 0) {
			if (stcp->lrecl != stcp->blksize) {
				stcp->lrecl = stcp->blksize;
				has_been_updated = 1;
			}
		}
		if ((fid != NULL) && (strcmp(stcp->u1.t.fid,fid) != 0)) {
			strncpy (stcp->u1.t.fid, fid, CA_MAXFIDLEN);
			stcp->u1.t.fid[CA_MAXFIDLEN] = '\0';
			has_been_updated = 1;
		}
		if ((fseq != NULL) &&
				(stcp->u1.t.fseq[0] == 'u' || stcp->u1.t.fseq[0] == 'n')) {
			if (strcmp(stcp->u1.t.fseq,fseq) != 0) {
				strncpy (stcp->u1.t.fseq, fseq, CA_MAXFSEQLEN);
				stcp->u1.t.fseq[CA_MAXFSEQLEN] = '\0';
				has_been_updated = 1;
			}
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
		int save_wfp_subreqid = wfp->subreqid;
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
					ifileclass = upd_fileclass(NULL,stcp,0,0,0);
				else
					ifileclass = -1;
				/*
				 * stcp, here, already points to the entry for whith stcp->reqid == wfp->subreqid
				 */
				/*
				for (stcp = stcs; stcp < stce; stcp++)
					if (stcp->reqid == wfp->subreqid) break;
				*/
				if (wqp->copytape)
					sendinfo2cptape (&(wqp->rpfd), stcp);
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
						struct stgcat_entry save_stcp_rdonly;
						struct stgcat_entry save_stcp_other_migrated;
						struct stgcat_entry save_stcp_other_migration;
						int save_status = stcp->status;
						int force_delete = 0;

						save_stcp_rdonly.reqid = 0;
						save_stcp_other_migrated.reqid = 0;
						save_stcp_other_migration.reqid = 0;
						if ((save_stcp->t_or_d == 'h') && (ISSTAGEWRT(save_stcp) || ISSTAGEPUT(save_stcp)) && HAVE_SENSIBLE_STCP(save_stcp)) {
							/* If this entry is a CASTOR file and has tppool[0] != '\0' this must/might be */
							/* a corresponding stcp entry with the stageout matching STAGEOUT|CAN_BE_MIGR|BEING_MIGR */
							stcp_found = NULL;
							for (stcp_search = stcs; stcp_search < stce; stcp_search++) {
								if (stcp_search->reqid == 0) break;
								if ((save_stcp->poolname[0] != '\0') &&
									(strcmp(stcp_search->poolname, save_stcp->poolname) != 0)) continue;
								if (! ( (strcmp(stcp_search->u1.h.xfile,save_stcp->u1.h.xfile) == 0) &&
										(strcmp(stcp_search->u1.h.server,save_stcp->u1.h.server) == 0) &&
										(stcp_search->u1.h.fileid == save_stcp->u1.h.fileid) &&
										(stcp_search->u1.h.fileclass == save_stcp->u1.h.fileclass))) continue;
								if (save_stcp_rdonly.reqid == 0) {
									if (stcp_search->status == (STAGEIN|STAGED|STAGE_RDONLY)) {
										save_stcp_rdonly = *stcp_search;
										continue;
									}
								}
								if (save_stcp_other_migrated.reqid == 0) {
									if ((stcp_search->status == (STAGEOUT|STAGED)) ||
										(stcp_search->status == (STAGEPUT|STAGED))) {
										save_stcp_other_migrated = *stcp_search;
										continue;
									}
								}
								if (! ISCASTORBEINGMIG(stcp_search)) continue;
								if (strcmp(stcp_search->u1.h.tppool,save_stcp->u1.h.tppool) == 0) {
									/* We do the ++ because of the possible loop below */
									stcp_found = stcp_search++;
									break;
								} else if (save_stcp_other_migration.reqid == 0) {
									/* No matter if we overwrite more than one the content */
									save_stcp_other_migration = *stcp_search;
									continue;
								}
							}
							if ((save_stcp_rdonly.reqid == 0) ||
								(save_stcp_other_migrated.reqid == 0) ||
								(save_stcp_other_migration.reqid == 0)
								) {
								/* We continue the search of a STAGEIN|STAGED|STAGE_RDONLY entry */
								/* or the search of another (migration,migrated) entry */
								for ( ; stcp_search < stce; stcp_search++) {
									if ((save_stcp_rdonly.reqid != 0) &&
										(save_stcp_other_migrated.reqid != 0) &&
										(save_stcp_other_migration.reqid != 0)
										) break;
									if (stcp_search->reqid == 0) break;
									if ((save_stcp->poolname[0] != '\0') &&
										(strcmp(stcp_search->poolname, save_stcp->poolname) != 0)) continue;
									if (! ( (strcmp(stcp_search->u1.h.xfile,save_stcp->u1.h.xfile) == 0) &&
											(strcmp(stcp_search->u1.h.server,save_stcp->u1.h.server) == 0) &&
											(stcp_search->u1.h.fileid == save_stcp->u1.h.fileid) &&
											(stcp_search->u1.h.fileclass == save_stcp->u1.h.fileclass))) continue;
									if (save_stcp_rdonly.reqid == 0) {
										if (stcp_search->status == (STAGEIN|STAGED|STAGE_RDONLY)) {
											save_stcp_rdonly = *stcp_search;
											continue;
										}
									}
									if (save_stcp_other_migrated.reqid == 0) {
										if ((stcp_search->status == (STAGEOUT|STAGED)) ||
											(stcp_search->status == (STAGEPUT|STAGED))) {
											save_stcp_other_migrated = *stcp_search;
											continue;
										}
									}
									if (save_stcp_other_migration.reqid == 0) {
										if (! ISCASTORBEINGMIG(stcp_search)) continue;
										if ((strcmp(stcp_search->u1.h.tppool,save_stcp->u1.h.tppool) != 0)) {
											save_stcp_other_migration = *stcp_search;
											continue;
										}
									}
								}
							}
							if (stcp_found == NULL) {
								/* This is acceptable only if the request is an explicit STAGEPUT */
								/*
								if (stcp->status != STAGEPUT) {
									sendrep (&(wqp->rpfd), MSG_ERR, STG22);
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
							if (update_hsm_a_time(stcp_found) == ENOENT) {
								/* Do not exist anymore in the CASTOR Name Server */
								if ((wqp->flags & STAGE_HSM_ENOENT_OK) == STAGE_HSM_ENOENT_OK)
									/* We force a delreq() or delfile() in such a case */
									force_delete = 1;
							}
#ifdef USECDB
							if (stgdb_upd_stgcat(&dbfd,stcp_found) != 0) {
								stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
							}
#endif
							if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp_found, wqp->magic);
						} else {
							stcp_found = stcp;
							goto delete_entry;
						}
						if (stcp_found != NULL) {
							if ((stcp_found != save_stcp) ||           /* STAGEWRT */
								((save_status & STAGEPUT) == STAGEPUT) /* STAGEPUT */
								) {
								int continue_flag = 0;
								/* The found element is not the STAGEWRT itself, or is the STAGEPUT */
								/* this means it was a migration request */
								if ((ifileclass >= 0) && (save_stcp_other_migration.reqid == 0)) {
									/* No other concurrent migration running : it is meaningful to check */
									/* if this file can be deleted */
									time_t thistime = time(NULL);
									int thisretenp;

									if ((thisretenp = stcp_found->u1.h.retenp_on_disk) < 0) thisretenp = retenp_on_disk(ifileclass);

									/* Fileclass's retention period on disk larger than current lifetime ? */
									if ((thisretenp == INFINITE_LIFETIME) || (thisretenp == AS_LONG_AS_POSSIBLE)) {
										stglogit(func, STG142, stcp_found->u1.h.xfile, (thisretenp == INFINITE_LIFETIME) ? "INFINITE_LIFETIME" : "AS_LONG_AS_POSSIBLE");
										continue_flag = 1;
									} else if (((int) (thistime - stcp_found->a_time)) < thisretenp) {
										stglogit(func, STG131, stcp_found->u1.h.xfile, thisretenp, (thistime - stcp_found->a_time));
										continue_flag = 1;
									} else {
										if (stcp_found->u1.h.retenp_on_disk >= 0) {
											/* User defined retention period is in action */
											stglogit (func, STG158, stcp_found->u1.h.xfile, fileclasses[ifileclass].Cnsfileclass.name, stcp_found->u1.h.server, fileclasses[ifileclass].Cnsfileclass.classid, thisretenp, stcp_found->u1.h.retenp_on_disk, (int) (thistime - stcp_found->a_time));
										} else {
											/* Default fileclass retention period is in action */
											stglogit (func, STG133, stcp_found->u1.h.xfile, fileclasses[ifileclass].Cnsfileclass.name, stcp_found->u1.h.server, fileclasses[ifileclass].Cnsfileclass.classid, thisretenp, (int) (thistime - stcp_found->a_time));
										}
									}
								}
								if ((save_stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
									/* save_stcp is not a real valid pointer but we don't care because */
									/* this can change only if update_migpool() calls newreq(), e.g. when */
									/* the second argument is 1 */
									update_migpool(&save_stcp,-1,0);
								}
								if (! ((stcp_found == save_stcp) && (save_status & STAGEPUT) == STAGEPUT))
									delreq(save_stcp,0);      /* Deletes the STAGEWRT */
								if (force_delete != 0) {      /* Precedence is given to forced deletion */
									if ((continue_flag != 0) && (stcp_found->t_or_d == 'h')) {
										char tmpbuf1[21];
										/* We neverthless log to indicate that, regardless of continue_flag */
										/* we decided anyway to force the deletion */
										sendrep (&(wqp->rpfd), MSG_ERR, STG175,
												 stcp_found->u1.h.xfile,
												 u64tostr((u_signed64) stcp_found->u1.h.fileid, tmpbuf1, 0),
												 stcp_found->u1.h.server,
												 "forced deletion regardless of retention period : ENOENT + STAGE_HSM_ENOENT_OK flag");
									}
									goto delete_entry;
								} else if (continue_flag != 0) { /* Then to forced continuation */
									goto continue_entry;
								} else {
									goto delete_entry;           /* Then to the default (deletion) */
								}
							} else {
							delete_entry:
								if (save_stcp_other_migration.reqid != 0) {
									/* This is a migration callback and there is another migration */
									/* on same file running */
									delreq(stcp_found,0);
								} else {
									/* We can delete this entry */
									if (delfile (stcp_found, 0, 1, 1, ((save_status & 0xF) == STAGEWRT) ? "stagewrt ok" : "stageput ok", uid, gid, 0, ((save_status & 0xF) == STAGEWRT) ? 1 : 0, 0) < 0) {
										sendrep (&(wqp->rpfd), MSG_ERR, STG02, stcp_found->ipath,
													 RFIO_UNLINK_FUNC(stcp_found->ipath), rfio_serror());
									}
								}
							}
						}
					continue_entry:
						/* Suppose now that we have also found a stcp in read-only mode */
						/* or another entry yet migrated */
						/* Since the logic is the same : Deleted current migration records and save */
						/* meaningful status in the other one, STAGED or STAGED|STAGED_RDONLY */
						/* We decide to give precedence to the other migrated entry */
						if (save_stcp_other_migrated.reqid > 0)
							save_stcp_rdonly = save_stcp_other_migrated;
						if ((save_stcp_rdonly.reqid > 0) && (save_stcp_other_migration.reqid == 0)) {
							/* Because of the delreq()s upper we must scan again the catalog */
							struct stgcat_entry *stcp_found_rdonly_or_migrated = NULL;
							struct stgcat_entry *stcp_found_staged = NULL;

							for (stcp_search = stcs; stcp_search < stce; stcp_search++) {
								if (stcp_search->reqid == 0) break;
								if (stcp_search->reqid == save_stcp_rdonly.reqid) {
									stcp_found_rdonly_or_migrated = stcp_search;
									continue;
								}
								if ((save_stcp_rdonly.poolname[0] != '\0') &&
									(strcmp(stcp_search->poolname, save_stcp_rdonly.poolname) != 0)) continue;
								if (! ( (strcmp(stcp_search->u1.h.xfile,save_stcp_rdonly.u1.h.xfile) == 0) &&
										(strcmp(stcp_search->u1.h.server,save_stcp_rdonly.u1.h.server) == 0) &&
										(stcp_search->u1.h.fileid == save_stcp_rdonly.u1.h.fileid) &&
										(stcp_search->u1.h.fileclass == save_stcp_rdonly.u1.h.fileclass))) continue;
								if (stcp_search->reqid == save_stcp_other_migrated.reqid) {
									/* This is not this STAGED only entry that we want to match */
									continue;
								}
								if ((stcp_search->status == (STAGEOUT|STAGED)) ||
									(stcp_search->status == (STAGEPUT|STAGED))
									) {
									stcp_found_staged = stcp_search;
								}
								if ((stcp_found_rdonly_or_migrated != NULL) && (stcp_found_staged != NULL)) {
									break;
								}
							}
							if ((stcp_found_rdonly_or_migrated != NULL) || (stcp_found_staged != NULL)) {
								if (stcp_found_rdonly_or_migrated != NULL) {
									if ((stcp_found_rdonly_or_migrated->status & STAGE_RDONLY) == STAGE_RDONLY)
										stcp_found_rdonly_or_migrated->status = STAGEIN|STAGED;
									if (stcp_found_rdonly_or_migrated->t_or_d == 'h')
										stcp_found_rdonly_or_migrated->u1.h.tppool[0] = '\0'; /* We reset the poolname */
									if (stcp_found_staged != NULL) {
										stcp_found_rdonly_or_migrated->nbaccesses += (stcp_found_staged->nbaccesses - 1);
									}
#ifdef USECDB
									if (stgdb_upd_stgcat(&dbfd,stcp_found_rdonly_or_migrated) != 0) {
										stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
									}
#endif
								}
								if (stcp_found_staged != NULL) {
									delreq(stcp_found_staged,0);
								}
								savereqs();
							}
						}
					}
				} else {
					struct stgcat_entry *save_stcp = stcp;
					struct stgcat_entry *stcp_search, *stcp_found, *stcp_other_migration_found, *stcp_other_migrated_found;

					if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
						update_migpool(&stcp,-1,0);
						stcp->status &= ~CAN_BE_MIGR;
					}
					stcp->status |= STAGED;
					update_hsm_a_time(stcp);
					/* We send the reply right now */
					if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
					/* We check if there is any STAGEIN|STAGED|STAGE_RDONLY entry pending */
					if (stcp->t_or_d != 'h') goto not_an_HSM_castor_callback;
					stcp_found = stcp_other_migration_found = stcp_other_migrated_found = NULL;
					for (stcp_search = stcs; stcp_search < stce; stcp_search++) {
						if ((stcp_found != NULL) &&
							(stcp_other_migrated_found != NULL) &&
							(stcp_other_migration_found != NULL)
							) break;
						if (stcp_search->reqid == 0) break;
						if (stcp_search == stcp) continue;
						if ((save_stcp->poolname[0] != '\0') &&
							(strcmp(stcp_search->poolname, save_stcp->poolname) != 0)) continue;
						if (! (	(strcmp(stcp_search->u1.h.xfile,save_stcp->u1.h.xfile) == 0) &&
								(strcmp(stcp_search->u1.h.server,save_stcp->u1.h.server) == 0) &&
								(stcp_search->u1.h.fileid == save_stcp->u1.h.fileid) &&
								(stcp_search->u1.h.fileclass == save_stcp->u1.h.fileclass))) continue;
						if (stcp_found == NULL) {
							if (stcp_search->status == (STAGEIN|STAGED|STAGE_RDONLY)) {
								stcp_found = stcp_search;
								continue;
							}
						}
						if (stcp_other_migrated_found == NULL) {
							if ((stcp_search->status == (STAGEOUT|STAGED)) ||
								(stcp_search->status == (STAGEPUT|STAGED))
								) {
								stcp_other_migrated_found = stcp_search;
								continue;
							}
						}
						if (stcp_other_migration_found == NULL) {
							if (! ISCASTORBEINGMIG(stcp_search)) continue;
							if ((strcmp(stcp_search->u1.h.tppool,save_stcp->u1.h.tppool) != 0)) {
								stcp_other_migration_found = stcp_search;
								continue;
							}
						}
					}
					if ((stcp_found != NULL) && (stcp_other_migrated_found != NULL)) {
						/* We give precedence to the other found migrated entry */
						stcp_found = stcp_other_migrated_found;
                    }
					if ((stcp_found != NULL) && (stcp_other_migration_found == NULL)) {
						/* Here we are : the file has been migrated but has been accessed before the */
						/* end of the migration in read-only mode, or yet migrated (STAGEOUT|STAGED) */
						/* There is no other migration for this file running */

						if ((stcp_found->status & STAGE_RDONLY) == STAGE_RDONLY)
							/* We change the status of stcp to be a classic STAGEIN|STAGED one, adding */
							/* all nbaccessses - 1 because, at creation, we incremented the migration entry's */
							/* nbaccesses as well as created a new entry with one for nbaccesses */
							stcp_found->status = (STAGEIN|STAGED);
						if (stcp_found->t_or_d == 'h')
							stcp_found->u1.h.tppool[0] = '\0'; /* We reset the poolname if any */
						stcp_found->nbaccesses += (stcp->nbaccesses - 1);
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp_found) != 0) {
							stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
						/* And we delete the now virtual STAGEIN|STAGED|STAGE_RDONLY entry */
						/* Be aware that this can shift the whole catalog in memory - that's why */
						/* it has to be done at the very end of current processing */
						delreq(stcp,0);
					} else if (stcp_found != NULL) {
						/* The file has been migrated but has been accessed before the */
						/* end of the migration in read-only mode, or yet migrated, and */
						/* there is another migration running */

						/* We remove current migration stuff */
						/* all nbaccessses - 1 because, at creation, we incremented the migration entry's */
						/* nbaccesses as well as created a new entry with one for nbaccesses */
						if (stcp_found->t_or_d == 'h')
							stcp_found->u1.h.tppool[0] = '\0'; /* We reset the poolname if any */
						stcp_found->nbaccesses += (stcp->nbaccesses - 1);
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp_found) != 0) {
							stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
						/* And we delete the now virtual STAGEIN|STAGED|STAGE_RDONLY entry */
						/* Be aware that this can shift the whole catalog in memory - that's why */
						/* it has to be done at the very end of current processing */
						delreq(stcp,0);
					} else if (stcp_other_migrated_found != NULL) {
						/* The file has been migrated and there is another instance of it yet marked */
						/* as migrated - we know per construction that there is no other instance of */
						/* running migration, or accessed in read-only mode, of that file */

						/* We remove current migration stuff */
						/* all nbaccessses - 1 because, at creation, we incremented the migration entry's */
						/* nbaccesses as well as created a new entry with one for nbaccesses */
						if (stcp_other_migrated_found->t_or_d == 'h')
							stcp_other_migrated_found->u1.h.tppool[0] = '\0'; /* We reset the poolname if any */
						stcp_other_migrated_found->nbaccesses += (stcp->nbaccesses - 1);
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp_other_migrated_found) != 0) {
							stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
						/* And we delete the now virtual STAGEIN|STAGED|STAGE_RDONLY entry */
						/* Be aware that this can shift the whole catalog in memory - that's why */
						/* it has to be done at the very end of current processing */
						delreq(stcp,0);
					} else {
					not_an_HSM_castor_callback:
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
							stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
					}
				}
			}
			/* In STAGEWRT mode we have to take care that async callback may be */
			/* enabled */
			if ((callback_index < 0) || (wqp->nbdskf == 0)) {
				/* No STAGEWRT async callback : we do normal crunch of waiting files */
				/* If wqp->nbdskf the memcpy() below will not be executed... */
				/* so it will be no point to search in the else branch lower */
				i = 0;		/* reset these variables changed by the above loop */
				wfp = wqp->wf;	/* and needed in the loop below */
			} else {
				/* We search the exact index in the waiting files corresponding to the current */
				/* one */
				int iwf;
				struct waitf *iwfp;

				for (iwf = 0, iwfp = wqp->wf; iwf < (wqp->nbdskf + 1); iwf++, iwfp++) {
					if (iwfp->subreqid == save_wfp_subreqid) {
						i = iwf;
						wfp = iwfp;
						break;
					}
				}
            }
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
			if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
			if (wqp->copytape)
				sendinfo2cptape (&(wqp->rpfd), stcp);
			if (*(wfp->upath) && strcmp (stcp->ipath, wfp->upath))
				create_link (stcp, wfp->upath);
			if (wqp->Upluspath && *((wfp+1)->upath) &&
					strcmp (stcp->ipath, (wfp+1)->upath))
				create_link (stcp, (wfp+1)->upath);
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
						(signed64) (((signed64) stcp->size) - (signed64) actual_size_block));
			check_waiting_on_req (subreqid, STAGED);
		}
		savereqs();
		for ( ; i < wqp->nbdskf; i++, wfp++) {
			memcpy(wfp,wfp+1,sizeof(struct waitf));
		}
	}
	if ((rc == MNYPARI) || ((wqp->nbdskf <= 0) && ((rc == TPE_LSZ) || (rc == LIMBYSZ) || (rc == BLKSKPD)))) wqp->status = rc_shift2castor(STGMAGIC,rc);
#ifdef STAGER_DEBUG
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] procupd : Waitq->wf : \n");
		for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				char tmpbuf1[21];
				char tmpbuf2[21];

				if (wfp->subreqid == stcp->reqid) {
					switch (stcp->t_or_d) {
					case 't':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] procupd : No %3d    : VID.FSEQ=%s.%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.t.vid[0], stcp->u1.t.fseq,wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'd':
					case 'a':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] procupd : No %3d    : u1.d.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.d.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'm':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] procupd : No %3d    : u1.m.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.m.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'h':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] procupd : No %3d    : u1.h.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s, size_to_recall=%s, size_yet_recalled=%s\n",i,stcp->u1.h.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath, u64tostr((u_signed64) wfp->size_to_recall, tmpbuf1, 0), u64tostr((u_signed64) wfp->size_yet_recalled, tmpbuf2, 0));
						break;
					default:
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] procupd : No %3d    : <unknown to_or_d=%c>\n",i,stcp->t_or_d);
						break;
					}
					break;
				}
			}
		}
	}
#endif
 reply:
	if (argv != NULL) free (argv);
	if (stpp_input != NULL) free(stpp_input);
	reqid = upd_reqid;
	rpfd = upd_rpfd;
	if ((req_type == STAGE_UPDC) && (rc == ENOSPC) && (wqp != NULL) && (c == 0)) {
		/* We have successfully created a new entry in the waitq */
		wqp->status = 0;
		wqp->nb_clnreq++;
		wqp->clnreq_reqid = upd_reqid;
		wqp->clnreq_rpfd = rpfd;
		cleanpool (wqp->waiting_pool);
	} else {
		sendrep (&rpfd, STAGERC, STAGEUPDC, magic, c);
	}
}

int update_hsm_a_time(stcp)
	struct stgcat_entry *stcp;
{
	struct stat64 statbuf;
	struct Cns_fileid Cnsfileid;
	struct Cns_filestat Cstatbuf;
	int rc = 0;
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */
	
	switch (stcp->t_or_d) {
	case 'm':
		/* HPSS */
		setegid(stcp->gid);
		seteuid(stcp->uid);
		PRE_RFIO;
		rc = rfio_stat64(stcp->u1.m.xfile, &statbuf);
		setegid(start_passwd.pw_gid);
		seteuid(start_passwd.pw_uid);
		if (rc == 0) {
			stcp->a_time = statbuf.st_mtime;
		} else {
			rc = rfio_serrno();
			stglogit (func, STG02, stcp->u1.m.xfile, "rfio_stat64", rfio_serror());
		}
		break;
	case 'h':
		/* CASTOR */
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
		setegid(stcp->gid);
		seteuid(stcp->uid);
		rc = Cns_statx(stcp->u1.h.xfile, &Cnsfileid, &Cstatbuf);
		setegid(start_passwd.pw_gid);
		seteuid(start_passwd.pw_uid);
		if (rc == 0) {
			stcp->a_time = Cstatbuf.mtime;
		} else {
			rc = serrno;
			stglogit (func, STG02, stcp->u1.h.xfile, "Cns_statx", sstrerror(serrno));
		}
		break;
	default:
		break;
	}
	return(rc);
}
