/*
 * $Id: procclr.c,v 1.63 2002/06/05 13:22:34 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procclr.c,v $ $Revision: 1.63 $ $Date: 2002/06/05 13:22:34 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <grp.h>
#include <string.h>
#ifndef _WIN32
#include <unistd.h>
#endif
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif
#include "marshall.h"
#undef  unmarshall_STRING
#define unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#include "stage.h"
#include "stage_api.h"
#if SACCT
#include "../h/sacct.h"
#endif
#include "osdep.h"
#include "Cgrp.h"
#include "Cgetopt.h"
#include "Cns_api.h"
#include "rfio_api.h"
#include "Cupv_api.h"
#include "serrno.h"
#ifdef STAGER_DEBUG
#ifndef _WIN32
#include <unistd.h>
#include <sys/times.h>
struct _stage_times {
	struct tms tms;
	clock_t     time;
};
typedef struct _stage_times _stage_times_t;
#define STAGE_TIME_START(comment) { \
	char *thisfile = __FILE__; \
	int thisline = __LINE__; \
    char *thiscomment = comment; \
    _stage_times_t _stage_times_start; \
	_stage_times_t _stage_times_end; \
	long clktck; \
	_stage_times_start.time = times(&_stage_times_start.tms);
#define STAGE_TIME_END \
	_stage_times_end.time = times(&_stage_times_end.tms); \
	clktck = sysconf(_SC_CLK_TCK); \
	stglogit("timer", "[%s] Between %s:%d and %s:%d, timer gives:\n", thiscomment, thisfile, thisline, __FILE__, __LINE__); \
	stglogit("timer", "[%s] Real Time : %7.2f (%ld ticks)\n", thiscomment, (_stage_times_end.time - _stage_times_start.time) / (double) clktck, (long) (_stage_times_end.time - _stage_times_start.time) ); \
	stglogit("timer", "[%s] User Time : %7.2f\n", thiscomment, (_stage_times_end.tms.tms_utime - _stage_times_start.tms.tms_utime) / (double) clktck); \
	stglogit("timer", "[%s] Sys  Time : %7.2f\n", thiscomment, (_stage_times_end.tms.tms_stime - _stage_times_start.tms.tms_stime) / (double) clktck); \
}
#else
#define STAGE_TIME_START(comment) {}
#define STAGE_TIME_END {}
#endif
#else
#define STAGE_TIME_START(comment) {}
#define STAGE_TIME_END {}
#endif

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

void procclrreq _PROTO((int, int, char *, char *));

extern char func[16];
extern int nbcat_ent;
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgpath_entry *stpe;	/* end of stage path catalog */
extern struct stgpath_entry *stps;	/* start of stage path catalog */
extern struct waitq *waitqp;
extern int req2argv _PROTO((char *, char ***));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int stglogit _PROTO(());
extern int isvalidpool _PROTO((char *));
extern void dellink _PROTO((struct stgpath_entry *));
extern int enoughfreespace _PROTO((char *, int));
extern int checklastaccess _PROTO((char *, time_t));
extern int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int, int, int));
extern int savepath _PROTO(());
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern void rwcountersfs _PROTO((char *, char *, int, int));
extern char *stglogflags _PROTO((char *, char *, u_signed64));
extern int unpackfseq _PROTO((char *, int, char *, fseq_elem **, int, int *));
extern char *findpoolname _PROTO((char *));

int check_delete _PROTO((struct stgcat_entry *, gid_t, uid_t, char *, char *, int, int, int, char *));

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

extern u_signed64 stage_uniqueid;
extern char *localhost;  /* Fully qualified local hostname */

static int nbtpf = 0;
static int side_flag = 0;
static int reqid_flag = 0;
static int nodisk_flag = 0;

void procclrreq(req_type, magic, req_data, clienthost)
	int req_type;
	int magic;
	char *req_data;
	char *clienthost;
{
	char **argv = NULL;
	int c, i, j;
	int Fflag = 0;
	int cflag = 0;
	char *dp;
	int errflg = 0;
	int found;
	char *fseq = NULL;
	gid_t gid;
	char group[CA_MAXGRPNAMELEN + 1];
	struct group *gr;
	char *lbl = NULL;
	char *linkname = NULL;
	char *mfile = NULL;
	int gc_stop_thresh = 0;
	int nargs;
	int numvid = 0;
	char *path = NULL;
	int poolflag = 0;
	char poolname[CA_MAXPOOLNAMELEN + 1];
	char *q;
	char *rbp;
	int rflag = 0;
	int silentflag = 0;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	uid_t uid;
	char *user;
	char vid[MAXVSN][7];
	char *xfile = NULL;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
	int t_or_d, nstcp_input, nstpp_input;
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */
	int save_rpfd;
	char trailing;
	fseq_elem *fseq_list = NULL;
	int inbtpf;
	int this_reqid = 0;
	int side = 0; /* The default */
	int have_parsed_side = 0;
	static struct Coptions longopts[] =
	{
		{"conditionnal",       NO_ARGUMENT,        NULL,      'c'},
		{"force",              NO_ARGUMENT,        NULL,      'F'},
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"input",              NO_ARGUMENT,        NULL,      'i'},
		{"link_name",          REQUIRED_ARGUMENT,  NULL,      'L'},
		{"label_type",         REQUIRED_ARGUMENT,  NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"minfree",            REQUIRED_ARGUMENT,  NULL,      'm'},
		{"pathname",           REQUIRED_ARGUMENT,  NULL,      'P'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"file_range",         REQUIRED_ARGUMENT,  NULL,      'Q'},
		{"nodisk",             NO_ARGUMENT,       &nodisk_flag, 1},
		{"r",                  REQUIRED_ARGUMENT,  NULL,      'r'},
		{"reqid",              REQUIRED_ARGUMENT, &reqid_flag,  1},
		{"side",               REQUIRED_ARGUMENT, &side_flag,   1},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{NULL,                 0,                  NULL,        0}
	};

	nodisk_flag = 0;
	reqid_flag = 0;
	side_flag = 0;
	c = 0;
	save_rpfd = rpfd;
	poolname[0] = '\0';
	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	if (req_type > STAGE_00) {
		unmarshall_LONG (rbp, uid);
		unmarshall_LONG (rbp, gid);
	} else {
		unmarshall_WORD (rbp, uid);
		unmarshall_WORD (rbp, gid);
	}
#ifndef __INSURE__
	stglogit (func, STG92, (req_type > STAGE_00) ? "stage_clr" : "stageclr", user, uid, gid, clienthost);
#endif
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
			   reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif
	if ((gr = Cgetgrgid (gid)) == NULL) {
		if (errno != ENOENT) sendrep (rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = (serrno == ENOENT) ? ESTGROUP : SESYSERR;
		goto reply;
	}
	strncpy (group, gr->gr_name, CA_MAXGRPNAMELEN);
	group[CA_MAXGRPNAMELEN] = '\0';
	if (req_type > STAGE_00) {
		u_signed64 flags;
		struct stgcat_entry stcp_input;
		struct stgpath_entry stpp_input;

		unmarshall_HYPER (rbp, stage_uniqueid);
		unmarshall_HYPER (rbp, flags);
		{
			char tmpbyte;
			unmarshall_BYTE(rbp, tmpbyte);
			t_or_d = tmpbyte;
		}
		unmarshall_LONG (rbp, nstcp_input);
		unmarshall_LONG (rbp, nstpp_input);
		/* We support one nstcp_input == 1 xor nstpp_input == 1 */
		if (((nstcp_input <= 0) && (nstpp_input <= 0)) ||
			((nstcp_input >  0) && (nstpp_input >  0)) ||
			((nstcp_input >  0) && (nstcp_input != 1)) ||
            ((nstpp_input >  0) && (nstpp_input != 1))) {
			sendrep(rpfd, MSG_ERR, "STG02 - Only exactly one stcp (%d here) or one stpp (%d here) input is supported\n", nstcp_input, nstpp_input);
			c = EINVAL;
			goto reply;
        }
		if ((flags & STAGE_MULTIFSEQ) == STAGE_MULTIFSEQ) {
			if (t_or_d != 't') {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_MULTIFSEQ flag is valid only for t_or_d == 't'\n");
				c = EINVAL;
				goto reply;
			}
		}
		/* We makes sure there is only one entry input, of any type, so no memory allocation needed */
		if (nstcp_input > 0) {
			memset((void *) &stcp_input, (int) 0, sizeof(struct stgcat_entry));
			{
				char logit[BUFSIZ + 1];
				int struct_status = 0;

				stcp_input.reqid = -1;
				unmarshall_STAGE_CAT(magic, STGDAEMON_LEVEL, STAGE_INPUT_MODE, struct_status, rbp, &(stcp_input));
				if (struct_status != 0) {
					sendrep(rpfd, MSG_ERR, "STG02 - Bad catalog entry input\n");
					c = SEINTERNAL;
					goto reply;
				}
				logit[0] = '\0';
				stcp_input.t_or_d = t_or_d;
				if ((flags & STAGE_SIDE) != STAGE_SIDE) {
					/* We prepare the fact that there is no explicit side, otherwise stage_stcp2buf() would always log --side 0 */
					if (t_or_d == 't') stcp_input.u1.t.side = -1;
				}
				if (stage_stcp2buf(logit,BUFSIZ,&(stcp_input)) == 0 || serrno == SEUMSG2LONG) {
					logit[BUFSIZ] = '\0';
					stglogit("stage_clr","stcp[1/1] : %s\n",logit);
	 			}
				if ((flags & STAGE_REQID) == STAGE_REQID) {
					this_reqid = stcp_input.reqid;
				}
			}
			/* We set the flags */
			switch (t_or_d) {
			case 'd':
				xfile = stcp_input.ipath;
				break;
			case 'm':
			case 'h':
				mfile = stcp_input.u1.m.xfile;
				break;
			case 't':
				if (stcp_input.u1.t.lbl[0] != '\0') lbl = stcp_input.u1.t.lbl;
				if (stcp_input.u1.t.fseq[0] != '\0') {
					if ((flags & STAGE_MULTIFSEQ) == STAGE_MULTIFSEQ) {
						if ((nbtpf = unpackfseq (stcp_input.u1.t.fseq, STAGECLR, &trailing, &fseq_list, 0, NULL)) == 0) {
							sendrep(rpfd, MSG_ERR, "STG02 - STAGE_MULTIFSEQ option value (u1.t.fseq) invalid\n");
							c = EINVAL;
							goto reply;
						}
					} else {
						fseq = stcp_input.u1.t.fseq;
					}
				}
				for (i = 0; i < MAXVSN; i++) {
					if (stcp_input.u1.t.vid[i][0] != '\0') {
						strcpy (vid[numvid], stcp_input.u1.t.vid[i]);
						UPPER (vid[numvid]);
						numvid++;
					}
				}
				break;
			}
			if (stcp_input.poolname[0] != '\0') {
				if (strcmp (stcp_input.poolname, "NOPOOL") == 0 ||
					isvalidpool (stcp_input.poolname)) {
					strcpy (poolname, stcp_input.poolname);
				} else {
					sendrep (rpfd, MSG_ERR, STG32, stcp_input.poolname);
					c = EINVAL;
					goto reply;
				}
			}
		} else {
			int path_status = 0;
			unmarshall_STAGE_PATH(magic, STGDAEMON_LEVEL, STAGE_INPUT_MODE, path_status, rbp, &(stpp_input));
			if (path_status != 0) {
				sendrep(rpfd, MSG_ERR, "STG02 - Bad input (path input structure)\n");
				c = EINVAL;
				goto reply;
			}
			stglogit(func,"stpp[1/1] : %s\n",stpp_input.upath);
			if ((flags & STAGE_REQID) == STAGE_REQID) {
				this_reqid = stpp_input.reqid;
			}
			/* We set the flags */
			if ((flags & STAGE_LINKNAME) == STAGE_LINKNAME) linkname = stpp_input.upath;
			if ((flags & STAGE_PATHNAME) == STAGE_PATHNAME) path = stpp_input.upath;
			if ((linkname != NULL) && (path != NULL)) {
				sendrep(rpfd, MSG_ERR, "STG02 - Both STAGE_LINKNAME and STAGE_PATHNAME is not allowed\n");
				c = EINVAL;
				goto reply;
			}
		}
		if ((flags & STAGE_CONDITIONAL) == STAGE_CONDITIONAL) cflag = 1;
		if ((flags & STAGE_FORCE) == STAGE_FORCE) Fflag = 1;
		if ((flags & STAGE_REMOVEHSM) == STAGE_REMOVEHSM) rflag = 1;
		if ((flags & STAGE_NODISK) == STAGE_NODISK) nodisk_flag = 1;
		if ((flags & STAGE_SILENT) == STAGE_SILENT) {
			silentflag = 1;
			rpfd = -1;
		}
		if ((flags & STAGE_SIDE) == STAGE_SIDE) {
			side_flag++;
			if (stcp_input.t_or_d == 't') {
				if ((side = stcp_input.u1.t.side) < 0) {
					sendrep (rpfd, MSG_ERR, STG06, "u1.t.side (STAGE_SIDE in the flags)");
					c = EINVAL;
					goto reply;
				}
			} else {
				/* Hmmm.... STAGE_SIDE flag and not a 't' request might indicate something more wrong than expected */
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_SIDE flag is valid only for t_or_d == 't'\n");
				c = EINVAL;
				goto reply;
			}
		}
		/* Print the flags */
		stglogflags(func,LOGFILE,(u_signed64) flags);
	} else {
		nargs = req2argv (rbp, &argv);

		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt_long (nargs, argv, "cFGh:I:iL:l:M:m:P:p:q:Q:r:V:", longopts, NULL)) != -1) {
			switch (c) {
			case 'c':
				cflag++;
				break;
			case 'F':
				Fflag++;
				break;
			case 'G':
				break;
			case 'h':
				break;
			case 'I':
				xfile = Coptarg;
				break;
			case 'i':
				break;
			case 'L':
				linkname = Coptarg;
				break;
			case 'l':	/* label type (al, nl, sl or blp) */
				lbl = Coptarg;
				break;
			case 'M':
				mfile = Coptarg;
				break;
			case 'm':
				stage_strtoi(&gc_stop_thresh, Coptarg, &dp, 10);
				if (*dp != '\0' || gc_stop_thresh > 100) {
					sendrep (rpfd, MSG_ERR, STG06, "-m");
					errflg++;
				}
				break;
			case 'P':
				path = Coptarg;
				if (*path == '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-P");
					errflg++;
				}
				break;
			case 'p':
				if (strcmp (Coptarg, "NOPOOL") == 0 ||
					isvalidpool (Coptarg)) {
					strcpy (poolname, Coptarg);
				} else {
					sendrep (rpfd, MSG_ERR, STG32, Coptarg);
					errflg++;
				}
				break;
			case 'q':	/* file sequence number(s) */
				fseq = Coptarg;
				break;
			case 'Q':	/* file sequence range */
				/* compute number of tape files */
				if ((nbtpf = unpackfseq (Coptarg, STAGECLR, &trailing, &fseq_list, 0, NULL)) == 0)
					errflg++;
				break;
			case 'r':
				/* Coptarg is equal to emove_from_hsm */
				/* because we only allows this in the stageclr command line */
				rflag = 1;
				break;
			case 'V':	/* visual identifier(s) */
				q = strtok (Coptarg, ":");
				while (q != NULL) {
					strcpy (vid[numvid], q);
					UPPER (vid[numvid]);
					numvid++;
					q = strtok (NULL, ":");
				}
				break;
			case 0:
				if ((side_flag != 0) && (! have_parsed_side)) {  /* Not yet done */
					stage_strtoi(&side, Coptarg, &dp, 10);
					if ((*dp != '\0') || (side < 0)) {
						sendrep (rpfd, MSG_ERR, STG06, "--side");
						errflg++;
					}
					have_parsed_side = 1;
				}
				if ((reqid_flag != 0) && (! this_reqid)) {  /* Not yet done */
					stage_strtoi(&this_reqid, Coptarg, &dp, 10);
					if ((*dp != '\0') || (this_reqid < 0)) {
						sendrep (rpfd, MSG_ERR, STG06, "--reqid");
						errflg++;
					}
				}
				break;
			}
		}
	}
	/* -L linkname and -remove_from_hsm is not allowed */
	if (linkname && rflag)
		errflg++;

	if ((fseq != NULL) && (fseq_list != NULL)) {
		sendrep (rpfd, MSG_ERR, STG35, "-q", "-Q");
		errflg++;
    }

	if (errflg) {
		c = EINVAL;
		goto reply;
	}

	c = 0;
	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;
	found = 0;

	if (linkname) {
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if (this_reqid && (stpp->reqid != this_reqid)) continue;
			if (strcmp (linkname, stpp->upath) == 0) {
				found = 1;
				break;
			}
		}
		if (found) {
			dellink (stpp);
			savepath ();
		} else {
			sendrep (rpfd, MSG_ERR, STG33, linkname, "file not found");
			c = EINVAL;
			goto reply;
		}
	} else if (path) {
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if (this_reqid && (stpp->reqid != this_reqid)) continue;
			if (strcmp (path, stpp->upath) == 0) {
				found = 1;
				break;
			}
		}
		if (found) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stpp->reqid == stcp->reqid) break;
			}
			if (cflag && stcp->poolname[0] &&
				enoughfreespace (stcp->poolname, gc_stop_thresh)) {
				c = ENOUGHF;
				goto reply;
			}
			if (cflag && uid == 0 &&	/* probably garbage collector */
				checklastaccess (stcp->poolname, stcp->a_time)) {
				c = EBUSY;
				goto reply;
			}
			STAGE_TIME_START("check_delete");
			i = check_delete (stcp, gid, uid, group, user, rflag, Fflag, nodisk_flag, clienthost);
			STAGE_TIME_END;
			if (i > 0) {
				c = i;
				goto reply;
			}
		} else {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if (this_reqid && (stcp->reqid != this_reqid)) continue;
				if (strcmp (path, stcp->ipath) == 0) {
					found = 1;
					if (cflag && stcp->poolname[0] &&
						enoughfreespace (stcp->poolname, gc_stop_thresh)) {
						c = ENOUGHF;
						goto reply;
					}
					if (cflag && uid == 0 &&
						checklastaccess (stcp->poolname, stcp->a_time)) {
						c = EBUSY;
						goto reply;
					}
					STAGE_TIME_START("check_delete");
					i = check_delete (stcp, gid, uid, group, user, rflag, Fflag, nodisk_flag, clienthost);
					STAGE_TIME_END;
					if (i > 0) {
						c = i;
						goto reply;
					}
					stcp += i;
				}
			}
		}
		if (! found) {
			sendrep (rpfd, MSG_ERR, STG33, path, "file not found");
			c = EINVAL;
			goto reply;
		}
	} else {
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (this_reqid && (stcp->reqid != this_reqid)) continue;
			if (poolflag < 0) {	/* -p NOPOOL */
				if (stcp->poolname[0]) continue;
			} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
			if (numvid) {
				if (stcp->t_or_d != 't') continue;
				for (j = 0; j < numvid; j++)
					if (strcmp (stcp->u1.t.vid[0], vid[j]) == 0) break;
				if (j == numvid) continue;
			}
			if (lbl) {
				if (stcp->t_or_d != 't') continue;
				if (strcmp (lbl, stcp->u1.t.lbl)) continue;
			}
			if (fseq) {
				if (stcp->t_or_d != 't') continue;
				if (strcmp (fseq, stcp->u1.t.fseq)) continue;
			}
			if (side_flag) {
				if (stcp->t_or_d != 't') continue;
				if (side != stcp->u1.t.side) continue;
			}
			if (fseq_list) {
				for (inbtpf = 0; inbtpf < nbtpf; inbtpf++) {
					if (strcmp (stcp->u1.t.fseq, fseq_list[inbtpf]) == 0) break;
					if (stcp->u1.t.side != side) break;
				}
				if (inbtpf == nbtpf) continue;
			}
			if (xfile) {
				if (stcp->t_or_d != 'd') continue;
				if (strcmp (xfile, stcp->u1.d.xfile)) continue;
			}
			if (mfile) {
				if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') continue;
				if (stcp->t_or_d == 'm' && strcmp (mfile, stcp->u1.m.xfile)) continue;
				if (stcp->t_or_d == 'h' && strcmp (mfile, stcp->u1.h.xfile)) continue;
				if (stcp->t_or_d == 'h') {
					/* Name matched but perhaps it was... renamed ?*/
					if ((stcp->u1.h.server[0] != '\0') && (stcp->u1.h.fileid != 0)) {
						char checkpath[CA_MAXPATHLEN+1];
						if (Cns_getpath(stcp->u1.h.server, stcp->u1.h.fileid, checkpath) == 0) {
							if (strcmp(checkpath, stcp->u1.h.xfile) != 0) {
								char tmpbuf[21];
								
								sendrep (rpfd, MSG_ERR, STG157, stcp->u1.h.xfile, checkpath, u64tostr((u_signed64) stcp->u1.h.fileid, tmpbuf, 0), stcp->u1.h.server);
								strncpy(stcp->u1.h.xfile, checkpath, (CA_MAXHOSTNAMELEN+MAXPATH));
								stcp->u1.h.xfile[(CA_MAXHOSTNAMELEN+MAXPATH)] = '\0';
								continue;
							}
						}

					}
				}
			}
			found = 1;
			if (cflag && stcp->poolname[0] &&
				enoughfreespace (stcp->poolname, gc_stop_thresh)) {
				c = ENOUGHF;
				goto reply;
			}
 			STAGE_TIME_START("check_delete");
			i = check_delete (stcp, gid, uid, group, user, rflag, Fflag, nodisk_flag, clienthost);
 			STAGE_TIME_END;
			if (i > 0) {
				c = i;
				goto reply;
			}
			stcp += i;
		}
		if (! found) {
			if (mfile && rflag && Fflag) {
				/* User wanted anyway to delete the HSM file */
				/* RFIO is anyway already interfaced to the name server - we use immediately its interface */
				setegid(gid);
				seteuid(uid);
				if (ISCASTOR(mfile)) {
					if (Cns_unlink (mfile) == 0) {
						stglogit (func, STG95, mfile, user);
					} else {
						c = serrno;
						sendrep (rpfd, MSG_ERR, STG02, mfile, "Cns_unlink", sstrerror(serrno));
					}
				} else {
					PRE_RFIO;
					if (rfio_unlink (mfile) == 0) {
						stglogit (func, STG95, mfile, user);
					} else {
						int save_serrno = rfio_serrno();
						sendrep (rpfd, MSG_ERR, STG02, mfile, "rfio_unlink", rfio_serror());
						c = save_serrno;
					}
				}
				setegid(start_passwd.pw_gid);
				seteuid(start_passwd.pw_uid);
			} else {
				sendrep (rpfd, MSG_ERR, STG33, (mfile ? mfile : (xfile ? xfile : "")), "file not found");
				c = EINVAL;
			}
		}
	}
  reply:
	if (argv != NULL) free (argv);
	if (fseq_list != NULL) free(fseq_list);
	rpfd = save_rpfd;
	sendrep (rpfd, STAGERC, STAGECLR, magic, c);
}

int check_delete(stcp, gid, uid, group, user, rflag, Fflag, nodisk_flag, clienthost)
	struct stgcat_entry *stcp;
	gid_t gid;
	uid_t uid;
	char *group;
	char *user;
	int rflag; /* True if HSM source file has to be removed */
	int Fflag; /* Forces deletion of request in memory instead of internal error */
	int nodisk_flag; /* No disk access - pure catalog removal */
	char *clienthost;
{
	int found;
	int i;
	int savereqid;
	struct waitf *wfp;
	struct waitq *wqp;
	int isadmin; /* Case of granted privilege */

	/*	return	<0	request deleted
	 *		 0	running request (status set to ESTCLEARED and req signalled)
	 *		>0	in case of error
	 */

	/* We check admin privilege if necessary */
	if ((ISCASTORWAITINGMIG(stcp) || ISCASTORBEINGMIG(stcp) || ((stcp->t_or_d == 'h') && (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)) && (! ISSTAGED(stcp)))) ||
		((strcmp (group, STGGRP) != 0) && (gid != stcp->gid)) ||
		(uid != stcp->uid) ||
		(Fflag)) {
		switch (Cupv_check(uid, gid, clienthost, localhost, P_ADMIN)) {
		case 0:
			/* Yes */
			isadmin = 1;
			break;
		default:
			/* No */
			isadmin = 0;
			break;
		}
	}

    /* Special case of intermediate state or CASTOR-migration state (original record + stream) */
	if ((ISCASTORWAITINGMIG(stcp) || ISCASTORBEINGMIG(stcp) || ((stcp->t_or_d == 'h') && (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)) && (! ISSTAGED(stcp)))) && (! isadmin)) {
		sendrep (rpfd, MSG_ERR, STG33, stcp->u1.h.xfile, strerror(EBUSY));
		return (EBUSY);
	}

	if ((strcmp (group, STGGRP) != 0) && (gid != stcp->gid) && (! isadmin)) {
		sendrep (rpfd, MSG_ERR, STG33, "", "permission denied");
		return (EACCES);
	}

	if (((stcp->status & 0xF0) == STAGED) ||
		((stcp->status & (STAGEOUT|PUT_FAILED)) == (STAGEOUT|PUT_FAILED)) ||
		(stcp->status == (STAGEOUT|CAN_BE_MIGR))) {
		if (delfile (stcp, 0, 1, 1, user, uid, gid, rflag, 1, nodisk_flag) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath),
					 rfio_serror());
			return (rfio_serrno() > 0 ? rfio_serrno() : SESYSERR);
		}
	} else if (ISSTAGEOUT(stcp) || ISSTAGEALLOC(stcp)) {
		if ((stcp->status == STAGEOUT) || (stcp->status == STAGEALLOC)) {
			rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, STAGEUPDC);
		}
		if (delfile (stcp, 1, 1, 1, user, uid, gid, rflag, 1, nodisk_flag) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath),
					 rfio_serror());
			return (rfio_serrno() > 0 ? rfio_serrno() : SESYSERR);
		}
	} else {	/* the request should be in the active/wait queue */
		found = 0;
		for (wqp = waitqp; wqp; wqp = wqp->next) {
			for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
				if (wfp->subreqid == stcp->reqid) {
					found = 1;
					break;
				}
			}
			if (found) {
				savereqid = reqid;
				reqid = wqp->reqid;
				sendrep (wqp->rpfd, MSG_ERR, STG95, "request", user);
				reqid = savereqid;
				wqp->status = ESTCLEARED;
				/* is there an active stager overlay for this file? */
				if (wqp->ovl_pid) {
					stglogit (func, "killing process %d\n",
							  wqp->ovl_pid);
					kill (wqp->ovl_pid, SIGINT);
				}
				return (0);
			}
		}
		if (Fflag && isadmin) {
			sendrep (rpfd, MSG_ERR,
					 "Status=0x%x but req not in waitq - Deleting reqid %d\n",
					 stcp->status, stcp->reqid);
			if ((stcp->status & 0xF) == STAGEOUT || (stcp->status & 0xF) == STAGEALLOC) {
				if ((ISSTAGEOUT(stcp) && (! ISCASTORMIG(stcp))) || (ISSTAGEALLOC(stcp) && ((stcp->status | STAGED) != STAGED))) {
					rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, STAGEUPDC);
				}
			}
			if (delfile (stcp, 1, 1, 1, user, uid, gid, rflag, 1, nodisk_flag) < 0) {
				sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath),
						 rfio_serror());
				return (rfio_serrno() > 0 ? rfio_serrno() : SESYSERR);
			}
		} else {
			sendrep (rpfd, MSG_ERR, STG104, stcp->status);
			return (rfio_serrno() > 0 ? rfio_serrno() : SESYSERR);
		}
	}
	return (-1);
}
