/*
 * $Id: procio.c,v 1.213 2004/11/11 16:32:02 bcouturi Exp $
 */

/*
 * Copyright (C) 1993-2002 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procio.c,v $ $Revision: 1.213 $ $Date: 2004/11/11 16:32:02 $ CERN IT-DS/HSM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#else
#include <netinet/in.h>
#include <sys/time.h>
#include <unistd.h>
#endif
#include <sys/stat.h>
#include <errno.h>
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
#include "Cns_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "rfio_api.h"
#include "Cgetopt.h"
#include "u64subr.h"

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

#define SET_CORRECT_OKMODE { \
	if (have_save_stcp_for_Cns_creatx) { \
		if ((save_stcp_for_Cns_creatx.uid != stage_passwd.pw_uid) || \
			(save_stcp_for_Cns_creatx.gid != stage_passwd.pw_gid)) { \
			if ((stgreq.uid == stage_passwd.pw_uid) && (stgreq.gid == stage_passwd.pw_gid)) { \
				if ((api_out == 0) || (openmode == 0)) { \
					okmode = ( 0777 & ~ save_stcp_for_Cns_creatx.mask); \
				} else { \
					okmode = (07777 & (openmode & ~ save_stcp_for_Cns_creatx.mask)); \
				} \
			} else { \
				if ((api_out == 0) || (openmode == 0)) { \
					okmode = ( 0777 & ~ stgreq.mask); \
				} else { \
					okmode = (07777 & (openmode & ~ stgreq.mask)); \
				} \
			} \
		} else { \
			if ((api_out == 0) || (openmode == 0)) { \
				okmode = ( 0777 & ~ stgreq.mask); \
			} else { \
				okmode = (07777 & (openmode & ~ stgreq.mask)); \
			} \
		} \
	} else { \
		if ((api_out == 0) || (openmode == 0)) { \
			okmode = ( 0777 & ~ stgreq.mask); \
		} else { \
			okmode = (07777 & (openmode & ~ stgreq.mask)); \
		} \
	} \
}

#define SET_CORRECT_EUID_EGID { \
	if (have_save_stcp_for_Cns_creatx) { \
		if ((save_stcp_for_Cns_creatx.uid != stage_passwd.pw_uid) || \
			(save_stcp_for_Cns_creatx.gid != stage_passwd.pw_gid)) { \
			if ((stgreq.uid == stage_passwd.pw_uid) && (stgreq.gid == stage_passwd.pw_gid)) { \
				setegid(start_passwd.pw_gid); \
				seteuid(start_passwd.pw_uid); \
				setegid(save_stcp_for_Cns_creatx.gid); \
				seteuid(save_stcp_for_Cns_creatx.uid); \
			} else { \
				setegid(start_passwd.pw_gid); \
				seteuid(start_passwd.pw_uid); \
				setegid(stgreq.gid); \
				seteuid(stgreq.uid); \
			} \
		} else { \
			setegid(start_passwd.pw_gid); \
			seteuid(start_passwd.pw_uid); \
			setegid(stgreq.gid); \
			seteuid(stgreq.uid); \
		} \
	} else { \
		setegid(start_passwd.pw_gid); \
		seteuid(start_passwd.pw_uid); \
		setegid(stgreq.gid); \
		seteuid(stgreq.uid); \
	} \
}

#ifdef HAVE_SENSIBLE_STCP
#undef HAVE_SENSIBLE_STCP
#endif
/* This macro will check sensible part of the CASTOR HSM union */
#define HAVE_SENSIBLE_STCP(stcp) (((stcp)->u1.h.xfile[0] != '\0') && ((stcp)->u1.h.server[0] != '\0') && ((stcp)->u1.h.fileid != 0) && ((stcp)->u1.h.fileclass != 0))

extern char defpoolname[];
extern char defpoolname_in[];
extern char defpoolname_out[];
extern char func[];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgcat_entry *newreq _PROTO((int));
extern char *findpoolname _PROTO((char *));
extern u_signed64 findblocksize _PROTO((char *));

int last_tape_file;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
static char one[2] = "1";
static int silent_flag = 0;
static int nowait_flag = 0;
static int tppool_flag = 0;
static int volatile_tppool_flag = 0;
static int noretry_flag = 0;
static int nohsmcreat_flag = 0;
static int keep_flag = 0;
static int rdonly_flag = 0;
static int side_flag = 0;
static int nocopy_flag = 0;
extern struct fileclass *fileclasses;

int stage_access _PROTO((uid_t, gid_t, int, struct stat64 *));
void procioreq _PROTO((int, int, char *, char *));
void procputreq _PROTO((int, int, char *, char *));
extern int isuserlevel _PROTO((char *));
int unpackfseq _PROTO((char *, int, char *, fseq_elem **, int, int *));
extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *, int, int));
extern int ask_stageout _PROTO((int, char *, struct stgcat_entry **));
extern struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, int **, char *, char *, int));
extern int nextreqid _PROTO(());
int isstaged _PROTO((struct stgcat_entry *, struct stgcat_entry **, int, char *, int, char *, int *, int *, struct Cns_filestat *, int));
int maxfseq_per_vid _PROTO((struct stgcat_entry *, int, char *, char *));
extern int update_migpool _PROTO((struct stgcat_entry **, int, int));
extern int updfreespace _PROTO((char *, char *, int, u_signed64 *, signed64));
extern u_signed64 stage_uniqueid;
extern void getdefsize _PROTO((char *, u_signed64 *));
int check_hsm_type _PROTO((char *, int *, int *, int *, int *, char *));
int check_hsm_type_light _PROTO((char *, char *));
#ifdef hpux
int create_hsm_entry _PROTO(());
#else
int create_hsm_entry _PROTO((int *, struct stgcat_entry *, int, mode_t, int));
#endif
int checkpath _PROTO((char *, char *));
extern int stglogit _PROTO((char *, char *, ...));
extern char *stglogflags _PROTO((char *, char *, u_signed64));
extern int stglogopenflags _PROTO((char *, u_signed64));
extern int stglogtppool _PROTO((char *, char *));
extern int req2argv _PROTO((char *, char ***));
extern int sendrep _PROTO((int *, int, ...));
extern int isvalidpool _PROTO((char *));
extern int ismetapool _PROTO((char *));
extern int packfseq _PROTO((fseq_elem *, int, int, int, char, char *, int));
extern int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int, int, int));
extern void sendinfo2cptape _PROTO((int *, struct stgcat_entry *));
extern void create_link _PROTO((int *, struct stgcat_entry *, char *));
extern int build_ipath _PROTO((char *, struct stgcat_entry *, char *, int, int, mode_t));
extern void delreq _PROTO((struct stgcat_entry *, int));
extern int savepath _PROTO(());
extern int savereqs _PROTO(());
extern int cleanpool _PROTO((char *));
extern int fork_exec_stager _PROTO((struct waitq *));
extern void rmfromwq _PROTO((struct waitq *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern int euid_egid _PROTO((uid_t *, gid_t *, char *, struct migrator *, struct stgcat_entry *, struct stgcat_entry *, char **, int, int));
extern int verif_euid_egid _PROTO((uid_t, gid_t, char *, char *));
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *, int, int, int));
extern char *next_tppool _PROTO((struct fileclass *));
extern void bestnextpool_out _PROTO((char *, int));
extern void bestnextpool_from_metapool _PROTO((char *, char *, int));
extern void rwcountersfs _PROTO((char *, char *, int, int));
int stageput_check_hsm _PROTO((struct stgcat_entry *, uid_t, gid_t, int, int *, struct Cns_filestat *, int));
extern int export_hsm_option _PROTO((char *));
extern int have_another_pool_with_export_hsm_option _PROTO((char *));
extern void update_last_allocation _PROTO((char *));

#ifdef MIN
#undef MIN
#endif
#define MIN(a,b) ((a) < (b) ? (a) : (b))
#ifdef MAX
#undef MAX
#endif
#define MAX(a,b) ((a) > (b) ? (a) : (b))

#ifdef COPY_SENSIBLE_STCP
#undef COPY_SENSIBLE_STCP
#endif
/* This macro will copy sensible part of the CASTOR HSM union */
#define COPY_SENSIBLE_STCP(out,in) {               \
	strcpy((out)->u1.h.xfile, (in)->u1.h.xfile);   \
	strcpy((out)->u1.h.server, (in)->u1.h.server); \
	(out)->u1.h.fileid = (in)->u1.h.fileid;        \
	(out)->u1.h.fileclass = (in)->u1.h.fileclass;  \
	strcpy((out)->u1.h.tppool, (in)->u1.h.tppool); \
	(out)->uid = (in)->uid;                        \
	(out)->gid = (in)->gid;                        \
}

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

void procioreq(req_type, magic, req_data, clienthost)
	int req_type;
	int magic;
	char *req_data;
	char *clienthost;
{
	int Aflag = 0;
	int actual_poolflag;
	char actual_poolname[CA_MAXPOOLNAMELEN + 1];
	char **argv = NULL;
	int c = 0, i, j, iokstagewrt;
	int concat_off = 0;
	int concat_off_fseq = 0;
	int clientpid;
	static char cmdnames[4][9] = {"", "stagein", "stageout", "stagewrt"};
	static char cmdnames_api[4][10] = {"", "stage_in", "stage_out", "stage_wrt"};
	int copytape = 0;
	char *dp;
	int errflg = 0;
	char *fid = NULL;
	struct stat64 filemig_stat;          /* For non-CASTOR HSM stat() */
	int have_Cnsfilestat;
	struct Cns_filestat Cnsfilestat;   /* For     CASTOR hsm stat() */
	struct Cns_fileid Cnsfileid;       /* For     CASTOR hsm IDs */
	struct stat64 *yetdone_rfio_stat;
	char *yetdone_rfio_stat_path;
	char *fseq = NULL;
	fseq_elem *fseq_list = NULL;
	struct group *gr;
	struct passwd *pw;
	char **hsmfiles = NULL;
#ifndef NON_WORD_READABLE_SUPPORT
	struct stat64 *hsmstat = NULL;
#endif
	int *hsmpoolflags = NULL;
	int ihsmfiles;
	int jhsmfiles;
	char *name;
	int nargs = 0;
	int nbdskf;
	int nbtpf;
	int nhsmfiles = 0;
	int nuserlevel = 0;
	int nexplevel = 0;
	int no_upath = 0;
	char *nread = NULL;
	int numvid, numvsn;
	char *p, *q;
	char *pool_user = NULL;
	char *poolname_exclusion;
	int poolflag = 0;
	char *rbp;
	int savereqid;
	char *size = NULL;
	struct stat64 st;
	struct stgcat_entry *stcp;
	struct stgcat_entry stgreq;
	char trailing;
	int Uflag = 0;
	int Upluspath = 0;
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;
	struct waitf *wfp;
	int *save_subreqid = NULL;
	struct waitq *wqp = NULL;
	int nhpssfiles = 0;
	int ncastorfiles = 0;
	char *User;
	char t_or_d;
	int  nstcp_input, struct_status;
	int  nstpp_input, path_status;
	u_signed64  flags = 0;
	mode_t openmode = 0;
	int openflags = 0;
	struct stgcat_entry *stcp_input = NULL;
	struct stgpath_entry *stpp_input = NULL;
	uid_t save_uid;
	gid_t save_gid;
#if defined(_WIN32)
	int save_mask;
#else
	mode_t	save_mask;
#endif
	int api_out = 0;
	char save_group[CA_MAXGRPNAMELEN+1];
	char save_user[CA_MAXUSRNAMELEN+1];
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */
	extern struct passwd stage_passwd;             /* Generic uid/gid stage:st */
	int have_tppool = 0;
	char *tppool = NULL;
	int side = 0; /* Default value */
	int have_parsed_side = 0;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
	uid_t euid = 0;
	gid_t egid = 0;
	uid_t uid_waitq;
	gid_t gid_waitq;
	char user_waitq[CA_MAXUSRNAMELEN+1];
	char group_waitq[CA_MAXGRPNAMELEN+1];
	int global_c_stagewrt = 0;
	int global_c_stagewrt_SYERR = 0;
	int global_c_stagewrt_last_serrno = 0;
	int isstaged_rc;
	int isstaged_nomore = 0;
	char this_tppool[CA_MAXPOOLNAMELEN+1];
	int save_rpfd = -1;

	static struct Coptions longopts[] =
	{
		{"allocation_mode",    REQUIRED_ARGUMENT,  NULL,      'A'},
		{"blocksize",          REQUIRED_ARGUMENT,  NULL,      'b'},
		{"charconv",           REQUIRED_ARGUMENT,  NULL,      'C'},
		{"concat",             REQUIRED_ARGUMENT,  NULL,      'c'},
		{"density",            REQUIRED_ARGUMENT,  NULL,      'd'},
		{"error_action",       REQUIRED_ARGUMENT,  NULL,      'E'},
		{"format",             REQUIRED_ARGUMENT,  NULL,      'F'},
		{"fid",                REQUIRED_ARGUMENT,  NULL,      'f'},
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"group_device",       REQUIRED_ARGUMENT,  NULL,      'g'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"keep",               NO_ARGUMENT,        NULL,      'K'},
		{"record_length",      REQUIRED_ARGUMENT,  NULL,      'L'},
		{"label_type",         REQUIRED_ARGUMENT,  NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"nread",              REQUIRED_ARGUMENT,  NULL,      'N'},
		{"new_fid",            NO_ARGUMENT,        NULL,      'n'},
		{"old_fid",            NO_ARGUMENT,        NULL,      'o'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"tape_server",        REQUIRED_ARGUMENT,  NULL,      'S'},
		{"side",               REQUIRED_ARGUMENT,  &side_flag,  1},
		{"size",               REQUIRED_ARGUMENT,  NULL,      's'},
		{"trailer_label_off",  NO_ARGUMENT,        NULL,      'T'},
		{"retention_period",   REQUIRED_ARGUMENT,  NULL,      't'},
		{"fortran_unit",       REQUIRED_ARGUMENT,  NULL,      'U'},
		{"user",               REQUIRED_ARGUMENT,  NULL,      'u'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"vsn",                REQUIRED_ARGUMENT,  NULL,      'v'},
		{"xparm",              REQUIRED_ARGUMENT,  NULL,      'X'},
		{"copytape",           NO_ARGUMENT,        NULL,      'z'},
		{"silent",             NO_ARGUMENT,  &silent_flag,      1},
		{"nowait",             NO_ARGUMENT,  &nowait_flag,      1},
		{"nocopy",             NO_ARGUMENT,  &nocopy_flag,      1},
		{"noretry",            NO_ARGUMENT,  &noretry_flag,      1},
		{"tppool",             REQUIRED_ARGUMENT, &tppool_flag, 1},
		{"volatile_tppool",    NO_ARGUMENT,  &volatile_tppool_flag, 1},
		{"nohsmcreat",         NO_ARGUMENT,  &nohsmcreat_flag,  1},
		{"rdonly",             NO_ARGUMENT,  &rdonly_flag,      1},
		{NULL,                 0,                  NULL,        0}
	};

	STAGE_TIME_START("procioreq");
	
	memset ((char *)&stgreq, 0, sizeof(stgreq));
	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	strncpy (stgreq.user, user, CA_MAXUSRNAMELEN);
	stgreq.user[CA_MAXUSRNAMELEN] = '\0';
	strcpy (save_user, stgreq.user);

	silent_flag = 0;
	nowait_flag = 0;
	nocopy_flag = 0;
	noretry_flag = 0;
	tppool_flag = 0;
	volatile_tppool_flag = 0;
	nohsmcreat_flag = 0;
	keep_flag = 0;
	rdonly_flag = 0;
	this_tppool[0] = '\0';
	side_flag = 0;

	local_unmarshall_STRING (rbp, name);
	if (req_type > STAGE_00) {
		if (magic == STGMAGIC) {
			/* First version of the API */
			unmarshall_LONG (rbp, stgreq.uid);
			unmarshall_LONG (rbp, stgreq.gid);
			unmarshall_LONG (rbp, stgreq.mask);
			unmarshall_LONG (rbp, clientpid);
		} else if (magic >= STGMAGIC2) {
			/* Second and more version of the API (header) */
			unmarshall_LONG (rbp, stgreq.uid);
			unmarshall_LONG (rbp, stgreq.gid);
			unmarshall_LONG (rbp, stgreq.mask);
			unmarshall_LONG (rbp, clientpid);
			unmarshall_HYPER (rbp, stage_uniqueid);
		}
	} else {
		unmarshall_WORD (rbp, stgreq.uid);
		unmarshall_WORD (rbp, stgreq.gid);
		unmarshall_WORD (rbp, stgreq.mask);
		unmarshall_WORD (rbp, clientpid);
	}
	save_uid = stgreq.uid;
	save_gid = stgreq.gid;
	save_mask = stgreq.mask;

#ifndef __INSURE__
	stglogit (func, STG92, req_type == STAGECAT ? "stagecat" : (req_type == STAGE_CAT ? "stage_cat" : (req_type < STAGE_00 ? cmdnames[req_type] : cmdnames_api[req_type - STAGE_00])),
			  stgreq.user, stgreq.uid, stgreq.gid, clienthost);
#else
    {
		char *stagecat = "stagecat";
		char *stage_cat = "stage_cat";
		char *this;
		switch (req_type) {
		case STAGECAT:
			this = stagecat;
			break;
		case STAGE_CAT:
			this = stage_cat;
			break;
		default:
			if (req_type < STAGE_00) {
				this = cmdnames[req_type];
			} else {
				this = cmdnames_api[req_type - STAGE_00];
			}
			break;
		}
		stglogit (func, STG92, this, stgreq.user, stgreq.uid, stgreq.gid, clienthost);
    }
#endif
#if SACCT
	stageacct (STGCMDR, stgreq.uid, stgreq.gid, clienthost,
			   reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif

	if (req_type > STAGE_00) {
		int reqid_flag = 0;
		
		/* This is coming from the API */
		api_out = 1;
		local_unmarshall_STRING(rbp, User);
		if (User[0] != '\0') pool_user = User;
		unmarshall_HYPER(rbp, flags);
		/* We have to trap silent_flag right now */
		if ((flags & STAGE_SILENT) == STAGE_SILENT) {
			save_rpfd = rpfd;
			rpfd = -1; /* From now on: silent mode */
		}
		if ((flags & STAGE_REQID) == STAGE_REQID) reqid_flag = 1;
		unmarshall_LONG(rbp, openflags);
		unmarshall_LONG(rbp, openmode);
		{
			char tmpbyte;
			unmarshall_BYTE(rbp, tmpbyte);
			t_or_d = tmpbyte;
		}
		unmarshall_LONG(rbp, nstcp_input);
		unmarshall_LONG(rbp, nstpp_input);
		if ((nstpp_input > 0) && ((flags & STAGE_COFF) == STAGE_COFF)) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Link file structures (%d of them) is not allowed in concatenation off mode\n", nstpp_input);
			c = EINVAL;
			goto reply;
		}
		if (nstcp_input <= 0) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid number of input structure (%d)\n", nstcp_input);
			c = EINVAL;
			goto reply;
		}
		if ((nstpp_input > 0) && (nstpp_input != nstcp_input)) {
			sendrep(&rpfd, MSG_ERR, "STG02 - Number of link file structures (%d) must match number of disk file structures (%d)\n",nstpp_input,nstcp_input);
			c = EINVAL;
			goto reply;
		}
		if ((stcp_input = (struct stgcat_entry *) calloc(nstcp_input,sizeof(struct stgcat_entry))) == NULL ||
			(nstpp_input > 0  && (stpp_input = (struct stgpath_entry *) calloc(nstpp_input,sizeof(struct stgpath_entry))) == NULL)) {
			sendrep(&rpfd, MSG_ERR, "STG02 - memory allocation error (%s)\n", strerror(errno));
			c = (api_out != 0) ? SEINTERNAL : SESYSERR;
			goto reply;
		}
		if ((hsmpoolflags = (int *) malloc((size_t) (nstcp_input * sizeof(int)))) == NULL) {
			sendrep(&rpfd, MSG_ERR, "STG02 - memory allocation error (%s)\n", strerror(errno));
			c = (api_out != 0) ? SEINTERNAL : SESYSERR;
			goto reply;
		}
		for (i = 0; i < nstcp_input; i++) {
			char logit[BUFSIZ + 1];

			struct_status = 0;
			unmarshall_STAGE_CAT(magic,STGDAEMON_LEVEL,STAGE_INPUT_MODE, struct_status, rbp, &(stcp_input[i]));
			if (struct_status != 0) {
				sendrep(&rpfd, MSG_ERR, "STG02 - Bad input (catalog input structure No %d/%d)\n", ++i, nstcp_input);
				c = SEINTERNAL;
				goto reply;
			}
			if ((t_or_d == 'm') || (t_or_d == 'h')) {
				/* Note - per construction u1.m.xfile and u1.h.xfile points to same area... */
				if ((c = check_hsm_type_light(stcp_input[i].u1.m.xfile,&(stcp_input[i].t_or_d))) != 0) {
					sendrep(&rpfd, MSG_ERR, "STG02 - Bad input (catalog input structure No %d/%d)\n", ++i, nstcp_input);
					goto reply;
				}
				if (stcp_input[i].t_or_d == 'h') {
					/* mintime and retenp are ignored through this interface */
					stcp_input[i].u1.h.retenp_on_disk = -1;
					stcp_input[i].u1.h.mintime_beforemigr = -1;
				}
			}
			logit[0] = '\0';
			if (! reqid_flag) stcp_input[i].reqid = 0; /* Disable explicitely eventual reqid in the protocol */
			if ((stage_stcp2buf(logit,BUFSIZ,&(stcp_input[i])) == 0) || (serrno == SEUMSG2LONG)) {
				logit[BUFSIZ] = '\0';
				stglogit(func,"stcp[%d/%d] :%s\n",i+1,nstcp_input,logit);
 			}
			if (stcp_input[i].poolname[0] == '\0') {
				/* poolname was not specified explicitely for this structure */
				switch (req_type) {
				case STAGE_IN:
					if (((openflags & O_RDWR) == O_RDWR) || ((openflags & O_WRONLY) == O_WRONLY)) {
						/* Either defpoolname_out is a metapool, either it is not */
						if (ismetapool(defpoolname_out)) {
							strcpy(stcp_input[i].poolname,defpoolname_out);
							strcpy(stgreq.poolname,stcp_input[i].poolname);
							/* We chose what is the best pool within the ones that have that metapool */
							bestnextpool_from_metapool(stcp_input[i].poolname,stgreq.poolname,WRITE_MODE);
							if (strcmp(stcp_input[i].poolname,stgreq.poolname) != 0) {
								stglogit(func,STG180,stcp_input[i].poolname,stgreq.poolname);
							}
							strcpy(stcp_input[i].poolname,stgreq.poolname);
							/* We do as if user's said explicitely -p metapool */
							hsmpoolflags[i] = 1;
						} else {
							bestnextpool_out(stcp_input[i].poolname,WRITE_MODE);
							strcpy(stgreq.poolname,stcp_input[i].poolname);
							hsmpoolflags[i] = 0;
						}
					} else {
						if (ismetapool(defpoolname_in)) {
							strcpy(stcp_input[i].poolname,defpoolname_in);
							strcpy(stgreq.poolname,stcp_input[i].poolname);
							/* We chose what is the best pool within the ones that have that metapool */
							bestnextpool_from_metapool(stcp_input[i].poolname,stgreq.poolname,READ_MODE);
							if (strcmp(stcp_input[i].poolname,stgreq.poolname) != 0) {
								stglogit(func,STG180,stcp_input[i].poolname,stgreq.poolname);
							}
							strcpy(stcp_input[i].poolname,stgreq.poolname);
							/* We do as if user's said explicitely -p metapool */
							hsmpoolflags[i] = 1;
						} else {
							strcpy(stcp_input[i].poolname,defpoolname_in);
							strcpy(stgreq.poolname,stcp_input[i].poolname);
							hsmpoolflags[i] = 0;
						}
					}
					break;
				case STAGE_OUT:
					/* Either defpoolname_out is a metapool, either it is not */
					if (ismetapool(defpoolname_out)) {
						strcpy(stcp_input[i].poolname,defpoolname_out);
						strcpy(stgreq.poolname,stcp_input[i].poolname);
						/* We chose what is the best pool within the ones that have that metapool */
						bestnextpool_from_metapool(stcp_input[i].poolname,stgreq.poolname,WRITE_MODE);
						if (strcmp(stcp_input[i].poolname,stgreq.poolname) != 0) {
							stglogit(func,STG180,stcp_input[i].poolname,stgreq.poolname);
						}
						strcpy(stcp_input[i].poolname,stgreq.poolname);
						/* We do as if user's said explicitely -p metapool */
						hsmpoolflags[i] = 1;
					} else {
						bestnextpool_out(stcp_input[i].poolname,WRITE_MODE);
						strcpy(stgreq.poolname,stcp_input[i].poolname);
						hsmpoolflags[i] = 0;
					}
					break;
				default:
					hsmpoolflags[i] = 0;
					break;
				}
			} else {
				if (! (isvalidpool (stcp_input[i].poolname) ||
					   ismetapool  (stcp_input[i].poolname) ||
					   strcmp (stcp_input[i].poolname, "NOPOOL") == 0)
					) {
					sendrep (&rpfd, MSG_ERR, STG32, stcp_input[i].poolname);
					c = EINVAL;
					goto reply;
				}
				if (strcmp (stcp_input[i].poolname, "NOPOOL") == 0) {
					hsmpoolflags[i] = -1;
					stcp_input[i].poolname[0] = '\0';
				} else {
					if (ismetapool(stcp_input[i].poolname)) {
						/* We chose what is the best pool within the ones that have that metapool */
						bestnextpool_from_metapool(stcp_input[i].poolname,stgreq.poolname,(req_type == STAGE_IN) ? READ_MODE : WRITE_MODE);
						if (strcmp(stcp_input[i].poolname,stgreq.poolname) != 0) {
							stglogit(func,STG180,stcp_input[i].poolname,stgreq.poolname);
						}
						strcpy(stcp_input[i].poolname,stgreq.poolname);
					}
					hsmpoolflags[i] = 1;
				}
			}
		}
		for (i = 0; i < nstpp_input; i++) {
			path_status = 0;
			unmarshall_STAGE_PATH(magic, STGDAEMON_LEVEL,STAGE_INPUT_MODE, path_status, rbp, &(stpp_input[i]));
			if (path_status != 0) {
				sendrep(&rpfd, MSG_ERR, "STG02 - Bad input (path input structure No %d/%d)\n", ++i, nstpp_input);
				c = SEINTERNAL;
				goto reply;
			}
			stglogit(func,"stpp[%d/%d] : %s\n",i+1,nstpp_input,stpp_input[i].upath);
		}
	} else {
		nargs = req2argv (rbp, &argv);
	}

	if (stage_util_validuser(NULL,(uid_t) stgreq.uid,(gid_t) stgreq.gid) != 0) {
		char uidstr[8];
		sprintf (uidstr, "%d", stgreq.uid);
		sendrep (&rpfd, MSG_ERR, STG11, uidstr);
		c = (api_out != 0) ? serrno : SESYSERR;
		goto reply;
	}
	if ((gr = Cgetgrgid (stgreq.gid)) == NULL) {
		if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (&rpfd, MSG_ERR, STG36, stgreq.gid);
		c = (api_out != 0) ? ESTGROUP : SESYSERR;
		goto reply;
	}
	strncpy (stgreq.group, gr->gr_name, CA_MAXGRPNAMELEN);
	stgreq.group[CA_MAXGRPNAMELEN] = '\0';
	strcpy (save_group, stgreq.group);

	numvid = 0;
	numvsn = 0;

	if (api_out != 0) {
		/* This comes for the API */
		switch (t_or_d) {
		case 't':
		case 'd':
		case 'a':
		case 'm':
		case 'h':
			break;
		default:
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid structure identifier ('%c')\n", t_or_d);
			errflg++;
			break;
 		}
		if (! (((openflags & O_RDWR) == O_RDWR) || ((openflags & O_WRONLY) == O_WRONLY))) rdonly_flag = 1;
		if ((flags & STAGE_DEFERRED) == STAGE_DEFERRED) Aflag = 1;
		if ((flags & STAGE_COFF)  == STAGE_COFF) concat_off = 1;
		if ((flags & STAGE_UFUN)  == STAGE_UFUN) Uflag = 1;
		if ((flags & STAGE_INFO)  == STAGE_INFO) copytape = 1;
		if ((flags & STAGE_SILENT)  == STAGE_SILENT) silent_flag = 1;
		if ((flags & STAGE_NORETRY)  == STAGE_NORETRY) noretry_flag = 1;
		if ((flags & STAGE_NOWAIT)  == STAGE_NOWAIT) nowait_flag = 1;
		if ((flags & STAGE_NOHSMCREAT)  == STAGE_NOHSMCREAT) nohsmcreat_flag = 1;
		if ((flags & STAGE_KEEP)  == STAGE_KEEP) keep_flag = 1;
		if ((flags & STAGE_VOLATILE_TPPOOL)  == STAGE_VOLATILE_TPPOOL) volatile_tppool_flag = 1;
		if ((concat_off != 0) && (req_type != STAGE_IN)) {
			sendrep (&rpfd, MSG_ERR, STG17, "-c off", "any request but stage_in");
			errflg++;
 		}
		if ((Aflag != 0) && (req_type != STAGE_IN) && (req_type != STAGE_WRT)) {
			sendrep (&rpfd, MSG_ERR, STG17, "-A deferred", "any request but stage_in or stage_wrt");
			errflg++;
 		}
		if ((Uflag != 0) && (req_type == STAGE_CAT)) {
			sendrep (&rpfd, MSG_ERR, STG17, "-U", "stage_cat");
			errflg++;
		}
		if (concat_off != 0) {
			if (((nstcp_input != 1) || (nstpp_input != 0))) {
				sendrep (&rpfd, MSG_ERR, STG17, "-c off", "stage_in and something else but exactly one disk file structure in input");
				errflg++;
			} else if (t_or_d != 't') {
				sendrep (&rpfd, MSG_ERR, STG17, "-c off", "non-tape structure in input (t_or_d != 't')");
				errflg++;
			} else if (stcp_input[0].u1.t.fseq[strlen(stcp_input[0].u1.t.fseq) -1] != '-') {
				sendrep (&rpfd, MSG_ERR, STG17, "-c off", "tape structure with fseq not ending with '-'");
				errflg++;
			} else if (stcp_input[0].u1.t.fid[0] != '\0') {
				sendrep (&rpfd, MSG_ERR, STG17, "-f <fid>", "stage_in -c off");
				errflg++;
			}
		}
		for (i = 0; i < nstcp_input; i++) {
			int ivid, ivsn;
			switch (t_or_d) {
			case 't':
				if (stcp_input[i].recfm[0] != '\0') {
					if (strcmp (stcp_input[i].recfm, "F")     && strcmp (stcp_input[i].recfm, "FB") &&
						strcmp (stcp_input[i].recfm, "FBS")   && strcmp (stcp_input[i].recfm, "FS") &&
						strcmp (stcp_input[i].recfm, "U")     && strcmp (stcp_input[i].recfm, "U,bin") &&
						strcmp (stcp_input[i].recfm, "U,f77") && strcmp (stcp_input[i].recfm, "F,-f77")) {
						sendrep (&rpfd, MSG_ERR, STG06, "-F");
						errflg++;
					}
				}
				if (((req_type == STAGE_WRT) || (req_type == STAGE_OUT)) && keep_flag) {
					stcp_input[i].keep = 1;
				}
				if ((req_type == STAGE_IN) && Aflag && stcp_input[i].size <= 0) {
					getdefsize(stcp_input[i].poolname,&(stcp_input[i].size));
				}
				if (stcp_input[i].u1.t.lbl[0] != '\0') {
					if (! (strcmp(stcp_input[i].u1.t.lbl, "blp") || (req_type != STAGE_OUT && req_type != STAGE_WRT))) {
						sendrep (&rpfd, MSG_ERR, STG17, "-l blp", "stage_out/stage_wrt");
						errflg++;
					}
				}
				if (stcp_input[i].recfm[0] == 'F' && req_type != STAGE_IN && stcp_input[i].lrecl == 0) {
					sendrep (&rpfd, MSG_ERR, STG20);
					errflg++;
				}
				for (ivid = 0; ivid < MAXVSN; ivid++) {
					if (stcp_input[i].u1.t.vid[ivid][0] != '\0') {
						if (stgreq.u1.t.vid[ivid][0] == '\0') {
							strcpy(stgreq.u1.t.vid[ivid],stcp_input[i].u1.t.vid[ivid]);
							numvid++;
						} else if (strcmp(stgreq.u1.t.vid[ivid],stcp_input[i].u1.t.vid[ivid]) != 0) {
							sendrep (&rpfd, MSG_ERR, "STG02 - You cannot change VID list from one structure (%s) to another (%s)\n",stgreq.u1.t.vid[ivid],stcp_input[i].u1.t.vid[ivid]);
							errflg++;
							break;
						}
					}
				}
				for (ivsn = 0; ivsn < MAXVSN; ivsn++) {
					if (stcp_input[i].u1.t.vsn[ivsn][0] != '\0') {
						if (stgreq.u1.t.vsn[ivsn][0] == '\0') {
							strcpy(stgreq.u1.t.vsn[ivsn],stcp_input[i].u1.t.vsn[ivsn]);
							numvsn++;
						} else if (strcmp(stgreq.u1.t.vsn[ivsn],stcp_input[i].u1.t.vsn[ivsn]) != 0) {
							sendrep (&rpfd, MSG_ERR, "STG02 - You cannot change VSN list from one structure (%s) to another (%s)\n",stgreq.u1.t.vsn[ivsn],stcp_input[i].u1.t.vsn[ivsn]);
							errflg++;
							break;
						}
					}
				}
				if (stcp_input[i].u1.t.lbl[0] != '\0') {
					if (stgreq.u1.t.lbl[0] == '\0') {
						strcpy(stgreq.u1.t.lbl,stcp_input[i].u1.t.lbl);
					} else if (strcmp(stgreq.u1.t.lbl,stcp_input[i].u1.t.lbl) != 0) {
						sendrep (&rpfd, MSG_ERR, "STG02 - You cannot change label from one structure (%s) to another (%s)\n",stgreq.u1.t.lbl,stcp_input[i].u1.t.lbl);
						errflg++;
						break;
					}
				}
				if (stcp_input[i].u1.t.dgn[0] != '\0') {
					if (stgreq.u1.t.dgn[0] == '\0') {
						strcpy(stgreq.u1.t.dgn,stcp_input[i].u1.t.dgn);
					} else if (strcmp(stgreq.u1.t.dgn,stcp_input[i].u1.t.dgn) != 0) {
						sendrep (&rpfd, MSG_ERR, "STG02 - You cannot change device group from one structure (%s) to another (%s)\n",stgreq.u1.t.dgn,stcp_input[i].u1.t.dgn);
						errflg++;
						break;
					}
				}
				if (stcp_input[i].u1.t.den[0] != '\0') {
					if (stgreq.u1.t.den[0] == '\0') {
						strcpy(stgreq.u1.t.den,stcp_input[i].u1.t.den);
					} else if (strcmp(stgreq.u1.t.den,stcp_input[i].u1.t.den) != 0) {
						sendrep (&rpfd, MSG_ERR, "STG02 - You cannot change density from one structure (%s) to another (%s)\n",stgreq.u1.t.den,stcp_input[i].u1.t.den);
						errflg++;
						break;
					}
				}
				if (stcp_input[i].u1.t.tapesrvr[0] != '\0') {
					if (stgreq.u1.t.tapesrvr[0] == '\0') {
						strcpy(stgreq.u1.t.tapesrvr,stcp_input[i].u1.t.tapesrvr);
					} else if (strcmp(stgreq.u1.t.tapesrvr,stcp_input[i].u1.t.tapesrvr) != 0) {
						sendrep (&rpfd, MSG_ERR, "STG02 - You cannot change tape server from one structure (%s) to another (%s)\n",stgreq.u1.t.tapesrvr,stcp_input[i].u1.t.tapesrvr);
						errflg++;
						break;
					}
				}
				break;
			case 'm':
			case 'h':
				if (nhsmfiles == 0) {
					if ((hsmfiles   = (char **)             malloc(sizeof(char *))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "malloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
#ifndef NON_WORD_READABLE_SUPPORT
					if ((hsmstat   = (struct stat64 *)        malloc(sizeof(struct stat64))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "malloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
#endif
				} else {
					char **dummy = hsmfiles;
#ifndef NON_WORD_READABLE_SUPPORT
					struct stat64 *dummy2 = hsmstat;
#endif
					if ((dummy  = (char **)             realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "realloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
					hsmfiles = dummy;
#ifndef NON_WORD_READABLE_SUPPORT
					if ((dummy2  = (struct stat64 *)    realloc(hsmstat,(nhsmfiles+1) * sizeof(struct stat64))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "realloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
					hsmstat = dummy2;
#endif
				}
                /*
				  if (Aflag && (stcp_input[i].size != 0) && (req_type == STAGE_IN)) {
				  sendrep (&rpfd, MSG_ERR, STG35, "-A deffered", "-s <size_in_MB>");
				  errflg++;
				  }
                */
				hsmfiles[nhsmfiles] = (t_or_d == 'm' ? stcp_input[i].u1.m.xfile : stcp_input[i].u1.h.xfile);
#ifndef NON_WORD_READABLE_SUPPORT
				memset(&hsmstat[nhsmfiles],0,sizeof(struct stat64));
#endif
				nhsmfiles++;
				if ((c = check_hsm_type(hsmfiles[nhsmfiles - 1],&nhpssfiles,&ncastorfiles,&nuserlevel,&nexplevel,&t_or_d)) != 0) {
					errflg++;
				}
				break;
			default:
				break;
			}
			if (errflg != 0) break;
		}
		/* Print the general flags */
		stglogflags(func,LOGFILE,(u_signed64) flags);
		/* Print the open flags (meaningful, currently, only if stage_in) */
		if (req_type == STAGE_IN) stglogopenflags(func,(u_signed64) openflags);
	} else {
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt_long (nargs, argv, "A:b:C:c:d:E:F:f:Gg:h:I:KL:l:M:N:nop:q:S:s:Tt:U:u:V:v:X:z", longopts, NULL)) != -1) {
			switch (c) {
			case 'A':	/* allocation mode */
				if (strcmp (Coptarg, "deferred") == 0) {
					if (req_type != STAGEIN && req_type != STAGEWRT) {
						/* We will better check afterwards for STAGEWRT */
						/* this depends on other option -M existence */
						sendrep (&rpfd, MSG_ERR, STG17, "-A deferred",
								 "stageout/stagecat");
						errflg++;
					} else
						Aflag = 1; /* deferred space allocation */
				} else if (strcmp (Coptarg, "immediate")) {
					sendrep (&rpfd, MSG_ERR, STG06, "-A");
					errflg++;
				}
				break;
			case 'b':
				stage_strtoi(&(stgreq.blksize), Coptarg, &dp, 10);
				if (*dp != '\0' || stgreq.blksize == 0) {
					sendrep (&rpfd, MSG_ERR, STG06, "-b");
					errflg++;
				}
				break;
			case 'C':	/* character conversion */
				p = strtok (Coptarg,",") ;
				while (p != NULL) {
					if (strcmp (p, "ebcdic") == 0) {
						stgreq.charconv |= EBCCONV;
					} else if (strcmp (p, "block") == 0) {
						stgreq.charconv |= FIXVAR;
					} else if (strcmp (p, "ascii") == 0) {
						stgreq.charconv |= ASCCONV;
					} else {
						sendrep (&rpfd, MSG_ERR, STG06, "-C");
						errflg++;
						break;
					}
					if ((p = strtok (NULL, ",")) != NULL) *(p - 1) = ',';
				}
				break;
			case 'c':	/* concatenation */
				if (strcmp (Coptarg, "off") == 0) {
					if (req_type != STAGEIN){
						sendrep (&rpfd, MSG_ERR, STG17, "-c off",
								 "stageout/stagewrt/stagecat");
						errflg++;
					} else
						concat_off = 1;
				} else if (strcmp (Coptarg, "on")) {
					sendrep (&rpfd, MSG_ERR, STG06, "-c");
					errflg++;
				}
				break;
			case 'd':
				strcpy (stgreq.u1.t.den, Coptarg);
				break;
			case 'E':
				if (strcmp (Coptarg, "skip") == 0)
					stgreq.u1.t.E_Tflags |= SKIPBAD;
				else if (strcmp (Coptarg, "keep") == 0)
					stgreq.u1.t.E_Tflags |= KEEPFILE;
				else if (strcmp (Coptarg, "ignoreeoi") == 0)
					stgreq.u1.t.E_Tflags |= IGNOREEOI;
				else {
					sendrep (&rpfd, MSG_ERR, STG06, "-E");
					errflg++;
				}
				break;
			case 'F':
				if (strcmp (Coptarg, "F") && strcmp (Coptarg, "FB") &&
					strcmp (Coptarg, "FBS") && strcmp (Coptarg, "FS") &&
					strcmp (Coptarg, "U") && strcmp (Coptarg, "U,bin") &&
					strcmp (Coptarg, "U,f77") && strcmp (Coptarg, "F,-f77")) {
					sendrep (&rpfd, MSG_ERR, STG06, "-F");
					errflg++;
				} else
					strncpy (stgreq.recfm, Coptarg, 3);
				break;
			case 'f':
				fid = Coptarg;
				UPPER (fid);
				break;
			case 'G':
				break;
			case 'g':
				strcpy (stgreq.u1.t.dgn, Coptarg);
				break;
			case 'h':
				break;
			case 'I':
				stgreq.t_or_d = 'd';
				strcpy (stgreq.u1.d.xfile, Coptarg);
				break;
			case 'K':
				stgreq.keep = 1;
				break;
			case 'L':
				stage_strtoi(&(stgreq.lrecl), Coptarg, &dp, 10);
				if (*dp != '\0' || stgreq.lrecl == 0) {
					sendrep (&rpfd, MSG_ERR, STG06, "-L");
					errflg++;
				}
				break;
			case 'l':
				if (strcmp (Coptarg, "blp") ||
					(req_type != STAGEOUT && req_type != STAGEWRT))
					strcpy (stgreq.u1.t.lbl, Coptarg);
				else {
					sendrep (&rpfd, MSG_ERR, STG17, "-l blp", "stageout/stagewrt");
					errflg++;
				}
				break;
			case 'M':
				stgreq.t_or_d = 'm';
				if (nhsmfiles == 0) {
					if ((hsmfiles   = (char **)             malloc(sizeof(char *))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "malloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
#ifndef NON_WORD_READABLE_SUPPORT
					if ((hsmstat   = (struct stat64 *)      malloc(sizeof(struct stat64))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "malloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
#endif
				} else {
					char **dummy = hsmfiles;
#ifndef NON_WORD_READABLE_SUPPORT
					struct stat64 *dummy2 = hsmstat;
#endif
					if ((dummy  = (char **)             realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "realloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
					hsmfiles = dummy;
#ifndef NON_WORD_READABLE_SUPPORT
					if ((dummy2  = (struct stat64 *)    realloc(hsmstat,(nhsmfiles+1) * sizeof(struct stat64))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG33, "realloc", strerror(errno));
						c = (api_out != 0) ? SEINTERNAL : SESYSERR;
						goto reply;
					}
					hsmstat = dummy2;
#endif
				}
				hsmfiles[nhsmfiles] = Coptarg;
#ifndef NON_WORD_READABLE_SUPPORT
				memset(&hsmstat[nhsmfiles],0,sizeof(struct stat64));
#endif
				nhsmfiles++;
				if ((c = check_hsm_type(Coptarg,&nhpssfiles,&ncastorfiles,&nuserlevel,&nexplevel,&(stgreq.t_or_d))) != 0) {
					goto reply;
				}
				break;
			case 'N':
				nread = Coptarg;
				p = strtok (nread, ":");
				while (p != NULL) {
					stage_strtoi(&j, p, &dp, 10);
					if (*dp != '\0') {
						sendrep (&rpfd, MSG_ERR, STG06, "-N");
						errflg++;
					}
					if ((p = strtok (NULL, ":")) != NULL) *(p - 1) = ':';
				}
				break;
			case 'n':
				stgreq.u1.t.filstat = 'n';
				break;
			case 'o':
				stgreq.u1.t.filstat = 'o';
				break;
			case 'p':
				if (strcmp (Coptarg, "NOPOOL") == 0 ||
					isvalidpool (Coptarg) || ismetapool (Coptarg)) {
					strcpy (stgreq.poolname, Coptarg);
				} else {
					sendrep (&rpfd, MSG_ERR, STG32, Coptarg);
					errflg++;
				}
				break;
			case 'q':
				fseq = Coptarg;
				break;
			case 'S':
				strcpy (stgreq.u1.t.tapesrvr, Coptarg);
				break;
			case 's':
				size = Coptarg;
				p = strtok (size, ":");
				while (p != NULL) {
					int checkrc;

					if ((checkrc = stage_util_check_for_strutou64(p)) < 0) {
						sendrep (&rpfd, MSG_ERR, STG06, "-s");
						errflg++;
					} else {
						if (strutou64(p) <= 0) {
							sendrep (&rpfd, MSG_ERR, STG06, "-s");
							errflg++;
						}
					}
					if ((p = strtok (NULL, ":")) != NULL) *(p - 1) = ':';
				}
				break;
			case 'T':
				stgreq.u1.t.E_Tflags |= NOTRLCHK;
				break;
			case 't':
				stage_strtoi(&(stgreq.u1.t.retentd), Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-t");
					errflg++;
				}
				break;
			case 'u':
				pool_user = Coptarg;
				break;
			case 'U':
				if (req_type == STAGECAT) {
					sendrep (&rpfd, MSG_ERR, STG17, "-U", "stagecat");
					errflg++;
				} else
					Uflag++;
				break;
			case 'V':
				stgreq.t_or_d = 't';
				q = strtok (Coptarg, ":");
				while (q != NULL) {
					strcpy (stgreq.u1.t.vid[numvid], q);
					UPPER (stgreq.u1.t.vid[numvid]);
					numvid++;
					q = strtok (NULL, ":");
				}
				break;
			case 'v':
				stgreq.t_or_d = 't';
				q = strtok (Coptarg, ":");
				while (q != NULL) {
					strcpy (stgreq.u1.t.vsn[numvsn], q);
					UPPER (stgreq.u1.t.vsn[numvsn]);
					numvsn++;
					q = strtok (NULL, ":");
				}
				break;
			case 'X':
				if ((int) strlen (Coptarg) < sizeof(stgreq.u1.d.Xparm)) {
					strcpy (stgreq.u1.d.Xparm, Coptarg);
				} else {
					sendrep (&rpfd, MSG_ERR, STG06, "-X");
					errflg++;
				}
			case 'z':
				copytape++;
				break;
			case 0:
				/* Long option without short option correspondance */
				if ((tppool_flag != 0) && (tppool == NULL)) { /* Not yet done */
					tppool = Coptarg;
				}
				if ((side_flag != 0) && (! have_parsed_side)) {  /* Not yet done */
					stage_strtoi(&side, Coptarg, &dp, 10);
					if ((*dp != '\0') || (side < 0)) {
						sendrep (&rpfd, MSG_ERR, STG06, "--side");
						errflg++;
					}
					have_parsed_side = 1;
				}
				break;
			case '?':
				errflg++;
				break;
			default:
				errflg++;
				break;
			}
            if (errflg != 0) break;
		}
		if (noretry_flag != 0) {
			/* Makes sure flags contains STAGE_NORETRY */
			flags |= STAGE_NORETRY;
		}
		/* We have to trap silent_flag right now */
		if (silent_flag != 0) {
			save_rpfd = rpfd;
			rpfd = -1; /* From now on: silent mode */
		}
	}

	/* We copy in stgreq the permanent information when request comes from the API */
	if (api_out != 0) {
		/* Internal remapping of API commands to command-line equivalents */
		switch (req_type) {
		case STAGE_IN:
			req_type = STAGEIN;
			break;
		case STAGE_WRT:
			req_type = STAGEWRT;
			break;
		case STAGE_OUT:
			req_type = STAGEOUT;
			break;
		case STAGE_CAT:
			req_type = STAGECAT;
			break;
		default:
			sendrep(&rpfd, MSG_ERR, "### Cannot get API command-line's request equivalent of req_type=%d (!?)\n",req_type);
			c = EINVAL;
			goto reply;
		}
		stgreq.t_or_d = (int) t_or_d;
		nargs = nstcp_input + nstpp_input;
		Coptind = nstcp_input;
		if (concat_off != 0 && stcp_input[0].u1.t.fseq[0] != '\0') fseq = stcp_input[0].u1.t.fseq;
	}

	if ((req_type == STAGEWRT) && (stgreq.t_or_d == 't')) {
		char *thisfseq = (fseq != NULL) ? fseq : (stcp_input != NULL ? stcp_input[0].u1.t.fseq : NULL);
		if (thisfseq != NULL) {
			/* Note: if fseq != NULL this is command-line, otherise this is API */
			if (thisfseq[0] == 'n') {
				/* Note: if *(thisfseq+1) == '\0', atoi() would return 0 */
				int nwant = thisfseq[1] != '\0' ? atoi(thisfseq+1) : 1;
				/* We require exactly nwant disk files */
				if ((nargs - Coptind) != nwant) {
					sendrep (&rpfd, MSG_ERR, STG19);
					c = EINVAL;
					goto reply;
				}
			}
		}
	}

	if (side_flag) {
		if (stgreq.t_or_d != 't') {
			/* We reject side argument in the request is not a pure tape request */
			sendrep (&rpfd, MSG_ERR, STG17, "--side", "any request but stage of explicit tape files");
			c = EINVAL;
			goto reply;
		}
		if (api_out == 0) {
			/* This is not yet in the default stgreq (command-line mode) */
			/* Btw note that this is natively inside the input structures (API-mode) */
			stgreq.u1.t.side = side;
		}
	}

	/* We makes sure rdonly_flag is zero if it is not a STAGEIN because it has serious implications */
	/* onto the crucial routine isstaged() */
	if (req_type != STAGEIN) rdonly_flag = 0;

	if (nhsmfiles > 0) {
		/* We check that there is no redundant hsm files (multiple equivalent ones) */

		if ((api_out) && (ncastorfiles)) {
			for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
				stcp_input[ihsmfiles].filler[0] = '\0'; /* We will use this internal flag few lines below */
			}
		}
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			if ((api_out) && (ncastorfiles)) {
				if (stcp_input[ihsmfiles].filler[0] != '\0') continue; /* Yet skipped */
			}
			for (jhsmfiles = ihsmfiles + 1; jhsmfiles < nhsmfiles; jhsmfiles++) {
				if ((api_out) && (ncastorfiles)) {
					if (stcp_input[jhsmfiles].filler[0] != '\0') continue; /* Yet skipped */
				}
				if (strcmp(hsmfiles[ihsmfiles],hsmfiles[jhsmfiles]) == 0) {
					if ((api_out) && (ncastorfiles) &&
						(stcp_input[ihsmfiles].u1.h.server[0] != '\0') &&
						(strcmp(stcp_input[ihsmfiles].u1.h.server, stcp_input[jhsmfiles].u1.h.server) == 0) &&
						(stcp_input[ihsmfiles].u1.h.fileid != 0) &&
						(stcp_input[ihsmfiles].u1.h.fileid == stcp_input[jhsmfiles].u1.h.fileid)) {
						char tmpbuf[21];

						/* If both ihsmfiles and jhsmfiles have exactly the same invariants */
						/* We neverthless continue, skipping the offending entry */
						stcp_input[jhsmfiles].filler[0] = 'D';
						sendrep (&rpfd, MSG_ERR, STG172, hsmfiles[ihsmfiles], u64tostr((u_signed64) stcp_input[ihsmfiles].u1.h.fileid, tmpbuf, 0), stcp_input[ihsmfiles].u1.h.server);
					} else {
						/* If ihsmfiles and jhsmfiles do not have exactly the same invariants */
						/* We check if one of them at least is irrelevant - we accept to withdraw */
						/* an entry from migration only if we are sure that it has been deleted from */
						/* the name server, e.g. Cns_statx() error with serrno == ENOENT */
						int was_able_to_repair = 0;
						char tmpbuf[21];
						
						if ((api_out) && (ncastorfiles) &&
							(stcp_input[ihsmfiles].u1.h.server[0] != '\0') &&
							(stcp_input[ihsmfiles].u1.h.fileid != 0)) {
							strcpy(Cnsfileid.server,stcp_input[ihsmfiles].u1.h.server);
							Cnsfileid.fileid = stcp_input[ihsmfiles].u1.h.fileid;
							setegid(stgreq.gid);
							seteuid(stgreq.uid);
							if ((Cns_statx(hsmfiles[ihsmfiles], &Cnsfileid, &Cnsfilestat) != 0) && (serrno == ENOENT)) {
								setegid(start_passwd.pw_gid);
								seteuid(start_passwd.pw_uid);
								sendrep (&rpfd, MSG_ERR, STG174,
										 hsmfiles[ihsmfiles],
										 u64tostr((u_signed64) stcp_input[ihsmfiles].u1.h.fileid, tmpbuf, 0),
										 stcp_input[ihsmfiles].u1.h.server,
										 "skipped",
										 "Cns_statx",
										 sstrerror(serrno));
								stcp_input[ihsmfiles].filler[0] = 'D';
								was_able_to_repair++;
							} else {
								setegid(start_passwd.pw_gid);
								seteuid(start_passwd.pw_uid);
							}
						}

						if ((api_out) && (ncastorfiles) &&
							(stcp_input[jhsmfiles].u1.h.server[0] != '\0') &&
							(stcp_input[jhsmfiles].u1.h.fileid != 0)) {
							strcpy(Cnsfileid.server,stcp_input[jhsmfiles].u1.h.server);
							Cnsfileid.fileid = stcp_input[jhsmfiles].u1.h.fileid;
							setegid(stgreq.gid);
							seteuid(stgreq.uid);
							if ((Cns_statx(hsmfiles[jhsmfiles], &Cnsfileid, &Cnsfilestat) != 0) && (serrno == ENOENT)) {
								setegid(start_passwd.pw_gid);
								seteuid(start_passwd.pw_uid);
								sendrep (&rpfd, MSG_ERR, STG174,
										 hsmfiles[jhsmfiles],
										 u64tostr((u_signed64) stcp_input[jhsmfiles].u1.h.fileid, tmpbuf, 0),
										 stcp_input[jhsmfiles].u1.h.server,
										 "skipped",
										 "Cns_statx",
										 sstrerror(serrno));
								stcp_input[jhsmfiles].filler[0] = 'D';
								was_able_to_repair++;
							} else {
								setegid(start_passwd.pw_gid);
								seteuid(start_passwd.pw_uid);
							}
						}
						if (! was_able_to_repair) {
							sendrep (&rpfd, MSG_ERR, STG59, hsmfiles[ihsmfiles]);
							errflg++;
						}
					}
				} else if ((api_out) && (ncastorfiles) &&
						   (stcp_input[ihsmfiles].u1.h.server[0] != '\0') &&
						   (strcmp(stcp_input[ihsmfiles].u1.h.server, stcp_input[jhsmfiles].u1.h.server) == 0) &&
						   (stcp_input[ihsmfiles].u1.h.fileid != 0) &&
						   (stcp_input[ihsmfiles].u1.h.fileid == stcp_input[jhsmfiles].u1.h.fileid)) {
					/* User lied using the hsm filenames - but invariants clearly indicates same files... */
					char tmpbuf[21];
					stcp_input[jhsmfiles].filler[0] = 'D';
					sendrep (&rpfd, MSG_ERR, STG172, hsmfiles[ihsmfiles], u64tostr((u_signed64) stcp_input[ihsmfiles].u1.h.fileid, tmpbuf, 0), stcp_input[ihsmfiles].u1.h.server);
				}
			}
		}
	}

	if (Aflag && strcmp (stgreq.poolname, "NOPOOL") == 0) {
		sendrep (&rpfd, MSG_ERR, STG17, "-A deferred", "-p NOPOOL");
		errflg++;
	}

	if ((rdonly_flag != 0) && (ncastorfiles == 0)) {
		rdonly_flag = 0;
		/* rfonly flag makes sense only for STAGEIN on CASTOR files */
	}

	if (Aflag && req_type == STAGEWRT && ncastorfiles == 0) {
		/* If deferred allocation for stagewrt, we check for existence of -M option */
		/* The -M option values validity themselves will be checked later */
		sendrep (&rpfd, MSG_ERR, STG47, "stagewrt -A deferred is valid only with CASTOR HSM files (-M /castor/...)\n");
		errflg++;
	}
	if (concat_off != 0 && strcmp (stgreq.poolname, "NOPOOL") == 0) {
		sendrep (&rpfd, MSG_ERR, STG17, "-c off", "-p NOPOOL");
		errflg++;
	}
	if (concat_off != 0 && stgreq.t_or_d != 't') {
		sendrep (&rpfd, MSG_ERR, "STG17 - option -c off is only valid with -V option\n");
		errflg++;
	}
	if (concat_off != 0 && (nargs - Coptind != 0)) {
		sendrep (&rpfd, MSG_ERR, "STG17 - option -c off is only valid without explicit disk files\n");
		errflg++;
	}
	if ((concat_off != 0) && (req_type != STAGEIN)) {
		sendrep (&rpfd, MSG_ERR, STG17, "-c off", "any request but stage_in");
		errflg++;
	}
	if (api_out == 0) {
		/* Api checksum for this thing is done before */
		if (*stgreq.recfm == 'F' && req_type != STAGEIN && stgreq.lrecl == 0) {
			sendrep (&rpfd, MSG_ERR, STG20);
			errflg++;
		}
		if (stgreq.t_or_d == '\0') {
			sendrep(&rpfd, MSG_ERR, "STG02 - Invalid structure identifier\n");
			errflg++;
		}
	} else if (stcp_input[0].t_or_d == 'h') {
		/* We check if there the tppool member is specified for any of the stcp_input in case of CASTOR files */
		for (i = 0; i < nstcp_input; i++) {
			if (stcp_input[i].u1.h.tppool[0] != '\0') {
				++have_tppool;
			}
		}
		if (have_tppool > 0) {
			if (have_tppool != nstcp_input) {
				/* If the number of tape pools is > 0 but not equal to total number of inputs - error */
				sendrep (&rpfd, MSG_ERR, STG116);
				errflg++;
			} else {
				/* We check that they are all the same */
				for (i = 1; i < nstcp_input; i++) {
					if (strcmp(stcp_input[i-1].u1.h.tppool, stcp_input[i].u1.h.tppool) != 0) {
						sendrep (&rpfd, MSG_ERR, STG117,
								 i-1,
								 stcp_input[i-1].u1.h.xfile,
								 i,
								 stcp_input[i].u1.h.xfile,
								 stcp_input[i-1].u1.h.tppool,
								 stcp_input[i].u1.h.tppool);
						errflg++;
						break;
					}
				}
			}
		}
		if (errflg == 0) {
			tppool = stcp_input[0].u1.h.tppool;
		}
	}

	if ((have_tppool != 0) || (tppool_flag != 0)) {
		if ((req_type != STAGEWRT) && (req_type != STAGEOUT)) {
			sendrep (&rpfd, MSG_ERR, STG17, "--tppool", (req_type == STAGEIN) ? "stagein" : "stagecat");
			errflg++;
		} else {
			/* Print the flags */
			stglogtppool(func,tppool);
		}
	}

	if (errflg != 0) {
		c = EINVAL;
		goto reply;
	}

	/* setting defaults */

	if (concat_off != 0)
		Aflag = 1;	/* force deferred space allocation */
	/* Api checksum for this thing is done before */
	if (req_type == STAGEIN && stgreq.t_or_d == 'm' && ! size)
		Aflag = 1;  /* force deferred policy if stagein of an hsm file without -s option */
	if (api_out == 0) {
		/* Not in API mode - check the poolname */
		if (*stgreq.poolname == '\0') {
			poolflag = 0;
			if (req_type != STAGEWRT && req_type != STAGECAT) {
				switch (req_type) {
				case STAGEIN:
					strcpy(stgreq.poolname,defpoolname_in);
					break;
				case STAGEOUT:
					bestnextpool_out(stgreq.poolname,WRITE_MODE);
					break;
				default:
					break;
				}
			}
		} else if (strcmp (stgreq.poolname, "NOPOOL") == 0) {
			poolflag = -1;
			stgreq.poolname[0] = '\0';
		} else {
			if (ismetapool(stgreq.poolname)) {
				char temppoolname[CA_MAXPOOLNAMELEN + 1];
				/* We chose what is the best pool within the ones that have that metapool */
				bestnextpool_from_metapool(stgreq.poolname,temppoolname,(req_type == STAGEIN) ? READ_MODE : WRITE_MODE);
				if (strcmp(stgreq.poolname,temppoolname) != 0) {
					stglogit(func,STG180,stgreq.poolname,temppoolname);
				}
				strcpy(stgreq.poolname,temppoolname);
			}
			poolflag = 1;
		}
	}
	if (pool_user == NULL)
		pool_user = STAGERSUPERUSER;

	if (req_type == STAGEIN && stgreq.t_or_d == 't' && Aflag) {
		if (api_out == 0 && ! size) {
			getdefsize(stgreq.poolname,&(stgreq.size));  /* Size for deferred stagein of a tape file */
			/* For the API method it is done upper when checking structures contents */
		}
	}

	if (stgreq.t_or_d == 't') {
		if (numvid == 0) {
			for (i = 0; i < numvsn; i++)
				strcpy (stgreq.u1.t.vid[i], stgreq.u1.t.vsn[i]);
			numvid = numvsn;
		}
		if ((concat_off != 0) && (numvid != 1)) {
			sendrep (&rpfd, MSG_ERR, "STG02 - option -c off is not compatible with volume spanning\n");
			errflg++;
			c = EINVAL;
			goto reply;
		}
		if ((concat_off != 0) && (fid != NULL)) {
			sendrep (&rpfd, MSG_ERR, STG17, "-f <fid>", "stagein -c off");
			errflg++;
			c = EINVAL;
			goto reply;
		}

		if (fseq == NULL) fseq = one;

		if (concat_off != 0 || api_out == 0) {
			/* compute number of tape files */
			/* If not api_out then it is a command-line tape stagein call */
			/* otherwise it is a "-c off" API stagein call, for which we have already forced fseq pointer */
			if ((nbtpf = unpackfseq (fseq, req_type, &trailing, &fseq_list, concat_off, &concat_off_fseq)) == 0) {
				errflg++;
			}
		} else {
			/* Here we know that api_out != 0, e.g. API call */
			nbtpf = nstcp_input;
		}
		if (concat_off != 0 && ! errflg) {
			int maxfseq;
			char concat_off_flag;

			if (trailing != '-' || nbtpf != 1) {
				sendrep (&rpfd, MSG_ERR, "STG02 - option -c off requires exactly one tapefile, ending with '-'\n");
				errflg++;
			}
			/* We want to determine which highest fseq is already staged for this vid */
			/* In API mode - there is only one stcp in input, so poolflag is hsmpoolflags[0] */
			/* and then the poolname specified by the user is stcp_input[0].poolname */
			if ((maxfseq = maxfseq_per_vid(&stgreq,
										   (api_out != 0) ? hsmpoolflags[0] : poolflag,
										   (api_out != 0) ? stcp_input[0].poolname : stgreq.poolname,
										   &concat_off_flag)) > 0) {
				fseq_elem *newfseq_list = NULL;

				if (maxfseq >= concat_off_fseq) {
					int new_nbtpf = maxfseq - concat_off_fseq + (concat_off_flag == 'c' ? 1 : 2);
					/* There is overlap from concat_off_fseq to maxfseq */

					if ((newfseq_list = realloc(fseq_list,new_nbtpf * sizeof(fseq_elem))) == NULL) {
						sendrep (&rpfd, MSG_ERR, "STG02 - realloc() error (%s)\n", strerror(errno));
						free(fseq_list);
						errflg++;
					}
					fseq_list = newfseq_list;
					for (i = 0; i <= (maxfseq - concat_off_fseq - (concat_off_flag == 'c' ? 1 : 0)); i++) {
						sprintf ((char *)(fseq_list + i), "%d", concat_off_fseq + i);
					}
					sprintf((char *)(fseq_list + new_nbtpf - 1), "%d", maxfseq + (concat_off_flag == 'c' ? 0 : 1));
					/* Nota : packfseq will take care of adding the leading "-" ... */
					nbtpf = new_nbtpf;
					/* concat_off_fseq is now the one after maxfseq */
					concat_off_fseq = maxfseq + 1;
				}
			}
		}
	} else nbtpf = 1;

	/* Root protection */
	if (req_type != STAGECAT) {
		if (ISROOT(save_uid,save_gid)) {
			sendrep (&rpfd, MSG_ERR, STG156, ROOTUIDLIMIT, ROOTGIDLIMIT);
			errflg++;
		}
	}

	if (errflg != 0) {
		c = EINVAL;
		goto reply;
	}

	/* compute number of disk files */
	nbdskf = nargs - Coptind;
	if (nbdskf == 0) {
		if (api_out == 0) {
			/* Command-line mode */
			if (poolflag < 0) {	/* -p NOPOOL */
				sendrep (&rpfd, MSG_ERR, STG07);
				c = EINVAL;
				goto reply;
			}
		} else {
			/* API mode */
			for (i = 0; i < nstcp_input; i++) {
				if (hsmpoolflags[i] < 0) {	/* "NOPOOL" in the API structure */
					sendrep (&rpfd, MSG_ERR, STG07);
					c = EINVAL;
					goto reply;
				}
			}
		}
		nbdskf = nbtpf;
		no_upath = 1;
		upath[0] = '\0';
	}

	/* In case of hsm request verify the exact mapping between number of hsm files */
	/* and number of disk files.                                                   */
	if (nhsmfiles > 0) {
		if ((nargs - Coptind) > 0 && nbdskf != nhsmfiles) {
			sendrep (&rpfd, MSG_ERR, STG19);
			c = EINVAL;
			goto reply;
		}
	} else if (concat_off != 0 || api_out == 0) {
		if ((req_type == STAGEIN &&
			 ! Uflag && nbdskf > nbtpf && trailing != '-') ||
			((req_type != STAGEIN && req_type != STAGECAT) && nbtpf > nbdskf) ||
			(Uflag && nbdskf > 2)) {
			sendrep (&rpfd, MSG_ERR, STG19);
			c = EINVAL;
			goto reply;
		}
		if (req_type == STAGEIN && ! Uflag && nbdskf > nbtpf) {
			fseq_list = (fseq_elem *) realloc (fseq_list, nbdskf * sizeof(fseq_elem));
			stage_strtoi(&j, (char *)(fseq_list + nbtpf - 1), &dp, 10); j++;
			for (i = nbtpf; i < nbdskf; i++, j++)
				sprintf ((char *)(fseq_list + i), "%d", j);
			nbtpf = nbdskf;
		}
		if (Uflag && nbdskf == 2)
			Upluspath = 1;
	}

	/* building catalog entries */

	ihsmfiles = -1;
	if (nhsmfiles > 0) {
		/* User either gave either no link name, either exactly nhsmfiles : we construct */
		/* anyway nhsmfiles entries...                                                   */
		nbdskf = nhsmfiles;
	}

	iokstagewrt = -1;
	for (i = 0; i < nbdskf; i++) {
		int forced_Cns_creatx;
		int forced_rfio_stat;
		u_signed64 correct_size;
		u_signed64 hsmsize;
		u_signed64 size_to_recall;
		int stage_wrt_migration;
		struct stgcat_entry save_stcp_for_Cns_creatx;
		int have_save_stcp_for_Cns_creatx;
		mode_t stagewrt_Cns_creatx_mode = 0;
		int forced_yet_staged_branch;
		int isstaged_count;

		isstaged_count = 0;
		have_save_stcp_for_Cns_creatx = 0;
		global_c_stagewrt = 0;
		global_c_stagewrt_SYERR = 0;
		forced_yet_staged_branch = 0;

		if (api_out != 0) {
			memcpy(&stgreq,&(stcp_input[i < nstcp_input ? i : nstcp_input - 1]),sizeof(struct stgcat_entry));
			stgreq.t_or_d = stcp_input[0].t_or_d; /* Might have been overwriten ('m' -> 'h') */
			stgreq.uid = save_uid;
			stgreq.gid = save_gid;
			stgreq.mask = save_mask;
			strcpy(stgreq.user,save_user);
			strcpy(stgreq.group,save_group);
			poolflag = hsmpoolflags[i < nstcp_input ? i : nstcp_input - 1];
		}

		forced_Cns_creatx = forced_rfio_stat = 0;
		correct_size = hsmsize = size_to_recall = 0;
		ihsmfiles++;
		if (Uflag && i > 0) break;
		if (stgreq.t_or_d == 't') {
			if (concat_off != 0 || api_out == 0) {
				if (fid) {
					if ((p = strchr (fid, ':')) != NULL) *p = '\0';
					if ((j = strlen (fid) - 17) > 0) fid += j;
					strcpy (stgreq.u1.t.fid, fid);
					if (p != NULL) {
						*p = ':';
						fid = p + 1;
					}
				}
				if (packfseq (fseq_list, i, nbdskf, nbtpf, trailing,
							  stgreq.u1.t.fseq, CA_MAXFSEQLEN + 1)) {
					sendrep (&rpfd, MSG_ERR, STG21);
					c = EINVAL;
					goto reply;
				}
#ifdef STAGER_DEBUG
				sendrep (&rpfd, MSG_ERR, "[DEBUG] Packed fseq \"%s\"\n", stgreq.u1.t.fseq);
#endif
			}
			if (concat_off_fseq > 0) {
				stgreq.filler[0] = 'c';
#ifdef STAGER_DEBUG
				sendrep (&rpfd, MSG_ERR, "[DEBUG] Put 'c' in filler[0]\n");
#endif
			}
		} else if (stgreq.t_or_d == 'm') {
			strcpy(stgreq.u1.m.xfile,hsmfiles[ihsmfiles]);
		} else if (stgreq.t_or_d == 'h') {
			if ((api_out) && (stcp_input[ihsmfiles].filler[0] != '\0')) continue; /* Skipped */
			strcpy(stgreq.u1.h.xfile,hsmfiles[ihsmfiles]);
			/* Unless API request we make sure those fields are initialized */
			if (api_out == 0) {
				stgreq.u1.h.server[0] = '\0';
				stgreq.u1.h.fileid = 0;
				stgreq.u1.h.fileclass = 0;
				stgreq.u1.h.retenp_on_disk = -1;
				stgreq.u1.h.mintime_beforemigr = -1;
			}
			if (tppool != NULL) {
				/* Given as argument */
				strcpy(stgreq.u1.h.tppool,tppool);
			} else {
				stgreq.u1.h.tppool[0] = '\0';
			}
		}
		if (nread) {
			/* Only set with command-line options */
			if ((p = strchr (nread, ':')) != NULL) *p = '\0';
			stage_strtoi(&(stgreq.nread), nread, &dp, 10);
			if (p != NULL) {
				*p = ':';
				nread = p + 1;
			}
		}
		if ((req_type == STAGEIN) && (nhsmfiles > 0)) {
			/* We get statistic information from the nameserver */
			struct Cns_fileid Cnsfileid;

			setegid(stgreq.gid);
			seteuid(stgreq.uid);
			switch (stgreq.t_or_d) {
			case 'h':
				if ((api_out == 0) || ((stgreq.u1.h.server[0] == '\0') || (stgreq.u1.h.fileid == 0))) {
					memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
				} else {
					strcpy(Cnsfileid.server,stgreq.u1.h.server);
					Cnsfileid.fileid = stgreq.u1.h.fileid;
				}
				if (Cns_statx(hsmfiles[ihsmfiles], &Cnsfileid, &Cnsfilestat) != 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_statx", sstrerror(serrno));
					c = (api_out != 0) ? serrno : EINVAL;
					goto reply;
				}
				/* Can it be a directory ? */
				if (S_ISDIR(Cnsfilestat.filemode)) {
					serrno = EISDIR;
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "user", sstrerror(serrno));
					c = (api_out != 0) ? EISDIR : EINVAL;
					goto reply;
				}
				strcpy(stgreq.u1.h.server,Cnsfileid.server);
				stgreq.u1.h.fileid = Cnsfileid.fileid;
				stgreq.u1.h.fileclass = Cnsfilestat.fileclass;
				hsmsize = Cnsfilestat.filesize;
				break;
			case 'm':
				PRE_RFIO;
				if (rfio_stat64(hsmfiles[ihsmfiles], &filemig_stat) < 0) {
					int save_serrno = rfio_serrno();
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "rfio_stat64", rfio_serror());
					c = (api_out != 0) ? save_serrno : EINVAL;
					goto reply;
				}
				/* Can it be a directory ? */
				if (S_ISDIR(filemig_stat.st_mode)) {
					serrno = EISDIR;
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "user", sstrerror(serrno));
					c = (api_out != 0) ? EISDIR : EINVAL;
					goto reply;
				}
				hsmsize = (u_signed64) filemig_stat.st_size;
				break;
			default:
				break;
			}
			setegid(start_passwd.pw_gid);
			seteuid(start_passwd.pw_uid);
			if (((api_out == 0) && (! size)) || (api_out != 0 && ! stgreq.size)) {
				/* We force size to be allocated to exactly what we need */
				stgreq.size = hsmsize;
				if (ncastorfiles > 0) size_to_recall = hsmsize;
			} else {
				if (size != NULL) goto get_size_from_user;
			}
		} else if (size) {
			int checkrc;

		  get_size_from_user:
			if ((p = strchr (size, ':')) != NULL) *p = '\0';
			if ((checkrc = stage_util_check_for_strutou64(size)) < 0) {
				sendrep (&rpfd, MSG_ERR, STG06, "-s");
				c = EINVAL;
				goto reply;
			}
			stgreq.size = strutou64(size);
			if (stgreq.size <= 0) {
				sendrep (&rpfd, MSG_ERR, STG06, "-s");
				c = EINVAL;
				goto reply;
			}
			if (checkrc == 0) stgreq.size *= ONE_MB; /* Not unit : default MB */
			if (p != NULL) {
				*p = ':';
				size = p + 1;
			}
			if (ncastorfiles > 0) {
				size_to_recall = stgreq.size;
				if (size_to_recall > hsmsize) {
					/* We take the minimum of -s option and real size in the nameserver */
					size_to_recall = hsmsize;
				}
			}
		}
		if (no_upath == 0) {
			if (api_out == 0) {
			        if (strlen( argv[Coptind+i]) > CA_MAXHOSTNAMELEN+MAXPATH) {
			          sendrep (&rpfd, MSG_ERR, STG27, "link",  argv[Coptind+i]);
				  c = SENAMETOOLONG;
			          goto reply;
			        }
				strcpy(upath, argv[Coptind+i]);
			} else {
			        if (strlen(stpp_input[i].upath) > CA_MAXHOSTNAMELEN+MAXPATH) {
			          sendrep (&rpfd, MSG_ERR, STG27, "link",  stpp_input[i].upath);
				  c = SENAMETOOLONG;
			          goto reply;
			        }
				strcpy(upath, stpp_input[i].upath);
			}
		}

		poolname_exclusion = NULL;

		switch (req_type) {
		case STAGEIN:
		  stagein_isstaged_retry:
		  switch (isstaged (&stgreq, &stcp, poolflag, stgreq.poolname, rdonly_flag, poolname_exclusion, &isstaged_nomore, NULL, NULL, isstaged_count++)) {
		  case ISSTAGEDBUSY:
			  c = EBUSY;
			  goto reply;
		  case ISSTAGEDSYERR:
			  c = SESYSERR;
			  goto reply;
		  case STAGEIN:	/* stage in progress */
		  case STAGEIN|WAITING_SPC:	/* waiting space */
			  if (poolname_exclusion != NULL) {
				  /* We do not generated WAITING_REQ entries when a record is found in another poolname */
				  /* than what the user requested */
				  goto notstaged;
			  }
			  stcp->nbaccesses++;
#ifdef USECDB
			  if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				  stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			  }
#endif
			  savereqid = stcp->reqid;
			  stcp = newreq((int) stgreq.t_or_d);
			  memcpy (stcp, &stgreq, sizeof(stgreq));
			  if (i > 0)
				  stcp->reqid = nextreqid();
			  else
				  stcp->reqid = reqid;
			  stcp->status = STAGEIN;
			  stcp->c_time = stcp->a_time = time(NULL);
			  stcp->nbaccesses++;
			  stcp->status |= WAITING_REQ;
#ifdef USECDB
			  if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
				  stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			  }
#endif
			  if (!wqp) {
				  wqp = add2wq (clienthost,
								user, stcp->uid, stcp->gid,
								user, save_group, stcp->uid, stcp->gid,
								clientpid,
								Upluspath, reqid, req_type, nbdskf, &wfp, &save_subreqid,
								stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq,
								(concat_off_fseq <= 0) ? 1 : 0);
				  /* Please note that we explicitely disabled async callbacks if */
				  /* option -c off is in action */
				  wqp->Aflag = Aflag;
				  wqp->copytape = copytape;
				  wqp->concat_off_fseq = concat_off_fseq;
				  wqp->api_out = api_out;
				  wqp->magic = magic;
				  wqp->uniqueid = stage_uniqueid;
				  wqp->silent = silent_flag;
				  wqp->noretry = noretry_flag;
				  wqp->flags = flags;
				  if (nowait_flag != 0) {
					  /* User said --nowait - we force silent flag anyway and reset rpfd to N/A */
					  wqp->silent = 1;
					  wqp->rpfd = -1;
				  } else if (save_rpfd >= 0) {
					  /* User said only --silent : we prepare restore original rpfd */
					  wqp->silent = 1;
					  wqp->rpfd = -1;
					  wqp->save_rpfd = save_rpfd;
				  }
			  }
			  wfp->subreqid = stcp->reqid;
			  /* Nota : save_subreqid is != NULL only if no '-c off' request */
			  if (save_subreqid != NULL) {
				  *save_subreqid = stcp->reqid;
				  save_subreqid++;
			  }
			  wfp->waiting_on_req = savereqid;
			  strcpy (wfp->upath, upath);
			  wfp->size_to_recall = size_to_recall;      /* Can be zero */
			  wfp->hsmsize = hsmsize;                    /* Can be zero */
			  wfp->size_yet_recalled = 0;
			  strcpy (wqp->pool_user, pool_user);
			  wqp->nb_waiting_on_req++;
			  wqp->nbdskf++;
			  wqp->nb_subreqs++;
			  wfp++;
			  if (Upluspath) {
				  wfp->subreqid = stcp->reqid;
				  strcpy (wfp->upath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
			  }
			  break;
		  case STAGED:		/* staged */
			  PRE_RFIO;
			  if (RFIO_STAT64(stcp->ipath, &st) < 0) {
				  stglogit (func, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
				  if (delfile (stcp, 0, 1, 1, "not on disk", 0, 0, 0, 1, 0) < 0) {
					  int save_serrno = rfio_serrno();
					  sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath,
							   RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
					  c = (api_out != 0) ? save_serrno : SESYSERR;
					  goto reply;
				  }
				  goto notstaged;
			  } else {
				  /*
				   * File exists, now check to see if it is
				   * a partial file, and replace it if current
				   * request is for a larger size.
				   */
				  if (((stcp->status == (STAGEIN|STAGED|STAGED_LSZ)) || ((stcp->status == (STAGEIN|STAGED)) && (stcp->t_or_d != 't'))) &&
					  (stgreq.size > stcp->size)) {
					  if (delfile (stcp, 0, 1, 1, "larger req", stgreq.uid, stgreq.gid, 0, 1, 0) < 0) {
						  int save_serrno = rfio_serrno();
						  sendrep (&rpfd, MSG_ERR,
								   STG02, stcp->ipath,
								   RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
						  c = (api_out != 0) ? save_serrno : SESYSERR;
						  goto reply;
					  }
					  goto notstaged;
				  }
			  }
			  /* There is intentionnaly no break here */
		  case STAGEWRT:
		  case STAGEWRT|CAN_BE_MIGR:
		  case STAGEOUT|CAN_BE_MIGR:
			forced_yet_staged_branch:
			if (poolname_exclusion != NULL) {
				char sav_ipath[(CA_MAXHOSTNAMELEN+MAXPATH)+1];
				char sav_poolname[CA_MAXPOOLNAMELEN+1];

				/* We have to generate a copy between pools */
				/* Note that by definition this apply only to */
				/* a single CASTOR HSM file request */

				/* This disk file will be used internally for the disk2disk copy */
				/* We update its a_time to try to make it not a candidate for */
				/* future garbage collection */
				stcp->a_time = time(NULL);
				stcp->nbaccesses++;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif

				strcpy(sav_ipath,stcp->ipath);
				strcpy(sav_poolname,stcp->poolname);
				stcp = newreq((int) stgreq.t_or_d);
				memcpy (stcp, &stgreq, sizeof(stgreq));
				if (i > 0)
					stcp->reqid = nextreqid();
				else
					stcp->reqid = reqid;
				stcp->status = STAGEIN;
				stcp->c_time = stcp->a_time = time(NULL);
				stcp->nbaccesses++;
				if (!wqp) {
					wqp = add2wq (clienthost,
								  user, stcp->uid, stcp->gid,
								  user, save_group, stcp->uid, stcp->gid,
								  clientpid, Upluspath, reqid, req_type, nbdskf, &wfp, &save_subreqid,
								  NULL, fseq,
								  1);
					/* Please note that we explicitely disabled async callbacks if */
					/* option -c off is in action */
					wqp->Aflag = Aflag;
					wqp->copytape = copytape;
					wqp->concat_off_fseq = concat_off_fseq;
					wqp->api_out = api_out;
					wqp->magic = magic;
					wqp->uniqueid = stage_uniqueid;
					wqp->silent = silent_flag;
					wqp->noretry = noretry_flag;
					wqp->flags = flags;
					if (nowait_flag != 0) {
						/* User said --nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
					} else if (save_rpfd >= 0) {
						/* User said only --silent : we prepare restore original rpfd */
						wqp->silent = 1;
						wqp->rpfd = -1;
						wqp->save_rpfd = save_rpfd;
					}
				}
				wfp->subreqid = stcp->reqid;
				/* Nota : save_subreqid is != NULL only if no '-c off' request */
				if (save_subreqid != NULL) {
					*save_subreqid = stcp->reqid;
					save_subreqid++;
				}
				wfp->size_to_recall = size_to_recall;   /* Can be zero */
				wfp->hsmsize = hsmsize;                 /* Can be zero */
				wfp->size_yet_recalled = 0;
				strcpy (wfp->upath, upath);
				strcpy (wqp->pool_user, pool_user);
				if (! Aflag) {
					if ((c = build_ipath (upath, stcp, pool_user, 0, api_out, (mode_t) openmode)) < 0) {
						if (noretry_flag != 0) {
							c = ENOSPC;
							delreq(stcp,1);
							goto reply;
						}
						stcp->status |= WAITING_SPC;
						strcpy (wqp->waiting_pool, stcp->poolname);
					} else if (c) {
						delreq(stcp,1);
						goto reply;
					}
				}
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				wqp->nbdskf++;
				wqp->nb_subreqs++;
				/* We lie to the system filling this ipath member of waitf */
				stglogit (func, "%s : Found in pool %s at %s\n", stgreq.u1.h.xfile, sav_poolname, sav_ipath);
				strcpy(wfp->ipath, sav_ipath);
				wfp++;
				if (Upluspath) {
					wfp->subreqid = stcp->reqid;
					strcpy (wfp->upath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
				}
				break;
			}
			if (stcp->t_or_d == 'h') {
				/* update access time in Cns */
				struct Cns_fileid Cnsfileid;
				strcpy (Cnsfileid.server, stcp->u1.h.server);
				Cnsfileid.fileid = stcp->u1.h.fileid;
				setegid(stgreq.gid);
				seteuid(stgreq.uid);
				(void) Cns_setatime(NULL, &Cnsfileid);
				setegid(start_passwd.pw_gid);
				seteuid(start_passwd.pw_uid);
			}
			stcp->a_time = time(NULL);
			stcp->nbaccesses++;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (forced_yet_staged_branch) {
				/* If we are there, this is because there was a stagein --rdonly on a file being migrated */
				/* We create a new entry with state STAGEIN|STAGED|STAGE_RDONLY */
				struct stgcat_entry save_this_stcp = *stcp;

				stcp = newreq((int) save_this_stcp.t_or_d);
				memcpy (stcp, &save_this_stcp, sizeof(struct stgcat_entry));
				if (i > 0)
					stcp->reqid = nextreqid();
				else
					stcp->reqid = reqid;
				stcp->status = STAGEIN|STAGED|STAGE_RDONLY;
				stcp->nbaccesses = 1;
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
			}
#if SACCT
			stageacct (STGFILS, stgreq.uid, stgreq.gid,
					   clienthost, reqid, req_type, 0, 0, stcp, "", (char) 0);
#endif
			{
				char tmpbuf[21];
				sendrep (&rpfd, RTCOPY_OUT, STG96,
						 strrchr (stcp->ipath, '/')+1,
						 u64tostr(stcp->actual_size,tmpbuf,0),
						 (float)(stcp->actual_size)/(1024.*1024.),
						 stcp->nbaccesses);
				/* When a file is already staged the last allocation timestamp is not automatically */
				/* changed - so we call explicitely a routine that will do that */
				update_last_allocation(stcp->ipath);
			}
			if (api_out != 0) sendrep(&rpfd, API_STCP_OUT, stcp, magic);
			if (copytape)
				sendinfo2cptape (&rpfd, stcp);
			if (*upath && strcmp (stcp->ipath, upath))
				create_link (&rpfd, stcp, upath);
			if (Upluspath && ((api_out == 0) ? (argv[Coptind+1][0] != '\0') : (stpp_input[1].upath[0] != '\0')) &&
				strcmp (stcp->ipath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath))
				create_link (&rpfd, stcp, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
			break;
		  case STAGEOUT:
		  case STAGEOUT|WAITING_SPC:
		  case STAGEOUT|WAITING_NS:
			  sendrep (&rpfd, MSG_ERR, STG37);
			  c = EINVAL;
			  goto reply;
		  case STAGEOUT|PUT_FAILED:
		  case STAGEOUT|CAN_BE_MIGR|PUT_FAILED:
			  /* We accept this only in read-only mode for CASTOR files */

			  /* This stcp is physically on disk forever - not migrated */
			  if (rdonly_flag && (stcp->t_or_d == 'h')) {
				  /* We force the yet staged branch */
				  forced_yet_staged_branch = 1;
				  goto forced_yet_staged_branch;
			  } else {
				  /* We go to the normal branch - backward compatibility */
				  goto notstaged;
			  }
		  case STAGEOUT|CAN_BE_MIGR|WAITING_MIGR:
		  case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
		  case STAGEPUT|CAN_BE_MIGR:
		  case STAGEWRT|CAN_BE_MIGR|BEING_MIGR:
			  /* This stcp is is physically being really migrated */
			  /* We accept a STAGEIN on it only if is explicitely gives the O_RDONLY flag */
			  if (! rdonly_flag) {
				  sendrep (&rpfd, MSG_ERR, STG37);
				  c = EBUSY;
				  goto reply;
			  } else {
				  /* We force the yet staged branch */
				  forced_yet_staged_branch = 1;
				  goto forced_yet_staged_branch;
			  }
			notstaged:
		  default:
			  if ((poolname_exclusion == NULL) &&
				  (! nocopy_flag) &&
				  (poolflag > 0) &&
				  (rdonly_flag) &&
				  (stgreq.t_or_d == 'h') &&
				  (have_another_pool_with_export_hsm_option(stgreq.poolname)) &&
				  (hsmsize > 0)
				  ) {
				  /* Unless user said he does not want to have internal copy between disk pools */
				  /* And if user did specified explicitely a pool (if not we would already have */
				  /* searched within all of them !) and if there is at least one pool other than */
				  /* the one he specified that have the export_hsm flag, then we have to look to */
				  /* other poolnames*/
				  /* This apply only on CASTOR-HSM files accessed in O_RDONLY mode */
				  /* The test on poolname_exclusion == NULL ensure that we do a second pass to isstaged() only once */

				  /* We do NOT apply this mechanism if hsmsize if == 0 */
				  /* stglogit (func, "%s : Searching in any other pool except %s\n", stgreq.u1.h.xfile, stgreq.poolname); */
				  poolname_exclusion = stgreq.poolname;
				  goto stagein_isstaged_retry;
			  }
			  /* If last_tape_file is set, then it is a 't' request, then trailing is set */
			  if (last_tape_file && trailing == '-' &&
				  atoi (stgreq.u1.t.fseq) > last_tape_file) {
				  /* requested file is not on tape */
				  stglogit (func, "requested file is not on tape\n");
				  nbdskf = i;
				  continue;	/* exit from the loop */
			  }
			  stcp = newreq((int) stgreq.t_or_d);
			  memcpy (stcp, &stgreq, sizeof(stgreq));
			  if (i > 0)
				  stcp->reqid = nextreqid();
			  else
				  stcp->reqid = reqid;
			  stcp->status = STAGEIN;
			  stcp->c_time = stcp->a_time = time(NULL);
			  /* Case of a recall of an HSM file of zero size */
			  if ((nhsmfiles > 0) && (hsmsize == 0)) {
				  int ifileclass;

				  if ((ncastorfiles > 0) && ((ifileclass = upd_fileclass(NULL,stcp,0,0,1)) < 0)) {
					  if ((serrno != CLEARED) && (serrno != ESTCLEARED)) {
						  sendrep (&rpfd, MSG_ERR, STG132, stcp->u1.h.xfile, sstrerror(serrno));
					  } else {
						  /* This is not really a stageclr but more a user error */
						  serrno = EINVAL;
					  }
					  c = (api_out != 0) ? serrno : EINVAL;
					  delreq(stcp,1);
					  goto reply;
				  }
				  if ((c = build_ipath (upath, stcp, pool_user, 1, api_out, (mode_t) openmode)) != 0) {
					  delreq(stcp,1);
					  goto reply;
				  }
				  stcp->status = STAGEIN|STAGED;
#ifdef USECDB
				  if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					  stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				  }
#endif
				  goto forced_yet_staged_branch;
			  } else {
				  stcp->nbaccesses++;
				  if (!wqp) {
					  wqp = add2wq (clienthost,
									user, stcp->uid, stcp->gid,
									user, save_group, stcp->uid, stcp->gid,
									clientpid, Upluspath, reqid, req_type, nbdskf, &wfp, &save_subreqid,
									stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq,
									(concat_off_fseq <= 0) ? 1 : 0);
					  /* Please note that we explicitely disabled async callbacks if */
					  /* option -c off is in action */
					  wqp->Aflag = Aflag;
					  wqp->copytape = copytape;
					  wqp->concat_off_fseq = concat_off_fseq;
					  wqp->api_out = api_out;
					  wqp->magic = magic;
					  wqp->uniqueid = stage_uniqueid;
					  wqp->silent = silent_flag;
					  wqp->noretry = noretry_flag;
					  wqp->flags = flags;
					  if (nowait_flag != 0) {
						  /* User said --nowait - we force silent flag anyway and reset rpfd to N/A */
						  wqp->silent = 1;
						  wqp->rpfd = -1;
					  } else if (save_rpfd >= 0) {
						  /* User said only --silent : we prepare restore original rpfd */
						  wqp->silent = 1;
						  wqp->rpfd = -1;
						  wqp->save_rpfd = save_rpfd;
					  }
				  }
				  wfp->subreqid = stcp->reqid;
				  /* Nota : save_subreqid is != NULL only if no '-c off' request */
				  if (save_subreqid != NULL) {
					  *save_subreqid = stcp->reqid;
					  save_subreqid++;
				  }
				  wfp->size_to_recall = size_to_recall;   /* Can be zero */
				  wfp->hsmsize = hsmsize;                 /* Can be zero */
				  wfp->size_yet_recalled = 0;
				  strcpy (wfp->upath, upath);
				  strcpy (wqp->pool_user, pool_user);
				  if (! Aflag) {
					  if ((c = build_ipath (upath, stcp, pool_user, 0, api_out, (mode_t) openmode)) < 0) {
						  stcp->status |= WAITING_SPC;
						  strcpy (wqp->waiting_pool, stcp->poolname);
					  } else if (c) {
						  delreq(stcp,1);
						  goto reply;
					  }
				  }
#ifdef USECDB
				  if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					  stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				  }
#endif
				  wqp->nbdskf++;
				  wqp->nb_subreqs++;
				  wfp++;
				  if (Upluspath) {
					  wfp->subreqid = stcp->reqid;
					  strcpy (wfp->upath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
				  }
			  }
		  }
		  break;
		case STAGEOUT:
			/* Note : create_hsm_entry will naturally fail if output is a directory */
/*
			if (stgreq.t_or_d == 'h') {
				if ((api_out == 0) || ((stgreq.u1.h.server[0] == '\0') || (stgreq.u1.h.fileid == 0))) {
					memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
				} else {
					strcpy(Cnsfileid.server,stgreq.u1.h.server);
					Cnsfileid.fileid = stgreq.u1.h.fileid;
				}
				setegid(stgreq.gid);
				seteuid(stgreq.uid);
				if (Cns_statx(hsmfiles[ihsmfiles], &Cnsfileid, &Cnsfilestat) == 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					strcpy(stgreq.u1.h.server,Cnsfileid.server);
					stgreq.u1.h.fileid = Cnsfileid.fileid;
					stgreq.u1.h.fileclass = Cnsfilestat.fileclass;
				} else {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
				}
			}
*/
			isstaged_rc = -1;
			STAGE_TIME_START("while (isstaged_rc != NOTSTAGED)");
			while (isstaged_rc != NOTSTAGED) {
				/* We have to loop up to when we will be sure that all copies are removed */
				isstaged_nomore = -1;
				STAGE_TIME_START("isstaged()");
				isstaged_rc = isstaged (&stgreq, &stcp, poolflag, stgreq.poolname, rdonly_flag, poolname_exclusion, &isstaged_nomore, NULL, NULL, isstaged_count++);
				STAGE_TIME_END;
				switch (isstaged_rc) {
				case ISSTAGEDBUSY:
					c = EBUSY;
					goto reply;
				case ISSTAGEDSYERR:
					c = SESYSERR;
					goto reply;
				case NOTSTAGED:
					break;
				case STAGEOUT|PUT_FAILED:
				case STAGEOUT|CAN_BE_MIGR|PUT_FAILED:
					/* We accept a STAGEOUT on a PUT_FAILED file only if it is a CASTOR HSM one and if the permissions are ok */
					if (! ((ncastorfiles > 0) && (stcp->t_or_d == 'h') && (stgreq.uid == stcp->uid) && (stgreq.gid == stcp->gid))) {
						sendrep (&rpfd, MSG_ERR, STG37);
						c = EINVAL;
						goto reply;
					}
					/* Here the logic is the same as if it was a STAGED file */
				case STAGED:
					if (delfile (stcp, 0, 1, 1, user, stgreq.uid, stgreq.gid, 0, 1, 0) < 0) {
						int save_serrno = rfio_serrno();
						sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath,
								 RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
						c = (api_out != 0) ? save_serrno : SESYSERR;
						goto reply;
					}
					break;
				case STAGEOUT|CAN_BE_MIGR:
					if (stgreq.t_or_d == 't' && *stgreq.u1.t.fseq == 'n') break;
					if (strcmp (user, stcp->user)) {
						sendrep (&rpfd, MSG_ERR, STG37);
						c = EINVAL;
						goto reply;
					}
					if ((stcp->t_or_d == 'h') && (stcp->status == STAGEOUT)) {
						/* We decrement the r/w counter on this file system */
						rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, STAGEUPDC);
					}
					if (delfile (stcp, 0, 1, 1, user, stgreq.uid, stgreq.gid, 0, 1, 0) < 0) {
						int save_serrno = rfio_serrno();
						sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath,
								 RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
						c = (api_out != 0) ? save_serrno : SESYSERR;
						goto reply;
					}
					break;
				case STAGEOUT:
				case STAGEOUT|WAITING_SPC:
				case STAGEOUT|WAITING_NS:
					if (stgreq.t_or_d == 't' && *stgreq.u1.t.fseq == 'n') break;
					if (strcmp (user, stcp->user)) {
						sendrep (&rpfd, MSG_ERR, STG37);
						c = EINVAL;
						goto reply;
					}
					if ((stcp->t_or_d == 'h') && (stcp->status == STAGEOUT)) {
						/* We decrement the r/w counter on this file system */
						rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, STAGEUPDC);
					}
					if (delfile (stcp, 1, 1, 1, user, stgreq.uid, stgreq.gid, 0, 1, 0) < 0) {
						int save_serrno = rfio_serrno();
						sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath,
								 RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
						c = (api_out != 0) ? save_serrno : SESYSERR;
						goto reply;
					}
					break;
				case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
				case STAGEOUT|CAN_BE_MIGR|WAITING_MIGR:
				case STAGEWRT|CAN_BE_MIGR:
				case STAGEWRT|CAN_BE_MIGR|BEING_MIGR:
				case STAGEPUT|CAN_BE_MIGR:
					sendrep (&rpfd, MSG_ERR, STG37);
					c = EBUSY;
					goto reply;
				default:
					sendrep (&rpfd, MSG_ERR, STG37);
					c = EINVAL;
					goto reply;
				}
				/* We loop unless isstaged() has been kind enough to detect this isn't necessary */
				if (isstaged_nomore) break;
			}
			STAGE_TIME_END;
			STAGE_TIME_START("stageout processing");
			STAGE_TIME_START("creation of stcp");
			stcp = newreq((int) stgreq.t_or_d);
			memcpy (stcp, &stgreq, sizeof(stgreq));
			/* memcpy overwritted the -1 default values */
			if (stcp->t_or_d == 'h') {
				stcp->u1.h.retenp_on_disk = -1;
				stcp->u1.h.mintime_beforemigr = -1;
			}
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = STAGEOUT;
			stcp->c_time = stcp->a_time = time(NULL);
			stcp->nbaccesses++;
			if (stgreq.t_or_d == 'h') {
				/* Special insert before build_ipath() call because of the possible WAITING_NS state */
				stcp->status |= WAITING_NS;
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs();
			}
			STAGE_TIME_END;
			STAGE_TIME_START("build_ipath");
			c = build_ipath (upath, stcp, pool_user, 0, api_out, (mode_t) openmode);
			STAGE_TIME_END;
			if (c < 0) {
				stcp->status |= WAITING_SPC;
				if (!wqp) {
					wqp = add2wq (clienthost,
								  user, stcp->uid, stcp->gid,
								  user, save_group, stcp->uid, stcp->gid,
								  clientpid,
								  Upluspath, reqid, req_type, nbdskf, &wfp, NULL, 
								  stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq, 0);
					wqp->api_out = api_out;
					wqp->magic = magic;
					wqp->openflags = openflags;
					wqp->openmode = openmode;
					wqp->uniqueid = stage_uniqueid;
					wqp->silent = silent_flag;
					wqp->noretry = noretry_flag;
					wqp->flags = flags;
					if (nowait_flag != 0) {
						/* User said --nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
					} else if (save_rpfd >= 0) {
						/* User said only --silent : we prepare restore original rpfd */
						wqp->silent = 1;
						wqp->rpfd = -1;
						wqp->save_rpfd = save_rpfd;
					}
				}
				wfp->subreqid = stcp->reqid;
				strcpy (wfp->upath, upath);
				wqp->nbdskf++;
				wfp++;
				if (Upluspath) {
					wfp->subreqid = stcp->reqid;
					strcpy (wfp->upath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
				}
				strcpy (wqp->pool_user, pool_user);
				strcpy (wqp->waiting_pool, stcp->poolname);
			} else if (c) {
				delreq(stcp,stcp->t_or_d == 'h' ? 0 : 1);
				goto reply;
			} else {
				/* Space successfully allocated - Try to get rid of WAITING_NS */
				if (stcp->t_or_d == 'h') {
					STAGE_TIME_START("create_hsm_entry");
					c = create_hsm_entry(&rpfd, (struct stgcat_entry *) stcp, (int) api_out, (mode_t) openmode, (int) 1);
					STAGE_TIME_END;
					if (c != 0) goto reply;
				}
				if (*upath && strcmp (stcp->ipath, upath))
					create_link (&rpfd, stcp, upath);
				if (Upluspath && ((api_out == 0) ? (argv[Coptind+1][0] != '\0') : (stpp_input[1].upath[0] != '\0')) &&
					strcmp (stcp->ipath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath))
					create_link (&rpfd, stcp, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
				STAGE_TIME_START("rwcountersfs");
				rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT);
				STAGE_TIME_END;
			}
#ifdef USECDB
			STAGE_TIME_START("stgdb_ins_stgcat");
			if (stcp->t_or_d != 'h') {
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
			}
			STAGE_TIME_END;
#endif
			STAGE_TIME_START("savereqs");
			savereqs();
			if (api_out != 0) sendrep(&rpfd, API_STCP_OUT, stcp, magic);
			STAGE_TIME_END;
			STAGE_TIME_END;
			break;
		case STAGEWRT:
			/* Note : create_hsm_entry will naturally fail if output is a directory */
			if ((p = findpoolname (upath)) != NULL) {
				if (poolflag < 0 ||
					(poolflag > 0 && strcmp (stgreq.poolname, p)))
					sendrep (&rpfd, MSG_ERR, STG49, upath, p);
				actual_poolflag = 1;
				strcpy (actual_poolname, p);
			} else {
				if (Aflag) {
					/* Must have a valid poolname in deferred mode */
					sendrep (&rpfd, MSG_ERR, STG33, upath, "must be in a valid pool if -A deferred mode");
                    c = EINVAL;
					goto reply;
				}
				if (poolflag > 0)
					sendrep (&rpfd, MSG_ERR, STG50, upath);
				actual_poolflag = -1;
				actual_poolname[0] = '\0';
			}
			stage_wrt_migration = 0;
			have_Cnsfilestat = 0;
			yetdone_rfio_stat = NULL;
			yetdone_rfio_stat_path = NULL;
			switch (isstaged (&stgreq, &stcp, actual_poolflag, actual_poolname, rdonly_flag, poolname_exclusion, &isstaged_nomore, &have_Cnsfilestat, &Cnsfilestat, isstaged_count++)) {
			case ISSTAGEDBUSY:
				global_c_stagewrt++;
				c = EBUSY;
				goto stagewrt_continue_loop;
			case ISSTAGEDSYERR:
				global_c_stagewrt++;
				global_c_stagewrt_SYERR++;
				global_c_stagewrt_last_serrno = SESYSERR;
				c = SESYSERR;
				goto stagewrt_continue_loop;
			case NOTSTAGED:
				break;
			case STAGED:
				if (stcp->poolname[0] && strcmp (stcp->ipath, upath)) {
					if (delfile (stcp, 0, 1, 1, user, stgreq.uid, stgreq.gid, 0, 1, 0) < 0) {
 						int save_serrno = rfio_serrno();
						sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath,
								 RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
						global_c_stagewrt++;
						global_c_stagewrt_SYERR++;
						global_c_stagewrt_last_serrno = save_serrno;
						c = (api_out != 0) ? save_serrno : SESYSERR;
						goto stagewrt_continue_loop;
					}
				} else {
					save_stcp_for_Cns_creatx = *stcp;
					have_save_stcp_for_Cns_creatx = 1;
					delreq(stcp,0);
				}
				break;
			case STAGEOUT|PUT_FAILED:
			case STAGEOUT|PUT_FAILED|CAN_BE_MIGR:
			{
				int save_stcp_status;
				u_signed64 actual_size_block;
				struct stat64 st;
				int ifileclass;
				struct pool *pool_p;
				int okpoolname = 0;
				int ipoolname;
				extern struct pool *pools;
				extern int nbpool;

				/* This work only for CASTOR files */
				if (stcp->t_or_d != 'h') {
					sendrep (&rpfd, MSG_ERR, STG37);
					global_c_stagewrt++;
					c = EINVAL;
					if (stgreq.t_or_d == 'h') {
						goto stagewrt_continue_loop;
					} else {
						goto reply;
					}
				}

				/* We need the address of the structure describing stcp->poolname */
				for (ipoolname = 0, pool_p = pools; ipoolname < nbpool; ipoolname++, pool_p++) {
					/* We search the pool structure for this stcp */
					if (strcmp(stcp->poolname,pool_p->name) != 0) continue;
					okpoolname = 1;
					break;
				}
				if (okpoolname == 0) {
					sendrep (&rpfd, MSG_ERR, STG32, stcp->poolname);
					global_c_stagewrt++;
					c = EINVAL;
					if (stgreq.t_or_d == 'h') {
						goto stagewrt_continue_loop;
					} else {
						goto reply;
					}
				}
				/* We need to know fileclass for our predicates */
				if ((ifileclass = upd_fileclass(pool_p,stcp,0,0,0)) < 0) {
					global_c_stagewrt++;
					c = EINVAL;
					if (stgreq.t_or_d == 'h') {
						goto stagewrt_continue_loop;
					} else {
						goto reply;
					}
				}

				/* We check stcp->ipath */
				PRE_RFIO;
				if (RFIO_STAT64(stcp->ipath, &st) == 0) {
					stcp->actual_size = st.st_size;
					if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
						actual_size_block = stcp->actual_size;
					}
				} else {
					sendrep(&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
					global_c_stagewrt++;
					c = rfio_serrno();
					if (stgreq.t_or_d == 'h') {
						goto stagewrt_continue_loop;
					} else {
						goto reply;
					}
				}

				/* We force the tape pool if any (implying no extension of this record for eventual other copy)  */
				/* Safe thing is to explicitely give a tape pool in arguments then - case of automatic */
				/* migration */
				strcpy(stcp->u1.h.tppool, stgreq.u1.h.tppool);

				/* We simulate a STAGEOUT followed by a STAGEUPDC */
				save_stcp_status = stcp->status;
				stcp->status = STAGEOUT;
				updfreespace (
					stcp->poolname,
					stcp->ipath,
					0,
					NULL,
					(signed64) ((signed64) actual_size_block - (signed64) stcp->size)
					);
				rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT);
				if ((c = upd_stageout(STAGEUPDC, NULL, NULL, 1, stcp, 1, nohsmcreat_flag)) != 0) {
					if ((c != CLEARED) && (c != ESTCLEARED)) {
						/* Restore original status */
						stcp->status = save_stcp_status;
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
							stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
						savereqs();
					}
					global_c_stagewrt++;
					c = EINVAL;
					if (stgreq.t_or_d == 'h') {
						goto stagewrt_continue_loop;
					} else {
						goto reply;
					}
				}
				stcp->status = STAGEOUT|CAN_BE_MIGR|BEING_MIGR;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				stage_wrt_migration = 1;
				save_stcp_for_Cns_creatx = *stcp;
				have_save_stcp_for_Cns_creatx = 1;

				/* We update the fileclass within this migrator watch variables */
				pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig++;
				pool_p->migr->fileclass_predicates[ifileclass].nbfiles_to_mig++;
				pool_p->migr->fileclass_predicates[ifileclass].space_beingmig += stcp->actual_size;
				pool_p->migr->fileclass_predicates[ifileclass].space_to_mig += stcp->actual_size;
				/* We update the migrator global watch variables */
				pool_p->migr->global_predicates.nbfiles_beingmig++;
				pool_p->migr->global_predicates.nbfiles_to_mig++;
				pool_p->migr->global_predicates.space_beingmig += stcp->actual_size;
				pool_p->migr->global_predicates.space_to_mig += stcp->actual_size;
				stcp->filler[0] = 'm'; /* 'm' for 'putted in canbemigr (non-delayed) mode' */

				break;
			}
			case STAGEOUT|CAN_BE_MIGR|WAITING_MIGR:
				/* We accept a stagewrt request on a record waiting for it only if both tape pool matches */
				/* and are non-null */
				if (! ((stgreq.t_or_d == 'h') && 
					   (stcp->u1.h.tppool[0] != '\0') && strcmp(stcp->u1.h.tppool,stgreq.u1.h.tppool) == 0)) {
					sendrep (&rpfd, MSG_ERR, STG37);
					global_c_stagewrt++;
					c = EINVAL;
					if (stgreq.t_or_d == 'h') {
						goto stagewrt_continue_loop;
					} else {
						goto reply;
					}
				}
				stcp->status = STAGEOUT|CAN_BE_MIGR|BEING_MIGR;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				stage_wrt_migration = 1;
				save_stcp_for_Cns_creatx = *stcp;
				have_save_stcp_for_Cns_creatx = 1;
				break;
			case STAGEOUT|CAN_BE_MIGR:
			case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
			case STAGEPUT|CAN_BE_MIGR:
			case STAGEWRT|CAN_BE_MIGR|BEING_MIGR:
				sendrep (&rpfd, MSG_ERR, STG37);
				c = EBUSY;
				goto reply;
			case STAGEWRT:
				if (stcp->t_or_d == 't' && *stcp->u1.t.fseq == 'n') {
					save_stcp_for_Cns_creatx = *stcp;
					have_save_stcp_for_Cns_creatx = 1;
					break;
				}
			default:
				sendrep (&rpfd, MSG_ERR, STG37);
				global_c_stagewrt++;
				c = EINVAL;
				if (stgreq.t_or_d == 'h') {
					goto stagewrt_continue_loop;
				} else {
					goto reply;
				}
			}

			if (have_save_stcp_for_Cns_creatx == 0) {
				struct stgcat_entry *stclp;
				/* We look if by chance there is an entry for the same disk file (case of automatic migration) */
				for (stclp = stcs; stclp < stce; stclp++) {
					if (stclp->reqid == 0) break;
					if ((stclp->status & 0xF0) != STAGED) continue;
					if (! (ISSTAGEIN(stclp) || ISSTAGEOUT(stclp) || ISSTAGEALLOC(stclp))) continue;
					if (strcmp(stclp->ipath, upath) != 0) continue;
					/* Got it */
					have_save_stcp_for_Cns_creatx = 1;
					save_stcp_for_Cns_creatx = *stclp;
					break;
				}
			}

			SET_CORRECT_EUID_EGID;
			switch (stgreq.t_or_d) {
			case 'h':
				/* If it is a stagewrt on CASTOR file and if this file have the same size we assume */
				/* that user wants to do a second copy. */
				if ((api_out == 0) || ((stgreq.u1.h.server[0] == '\0') || (stgreq.u1.h.fileid == 0))) {
					memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
				} else {
					strcpy(Cnsfileid.server,stgreq.u1.h.server);
					Cnsfileid.fileid = stgreq.u1.h.fileid;
				}
				/* The following allows to bypass call to Cns_statx if and only if have_Cnsfilestat is set */
				if ((have_Cnsfilestat) || ((! have_Cnsfilestat) && (Cns_statx(hsmfiles[ihsmfiles], &Cnsfileid, &Cnsfilestat) == 0))) {
					/* File already exist */
					/* We already checked that nbdskf is equal to nhsmfiles */
					/* So using index ihsmfiles onto argv[Coptind + ihsmfiles] will point */
					/* immediately to the correct disk file associated with this HSM file */
					/* We compare the size of the disk file with the size in Name Server */
					if (api_out != 0 && (nstpp_input != nstcp_input)) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						/* Note - per construction u1.m.xfile and u1.h.xfile points to same string */
						sendrep (&rpfd, MSG_ERR, STG119, stcp_input[ihsmfiles].u1.h.xfile);
						global_c_stagewrt++;
						c = (api_out != 0) ? serrno : EINVAL;
						goto stagewrt_continue_loop;
					}

					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					PRE_RFIO;
					yetdone_rfio_stat_path = (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath;
					if (RFIO_STAT64(yetdone_rfio_stat_path, &filemig_stat) < 0) {
						int save_serrno = rfio_serrno();
						sendrep (&rpfd, MSG_ERR, STG02, (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath, RFIO_STAT64_FUNC((api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath), rfio_serror());
						global_c_stagewrt++;
						c = (api_out != 0) ? save_serrno : EINVAL;
						goto stagewrt_continue_loop;
					}
					yetdone_rfio_stat = &filemig_stat;
					SET_CORRECT_EUID_EGID;
					correct_size = (u_signed64) filemig_stat.st_size;
					if (stgreq.size && (stgreq.size < correct_size)) {
						/* If user specified a maxsize of bytes to transfer and if this */
						/* maxsize is lower than physical file size, then the size of */
						/* of the migrated file will be the minimum of the twos */
						correct_size = stgreq.size;
					}
					if (correct_size > 0 && correct_size == Cnsfilestat.filesize) {
						/* Same size and > 0 : we assume user asks for a new copy */
						strcpy(stgreq.u1.h.server,Cnsfileid.server);
						stgreq.u1.h.fileid = Cnsfileid.fileid;
						stgreq.u1.h.fileclass = Cnsfilestat.fileclass;
					} else if (correct_size <= 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (&rpfd, MSG_ERR,
								 "STG98 - %s size is of zero size - not migrated\n",
								 (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath);
						global_c_stagewrt++;
						c = EINVAL;
						goto stagewrt_continue_loop;
					} else {
						/* Not the same size or diskfile size is zero */
						if (Cnsfilestat.filesize > 0) {
							setegid(start_passwd.pw_gid);
							seteuid(start_passwd.pw_uid);
							sendrep (&rpfd, MSG_ERR,
									 "STG98 - %s renewed (size differs vs. %s)\n",
									 hsmfiles[ihsmfiles],
									 (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath);
							setegid(stgreq.gid);
							seteuid(stgreq.uid);
						}
						stagewrt_Cns_creatx_mode = (07777 & filemig_stat.st_mode);
						forced_Cns_creatx = 1;
					}
				} else if (nohsmcreat_flag != 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_statx", sstrerror(serrno));
					global_c_stagewrt++;
					c = (api_out != 0) ? serrno : EINVAL;
					goto stagewrt_continue_loop;
				} else {
					/* It is not point to try to migrated something of zero size */
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					PRE_RFIO;
					yetdone_rfio_stat_path = (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath;
					if (RFIO_STAT64(yetdone_rfio_stat_path, &filemig_stat) < 0) {
						int save_serrno = rfio_serrno();
						sendrep (&rpfd, MSG_ERR, STG02, (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath, RFIO_STAT64_FUNC((api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath), rfio_serror());
						global_c_stagewrt++;
						c = (api_out != 0) ? save_serrno : EINVAL;
						goto stagewrt_continue_loop;
					}
					yetdone_rfio_stat = &filemig_stat;
					SET_CORRECT_EUID_EGID;
					if (filemig_stat.st_size <= 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (&rpfd, MSG_ERR,
								 "STG98 - %s size is of zero size - not migrated\n",
								 (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath);
						global_c_stagewrt++;
						c = EINVAL;
						goto stagewrt_continue_loop;
					}
					/* We will have to create it */
					stagewrt_Cns_creatx_mode = (07777 & filemig_stat.st_mode);
					forced_Cns_creatx = 1;
					forced_rfio_stat = 1;
				}
				if (forced_Cns_creatx != 0) {
					/* mode_t okmode; */

					/* SET_CORRECT_OKMODE; */
					SET_CORRECT_EUID_EGID;
					if (Cns_creatx(hsmfiles[ihsmfiles], stagewrt_Cns_creatx_mode, &Cnsfileid) != 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_creatx", sstrerror(serrno));
						global_c_stagewrt++;
						c = (api_out != 0) ? serrno : EINVAL;
						goto stagewrt_continue_loop;
					}
					strcpy(stgreq.u1.h.server,Cnsfileid.server);
					stgreq.u1.h.fileid = Cnsfileid.fileid;
					/* The fileclass will be added if necessary by update_migpool() called below */
				}
				/* Here, in any case, Cnsfileid is filled */
				break;
			case 'm':
				/* Overwriting an existing non-CASTOR HSM file is not allowed */
				PRE_RFIO;
				if (rfio_stat64(hsmfiles[ihsmfiles], &filemig_stat) == 0) {
					int save_serrno = rfio_serrno();
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "rfio_stat64", "file already exists");
					global_c_stagewrt++;
					c = (api_out != 0) ? save_serrno : EINVAL;
					goto stagewrt_continue_loop;
				}
				break;
			default:
				break;
			}
			setegid(start_passwd.pw_gid);
			seteuid(start_passwd.pw_uid);
			if (stgreq.t_or_d == 'h' && forced_Cns_creatx != 0) {
				if (forced_rfio_stat != 0) {
					int rfio_stat_rc;
					
					if ((yetdone_rfio_stat_path != NULL) && (strcmp(yetdone_rfio_stat_path,upath) == 0)) {
						memcpy(&st,yetdone_rfio_stat, sizeof(struct stat64));
						rfio_stat_rc = 0;
					} else {
						PRE_RFIO;
						rfio_stat_rc = RFIO_STAT64(upath, &st);
					}
					if (rfio_stat_rc == 0) {
						correct_size = (u_signed64) st.st_size;
						if (stgreq.size && (stgreq.size < correct_size)) {
							/* If user specified a maxsize of bytes to transfer and if this */
							/* maxsize is lower than physical file size, then the size of */
							/* of the migrated file will be the minimum of the twos */
							correct_size = stgreq.size;
						}
					} else {
						/* This must have fail because of RFIO */
						int save_serrno = rfio_serrno();
						sendrep (&rpfd, MSG_ERR, STG02, upath,
								 RFIO_STAT64_FUNC(upath), rfio_serror());
						global_c_stagewrt++;
						c = (api_out != 0) ? save_serrno : EINVAL;
						goto stagewrt_continue_loop;
					}
				}
				/* We set the size in the name server */
				SET_CORRECT_EUID_EGID;
				if (Cns_setfsize(NULL,&Cnsfileid,correct_size) != 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (&rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_setfsize", sstrerror(serrno));
					global_c_stagewrt++;
					global_c_stagewrt_SYERR++;
					global_c_stagewrt_last_serrno = serrno;
					c = (api_out != 0) ? serrno : SESYSERR;
					goto stagewrt_continue_loop;
				}
				setegid(start_passwd.pw_gid);
				seteuid(start_passwd.pw_uid);
			}
			if ((stgreq.t_or_d == 'h') && (stage_wrt_migration != 0)) {
				struct stgcat_entry save_stcp = *stcp;
				/* We copy sensible part of the structure */
				stcp = newreq((int) stgreq.t_or_d);
				memcpy (stcp, &stgreq, sizeof(stgreq));
				COPY_SENSIBLE_STCP(stcp,&save_stcp);
			} else {
				stcp = newreq((int) stgreq.t_or_d);
				memcpy (stcp, &stgreq, sizeof(stgreq));
				/* memcpy overwritted the -1 default values */
				if (stcp->t_or_d == 'h') {
					stcp->u1.h.retenp_on_disk = -1;
					stcp->u1.h.mintime_beforemigr = -1;
				}
			}
			if (stgreq.t_or_d == 'h') {
				stcp->u1.h.flag = 0;
				switch (stgreq.u1.h.tppool[0]) {
				case '\0':
					/* We will select a tape pool - giving us possiblity to change while migrating, if necessary */
					stcp->u1.h.flag |= STAGE_AUTO_TPPOOL;
					break;
				default:
					/* Use gave a tape pool and may have give us possibility to change while migrating, if necessary */
					if (volatile_tppool_flag) stcp->u1.h.flag |= STAGE_AUTO_TPPOOL;
					break;
				}
			}
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = (Aflag ? (STAGEOUT|CAN_BE_MIGR) : STAGEWRT);
			strcpy (stcp->poolname, actual_poolname);
			if (stgreq.t_or_d == 'h') {
				stcp->actual_size = correct_size;
			}
			{
				int rfio_stat_rc;
					
				if ((yetdone_rfio_stat_path != NULL) && (strcmp(yetdone_rfio_stat_path,upath) == 0)) {
					memcpy(&st,yetdone_rfio_stat, sizeof(struct stat64));
					rfio_stat_rc = 0;
				} else {
					PRE_RFIO;
					rfio_stat_rc = RFIO_STAT64(upath, &st);
				}
				if (rfio_stat_rc == 0) {
					if (stgreq.t_or_d != 'h') {
						stcp->actual_size = st.st_size;
					}
#ifndef NON_WORD_READABLE_SUPPORT
					else {
						/* Note: we made sure this is executed only for CASTOR files (t_or_d == 'h') */
						hsmstat[ihsmfiles] = st;
					}
#endif
					stcp->c_time = st.st_mtime;
				} else {
					/* This must have fail because of RFIO */
					stglogit(func, STG02, upath, RFIO_STAT64_FUNC(upath), rfio_serror());
				}
			}
			stcp->a_time = time(NULL);
			if (! stcp->c_time) stcp->c_time = stcp->a_time;
			strcpy (stcp->ipath, upath);
			stcp->nbaccesses = 1;
			iokstagewrt++;
			if (stcp->t_or_d == 'h') {
				int ifileclass;

				if (stcp->u1.h.tppool[0] == '\0') {
					if ((ifileclass = upd_fileclass(NULL,stcp,0,0,1)) < 0) {
						if ((serrno != CLEARED) && (serrno != ESTCLEARED)) {
							sendrep (&rpfd, MSG_ERR, STG132, stcp->u1.h.xfile, sstrerror(serrno));
						} else {
							/* This is not really a stageclr but more a user error */
							serrno = EINVAL;
						}
						global_c_stagewrt++;
						c = (api_out != 0) ? serrno : EINVAL;
						delreq(stcp,1);
						goto stagewrt_continue_loop;
					}
					if (! Aflag) {
						strcpy(stcp->u1.h.tppool,(this_tppool[0] != '\0') ? this_tppool : next_tppool(&(fileclasses[ifileclass])));
					}
					/* Remember this for eventual next iteration */
					if (this_tppool[0] == '\0') {
						strcpy(this_tppool,stcp->u1.h.tppool);
					}
				} else {
					if (Aflag) {        /* If Aflag is not set - check is done a little bit after */
						/* User specified a tape pool - We check the permission to access it */
						{
							struct stgcat_entry stcx = *stcp;
							stcx.uid = save_uid;
							stcx.gid = save_gid;
							/* stcx is a dummy req which inherits the uid/gid of the caller. */
							/* We use it at the first round to check that requestor's uid/gid is compatible */
							/* the uid/gid under which migration will effectively run might be different (case of stage:st) */
							if (euid_egid(&euid,&egid,stcp->u1.h.tppool,NULL,stcp,(iokstagewrt == 0) ? &stcx : NULL,NULL,0,0) != 0) {
								global_c_stagewrt++;
								c = (api_out != 0) ? serrno : EINVAL;
								delreq(stcp,1);
								goto stagewrt_continue_loop;
							}
							/* Remember this for eventual next iteration */
							if (this_tppool[0] == '\0') strcpy(this_tppool,stcp->u1.h.tppool);
						}
						if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
							global_c_stagewrt++;
							c = (api_out != 0) ? serrno : EINVAL;
							delreq(stcp,1);
							goto stagewrt_continue_loop;
						}
					}
				}
				if ((stage_wrt_migration == 0) && (actual_poolname[0] != '\0')) {
					/* This entry is in the pool anyway and this is not for internal migration */
					/* if (! Aflag) update_migpool(&stcp,1,4); */
					/* Upper line comment: don't needed in theory - Condition move DELAY -> CAN_BE_MIGR */
#ifdef USECDB
					/* Because update_migpool can do a call to stgdb_upd_stgcat() */
					if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
						stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					if (update_migpool(&stcp,1,Aflag ? 0 : 1) != 0) {
						global_c_stagewrt++;
						c = (api_out != 0) ? serrno : EINVAL;
						delreq(stcp,0);
						goto stagewrt_continue_loop;
					}
				}
			}
#ifdef USECDB
			if (! ((stcp->t_or_d == 'h') && (stage_wrt_migration == 0) && (actual_poolname[0] != '\0'))) {
				/* Avoid doing twice an insert */
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
			}
#endif
			if (! Aflag) {
				/* This is immediate migration */
				if (!wqp) {
					/* We determine under which euid:egid this migration has to run */
					if (ncastorfiles > 0) {
						{
							struct stgcat_entry stcx = *stcp;
							stcx.uid = save_uid;
							stcx.gid = save_gid;
							/* stcx is a dummy req which inherits the uid/gid of the caller. */
							/* We use it at the first round to check that requestor's uid/gid is compatible */
							/* the uid/gid under which migration will effectively run might be different (case of stage:st) */
							if (euid_egid(&euid,&egid,(this_tppool[0] != '\0') ? this_tppool : tppool,NULL,stcp,(iokstagewrt == 0) ? &stcx : NULL,&tppool,0,0) != 0) {
								global_c_stagewrt++;
								c = (api_out != 0) ? serrno : EINVAL;
								delreq(stcp,0);
								goto stagewrt_exit_loop;
							}
							/* Remember this for eventual next iteration */
							if (this_tppool[0] == '\0') strcpy(this_tppool,tppool);
						}
						if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
							global_c_stagewrt++;
							c = (api_out != 0) ? serrno : EINVAL;
							delreq(stcp,0);
							goto stagewrt_exit_loop;
						}
					} else {
						euid = stgreq.uid;
						egid = stgreq.gid;
						strcpy(user_waitq,save_user);
						strcpy(group_waitq,save_group);
					}
					uid_waitq = euid;
					gid_waitq = egid;
#ifndef NON_WORD_READABLE_SUPPORT
					if (stcp->t_or_d == 'h') {
						/* We check if (euid,egid) will be able to open that file */
						if (stage_access(euid,egid,R_OK,&hsmstat[ihsmfiles]) != 0) {
							sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "migrator access", sstrerror(serrno));
							/* Try to change mode bits to world-readable */
							hsmstat[ihsmfiles].st_mode |= S_IRUSR|S_IRGRP|S_IROTH;
							if (rfio_chmod(stcp->ipath,hsmstat[ihsmfiles].st_mode) != 0) {
								global_c_stagewrt++;
								c = (api_out != 0) ? serrno : EINVAL;
								delreq(stcp,1);
								goto stagewrt_continue_loop;
							} else {
								sendrep (&rpfd, MSG_ERR, STG33, stcp->ipath, "Forced world readable-access");
							}
						}
					}
#endif
					wqp = add2wq (clienthost,
								  user, stcp->uid, stcp->gid,
								  user_waitq, group_waitq, uid_waitq, gid_waitq,
								  clientpid, Upluspath, reqid, req_type,
								  nbdskf, &wfp, (nohsmcreat_flag != 0) ? &save_subreqid : NULL, 
								  stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq,
								  (nohsmcreat_flag != 0) ? 1 : 0);
					wqp->copytape = copytape;
					wqp->api_out = api_out;
					wqp->magic = magic;
					wqp->uniqueid = stage_uniqueid;
					wqp->silent = silent_flag;
					wqp->noretry = noretry_flag;
					wqp->flags = flags;
					if (nowait_flag != 0) {
						/* User said --nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
					} else if (save_rpfd >= 0) {
						/* User said only --silent : we prepare restore original rpfd */
						wqp->silent = 1;
						wqp->rpfd = -1;
						wqp->save_rpfd = save_rpfd;
					}
				}
#ifndef NON_WORD_READABLE_SUPPORT
				else {
					if (stcp->t_or_d == 'h') {
						/* We check if (euid,egid) will be able to open that file */
						if (stage_access(euid,egid,R_OK,&hsmstat[ihsmfiles]) != 0) {
							sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "migrator access", sstrerror(serrno));
							/* Try to change mode bits to world-readable */
							hsmstat[ihsmfiles].st_mode |= S_IRUSR|S_IRGRP|S_IROTH;
							if (rfio_chmod(stcp->ipath,hsmstat[ihsmfiles].st_mode) != 0) {
								global_c_stagewrt++;
								c = (api_out != 0) ? serrno : EINVAL;
								delreq(stcp,1);
								goto stagewrt_continue_loop;
							} else {
								sendrep (&rpfd, MSG_ERR, STG33, stcp->ipath, "Forced world readable-access");
							}
						}
					}
				}
#endif

				wfp->subreqid = stcp->reqid;
				if (save_subreqid != NULL) {
					*save_subreqid = stcp->reqid;
					save_subreqid++;
				}
				wqp->nbdskf++;
				if (i < nbtpf || nhsmfiles > 0)
					wqp->nb_subreqs++;
				wfp++;
			}
			break;
		  stagewrt_continue_loop:
			/* Some cleaning in case... */
			if ((stage_wrt_migration != 0) && (have_save_stcp_for_Cns_creatx != 0)) {
				struct stgcat_entry *stclp;

				have_save_stcp_for_Cns_creatx = 0;
				for (stclp = stcs; stclp < stce; stclp++) {
					if (stclp->reqid == 0) break;
					if (stclp->reqid != save_stcp_for_Cns_creatx.reqid) continue;
					update_migpool(&stclp,-1,0);
					stclp->status |= PUT_FAILED;
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stclp) != 0) {
						stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					break;
				}
			}
			break;
		  stagewrt_exit_loop:
			/* Some cleaning in case... */
			if ((stage_wrt_migration != 0) && (have_save_stcp_for_Cns_creatx != 0)) {
				struct stgcat_entry *stclp;

				have_save_stcp_for_Cns_creatx = 0;
				for (stclp = stcs; stclp < stce; stclp++) {
					if (stclp->reqid == 0) break;
					if (stclp->reqid != save_stcp_for_Cns_creatx.reqid) continue;
					update_migpool(&stclp,-1,0);
					stclp->status |= PUT_FAILED;
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stclp) != 0) {
						stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					break;
				}
			}
			goto reply;
		case STAGECAT:
			if ((p = findpoolname (upath)) != NULL) {
				if (poolflag < 0 ||
					(poolflag > 0 && strcmp (stgreq.poolname, p)))
					sendrep (&rpfd, MSG_ERR, STG49, upath, p);
				actual_poolflag = 1;
				strcpy (actual_poolname, p);
			} else {
				if (poolflag > 0)
					sendrep (&rpfd, MSG_ERR, STG50, upath);
				actual_poolflag = -1;
				actual_poolname[0] = '\0';
			}
			switch (isstaged (&stgreq, &stcp, actual_poolflag, actual_poolname, rdonly_flag, poolname_exclusion, &isstaged_nomore, NULL, NULL, isstaged_count++)) {
			case ISSTAGEDBUSY:
				c = EBUSY;
				goto reply;
			case ISSTAGEDSYERR:
				c = SESYSERR;
				goto reply;
			case NOTSTAGED:
				break;
			default:
				sendrep (&rpfd, MSG_ERR, STG40);
				c = EINVAL;
				goto reply;
			}
			stcp = newreq((int) stgreq.t_or_d);
			memcpy (stcp, &stgreq, sizeof(stgreq));
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = STAGEIN | STAGED;
			strcpy (stcp->poolname, actual_poolname);
			PRE_RFIO;
			if (RFIO_STAT64(upath, &st) < 0) {
				sendrep (&rpfd, MSG_ERR, STG02, upath, RFIO_STAT64_FUNC(upath),
						 rfio_serror());
				delreq(stcp,1);
				goto reply;
			}
			/* If group is different than requestor's group we verify its existence in group file */
			/* But if it fails this is not formally an error : we will let the requestor's group in stgreq */
			stcp->gid = st.st_gid;
			if ((gr = Cgetgrgid (stcp->gid)) == NULL) {
				if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
				sendrep (&rpfd, MSG_ERR, STG36, st.st_gid);
			} else {
				strncpy (stcp->group, gr->gr_name, CA_MAXGRPNAMELEN);
				stcp->group[CA_MAXGRPNAMELEN] = '\0';
			}
			/* If user is different than requestor's group we verify its existence in password file */
			/* But if it fails this is not formally an error : we will let the requestor's name in stgreq */
			stcp->uid = st.st_uid;
			if ((pw = Cgetpwuid (stcp->uid)) == NULL) {
				char uidstr[8];
				if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetpwuid", strerror(errno));
				sprintf (uidstr, "%d", stgreq.uid);
				sendrep (&rpfd, MSG_ERR, STG11, uidstr);
			} else {
				strncpy (stcp->user, pw->pw_name, CA_MAXUSRNAMELEN);
				stcp->user[CA_MAXUSRNAMELEN] = '\0';
			}
			if (stcp->size <= 0) stcp->size = st.st_size;
			stcp->actual_size = st.st_size;
			stcp->c_time = st.st_mtime;
			stcp->a_time = st.st_atime;
			stcp->nbaccesses = 1;
			strcpy (stcp->ipath, upath);
#ifdef USECDB
			if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (api_out != 0) sendrep(&rpfd, API_STCP_OUT, stcp, magic);
			break;
		}
	}
	if ((req_type == STAGEWRT) && (global_c_stagewrt != 0) && (global_c_stagewrt == nbdskf)) {
		/* All entries have failed... */
		c = (global_c_stagewrt_SYERR ? global_c_stagewrt_last_serrno : EINVAL);
		goto reply;
	}
	savepath ();
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	if (fseq_list != NULL) {free (fseq_list); fseq_list = NULL;}
	if (argv != NULL) {free (argv); argv = NULL;}
	if (*(wqp->waiting_pool)) {
		wqp->nb_clnreq++;
		cleanpool (wqp->waiting_pool);
	} else if (wqp->nb_subreqs > wqp->nb_waiting_on_req) {
		if ((c = fork_exec_stager (wqp)) != 0) goto reply;
	}
	if (save_rpfd >= 0) {
		/* Next sendrep will be the STAGERC. This one is silent by default */
		/* (from stgdaemon point of view). We have to restore now only */
		/* the true rpfd in case user ran in --silent mode */
		rpfd = save_rpfd;
		save_rpfd = -1;
	}
	if (nowait_flag != 0) {
		sendrep (&rpfd, STAGERC, req_type, magic, c);
		rpfd = -1;
	}
	if (hsmfiles != NULL) free(hsmfiles);
#ifndef NON_WORD_READABLE_SUPPORT
	if (hsmstat != NULL) free(hsmstat);
#endif
	if (hsmpoolflags != NULL) free(hsmpoolflags);
	if (stcp_input != NULL) free(stcp_input);
	if (stpp_input != NULL) free(stpp_input);
	return;
  reply:
	STAGE_TIME_START("end of processing");
	STAGE_TIME_START("end of processing : free ressources");
	if (fseq_list != NULL) free (fseq_list);
	if (argv != NULL) free (argv);
	if (hsmfiles != NULL) free(hsmfiles);
#ifndef NON_WORD_READABLE_SUPPORT
	if (hsmstat != NULL) free(hsmstat);
#endif
	if (hsmpoolflags != NULL) free(hsmpoolflags);
	if (stcp_input != NULL) free(stcp_input);
	if (stpp_input != NULL) free(stpp_input);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
			   reqid, req_type, 0, c, NULL, "", (char) 0);
#endif
	STAGE_TIME_END;
	if (save_rpfd >= 0) {
		/* Next sendrep will be the STAGERC. This one is silent by default */
		/* (from stgdaemon point of view). We have to restore now only */
		/* the true rpfd in case user ran in --silent mode */
		rpfd = save_rpfd;
		save_rpfd = -1;
	}
	STAGE_TIME_START("answer to client");
	sendrep (&rpfd, STAGERC, req_type, magic, c);
	STAGE_TIME_END;
	if (c && wqp) {
		int found;
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			found = 0;
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid) {
					found = 1;
					break;
				}
			}
			if (found == 0) continue;
			if (! wfp->waiting_on_req)
				updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
							  (signed64) stcp->size);
			if (ISSTAGEWRT(stcp) && (t_or_d == 'h') && HAVE_SENSIBLE_STCP(stcp)) {
				/* This was a stagewrt on CASTOR hsm files. */
				/* We have to find the corresponding record that is in STAGEOUT|CAN_BE_MIGR|BEING_MIGR */
				struct stgcat_entry *stcp_search, *stcp_found;
				struct stgcat_entry *save_stcp = stcp;
				int logged_once = 0;

				stcp_found = NULL;
				for (stcp_search = stcs; stcp_search < stce; stcp_search++) {
					if (stcp_search->reqid == 0) break;
					if (! (ISCASTORBEINGMIG(stcp_search) || ISCASTORWAITINGMIG(stcp))) continue;
					if ((strcmp(stcp_search->u1.h.xfile,save_stcp->u1.h.xfile) == 0) &&
						(strcmp(stcp_search->u1.h.server,save_stcp->u1.h.server) == 0) &&
						(stcp_search->u1.h.fileid == save_stcp->u1.h.fileid) &&
						(stcp_search->u1.h.fileclass == save_stcp->u1.h.fileclass) &&
						(strcmp(stcp_search->u1.h.tppool,save_stcp->u1.h.tppool) == 0)) {
						if (stcp_found == NULL) {
							stcp_found = stcp_search;
							break;
						}
					}
				}
				if (stcp_found != NULL) {
					update_migpool(&stcp_found,-1,0);
					stcp_found->status = STAGEOUT | PUT_FAILED | CAN_BE_MIGR;
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp_found) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					savereqs();
				} else {
					update_migpool(&stcp,-1,0);
					if (! logged_once) {
						/* Print this only once per migration request */
						stglogit(func, "STG02 - Could not find corresponding original request - decrementing anyway migrator counters\n");
						logged_once++;
					}
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					savereqs();
				}
			}
			delreq(stcp,0);
		}
		rmfromwq (wqp);
	}
	STAGE_TIME_END;
	STAGE_TIME_END;
	return;
}

void procputreq(req_type, magic, req_data, clienthost)
	int req_type;
	int magic;
	char *req_data;
	char *clienthost;
{
	char **argv = NULL;
	int c, i;
	int clientpid;
	char *dp;
	int errflg = 0;
	char *externfile = NULL;
	int found;
	char fseq[CA_MAXFSEQLEN + 1];
	gid_t gid;
	struct group *gr;
	int Iflag = 0;
	int Mflag = 0;
	char *name;
	int nargs = 0;
	int nbdskf;
	int numvid = 0;
	int n1 = 0;
	int n2 = 0;
	char *q;
	char *rbp;
	struct stat64 st;
	struct stgcat_entry *stcp;
	int subreqid;
	int Upluspath = 0;
	uid_t uid;
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;
	char vid[7];
	struct waitf *wfp;
	struct waitq *wqp = NULL;
	char **hsmfiles = NULL;
	struct stgcat_entry **hsmfilesstcp = NULL;
	int nhsmfiles = 0;
	int ihsmfiles;
	int jhsmfiles;
	int nhpssfiles = 0;
	int ncastorfiles = 0;
	int ntapefiles = 0;
	int ndiskfiles = 0;
	int nuserlevel = 0;
	int nexplevel = 0;
	uid_t euid = 0;
	gid_t egid = 0;
	uid_t uid_waitq = 0;
	gid_t gid_waitq = 0;
	char user_waitq[CA_MAXUSRNAMELEN+1];
	char group_waitq[CA_MAXGRPNAMELEN+1];
	char *tppool = NULL;
	int ifileclass;
	char save_group[CA_MAXGRPNAMELEN+1];
	char save_user[CA_MAXUSRNAMELEN+1];
	char this_tppool[CA_MAXPOOLNAMELEN+1];
	static struct Coptions longopts[] =
	{
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"fortran_unit",       REQUIRED_ARGUMENT,  NULL,      'U'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"nowait",             NO_ARGUMENT,  &nowait_flag,      1},
		{"noretry",            NO_ARGUMENT,  &noretry_flag,      1},
		{"tppool",             REQUIRED_ARGUMENT, &tppool_flag, 1},
		{NULL,                 0,                  NULL,        0}
	};

	this_tppool[0] = '\0';

	rbp = req_data;
	local_unmarshall_STRING (rbp, user);  /* login name */
	local_unmarshall_STRING (rbp, name);
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	unmarshall_WORD (rbp, clientpid);

	if (stage_util_validuser(NULL,(uid_t) uid, (gid_t) gid) != 0) {
		char uidstr[8];
		sprintf (uidstr, "%d", uid);
		sendrep (&rpfd, MSG_ERR, STG11, uidstr);
		c = SESYSERR;
		goto reply;
	}
	if ((gr = Cgetgrgid (gid)) == NULL) {
		if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (&rpfd, MSG_ERR, STG36, gid);
		c = SESYSERR;
		goto reply;
	}

	strncpy(save_user,user,CA_MAXUSRNAMELEN);
	save_user[CA_MAXUSRNAMELEN] = '\0';
	strncpy(save_group,gr->gr_name, CA_MAXGRPNAMELEN);
	save_group[CA_MAXGRPNAMELEN] = '\0';

	stglogit (func, STG92, "stageput", user, uid, gid, clienthost);
	nargs = req2argv (rbp, &argv);
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost,
			   reqid, STAGEPUT, 0, 0, NULL, "", (char) 0);
#endif

	Coptind = 1;
	Copterr = 0;
	nowait_flag = 0;
	noretry_flag = 0;
	tppool_flag = 0;
	while ((c = Cgetopt_long (nargs, argv, "Gh:I:M:q:U:V:", longopts, NULL)) != -1) {
		switch (c) {
		case 'G':
			break;
		case 'h':
			break;
		case 'I':
			externfile = Coptarg;
			Iflag = 1;
			break;
		case 'M':
			Mflag = 1;
			if (nhsmfiles == 0) {
				if ((hsmfiles = (char **) malloc(sizeof(char *))) == NULL ||
                    (hsmfilesstcp = (struct stgcat_entry **) malloc(sizeof(struct stgcat_entry *))) == NULL) {
					c = SESYSERR;
					goto reply;
				}
			} else {
				char **dummy = hsmfiles;
				struct stgcat_entry **dummy2 = hsmfilesstcp;
				if ((dummy = (char **) realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
					c = SESYSERR;
					goto reply;
				}
				hsmfiles = dummy;
				if ((dummy2 = (struct stgcat_entry **) realloc(hsmfilesstcp,(nhsmfiles+1) * sizeof(struct stgcat_entry *))) == NULL) {
					c = SESYSERR;
					goto reply;
				}
				hsmfilesstcp = dummy2;
			}
			hsmfiles[nhsmfiles++] = Coptarg;
			break;
		case 'q':       /* file sequence number(s) */
			if ((q = strchr (Coptarg, '-')) != NULL) {
				*q = '\0';
				stage_strtoi(&n2, q + 1, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				stage_strtoi(&n1, Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				*q = '-';
			} else {
				stage_strtoi(&n1, Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				n2 = n1;
			}
			break;
		case 'V':       /* visual identifier */
			strcpy (vid, Coptarg);
			UPPER (vid);
			numvid++;
			break;
		case 0:
			/* Long option without short option correspondance */
			if (tppool_flag != 0) {
				tppool = Coptarg;
			}
			break;
		case '?':
			errflg++;
			break;
		default:
			errflg++;
			break;
		}
		if (errflg != 0) break;
	}
	if (Iflag && numvid)
		errflg++;

	if (Mflag && numvid)
		errflg++;

	if ((Iflag || Mflag) && (Coptind != nargs))
		errflg++;

	if (errflg != 0) {
		c = EINVAL;
		goto reply;
	}

	/* In case of hsm request verify the exact mapping between number of hsm files */
	/* and number of disk files.                                                   */
	if (nhsmfiles > 0) {
		/* We also check that there is NO redundant hsm files (multiple equivalent ones) */
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			if ((c = check_hsm_type(hsmfiles[ihsmfiles],&nhpssfiles,&ncastorfiles,&nuserlevel,&nexplevel,NULL)) != 0) {
				goto reply;
			}
			for (jhsmfiles = ihsmfiles + 1; jhsmfiles < nhsmfiles; jhsmfiles++) {
				if (strcmp(hsmfiles[ihsmfiles],hsmfiles[jhsmfiles]) == 0) {
					sendrep (&rpfd, MSG_ERR, STG59, hsmfiles[ihsmfiles]);
					c = EINVAL;
					goto reply;
				}
			}
		}
	}

	if (numvid) {
		if (n1 == 0)
			n2 = n1 = 1;
		nbdskf = n2 - n1 + 1;
		for (i = n1; i <= n2; i++) {
			sprintf (fseq, "%d", i);
			found = 0;
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if (stcp->t_or_d != 't') continue;
				if (strcmp (stcp->u1.t.vid[0], vid) != 0) continue;
				if (strcmp (stcp->u1.t.fseq, fseq)) continue;
				found = 1;
				break;
			}
			if (found == 0 ||
				((stcp->status != STAGEOUT) &&
				 (stcp->status != (STAGEOUT|PUT_FAILED)))) {
				sendrep (&rpfd, MSG_ERR, STG22);
				c = ENOENT;
				goto reply;
			}
			if (stcp->status == STAGEOUT) {
				u_signed64 actual_size_block;
				PRE_RFIO;
				if (RFIO_STAT64(stcp->ipath, &st) == 0) {
					stcp->actual_size = st.st_size;
					if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
						actual_size_block = stcp->actual_size;
					}
				} else {
					sendrep(&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
					c = rfio_serrno();
					goto reply;
				}
				updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
							  (signed64) (((signed64) stcp->size) - (signed64) actual_size_block));
			}
			stcp->status = STAGEPUT;
			stcp->a_time = time(NULL);
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (!wqp) {
				wqp = add2wq (clienthost,
							  user, uid, gid,
							  user, save_group, uid, gid,
							  clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp,
							  NULL, NULL, NULL, 0);
				if (nowait_flag != 0) {
					/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
					wqp->silent = 1;
					wqp->rpfd = -1;
				}
				wqp->noretry = noretry_flag;
            }
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		}
	} else if (Mflag || Iflag) {
		nbdskf = Mflag ? nhsmfiles : 1;
		found = 0;
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (Mflag) {
				int gotit;

				if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') continue;
				if (stcp->t_or_d != 'm' && nhpssfiles > 0) continue;
				if (stcp->t_or_d != 'h' && ncastorfiles > 0) continue;
				/* It can happen that we get twice the same HSM filename on the same stageput */
				/* Neverthless the first one cannot be catched a second time because it will have */
				/* to fulfill also the correction condition on its current status */
				if (ISCASTORFREE4MIG(stcp)) {
					gotit = 0;
					for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
						/* We check if the HSM file name matches */
						if (strcmp (stcp->t_or_d == 'm' ?
									stcp->u1.m.xfile :
									stcp->u1.h.xfile,
									hsmfiles[ihsmfiles]) != 0) continue;
						hsmfilesstcp[found++] = stcp;
						gotit = 1;
						break;
					}
					if (gotit == 0) continue;
					/* We determine under which euid:egid this migration has to run */
					if (ncastorfiles > 0) {
						/* If tppool specified in input very that it is compatible with eventual yet asisgned tppool */
						/* if tppool is NULL at first iteration, it will be set by the euid_egid() call */
						if ((stcp->u1.h.tppool[0] != '\0') && (((tppool != NULL) && (strcmp(stcp->u1.h.tppool,tppool) != 0)) || ((this_tppool[0] != '\0') && (strcmp(stcp->u1.h.tppool,this_tppool) != 0)))) {
							/* We update the catalog : user wanted to specify explicitely a new tape pool */
							sendrep (&rpfd, MSG_ERR, STG159, stcp->u1.h.xfile, (tppool != NULL) ? tppool : this_tppool, stcp->u1.h.tppool);
							strcpy(stcp->u1.h.tppool,(tppool != NULL) ? tppool : this_tppool);
#ifdef USECDB
							if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
								stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
							}
#endif
						}
						if (stcp->u1.h.tppool[0] == '\0') {
							if ((ifileclass = upd_fileclass(NULL,stcp,5,0,0)) < 0) {
								if ((serrno != CLEARED) && (serrno != ESTCLEARED)) {
									sendrep (&rpfd, MSG_ERR, STG132, stcp->u1.h.xfile, sstrerror(serrno));
								} else {
									/* This is not really a stageclr but more a user error */
									serrno = EINVAL;
								}
								c = serrno;
								goto reply;
							}
							strcpy(stcp->u1.h.tppool,(this_tppool[0] != '\0') ? this_tppool : next_tppool(&(fileclasses[ifileclass])));
						}
						/* Remember this for eventual next iteration */
						if (this_tppool[0] == '\0') strcpy(this_tppool,stcp->u1.h.tppool);
						{
							struct stgcat_entry stcx = *stcp;
							stcx.uid = uid;
							stcx.gid = gid;
							/* stcx is a dummy req which inherits the uid/gid of the caller. */
							/* We use it at the first round to check that requestor's uid/gid is compatible */
							/* the uid/gid under which migration will effectively run might be different (case of stage:st) */
							if (euid_egid(&euid,&egid,(this_tppool[0] != '\0') ? this_tppool : tppool,NULL,stcp,(found == 1) ? &stcx : NULL,&tppool,0,0) != 0) {
								c = EINVAL;
								goto reply;
							}
							/* Remember this for eventual next iteration */
							if (this_tppool[0] == '\0') strcpy(this_tppool,tppool);
						}
						if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
							c = EINVAL;
							goto reply;
						}
					} else {
						euid = uid;
						egid = gid;
						strcpy(user_waitq,save_user);
						strcpy(group_waitq,save_group);
					}
					uid_waitq = euid;
					gid_waitq = egid;
					if (found == nhsmfiles) break;
				}
			} else {
				if (stcp->t_or_d != 'd') continue;
				if ((externfile != NULL) && (strcmp (stcp->u1.d.xfile, externfile) != 0)) continue;
				found = 1;
				break;
			}
		}
		if (Iflag) {
			if (found == 0 ||
				((stcp->status != STAGEOUT) &&
				 (stcp->status != (STAGEOUT|PUT_FAILED)))) {
				sendrep (&rpfd, MSG_ERR, STG22);
				c = ENOENT;
				goto reply;
			}
			if (stcp->status == STAGEOUT) {
				u_signed64 actual_size_block;
				PRE_RFIO;
				if (RFIO_STAT64(stcp->ipath, &st) == 0) {
					stcp->actual_size = st.st_size;
					if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
						actual_size_block = stcp->actual_size;
					}
				} else {
					sendrep(&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
					c = rfio_serrno();
					goto reply;
				}
				updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
							  (signed64) (((signed64) stcp->size) - (signed64) actual_size_block));
			}
			stcp->status = STAGEPUT;
			stcp->a_time = time(NULL);
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (!wqp) {
				wqp = add2wq (clienthost,
							  user, uid, gid,
							  user, save_group, uid, gid,
							  clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp,
							  NULL, NULL, NULL, 0);
				if (nowait_flag != 0) {
					/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
					wqp->silent = 1;
					wqp->rpfd = -1;
				}
				wqp->noretry = noretry_flag;
			}
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		} else {
			if (found != nhsmfiles) {
				sendrep (&rpfd, MSG_ERR, "STG02 - You requested %d hsm files while I found %d of them that you currently can migrate\n",nhsmfiles,found);
				c = EINVAL;
				goto reply;
			}
			for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
				int had_put_failed;

				had_put_failed = (((hsmfilesstcp[ihsmfiles]->status & PUT_FAILED) == PUT_FAILED) ? 1 : 0);
				if ((hsmfilesstcp[ihsmfiles]->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
					if (((hsmfilesstcp[ihsmfiles]->status) & (STAGEWRT|CAN_BE_MIGR)) == (STAGEWRT|CAN_BE_MIGR)) {
						hsmfilesstcp[ihsmfiles]->status |= BEING_MIGR;
					} else {
						hsmfilesstcp[ihsmfiles]->status = STAGEPUT|CAN_BE_MIGR;
					}
					if (! had_put_failed) update_migpool(&(hsmfilesstcp[ihsmfiles]),1,4); /* Condition move DELAY -> CAN_BE_MIGR */
					hsmfilesstcp[ihsmfiles]->a_time = time(NULL);
					strcpy(hsmfilesstcp[ihsmfiles]->u1.h.tppool,tppool);
					if (update_migpool(&(hsmfilesstcp[ihsmfiles]),1,had_put_failed ? 1 : 2) != 0) {
						c = EINVAL;
						goto reply;
					}
				} else {
					int save_status;

					/* We make upd_stageout believe that it is a normal stageput following a stageout */
					save_status = hsmfilesstcp[ihsmfiles]->status;
					hsmfilesstcp[ihsmfiles]->status = STAGEOUT;
					rwcountersfs(hsmfilesstcp[ihsmfiles]->poolname, hsmfilesstcp[ihsmfiles]->ipath, STAGEOUT, STAGEOUT);
					if ((c = upd_stageout (STAGEUPDC, hsmfilesstcp[ihsmfiles]->ipath, &subreqid, 0, NULL, 0, 0)) != 0) {
						if ((c != CLEARED) && (c != ESTCLEARED)) {
							hsmfilesstcp[ihsmfiles]->status = save_status;
						} else {
							/* Entry has been deleted - user asks to put on tape zero-length file !? */
							/* This is not really a stageclr but more a user error */
							c = EINVAL;
						}
						goto reply;
					}
					hsmfilesstcp[ihsmfiles]->status = STAGEPUT;
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,hsmfilesstcp[ihsmfiles]) != 0) {
						stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				}
				/* We check if the file yet exist in the name server and if we have to recreate it */
				if (hsmfilesstcp[ihsmfiles]->t_or_d == 'h') {
					if (stageput_check_hsm(hsmfilesstcp[ihsmfiles],uid,gid,0,NULL,NULL,0) != 0) {
						if ((hsmfilesstcp[ihsmfiles]->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
							update_migpool(&(hsmfilesstcp[ihsmfiles]),-1,0);
							if ((hsmfilesstcp[ihsmfiles]->status & STAGEWRT) == STAGEWRT) {
								delreq(hsmfilesstcp[ihsmfiles],0);
							} else {
								hsmfilesstcp[ihsmfiles]->status = STAGEOUT|PUT_FAILED|CAN_BE_MIGR;
							}
						} else {
							hsmfilesstcp[ihsmfiles]->status = STAGEOUT|PUT_FAILED;
						}
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,hsmfilesstcp[ihsmfiles]) != 0) {
							stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
		                continue;
    	            }
				}
				if (!wqp) {
					wqp = add2wq (clienthost,
								  user, uid, gid,
								  user_waitq, group_waitq, uid_waitq, gid_waitq,
								  clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp,
								  NULL, NULL, NULL, 0);
					if (nowait_flag != 0) {
						/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
					}
					wqp->noretry = noretry_flag;
				}
				wfp->subreqid = hsmfilesstcp[ihsmfiles]->reqid;
				wqp->nbdskf++;
				wqp->nb_subreqs++;
				wfp++;
			}
		}
	} else {
		struct stgcat_entry *found_stcp;

		/* compute number of disk files */
		nbdskf = nargs - Coptind;

		/* We verify that, if there is CASTOR HSM files, there is NO mixing between userlevel and explevel ones */
		/* and, if there are also other types of files, there is NO userlevel CASTOR's ones. */
		for (i = 0; i < nbdskf; i++) {
			strcpy (upath, argv[Coptind+i]);
			if ((c = ask_stageout (STAGEPUT, upath, &found_stcp)) != 0)
				goto reply;
			switch (found_stcp->t_or_d) {
			case 'm':
			case 'h':
				switch (found_stcp->t_or_d) {
				case 'm':
					++nhpssfiles;
                    /* We accept explicit migration only on absence of specific status */
                    if (! ISCASTORFREE4MIG(found_stcp)) {
						sendrep(&rpfd, MSG_ERR, STG33, upath, "is not eligible for explicit migration");
						c = EINVAL;
						goto reply;
                    }
					euid = uid;
					egid = gid;
					strcpy(user_waitq,save_user);
					strcpy(group_waitq,save_group);
					break;
				case 'h':
					++ncastorfiles;
                    /* We accept explicit migration only on absence of specific status */
                    if (! ISCASTORFREE4MIG(found_stcp)) {
						sendrep(&rpfd, MSG_ERR, STG33, upath, "is not eligible for explicit migration");
						c = EINVAL;
						goto reply;
                    }
					/* If tppool specified in input very that it is compatible with eventual yet asisgned tppool */
					/* if tppool is NULL at first iteration, it will be set by the euid_egid() call */
					if ((found_stcp->u1.h.tppool[0] != '\0') && (((tppool != NULL) && (strcmp(found_stcp->u1.h.tppool,tppool) != 0)) || ((this_tppool[0] != '\0') && (strcmp(found_stcp->u1.h.tppool,this_tppool) != 0)))) {
						/* We update the catalog : user wanted to specify explicitely a new tape pool */
						sendrep (&rpfd, MSG_ERR, STG159, found_stcp->u1.h.xfile, (tppool != NULL) ? tppool : this_tppool, found_stcp->u1.h.tppool);
						strcpy(found_stcp->u1.h.tppool,(tppool != NULL) ? tppool : this_tppool);
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,found_stcp) != 0) {
							stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
					}
					if (found_stcp->u1.h.tppool[0] == '\0') {
						if ((ifileclass = upd_fileclass(NULL,found_stcp,5,0,0)) < 0) {
							if ((serrno != CLEARED) && (serrno != ESTCLEARED)) {
								sendrep (&rpfd, MSG_ERR, STG132, found_stcp->u1.h.xfile, sstrerror(serrno));
							} else {
								/* This is not really a stageclr but more a user error */
								serrno = EINVAL;
							}
							c = serrno;
							goto reply;
						}
						strcpy(found_stcp->u1.h.tppool,(this_tppool[0] != '\0') ? this_tppool : next_tppool(&(fileclasses[ifileclass])));
					}
					/* Remember this for eventual next iteration */
					if (this_tppool[0] == '\0') strcpy(this_tppool,found_stcp->u1.h.tppool);
					{
						struct stgcat_entry stcx = *found_stcp;
						stcx.uid = uid;
						stcx.gid = gid;
						/* stcx is a dummy req which inherits the uid/gid of the caller. */
						/* We use it at the first round to check that requestor's uid/gid is compatible */
						/* the uid/gid under which migration will effectively run might be different (case of stage:st) */
						if (euid_egid(&euid,&egid,(this_tppool[0] != '\0') ? this_tppool : tppool,NULL,found_stcp,(i == 0) ? &stcx : NULL,&tppool,0,0) != 0) {
							c = EINVAL;
							goto reply;
						}
						/* Remember this for eventual next iteration */
						if (this_tppool[0] == '\0') strcpy(this_tppool,tppool);
					}
					if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
						c = EINVAL;
						goto reply;
					}
					break;
				}
                /* We will later remind the corresponding stcp */
				if (nhsmfiles == 0) {
					if ((hsmfilesstcp = (struct stgcat_entry **) malloc(sizeof(struct stgcat_entry *))) == NULL) {
						c = SESYSERR;
						goto reply;
					}
				} else {
					struct stgcat_entry **dummy2 = hsmfilesstcp;
					if ((dummy2 = (struct stgcat_entry **) realloc(hsmfilesstcp,(nhsmfiles+1) * sizeof(struct stgcat_entry *))) == NULL) {
						c = SESYSERR;
						goto reply;
					}
					hsmfilesstcp = dummy2;
				}
				hsmfilesstcp[nhsmfiles++] = found_stcp;
				break;
			case 't':
				++ntapefiles;
				euid = uid;
				egid = gid;
				break;
			case 'd':
				++ndiskfiles;
				euid = uid;
				egid = gid;
				break;
			default:
				sendrep (&rpfd, MSG_ERR, STG33, upath, "unknown type");
				c = EINVAL;
				goto reply;
			}
		}
		uid_waitq = euid;
		gid_waitq = egid;
		/* The stager branch is the following : */
		/* if ('d' or 'm') then                 */
		/*   filecopy (through rfio)            */
		/* else                                 */
		/*   if ('t') then                      */
		/*     tape_to_tape                     */
		/*   else             -- then it is 'h' */
		/*     castor_migration                 */
		/*   endif                              */
		/* endif                                */

		/* In the following we make sure that   */
		/* those conditions are fullfilled at   */
		/* fork_exec time.                      */

		/* It is not allowed to mix ('d' or 'm') with other types */
		if ((ndiskfiles > 0 || nhpssfiles > 0) && (ntapefiles > 0 || ncastorfiles > 0)) {
			sendrep (&rpfd, MSG_ERR, "STG02 - Mixing ('tape' or 'non-CASTOR') files with other types is not allowed\n");
			c = EINVAL;
			goto reply;
		}
		/* It is not allowed to mix 't' with 'h' type */
		if (ntapefiles > 0 && ncastorfiles > 0) {
			sendrep (&rpfd, MSG_ERR, "STG02 - Mixing 'tape' files with 'CASTOR' files is not allowed\n");
			c = EINVAL;
			goto reply;
		}
		/* CASTOR files ? */
		if (ncastorfiles > 0) {
			/* Simulate a STAGEUPDC call on all CASTOR HSM files */
			for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
				int save_status;
				int had_put_failed;

				had_put_failed = (((hsmfilesstcp[ihsmfiles]->status & PUT_FAILED) == PUT_FAILED) ? 1 : 0);
				if ((hsmfilesstcp[ihsmfiles]->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
					if (((hsmfilesstcp[ihsmfiles]->status) & (STAGEWRT|CAN_BE_MIGR)) == (STAGEWRT|CAN_BE_MIGR)) {
						hsmfilesstcp[ihsmfiles]->status |= BEING_MIGR;
					} else {
						hsmfilesstcp[ihsmfiles]->status = STAGEPUT|CAN_BE_MIGR;
					}
					if (! had_put_failed) update_migpool(&(hsmfilesstcp[ihsmfiles]),1,4); /* Condition move DELAY -> CAN_BE_MIGR */
					hsmfilesstcp[ihsmfiles]->a_time = time(NULL);
					strcpy(hsmfilesstcp[ihsmfiles]->u1.h.tppool,tppool);
					if (update_migpool(&(hsmfilesstcp[ihsmfiles]),1,had_put_failed ? 1 : 2) != 0) {
						c = EINVAL;
						goto reply;
					}
				} else {
					/* We make upd_stageout believe that it is a normal stageput following a stageupdc following a stageout */
					/* This will force Cns_setfsize to be called. */
					save_status = hsmfilesstcp[ihsmfiles]->status;
					hsmfilesstcp[ihsmfiles]->status = STAGEOUT;
					rwcountersfs(hsmfilesstcp[ihsmfiles]->poolname, hsmfilesstcp[ihsmfiles]->ipath, STAGEOUT, STAGEOUT);
					if ((c = upd_stageout (STAGEUPDC, hsmfilesstcp[ihsmfiles]->ipath, &subreqid, 0, hsmfilesstcp[ihsmfiles], 0, 0)) != 0) {
						if ((c != CLEARED) && (c != ESTCLEARED)) {
							hsmfilesstcp[ihsmfiles]->status = save_status;
						} else {
							/* Entry has been deleted - user asks to put on tape zero-length file !? */
							c = ESTCLEARED;
						}
						goto reply;
					}
					hsmfilesstcp[ihsmfiles]->status = STAGEPUT;
				}
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,hsmfilesstcp[ihsmfiles]) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
			}
		}

		for (i = 0; i < nbdskf; i++) {
			strcpy (upath, argv[Coptind+i]);
			if (ncastorfiles <= 0) {
				/* We call upd_stageout as normal for all files except CASTOR HSM ones */
				if ((c = upd_stageout (STAGEPUT, upath, &subreqid, 0, NULL, 0, 0)) != 0) {
					if ((c == CLEARED) || (c == ESTCLEARED)) {
						/* Entry has been deleted - user asks to put on tape zero-length file !? */
						/* Formally this not really a stageclr but rather a user error */
						c = EINVAL;
					}
					goto reply;
				}
			} else {
				/* We check if the file yet exist in the name server and if we have to recreate it */
				if (stageput_check_hsm(hsmfilesstcp[i],uid,gid,0,NULL,NULL,0) != 0) {
					if ((hsmfilesstcp[i]->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
						update_migpool(&(hsmfilesstcp[i]),-1,0);
						if ((hsmfilesstcp[i]->status & STAGEWRT) == STAGEWRT) {
							delreq(hsmfilesstcp[i],0);
						} else {
							hsmfilesstcp[i]->status = STAGEOUT|PUT_FAILED|CAN_BE_MIGR;
						}
					} else {
						hsmfilesstcp[i]->status = STAGEOUT|PUT_FAILED;
					}
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,hsmfilesstcp[i]) != 0) {
						stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					continue;
				}
				subreqid = hsmfilesstcp[i]->reqid;
			}
			if (!wqp) {
				wqp = add2wq (clienthost,
							  user, uid, gid,
							  user_waitq, group_waitq, uid_waitq, gid_waitq,
							  clientpid, Upluspath, reqid, STAGEPUT, nbdskf, &wfp,
							  NULL, NULL, NULL, 0);
				if (nowait_flag != 0) {
					/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
					wqp->silent = 1;
					wqp->rpfd = -1;
				}
				wqp->noretry = noretry_flag;
            }
			wfp->subreqid = subreqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		}
	}
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	if (argv != NULL) {free (argv); argv = NULL;}
	if ((c = fork_exec_stager (wqp)) != 0) goto reply;
	if (nowait_flag != 0) {
		sendrep (&rpfd, STAGERC, req_type, magic, c);
		rpfd = -1;
	}
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmfilesstcp != NULL) free(hsmfilesstcp);
	return;
  reply:
#if SACCT
	stageacct (STGCMDC, uid, gid, clienthost,
			   reqid, STAGEPUT, 0, c, NULL, "", (char) 0);
#endif
	sendrep (&rpfd, STAGERC, STAGEPUT, magic, c);
	if (c) {
		if (wqp) {
			for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
				found = 0;
				for (stcp = stcs; stcp < stce; stcp++) {
					if (wfp->subreqid == stcp->reqid) {
						found = 1;
						break;
					}
				}
				if (found == 0) continue;
				if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
					update_migpool(&stcp,-1,0);
					if ((stcp->status & STAGEWRT) == STAGEWRT) {
						delreq(stcp,0);
					} else {
						stcp->status = STAGEOUT|PUT_FAILED|CAN_BE_MIGR;
					}
				} else {
					stcp->status = STAGEOUT|PUT_FAILED;
				}
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
			}
			rmfromwq (wqp);
	    } else if (Mflag) {
			/* It was a stageput that failed immediately (for example, because gid is not in group file) */
			/* We need to check the specific case when the HSM file had the CAN_BE_MIGR flag */
			/* Please note that at this stage we have not yet filled the hsmfilesstcp array */
			Coptind = 1;
			Copterr = 0;
			while ((c = Cgetopt_long (nargs, argv, "Gh:I:M:q:U:V:", longopts, NULL)) != -1) {
				switch (c) {
				case 'G':
				case 'h':
				case 'I':
				case 'q':
				case 'U':
				case 'V':
					break;
				case 'M':
					/* HSM filename is pointed by Coptarg */
					for (stcp = stcs; stcp < stce; stcp++) {
						if (stcp->reqid == 0) break;
						if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') continue;
						if (strcmp(stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile, Coptarg) != 0) continue;
						if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
							update_migpool(&stcp,-1,0);
							if ((stcp->status & STAGEWRT) == STAGEWRT) {
								delreq(stcp,0);
							} else {
								stcp->status = STAGEOUT|PUT_FAILED|CAN_BE_MIGR;
							}
						}
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
							stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
					}
					break;
				case '?':
					errflg++;
					break;
				default:
					errflg++;
					break;
				}
				if (errflg != 0) break;
			}
		}
	}
	if (argv != NULL) {free (argv); argv = NULL;}
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmfilesstcp != NULL) free(hsmfilesstcp);
	return;
}

int
isstaged(cur, p, poolflag, poolname, rdonly, poolname_exclusion, isstaged_nomore, have_Cnsfilestat, Cnsfilestat, isstaged_count)
	struct stgcat_entry *cur;
	struct stgcat_entry **p;
	int poolflag;
	char *poolname;
	int rdonly;
	char *poolname_exclusion;
	int *isstaged_nomore;
	int *have_Cnsfilestat;
	struct Cns_filestat *Cnsfilestat;
	int isstaged_count;
{
	int found = 0;
	int found_regardless_of_rdonly = 0;
	int i;
	struct stgcat_entry *stcp;
	struct stgcat_entry *stcp_castor_name = NULL;
	struct stgcat_entry *stcp_castor_invariants = NULL;
	struct stgcat_entry *stcp_rdonly = NULL;
	struct stgcat_entry *stcp_regardless_of_rdonly = NULL;
	int Cns_getpath_on_input = 0;
	int Cns_stat_on_input = 0;
	char tmpbuf[21];
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */

	int Cns_getpath_on_input_sav = 0;
	struct stgcat_entry cur_sav;

	cur_sav.u1.h.server[0] = '\0';
	cur_sav.u1.h.fileid = 0;

	if (! isstaged_count) {
		/* This is first of all the passes to isstaged() for this record */
		if ((cur->t_or_d == 'h') && (cur->u1.h.server[0] != '\0') && (cur->u1.h.fileid != 0)) {
			char checkpath[CA_MAXPATHLEN+1];
			struct Cns_fileid Cnsfileid;       /* For     CASTOR hsm IDs */
			
			if (cur->reqid <= 0) {
				/* If the input is a CASTOR file and gives also (server,fileid) invariants we check right now */
				/* the avaibility of the name given as well */
				setegid(cur->gid);
				seteuid(cur->uid);
				if (Cns_getpath(cur->u1.h.server, cur->u1.h.fileid, checkpath) == 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					Cns_getpath_on_input = 1;
					if (strcmp(checkpath, cur->u1.h.xfile) != 0) {
						sendrep (&rpfd, MSG_ERR, STG157, cur->u1.h.xfile, checkpath, u64tostr((u_signed64) cur->u1.h.fileid, tmpbuf, 0), cur->u1.h.server);
						strncpy(cur->u1.h.xfile, checkpath, (CA_MAXHOSTNAMELEN+MAXPATH));
						cur->u1.h.xfile[(CA_MAXHOSTNAMELEN+MAXPATH)] = '\0';
					}
				} else {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
				}
			} else if ((have_Cnsfilestat != NULL) && (Cnsfilestat != NULL)) {
				/* If user gave reqid in input this is to speed up things - we do not check path if it is given with invariants */
				strcpy(Cnsfileid.server,cur->u1.h.server);
				Cnsfileid.fileid = cur->u1.h.fileid;
				/* ... But we instead check immediately filesize if it is CASTOR file with invariants */
				*have_Cnsfilestat = 0;
				Cns_stat_on_input = 1;
				setegid(cur->gid);
				seteuid(cur->uid);
				if (Cns_statx(cur->u1.h.xfile, &Cnsfileid, Cnsfilestat) == 0) {
					*have_Cnsfilestat = 1;
				}
				setegid(start_passwd.pw_gid);
				seteuid(start_passwd.pw_uid);
			}
		}
	}
	
	last_tape_file = 0;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		/* API calls may have set explicitely a reqid */
		if ((cur->reqid > 0) && (stcp->reqid != cur->reqid)) continue;
		if (cur->t_or_d != stcp->t_or_d) continue;
		/* If poolname_exclusion is set this mean that we want to check in ANY pool */
		/* except the one whose is poolname_exclusion and only if it have the */
		/* export_hsm flag */
		/* if no pool specified, the file may reside in any pool */
		/* By definition the caller made sure that poolflag is > 0 (e.g. original request want a specific poolname) */
		if (poolname_exclusion != NULL) {
			if (strcmp(poolname,stcp->poolname) == 0) continue;
			/* Does current poolname have export_hsm option ? */
			if (! export_hsm_option(stcp->poolname)) continue;
		} else {
			if (poolflag == 0) {
				if (stcp->poolname[0] == '\0') continue;
			} else {
				/* if a specific pool was requested, the file must be there */
				if (poolflag > 0) {
					if (strcmp (poolname, stcp->poolname)) continue;
				} else {
					/* if -p NOPOOL, the file should not reside in a pool */
					if (poolflag < 0) {
						if (stcp->poolname[0]) continue;
					}
				}
			}
		}
		if (cur->t_or_d == 't') {
			for (i = 0; i < MAXVSN; i++)
				if (strcmp (cur->u1.t.vid[i], stcp->u1.t.vid[i])) break;
			if (i < MAXVSN) continue;
			for (i = 0; i < MAXVSN; i++)
				if (strcmp (cur->u1.t.vsn[i], stcp->u1.t.vsn[i])) break;
			if (i < MAXVSN) continue;
			if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) continue;
			if (cur->u1.t.side != stcp->u1.t.side) continue;
			if (cur->u1.t.fseq[0] != 'u') {
				if ((stcp->status & 0xF000) == LAST_TPFILE)
					last_tape_file = atoi (stcp->u1.t.fseq);
				if (stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-' &&
					stcp->filler[0] == 'c' &&
					atoi(stcp->u1.t.fseq) <= atoi(cur->u1.t.fseq)) {
					/* If it has a trailing '-' && it is a concat_off && fseq <= cur one */
					/* then we found it. */
					found = 1;
					break;
				} else if (strcmp (cur->u1.t.fseq, stcp->u1.t.fseq)) {
					continue;
				}
			} else {
				if (strcmp (cur->u1.t.fid, stcp->u1.t.fid)) continue;
			}
		} else if (cur->t_or_d == 'm') {
			if (strcmp (cur->u1.m.xfile, stcp->u1.m.xfile)) continue;
		} else if (cur->t_or_d == 'h') {
			/* If tape pool is specified, this acts also as a filter */
			if ((cur->u1.h.tppool[0] != '\0') && (strcmp(cur->u1.h.tppool,stcp->u1.h.tppool) != 0)) continue;

			/* Remember if we found this HSM invariant pair */
			if ((cur->u1.h.server[0] != '\0') && (cur->u1.h.fileid != 0) &&
				(strcmp (cur->u1.h.server, stcp->u1.h.server) == 0) &&
				(cur->u1.h.fileid == stcp->u1.h.fileid)) {
				/* If invariants matches this means input got them, of course, and if, then, the initial Cns_getpath was successful */
				/* we check consistency of the filename as well also in case we did Cns_statx instead of Cns_getpath */
				if (((Cns_getpath_on_input != 0) || (Cns_stat_on_input != 0)) && (strcmp(stcp->u1.h.xfile, cur->u1.h.xfile) != 0)) {
					sendrep (&rpfd, MSG_ERR, STG154, stcp->u1.h.xfile, cur->u1.h.xfile, u64tostr((u_signed64) stcp->u1.h.fileid, tmpbuf, 0), stcp->u1.h.server, stcp->reqid);
					strcpy(stcp->u1.h.xfile, cur->u1.h.xfile);
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					/* We must maintain consistency over all the entries that have those invariants */
					/* in one go (although stager is not transactionnal). As soon as we match an entry */
					/* with the correct invariants but not with the correct filename the probability to */
					/* have others affected is not negligible at all */
					{
						struct stgcat_entry *stclp;
						for (stclp = stcs; stclp < stce; stclp++) {
							if (stclp->reqid == 0) break;
							if (stclp->t_or_d != 'h') continue; /* Not CASTOR file */
							if (stclp->reqid == stcp->reqid) continue; /* Yet done */
							if (poolname_exclusion != NULL) {
								/* We know by definition that stcp that triggered this action is within pool stcp->poolname */
								if (strcmp(stclp->poolname,stcp->poolname) != 0) continue;
							} else {
								/* if no pool specified, the file may reside in any pool */
								if (poolflag == 0) {
									if (stclp->poolname[0] == '\0') continue;
								} else {
									/* if a specific pool was requested, the file must be there */
									if (poolflag > 0) {
										if (strcmp (poolname, stclp->poolname)) continue;
									} else {
										/* if -p NOPOOL, the file should not reside in a pool */
										if (poolflag < 0) {
											if (stclp->poolname[0]) continue;
										}
									}
								}
							}
							if ((stclp->u1.h.fileid == stcp->u1.h.fileid) &&
								(strcmp(stclp->u1.h.server,stcp->u1.h.server) == 0) &&
								(strcmp(stclp->u1.h.xfile,stcp->u1.h.xfile) != 0)) {
								sendrep (&rpfd, MSG_ERR, STG154, stclp->u1.h.xfile, stcp->u1.h.xfile, u64tostr((u_signed64) stclp->u1.h.fileid, tmpbuf, 0), stclp->u1.h.server, stclp->reqid);
								strcpy(stclp->u1.h.xfile, stcp->u1.h.xfile);
#ifdef USECDB
								if (stgdb_upd_stgcat(&dbfd,stclp) != 0) {
									stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
								}
#endif
							}
						}
                    }
					savereqs ();
				}
				stcp_castor_invariants = stcp;
            }

			/* Remember if we found this HSM name */
			if (strcmp (cur->u1.h.xfile, stcp->u1.h.xfile) == 0) {
				/* We check if the file has been renamed */
				if ((stcp->u1.h.server[0] != '\0') && (stcp->u1.h.fileid != 0)) {
					/* If the input contains as well a (server,fileid) invariant pair and if it the same as the input */
					/* we do not call again Cns_getpath in order to be as much atomic as possible [one Cns_getpath at a maximum] */
					/* We also bypath name checking when we did a Cns_statx on input */
					if (((Cns_getpath_on_input != 0) || (Cns_stat_on_input != 0)) &&
						(strcmp(cur->u1.h.server,stcp->u1.h.server) == 0) &&
						(cur->u1.h.fileid == stcp->u1.h.fileid)) {
						/* By definition, the name matches */
						stcp_castor_name = stcp;
					} else if ((Cns_getpath_on_input_sav != 0) &&
						(strcmp(cur_sav.u1.h.server,stcp->u1.h.server) == 0) &&
						(cur_sav.u1.h.fileid == stcp->u1.h.fileid)) {
						/* By definition, the name matches */
						stcp_castor_name = stcp;
					} else {
						char checkpath[CA_MAXPATHLEN+1];

						/* Name matches but either there was no Cns_getpath on input or stcp does not match */
						/* input invariants */
						if (Cns_getpath(stcp->u1.h.server, stcp->u1.h.fileid, checkpath) == 0) {
							if (strcmp(checkpath, stcp->u1.h.xfile) != 0) {
								sendrep (&rpfd, MSG_ERR, STG154, stcp->u1.h.xfile, checkpath, u64tostr((u_signed64) stcp->u1.h.fileid, tmpbuf, 0), stcp->u1.h.server, stcp->reqid);
								strncpy(stcp->u1.h.xfile, checkpath, (CA_MAXHOSTNAMELEN+MAXPATH));
								stcp->u1.h.xfile[(CA_MAXHOSTNAMELEN+MAXPATH)] = '\0';
#ifdef USECDB
								if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
									stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
								}
#endif
								savereqs ();
							} else {
								/* The getpath is successful and name matches - this mean that these */
								/* are the invariants that correspond to */
								/* the filename given in input */
								/* So we simulate a successful getpath on input filling cur_sav.u1.h.fileid */
								/* and cur_sav.u1.h.server */
								if ((stcp->u1.h.server[0] != '\0') && (stcp->u1.h.fileid != 0)) {
									strcpy(cur_sav.u1.h.server,stcp->u1.h.server);
									cur_sav.u1.h.fileid = stcp->u1.h.fileid;
									Cns_getpath_on_input_sav = 1;
								}
								stcp_castor_name = stcp;
							}
						} else {
							stcp_castor_name = stcp;
						}
					}
				} else {
					stcp_castor_name = stcp;
				}
			}

			/* If this stcp has matched - do some work with it */
			if ((stcp_castor_name == stcp) || (stcp_castor_invariants == stcp)) {
				if (stcp_castor_name != NULL) {
					if ((stcp_castor_name == stcp_castor_invariants) || ((cur->u1.h.server[0] == '\0') || (cur->u1.h.fileid == 0))) {
						/* If we matched totally name + invariants, with same stcp, we got it */
						/* If we matched totally name and there is no valid invariant pair in input, we also got it */
	
						/* By construction, here, either stcp_castor_name == stcp_castor_invariants, either */
						/* stcp_castor_invariants == NULL and will remain like this forever because no invariants supplied */

						if (! rdonly) {
							/* We are not obliged to scan the whole catalog - we can break right now */
							found = 1;
							break;
						} else {
							/* We are obliged to check if there is a STAGEIN|STAGED|STAGE_RDONLY entry anywhere else */
							if (stcp_castor_name->status == (STAGEIN|STAGED|STAGE_RDONLY)) {
								/* If by chance we get it right now, that's perfect */
								found = 1;
								stcp_rdonly = stcp;
								break;
							} else {
								found_regardless_of_rdonly = 1;
								if (stcp_regardless_of_rdonly == NULL) {
									/* First time we execute this branch : no stcp_regardless_of_rdonly yet */
									stcp_regardless_of_rdonly = stcp;
								} else {
									/* We are in a readonly mode - this mean that preference is always given to an */
									/* entry not matching STAGEWRT or STAGEPUT or not PUT_FAILED */
									if (
										(
											(
												ISSTAGEWRT(stcp_regardless_of_rdonly) ||
												ISSTAGEPUT(stcp_regardless_of_rdonly)
												) &&
											ISCASTORMIG(stcp)
											) ||
										(
											(
												((stcp->status & STAGED) == STAGED) && 
												((stcp_regardless_of_rdonly->status & STAGED) != STAGED)
												)
											)
										) {
										stcp_regardless_of_rdonly = stcp;
									}
								}
							}
							/* Otherwise we will continue to scan the catalog anyway */
						}
					} else if (stcp_castor_invariants != NULL) {
						/* If we matched totally name + invariants, with not same stcp, we neverthless get out */
						/* unless we match other entries with same invariants, we are in read-only mode and current */
						/* found invariant does NOT have this read-only mode. In any case we will exit the loop */
						/* at the end of this else branch */
						/* The stcp_castor_name pointer will be deleted because of wrong invariants */
						if (rdonly && (stcp_castor_invariants->status != (STAGEIN|STAGED|STAGE_RDONLY))) {
							struct stgcat_entry *stclp;
							for (stclp = stcp_castor_invariants + 1; stclp < stce; stclp++) {
								if (stclp->reqid == 0) break;
								if (stclp->t_or_d != 'h') continue; /* Not CASTOR file */
								if (poolname_exclusion != NULL) {
									/* We know by definition that stcp that triggered this action is within pool stcp->poolname */
									if (strcmp(stclp->poolname,stcp->poolname) != 0) continue;
								} else {
									/* if no pool specified, the file may reside in any pool */
									if (poolflag == 0) {
										if (stclp->poolname[0] == '\0') continue;
									} else {
										/* if a specific pool was requested, the file must be there */
										if (poolflag > 0) {
											if (strcmp (poolname, stclp->poolname)) continue;
										} else {
											/* if -p NOPOOL, the file should not reside in a pool */
											if (poolflag < 0) {
												if (stclp->poolname[0]) continue;
											}
										}
									}
								}
								if ((stclp->u1.h.fileid == stcp_castor_invariants->u1.h.fileid) &&
									(strcmp(stclp->u1.h.server,stcp_castor_invariants->u1.h.server) == 0)) {
									/* Who knows */
									if (strcmp(stclp->u1.h.xfile,stcp_castor_invariants->u1.h.xfile) != 0) {
										sendrep (&rpfd, MSG_ERR, STG154, stclp->u1.h.xfile, stcp_castor_invariants->u1.h.xfile, u64tostr((u_signed64) stclp->u1.h.fileid, tmpbuf, 0), stclp->u1.h.server, stclp->reqid);
										strcpy(stclp->u1.h.xfile, stcp_castor_invariants->u1.h.xfile);
#ifdef USECDB
										if (stgdb_upd_stgcat(&dbfd,stclp) != 0) {
											stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
										}
#endif
										savereqs ();
									}
									if (stclp->status == (STAGEIN|STAGED|STAGE_RDONLY)) {
										stcp_castor_invariants = stclp;
										break;
									}
								}
							}
						}
						break;
					}
				}
			}
			/* We did not match name nor invariant */
			continue;
		} else {
			if (strcmp (cur->u1.d.xfile, stcp->u1.d.xfile)) continue;
		}
		found = 1;
		break;
	}
	/* We handle here the special case where global 'found' variable is not set, isstaged() called with */
	/* rdonly flag and indeed, entry was found but not with a matching STAGEIN|STAGED|STAGE_RDONLY status */
	if (found_regardless_of_rdonly && ! found) {
		stcp = stcp_regardless_of_rdonly;
		found = 1;
	}
	if (! found) {
		if ((cur->t_or_d == 'h') && ((stcp_castor_name != NULL) || (stcp_castor_invariants != NULL))) {
			if (stcp_castor_name != NULL) {
				int stcp_castor_invariants_reqid = ((stcp_castor_invariants != NULL) ?
													stcp_castor_invariants->reqid : 0);
				/* Matched name only - this stcp is always to be removed if possible */
				if (ISCASTORBEINGMIG(stcp_castor_name) || ISCASTORWAITINGMIG(stcp_castor_name)) {
					/* We cannot delete this file - it is busy */
					if (ISCASTORBEINGMIG(stcp_castor_name) && (stcp_castor_invariants == NULL) && ! ISCASTORWAITINGMIG(stcp_castor_name)) {
						sendrep(&rpfd, MSG_ERR, "STG02 - Cannot delete obsolete (invariants differs) but busy file %s\n", stcp_castor_name->u1.h.xfile);
						return(ISSTAGEDBUSY);
					} else {
						stcp = stcp_castor_name;
						goto isstaged_return;
					}
				}
				if (delfile (stcp_castor_name, ((stcp_castor_name->status & 0xF0) == STAGED ||
												(stcp_castor_name->status & (STAGEOUT | PUT_FAILED)) == (STAGEOUT | PUT_FAILED)) ?
							 0 : 1,
							 1, 1, "in catalog with wrong invariants", cur->uid, cur->gid, 0, 1, 0) < 0) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp_castor_name->ipath, RFIO_UNLINK_FUNC(stcp_castor_name->ipath), rfio_serror());
					return(ISSTAGEDSYERR);
				}
				if (stcp_castor_invariants_reqid != 0) {
					int found_stcp_invariants = 0;
					/* We called delfile that changed stcp locations in memory (it is doing a memcpy()) */
					/* The only way to protect us against this memcpy() is to search again in memory */
					/* This operation is quite quick */
					for (stcp = stcs; stcp < stce; stcp++) {
						if (stcp->reqid == 0) break;
						if (stcp->reqid == stcp_castor_invariants_reqid) {
							found_stcp_invariants = 1;
							stcp_castor_invariants = stcp;
							break;
						}
					}
					if (found_stcp_invariants == 0) {
						sendrep(&rpfd, MSG_ERR, STG105, "isstaged", "Cannot recover stcp_castor_invariants");
						return(ISSTAGEDSYERR);
					}
				}
			}
			if (stcp_castor_invariants != NULL) {
				/* Matched invariants which is to be kept and renamed */
				if (strcmp(cur->u1.h.xfile, stcp_castor_invariants->u1.h.xfile) != 0) {
					sendrep(&rpfd, MSG_ERR, STG101, cur->u1.h.xfile, stcp_castor_invariants->u1.h.xfile, u64tostr((u_signed64) stcp_castor_invariants->u1.h.fileid, tmpbuf, 0), stcp_castor_invariants->u1.h.server);
					strcpy(stcp_castor_invariants->u1.h.xfile,cur->u1.h.xfile);
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp_castor_invariants) != 0) {
						stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					savereqs ();
				}
				stcp = stcp_castor_invariants;
				goto isstaged_return;
			}
			return (NOTSTAGED);
		}
		return (NOTSTAGED);
	} else {
	  isstaged_return:
		/* If user asked to a STAGEIN read-only and it if found, isstaged() will always return STAGED as for a normal */
		/* STAGEIN, and this will increment the nbaccesses of the read-only entry in the catalog */
		*p = ((stcp_rdonly != NULL) ? stcp_rdonly : stcp);
		if (*isstaged_nomore < 0) {
			/* Caller asks for a hint to know if a second entire loop is needed */
			/* Unless we are already at the end of the catalog, we go through to check if there is at least */
			/* one another entry matching same invariants than (*p) */
			/* Note that this apply ONLY for CASTOR files */

			/* Default is : no other loop */
			*isstaged_nomore = 1;
			if (*p < (stce - 1)) {
				/* Look forward */
				struct stgcat_entry *stclp;
				for (stclp = (*p) + 1; stclp < stce; stclp++) {
					if (stclp->reqid == 0) break;
					if (stclp->reqid == (*p)->reqid) continue; /* Yet done */
					if (stclp->t_or_d != 'h') continue; /* Not CASTOR file */
					if (poolname_exclusion != NULL) {
						/* We know by definition that (*p) that triggered this action is within pool (*p)->poolname */
						if (strcmp(stclp->poolname,(*p)->poolname) != 0) continue;
					} else {
						/* if no pool specified, the file may reside in any pool */
						if (poolflag == 0) {
							if (stclp->poolname[0] == '\0') continue;
						} else {
							/* if a specific pool was requested, the file must be there */
							if (poolflag > 0) {
								if (strcmp (poolname, stclp->poolname)) continue;
							} else {
								/* if -p NOPOOL, the file should not reside in a pool */
								if (poolflag < 0) {
									if (stclp->poolname[0]) continue;
								}
							}
						}
					}
					if ((stclp->u1.h.fileid == (*p)->u1.h.fileid) &&
						(strcmp(stclp->u1.h.server,(*p)->u1.h.server) == 0) &&
						(strcmp(stclp->u1.h.xfile,(*p)->u1.h.xfile) == 0)) {
						/* Found another entry */
						*isstaged_nomore = 0;
						break;
					}
				}
			}
		}
		if ((*p)->t_or_d == 'h') {
			/* If we were able to determine invariants */
			/* Save it for eventual future session (like STAGEOUT asking sometimes twice for isstaged()) */
			strcpy(cur->u1.h.server,(*p)->u1.h.server);
			cur->u1.h.fileid = (*p)->u1.h.fileid;
		}
		if (((*p)->status & 0xF0) == STAGED) {
			return (STAGED);
		} else {
			return ((*p)->status);
		}
	}
}

int
maxfseq_per_vid(cur, poolflag, poolname, concat_off_flag)
	struct stgcat_entry *cur;
	int poolflag;
	char *poolname;
	char *concat_off_flag;
{
	int found = 0;
	int i;
	struct stgcat_entry *stcp;
	int result = 0;

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if (stcp->t_or_d != 't') continue;
		/* if no pool specified, the file may reside in any pool */
		if (poolflag == 0) {
			if (stcp->poolname[0] == '\0') continue;
		} else
			/* if a specific pool was requested, the file must be there */
			if (poolflag > 0) {
				if (strcmp (poolname, stcp->poolname)) continue;
			} else
				/* if -p NOPOOL, the file should not reside in a pool */
				if (poolflag < 0) {
					if (stcp->poolname[0]) continue;
				}
		for (i = 0; i < MAXVSN; i++)
			if (strcmp (cur->u1.t.vid[i], stcp->u1.t.vid[i])) break;
		if (i < MAXVSN) continue;
		for (i = 0; i < MAXVSN; i++)
			if (strcmp (cur->u1.t.vsn[i], stcp->u1.t.vsn[i])) break;
		if (i < MAXVSN) continue;
		if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) continue;
		if ((found = atoi(stcp->u1.t.fseq)) < result) continue;
		*concat_off_flag = stcp->filler[0];
		result = found;
	}
	return(result);
}

int unpackfseq(fseq, req_type, trailing, fseq_list, concat_off, concat_off_fseq)
	char *fseq;
	int req_type;
	char *trailing;
	fseq_elem **fseq_list;
	int concat_off;
	int *concat_off_fseq;
{
	char *dp;
	int i;
	int n1, n2;
	int nbtpf;
	char *p, *q;
	int lasttpf = 0;
	int have_qn = 0;
	int have_qu = 0;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

	*trailing = *(fseq + strlen (fseq) - 1);
	if (*trailing == '-') {
		if (req_type != STAGEIN) {
			sendrep (&rpfd, MSG_ERR, STG18);
			return (0);
		}
		*(fseq + strlen (fseq) - 1) = '\0';
	}
	switch (*fseq) {
	case 'n':
		if (req_type == STAGEIN) {
			sendrep (&rpfd, MSG_ERR, STG17, "-qn", req_type == STAGEIN ? "stagein" : "stage_in");
			return (0);
		}
		++have_qn;
	case 'u':
		if (strlen (fseq) == 1) {
			nbtpf = 1;
		} else {
			stage_strtoi(&nbtpf, fseq + 1, &dp, 10);
			if (*dp != '\0') {
				sendrep (&rpfd, MSG_ERR, STG06, "-q");
				return (0);
			}
		}
		*fseq_list = (fseq_elem *) calloc (nbtpf, sizeof(fseq_elem));
		for (i = 0; i < nbtpf; i++) {
			sprintf ((char *)(*fseq_list + i), "%c", *fseq);
			lasttpf = atoi((char *)(*fseq_list + i));
		}
		++have_qu;
		break;
	default:
		nbtpf = 0;
		p = strtok (fseq, ",");
		while (p != NULL) {
			if ((q = strchr (p, '-')) != NULL) {
				*q = '\0';
				stage_strtoi(&n2, q + 1, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				stage_strtoi(&n1, p, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				*q = '-';
			} else {
				stage_strtoi(&n1, p, &dp, 10);
				if (*dp != '\0') {
					sendrep (&rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				n2 = n1;
			}
			if (n1 <= 0 || n2 < n1) {
				sendrep (&rpfd, MSG_ERR, STG06, "-q");
				return (0);
			}
			nbtpf += n2 - n1 + 1;
			if ((p = strtok (NULL, ",")) != NULL) *(p - 1) = ',';
		}
		*fseq_list = (fseq_elem *) calloc (nbtpf, sizeof(fseq_elem));
		nbtpf = 0;
		p = strtok (fseq, ",");
		while (p != NULL) {
			if ((q = strchr (p, '-')) != NULL) {
				*q = '\0';
				stage_strtoi(&n2, q + 1, &dp, 10);
				stage_strtoi(&n1, p, &dp, 10);
				*q = '-';
			} else {
				stage_strtoi(&n1, p, &dp, 10);
				n2 = n1;
			}
			for (i = n1; i <= n2; i++, nbtpf++) {
				sprintf ((char *)(*fseq_list + nbtpf), "%d", i);
				lasttpf = i;
			}
			p = strtok (NULL, ",");
		}
	}
	/* If the last entry is a '-' and concat_off flag is set and there is at least one fseq identified */
	if (nbtpf > 0 && *trailing == '-' && concat_off) {
		if (concat_off_fseq == NULL) {
			sendrep (&rpfd, MSG_ERR, "unpackfseq : Internal error : concat_off is ON and concat_off_fseq == NULL\n");
			return (0);
		}
		/* The fseq just before the '-' is the last element of the fseq_elem list */
		if ((*concat_off_fseq = lasttpf) <= 0) {
			sendrep (&rpfd, MSG_ERR, "unpackfseq : Internal error : concat_off is ON and nbtpf > 0 and *trailing == '-' and atoi(fseq_list[nbtpf-1]) <= 0\n");
			free(*fseq_list);
			*fseq_list = NULL;
			return (0);
		}
	}
	if (have_qu == 0 && have_qn == 0) {
		/* We verify the fseq list (if more than one element) */
		i = 1;
	  retry_verif:
		for (; i < nbtpf; i++) {
			if (strcmp((char *)(*fseq_list + i),(char *)(*fseq_list + i - 1)) == 0) {
				int j, new_nbtpf;
				fseq_elem *new_fseq_list;

				if ((new_fseq_list = (fseq_elem *) calloc (nbtpf - 1, sizeof(fseq_elem))) == NULL) {
					sendrep (&rpfd, MSG_ERR, "STG02 - calloc error (%s)\n", strerror(errno));
					free(*fseq_list);
					return(-1);
				}
				sendrep (&rpfd, MSG_ERR, STG60, (char *)(*fseq_list + i));
				new_nbtpf = 0;
				for (j = 0; j < nbtpf; j++) {
					if (j == i) continue;
					strcpy((char *)(new_fseq_list + new_nbtpf++),(char *)(*fseq_list + j));
				}
				free(*fseq_list);
				*fseq_list = new_fseq_list;
				nbtpf = new_nbtpf;
				goto retry_verif;
			}
		}
	}
	return (nbtpf);
}

int check_hsm_type(arg,nhpssfiles,ncastorfiles,nexplevel,nuserlevel,t_or_d)
	char *arg;
	int *nhpssfiles;
	int *ncastorfiles;
	int *nexplevel;
	int *nuserlevel;
	char *t_or_d;
{
	int found_hpss_type = 0;
	int found_castor_type = 0;

	/* We check that user do not mix different hsm types (hpss and castor) */
	if (ISHPSS(arg)) {
		if (ISCASTORHOST(arg)) {
			sendrep (&rpfd, MSG_ERR, STG102, "castor", "non-castor", arg);
			return(EINVAL);
		}
		++(*nhpssfiles);
		found_hpss_type++;
	}
	if (ISCASTOR(arg)) {
		if (ISHPSSHOST(arg)) {
			sendrep (&rpfd, MSG_ERR, STG102, "non-castor", "castor", arg);
			return(EINVAL);
		}
		++(*ncastorfiles);
		/* And we take the opportunity to decide which level of migration (user/exp) */
		if (isuserlevel(arg)) {
			(*nuserlevel)++;
		} else {
			(*nexplevel)++;
        }
		found_castor_type++;
	}
	/* No recognized type ? */
	if ((found_hpss_type == 0) && (found_castor_type == 0)) {
		if (ISHPSSHOST(arg)) {
			++(*nhpssfiles);
			found_hpss_type++;
		}
		if (ISCASTORHOST(arg)) {
			++(*nhpssfiles);
			found_hpss_type++;
		}
	}
	/* Really no recognized type ? */
	if ((found_hpss_type == 0) && (found_castor_type == 0)) {
		sendrep (&rpfd, MSG_ERR, "Cannot determine %s type (HPSS nor CASTOR)\n", arg);
		return(EINVAL);
	}
	/* Mixed HSM types ? */
	if (((*nhpssfiles) > 0) && ((*ncastorfiles) > 0)) {
		sendrep (&rpfd, MSG_ERR, "non-CASTOR and CASTOR files on the same command-line is not allowed\n");
		return(EINVAL);
	} else if (((*ncastorfiles) > 0)) {
		/* It is a CASTOR request, so stgreq.t_or_d is set to 'h' */
		if (t_or_d != NULL) *t_or_d = 'h';
		/* Check length */
		if (strlen(arg) > STAGE_MAX_HSMLENGTH) {
			serrno = SENAMETOOLONG;
			sendrep (&rpfd, MSG_ERR, "%s\n", sstrerror(serrno));
			return(EINVAL);
		}
		/* Remove characters that are causing trouble */
		if (checkpath(arg,arg) < 0) {
			sendrep (&rpfd, MSG_ERR, STG02, arg, "checkpath", sstrerror(serrno));
			return(EINVAL);
		}
	} else {
		/* It is a HPSS request, so stgreq.t_or_d is set to 'm' */
		if (t_or_d != NULL) *t_or_d = 'm';
		/* Check length */
		if (strlen(arg) > STAGE_MAX_HSMLENGTH) {
			serrno = SENAMETOOLONG;
			sendrep (&rpfd, MSG_ERR, "%s\n", sstrerror(serrno));
			return(EINVAL);
		}
		/* Remove characters that are causing trouble */
		if (checkpath(arg,arg) < 0) {
			sendrep (&rpfd, MSG_ERR, STG02, arg, "checkpath", sstrerror(serrno));
			return(EINVAL);
		}
	}
	/* Ok */
	return(0);
}

int check_hsm_type_light(arg,t_or_d)
	char *arg;
	char *t_or_d;
{
	int nhpssfiles = 0;
	int ncastorfiles = 0;

	/* We check that user do not mix different hsm types (hpss and castor) */
	if (ISHPSS(arg)) {
		if (ISCASTORHOST(arg)) {
			sendrep (&rpfd, MSG_ERR, STG102, "castor", "non-castor", arg);
			return(EINVAL);
		}
		++nhpssfiles;
	}
	if (ISCASTOR(arg)) {
		if (ISHPSSHOST(arg)) {
			sendrep (&rpfd, MSG_ERR, STG102, "non-castor", "castor", arg);
			return(EINVAL);
		}
		++ncastorfiles;
	}
	/* No recognized type ? */
	if ((nhpssfiles == 0) && (ncastorfiles == 0)) {
		if (ISHPSSHOST(arg)) {
			++nhpssfiles;
		}
		if (ISCASTORHOST(arg)) {
			++ncastorfiles;
		}
	}
	/* Really no recognized type ? */
	if ((nhpssfiles == 0) && (ncastorfiles == 0)) {
		sendrep (&rpfd, MSG_ERR, "Cannot determine %s type (HPSS nor CASTOR)\n", arg);
		return(EINVAL);
	}
	/* Mixed HSM types ? */
	if ((nhpssfiles > 0) && (ncastorfiles > 0)) {
		sendrep (&rpfd, MSG_ERR, "Non-CASTOR and CASTOR files on the same command-line is not allowed\n");
		return(EINVAL);
	} else if ((ncastorfiles > 0)) {
		/* It is a CASTOR request, so stgreq.t_or_d is set to 'h' */
		*t_or_d = 'h';
		if (strlen(arg) > STAGE_MAX_HSMLENGTH) {
			serrno = SENAMETOOLONG;
			sendrep (&rpfd, MSG_ERR, "%s\n", sstrerror(serrno));
			return(EINVAL);
		}
	} else {
		/* It is a HPSS request, so stgreq.t_or_d is set to 'm' */
		*t_or_d = 'm';
		if (strlen(arg) > STAGE_MAX_HSMLENGTH) {
			serrno = SENAMETOOLONG;
			sendrep (&rpfd, MSG_ERR, "%s\n", sstrerror(serrno));
			return(EINVAL);
		}
	}
	/* Ok */
	return(0);
}

int create_hsm_entry(rpfd,stcp,api_out,openmode,immediate_delete)
	int *rpfd;
	struct stgcat_entry *stcp;
	int api_out;
	mode_t openmode;
	int immediate_delete;
{
	mode_t okmode;
	struct Cns_fileid Cnsfileid;
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */
	char *func = "create_hsm_entry";

	memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
	setegid(stcp->gid);
	seteuid(stcp->uid);
	if ((api_out == 0) || (openmode == 0)) {
		okmode = ( 0777 & ~ stcp->mask);
	} else {
		okmode = (07777 & (openmode & ~ stcp->mask));
	}
	STAGE_TIME_START("Cns_creatx");
	if (Cns_creatx(stcp->u1.h.xfile, okmode, &Cnsfileid) != 0) {
		int save_serrno = serrno;
		setegid(start_passwd.pw_gid);
		seteuid(start_passwd.pw_uid);
		sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_creatx", sstrerror(serrno));
		if (immediate_delete != 0) {
			if (delfile (stcp, 1, 1, 1, stcp->user, stcp->uid, stcp->gid, 0, 1, 0) < 0) {
				sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
			}
		}
		serrno = save_serrno;
		if (api_out != 0) {
			return(serrno);
		} else {
			if ((serrno == SENOSHOST) || (serrno == SENOSSERV) || (serrno == SECOMERR) || (serrno == ENSNACT)) {
				return(SESYSERR);
			}
			return(EINVAL);
		}
	}
	STAGE_TIME_END;
	strcpy(stcp->u1.h.server,Cnsfileid.server);
	stcp->u1.h.fileid = Cnsfileid.fileid;
	stcp->status &= ~WAITING_NS;
	setegid(start_passwd.pw_gid);
	seteuid(start_passwd.pw_uid);
	/* The fileclass will be checked at next user call - in a stageupdc for example */
#ifdef USECDB
	STAGE_TIME_START("stgdb_upd_stgcat");
	if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
		stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif
	STAGE_TIME_END;
	savereqs();
	return(0);
}

/* Return 0 if ok                 */
/*       -1 if nothing to migrate */
/*      > 0 if error              */

int stageput_check_hsm(stcp,uid,gid,was_put_failed,yetdone_Cns_statx_flag,yetdone_Cns_statx,nohsmcreat_flag)
	struct stgcat_entry *stcp;
	uid_t uid;
	gid_t gid;
	int was_put_failed;
	int *yetdone_Cns_statx_flag;
	struct Cns_filestat *yetdone_Cns_statx;
	int nohsmcreat_flag;
{
	struct stat64 filemig_stat;          /* For non-CASTOR HSM stat() */
	struct Cns_filestat Cnsfilestat;   /* For     CASTOR hsm stat() */
	struct Cns_fileid Cnsfileid;       /* For     CASTOR hsm IDs */
	u_signed64 correct_size;
	u_signed64 hsmsize = 0;
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */
	extern struct passwd stage_passwd;             /* Generic uid/gid stage:st */
	int forced_Cns_creatx = 0;
	int forced_Cns_setfsize = 0;
	int have_okmode_before = 0;
	int this_okmode_before = 0;
	char tmpbuf[21];
	int thisrc = 0;
	int this_fileclass = 0;
	
	if ((stcp->u1.h.server[0] == '\0') || (stcp->u1.h.fileid == 0)) {
		memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
	} else {
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
	}
/*
	if ((stcp->u1.h.server[0] == '\0') || (stcp->u1.h.fileid == 0)) {
		memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
	} else {
		char checkpath[CA_MAXPATHLEN+1];

		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
		if (Cns_getpath(stcp->u1.h.server, stcp->u1.h.fileid, checkpath) == 0) {
			if (strcmp(checkpath, stcp->u1.h.xfile) != 0) {
				sendrep (&rpfd, MSG_ERR, STG157, stcp->u1.h.xfile, checkpath, u64tostr((u_signed64) stcp->u1.h.fileid, tmpbuf, 0), stcp->u1.h.server);
				strncpy(stcp->u1.h.xfile, checkpath, (CA_MAXHOSTNAMELEN+MAXPATH));
				stcp->u1.h.xfile[(CA_MAXHOSTNAMELEN+MAXPATH)] = '\0';
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs ();
			}
		}
	}
*/
	
	setegid(gid);
	seteuid(uid);
	thisrc = Cns_statx(stcp->u1.h.xfile, &Cnsfileid, &Cnsfilestat);
	setegid(start_passwd.pw_gid);
	seteuid(start_passwd.pw_uid);
	if (thisrc == 0) {
		if (yetdone_Cns_statx_flag != NULL) *yetdone_Cns_statx_flag = 1;
		if (yetdone_Cns_statx      != NULL) memcpy(yetdone_Cns_statx, &Cnsfilestat, sizeof(struct Cns_filestat));
		this_fileclass = Cnsfilestat.fileclass;
		have_okmode_before = 1;
		this_okmode_before = (07777 & Cnsfilestat.filemode);
		hsmsize = Cnsfilestat.filesize;
		/* We compare the size of the disk file with the size in Name Server */
		PRE_RFIO;
		if (RFIO_STAT64(stcp->ipath, &filemig_stat) < 0) {
			sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
			return(EINVAL);
		}
		correct_size = (u_signed64) filemig_stat.st_size;
		/* We always guarantee it is a new version if stcp current status is STAGEOUT */
		if ((stcp->status == STAGEOUT) && (! was_put_failed)) {
			if (hsmsize > 0) {
				if (Cnsfilestat.status == 'm') {
					/* If this file already has some migrated stuff we say to the user that we are going to recreate it */
					/* unless nohsmcreat_flag */
					if (nohsmcreat_flag) {
						sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "", strerror(EEXIST));
						return(EEXIST);
					}
					sendrep (&rpfd, MSG_ERR,
							 "STG98 - %s, size=%s renewed\n",
							 stcp->ipath, u64tostr(hsmsize, tmpbuf, 0));
					forced_Cns_creatx = 1;
				} else {
					/* Otherwise we just log it for our purpose */
					stglogit (func, 
							  "STG98 - %s, size=%s renewed\n",
							  stcp->ipath, u64tostr(hsmsize, tmpbuf, 0));
				}
				forced_Cns_setfsize = 1;
			} else {
				/* We will just call for a change of size because it is already zero size in the name server */
				forced_Cns_setfsize = 1;
            }
		} else {
			if ((correct_size > 0) && (correct_size == hsmsize)) {
				/* Same size and > 0 we assume user asks for a new copy */
				return(0);
			} else if (correct_size <= 0) {
				sendrep (&rpfd, MSG_ERR,
						 "STG98 - %s size is of zero size - not migrated\n",
						 stcp->ipath);
				return(-1);
			} else {
				/* Not the same size or diskfile size is zero */
				if (hsmsize > 0) {
					if (Cnsfilestat.status == 'm') {
						if (nohsmcreat_flag) {
							sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "", strerror(EEXIST));
							return(EEXIST);
						}
					}
					sendrep (&rpfd, MSG_ERR,
							 "STG98 - %s renewed (size differs vs. %s)\n",
							 stcp->u1.h.xfile,
							 stcp->ipath);
					if (Cnsfilestat.status == 'm') {
						forced_Cns_creatx = 1;
					}
					forced_Cns_setfsize = 1;
				} else {
					/* We will just call for a change of size because it is already zero size in the name server */
					forced_Cns_setfsize = 1;
				}
			}
		}
	} else {
		int save_serrno = serrno;
		/* It is not point to try to migrated something of zero size */
		PRE_RFIO;
		if (RFIO_STAT64(stcp->ipath, &filemig_stat) < 0) {
			sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
			return(EINVAL);
		}
		if (filemig_stat.st_size <= 0) {
			sendrep (&rpfd, MSG_ERR,
					 "STG98 - %s size is of zero size - not migrated\n",
					 stcp->ipath);
			return(-1);
		}
		/* We will have to create it unless we are said to not do so */
		if (nohsmcreat_flag) {
			sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_statx", sstrerror(save_serrno));
			return(save_serrno);
		}
		forced_Cns_creatx = forced_Cns_setfsize = 1;
		correct_size = (u_signed64) filemig_stat.st_size;
	}
	if ((forced_Cns_creatx != 0) || (forced_Cns_setfsize != 0)) {
		mode_t okmode;
		mode_t openmode = 0;
		/* The following will make sure that we use user's umask and uid/gid for creation */
		int have_save_stcp_for_Cns_creatx = 0;
		struct stgcat_entry save_stcp_for_Cns_creatx;
		struct stgcat_entry stgreq;
		int api_out = 0; /* There is not stageput API with magic number > STGMAGIC for the moment */

		stgreq.uid = uid;
		stgreq.gid = gid;
		stgreq.mask = stcp->mask;
		memset(&save_stcp_for_Cns_creatx, 0, sizeof(struct stgcat_entry));
		SET_CORRECT_OKMODE;
		if (have_okmode_before) okmode = this_okmode_before;

		if (forced_Cns_creatx != 0) {
			setegid(gid);
			seteuid(uid);
			thisrc = Cns_creatx(stcp->u1.h.xfile, okmode, &Cnsfileid);
			setegid(start_passwd.pw_gid);
			seteuid(start_passwd.pw_uid);
			if (thisrc != 0) {
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_creatx", sstrerror(serrno));
				return(serrno);
			}
			strcpy(stcp->u1.h.server,Cnsfileid.server);
			stcp->u1.h.fileid = Cnsfileid.fileid;
		}
		if (forced_Cns_setfsize != 0) {
			/* Always set if we are in this branch, btw */
			stcp->actual_size = correct_size;
			/* We set the size in the name server */
			setegid(gid);
			seteuid(uid);
			thisrc = Cns_setfsize(NULL,&Cnsfileid,correct_size);
			setegid(start_passwd.pw_gid);
			seteuid(start_passwd.pw_uid);
			if (thisrc != 0) {
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_setfsize", sstrerror(serrno));
				return(serrno);
			}
		}
		stcp->u1.h.fileclass = this_fileclass; /* Could be zero */
#ifdef USECDB
		if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
			stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		savereqs();
		return(0);
	} else if (this_fileclass > 0) {
		stcp->u1.h.fileclass = this_fileclass; /* Could be zero */
#ifdef USECDB
		if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
			stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		savereqs();
	}
	return(0);
}

/* Simple a-la-access() routine (no check for F_OK) */
int stage_access(uid,gid,amode,st)
	uid_t uid;
	gid_t gid;
	int amode;
	struct stat64 *st;
{
	int mode = (amode & (R_OK|W_OK|X_OK)) << 6;
	if (st->st_uid != uid) {
		mode >>= 3;
		if (st->st_gid != gid) {
			mode >>= 3;
		}
	}
	if ((st->st_mode & mode) != mode) {
		serrno = EACCES;
		return (-1);
	} else {
		return (0);
	}
}

/* Returns -1 if error, 0 if string not changed, 1 if string changed */
int checkpath(output,input)
	char *output;
	char *input;
{
	char *dup, *p;
	int rc;
	size_t dupsize;

	if (input == NULL) {
		serrno = EINVAL;
		rc = -1;
		goto checkpath_return;
	}
	
	if ((p = dup = strdup(input)) == NULL) {
		serrno = SEINTERNAL;
		rc = -1;
		goto checkpath_return;
	}
	
	/*
	 * Removes multiple occurences of '/' character
	 */
	
	rc = 0;
	while ((p = strchr(p,'/')) != NULL) {
		/* 'p' points to a '/' */
		size_t psize;
		if ((psize = strspn(p++,"/")) > 1) {
			/* And there is 'psize' of them */
			--psize;
			/* We copy the rest of the string keeping the first one */
			memmove(p,&(p[psize]),strlen(&(p[psize])) + 1);
			rc = 1;
		} else {
			++p;
		}
	}
	/* Any eventual trailing space ? */
	dupsize = strlen(dup);
	if ((dupsize > 0) && (dup[dupsize-1] == '/')) {
		serrno = EINVAL;
		rc = -1;
		goto checkpath_return;
	}
	if (rc == 1) {
		sendrep (&rpfd, MSG_ERR, "%s changed to %s\n", input, dup);
	}
	if (output != NULL) {
		/* User has to provide enough space for it */
		/* We do memmove() because output and input can be same area */
		memmove(output,dup,++dupsize);
	}
	free(dup);

  checkpath_return:
	return(rc);
}
