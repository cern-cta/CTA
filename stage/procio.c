/*
 * $Id: procio.c,v 1.105 2001/03/12 17:40:28 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procio.c,v $ $Revision: 1.105 $ $Date: 2001/03/12 17:40:28 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <grp.h>
#include <pwd.h>
#include <string.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
#else
#include <netinet/in.h>
#include <sys/time.h>
#include <unistd.h>
#endif
#include <sys/stat.h>
#include <errno.h>

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

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

extern char defpoolname[CA_MAXPOOLNAMELEN + 1];
extern char defpoolname_in[CA_MAXPOOLNAMELEN + 1];
extern char defpoolname_out[CA_MAXPOOLNAMELEN + 1];
extern char func[16];
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgcat_entry *newreq _PROTO(());
char *findpoolname();
int last_tape_file;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
static char one[2] = "1";
int tppool_flag = 0;
int silent_flag = 0;
int nowait_flag = 0;
extern struct fileclass *fileclasses;

void procioreq _PROTO((int, int, char *, char *));
void procputreq _PROTO((int, char *, char *));
extern int isuserlevel _PROTO((char *));
int unpackfseq _PROTO((char *, int, char *, fseq_elem **, int, int *));
extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *));
extern int ask_stageout _PROTO((int, char *, struct stgcat_entry **));
extern struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, int **, char *, char *, int));
extern int nextreqid _PROTO(());
int isstaged _PROTO((struct stgcat_entry *, struct stgcat_entry **, int, char *));
int maxfseq_per_vid _PROTO((struct stgcat_entry *, int, char *, char *));
extern int update_migpool _PROTO((struct stgcat_entry *, int, int));
extern int updfreespace _PROTO((char *, char *, signed64));
extern u_signed64 stage_uniqueid;
extern void getdefsize _PROTO((char *, int *));
int check_hsm_type _PROTO((char *, int *, int *, int *, int *, char *));
int check_hsm_type_light _PROTO((char *, char *));
#ifdef hpux
int create_hsm_entry _PROTO(());
#else
int create_hsm_entry _PROTO((int, struct stgcat_entry *, int, mode_t, int));
#endif
extern int stglogit _PROTO(());
extern int stglogflags _PROTO(());
extern int stglogtppool _PROTO(());
extern int req2argv _PROTO((char *, char ***));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int isvalidpool _PROTO((char *));
extern int packfseq _PROTO((fseq_elem *, int, int, int, char, char *, int));
extern int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int, int));
extern void sendinfo2cptape _PROTO((int, struct stgcat_entry *));
extern void create_link _PROTO((struct stgcat_entry *, char *));
extern int build_ipath _PROTO((char *, struct stgcat_entry *, char *));
extern void delreq _PROTO((struct stgcat_entry *, int));
extern int savepath _PROTO(());
extern int savereqs _PROTO(());
extern int cleanpool _PROTO((char *));
extern int fork_exec_stager _PROTO((struct waitq *));
extern void rmfromwq _PROTO((struct waitq *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *));
extern int euid_egid _PROTO((uid_t *, gid_t *, char *, struct migrator *, struct stgcat_entry *, struct stgcat_entry *, char **, int));
extern int verif_euid_egid _PROTO((uid_t, gid_t, char *, char *));
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *));
extern char *next_tppool _PROTO((struct fileclass *));
extern void bestnextpool_out _PROTO((char *, int));
extern void rwcountersfs _PROTO((char *, char *, int, int));

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
	int c, i, j;
	int concat_off = 0;
	int concat_off_fseq = 0;
	int clientpid;
	static char cmdnames[4][9] = {"", "stagein", "stageout", "stagewrt"};
	static char cmdnames_api[4][10] = {"", "stage_in", "stage_out", "stage_wrt"};
	int copytape = 0;
	char *dp;
	int errflg = 0;
	char *fid = NULL;
	struct stat filemig_stat;          /* For non-CASTOR HSM stat() */
	struct Cns_filestat Cnsfilestat;   /* For     CASTOR hsm stat() */
	struct Cns_fileid Cnsfileid;       /* For     CASTOR hsm IDs */
	char *fseq = NULL;
	fseq_elem *fseq_list = NULL;
	struct group *gr;
	char **hsmfiles = NULL;
	int ihsmfiles;
	int jhsmfiles;
	char *name;
	int nargs;
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
	int poolflag;
	char *rbp;
	int savereqid;
	char *size = NULL;
	struct stat st;
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
	u_signed64  flags;
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
	int have_tppool = 0;
	char *tppool = NULL;
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

	static struct Coptions longopts[] =
	{
		{"density",            REQUIRED_ARGUMENT,  NULL,      'd'},
		{"error_action",       REQUIRED_ARGUMENT,  NULL,      'E'},
		{"fid",                REQUIRED_ARGUMENT,  NULL,      'f'},
		{"grpuser",            REQUIRED_ARGUMENT,  NULL,      'G'},
		{"group_device",       REQUIRED_ARGUMENT,  NULL,      'g'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"label_type",         REQUIRED_ARGUMENT,  NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"new_fid",            NO_ARGUMENT,        NULL,      'n'},
		{"old_fid",            NO_ARGUMENT,        NULL,      'o'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"tape_server",        REQUIRED_ARGUMENT,  NULL,      'S'},
		{"trailer_label_off",  NO_ARGUMENT,        NULL,      'T'},
		{"retention_period",   REQUIRED_ARGUMENT,  NULL,      't'},
		{"fortran_unit",       REQUIRED_ARGUMENT,  NULL,      'U'},
		{"user",               REQUIRED_ARGUMENT,  NULL,      'u'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"vsn",                REQUIRED_ARGUMENT,  NULL,      'v'},
		{"copytape",           NO_ARGUMENT,        NULL,      'z'},
		{"silent",             NO_ARGUMENT,  &silent_flag,      1},
		{"nowait",             NO_ARGUMENT,  &nowait_flag,      1},
		{"tppool",             REQUIRED_ARGUMENT, &tppool_flag, 1},
		{NULL,                 0,                  NULL,        0}
	};

	memset ((char *)&stgreq, 0, sizeof(stgreq));
	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	strncpy (stgreq.user, user, CA_MAXUSRNAMELEN);
	stgreq.user[CA_MAXUSRNAMELEN] = '\0';
	strcpy (save_user, stgreq.user);

	tppool_flag = 0;
	silent_flag = 0;
	nowait_flag = 0;

	local_unmarshall_STRING (rbp, name);
	if (req_type > STAGE_00) {
		if (magic == STGMAGIC) {
			/* First version of the API */
			unmarshall_LONG (rbp, stgreq.uid);
			unmarshall_LONG (rbp, stgreq.gid);
			unmarshall_LONG (rbp, stgreq.mask);
			unmarshall_LONG (rbp, clientpid);
		} else if (magic == STGMAGIC2) {
			/* Second version of the API */
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
						 reqid, req_type, 0, 0, NULL, "");
#endif

	if (req_type > STAGE_00) {
		/* This is coming from the API */
		api_out = 1;
		local_unmarshall_STRING(rbp, User);
		if (User[0] != '\0') pool_user = User;
		unmarshall_HYPER(rbp, flags);
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
			sendrep(rpfd, MSG_ERR, "STG02 - Link file structures (%d of them) is not allowed in concatenation off mode\n", nstpp_input);
			c = USERR;
			goto reply;
		}
		if (nstcp_input <= 0) {
			sendrep(rpfd, MSG_ERR, "STG02 - Invalid number of input structure (%d)\n", nstcp_input);
			c = USERR;
			goto reply;
		}
		if ((nstpp_input > 0) && (nstpp_input != nstcp_input)) {
			sendrep(rpfd, MSG_ERR, "STG02 - Number of link file structures (%d) must match number of disk file structures (%d)\n",nstpp_input,nstcp_input);
			c = USERR;
			goto reply;
		}
		if ((stcp_input = (struct stgcat_entry *) calloc(nstcp_input,sizeof(struct stgcat_entry))) == NULL ||
			(nstpp_input > 0  && (stpp_input = (struct stgpath_entry *) calloc(nstpp_input,sizeof(struct stgpath_entry))) == NULL)) {
			sendrep(rpfd, MSG_ERR, "STG02 - memory allocation error (%s)\n", strerror(errno));
			c = SYERR;
			goto reply;
		}
		for (i = 0; i < nstcp_input; i++) {
			char logit[BUFSIZ + 1];

			struct_status = 0;
			unmarshall_STAGE_CAT(STAGE_INPUT_MODE, struct_status, rbp, &(stcp_input[i]));
			if (struct_status != 0) {
				sendrep(rpfd, MSG_ERR, "STG02 - Bad input (catalog input structure No %d/%d)\n", ++i, nstcp_input);
				c = USERR;
				goto reply;
			}
			if ((t_or_d == 'm') || (t_or_d == 'h')) {
				/* Note - per construction u1.m.xfile and u1.h.xfile points to same area... */
				if (check_hsm_type_light(stcp_input[i].u1.m.xfile,&(stcp_input[i].t_or_d)) != 0) {
					sendrep(rpfd, MSG_ERR, "STG02 - Bad input (catalog input structure No %d/%d)\n", ++i, nstcp_input);
					c = USERR;
					goto reply;
				}
			}
			logit[0] = '\0';
			if (stage_stcp2buf(logit,BUFSIZ,&(stcp_input[i])) == 0 || serrno == SEUMSG2LONG) {
				logit[BUFSIZ] = '\0';
				stglogit(func,"stcp[%d/%d] :%s\n",i+1,nstcp_input,logit);
 			}
			if (stcp_input[i].poolname[0] == '\0') {
				/* poolname was not specified explicitely for this structure */
				switch (req_type) {
				case STAGE_IN:
					strcpy(stcp_input[i].poolname,defpoolname_in);
					strcpy(stgreq.poolname,stcp_input[i].poolname);
					break;
				case STAGE_OUT:
					bestnextpool_out(stcp_input[i].poolname,WRITE_MODE);
					strcpy(stgreq.poolname,stcp_input[i].poolname);
					break;
				default:
					break;
				}
			} else {
				if (! (strcmp (stcp_input[i].poolname, "NOPOOL") == 0 ||
						isvalidpool (stcp_input[i].poolname))) {
					sendrep (rpfd, MSG_ERR, STG32, stcp_input[i].poolname);
					c = USERR;
					goto reply;
				}
			}
		}
		for (i = 0; i < nstpp_input; i++) {
			path_status = 0;
			unmarshall_STAGE_PATH(STAGE_INPUT_MODE, path_status, rbp, &(stpp_input[i]));
			if (path_status != 0) {
				sendrep(rpfd, MSG_ERR, "STG02 - Bad input (path input structure No %d/%d)\n", ++i, nstpp_input);
				c = USERR;
				goto reply;
			}
			stglogit(func,"stpp[%d/%d] : %s\n",i+1,nstpp_input,stpp_input[i].upath);
		}
	} else {
		nargs = req2argv (rbp, &argv);
	}

	if (Cgetpwuid (stgreq.uid) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", stgreq.uid);
		sendrep (rpfd, MSG_ERR, STG11, uidstr);
		c = SYERR;
		goto reply;
	}
	if ((gr = Cgetgrgid (stgreq.gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, stgreq.gid);
		c = SYERR;
		goto reply;
	}
	strcpy (stgreq.group, gr->gr_name);
	strncpy (save_group, gr->gr_name, CA_MAXGRPNAMELEN);
	save_group[CA_MAXGRPNAMELEN] = '\0';

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
			sendrep (rpfd, MSG_ERR, STG12);
			errflg++;
			break;
 		}
		if ((flags & STAGE_DEFERRED) == STAGE_DEFERRED) Aflag = 1;
		if ((flags & STAGE_COFF)  == STAGE_COFF) concat_off = 1;
		if ((flags & STAGE_UFUN)  == STAGE_UFUN) Uflag = 1;
		if ((flags & STAGE_INFO)  == STAGE_INFO) copytape = 1;
		if ((flags & STAGE_SILENT)  == STAGE_SILENT) silent_flag = 1;
		if ((concat_off != 0) && (req_type != STAGE_IN)) {
			sendrep (rpfd, MSG_ERR, STG17, "-c off", "any request but stage_in");
			errflg++;
 		}
		if ((Aflag != 0) && (req_type != STAGE_IN) && (req_type != STAGE_WRT)) {
			sendrep (rpfd, MSG_ERR, STG17, "-A deferred", "any request but stage_in or stage_wrt");
			errflg++;
 		}
		if ((Uflag != 0) && (req_type == STAGE_CAT)) {
			sendrep (rpfd, MSG_ERR, STG17, "-U", "stage_cat");
			errflg++;
		}
		if (concat_off != 0) {
			if (((nstcp_input != 1) || (nstpp_input != 0))) {
				sendrep (rpfd, MSG_ERR, STG17, "-c off", "stage_in and something else but exactly one disk file structure in input");
				errflg++;
			} else if (t_or_d != 't') {
				sendrep (rpfd, MSG_ERR, STG17, "-c off", "non-tape structure in input (t_or_d != 't')");
				errflg++;
			} else if (stcp_input[0].u1.t.fseq[strlen(stcp_input[0].u1.t.fseq) -1] != '-') {
				sendrep (rpfd, MSG_ERR, STG17, "-c off", "tape structure with fseq not ending with '-'");
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
						sendrep (rpfd, MSG_ERR, STG06, "-F");
						errflg++;
					}
				}
				if ((req_type == STAGE_IN) && Aflag && stcp_input[i].size <= 0) {
					getdefsize(stcp_input[i].poolname,&(stcp_input[i].size));
				}
				if (stcp_input[i].u1.t.lbl[0] != '\0') {
					if (! (strcmp(stcp_input[i].u1.t.lbl, "blp") || (req_type != STAGE_OUT && req_type != STAGE_WRT))) {
						sendrep (rpfd, MSG_ERR, STG17, "-l blp", "stage_out/stage_wrt");
						errflg++;
					}
				}
				if (stcp_input[i].recfm[0] == 'F' && req_type != STAGE_IN && stcp_input[i].lrecl == 0) {
					sendrep (rpfd, MSG_ERR, STG20);
					errflg++;
				}
				for (ivid = 0; ivid < MAXVSN; ivid++) {
					if (stcp_input[i].u1.t.vid[ivid][0] != '\0') {
						if (stgreq.u1.t.vid[ivid][0] == '\0') {
							strcpy(stgreq.u1.t.vid[ivid],stcp_input[i].u1.t.vid[ivid]);
							numvid++;
						} else if (strcmp(stgreq.u1.t.vid[ivid],stcp_input[i].u1.t.vid[ivid]) != 0) {
							sendrep (rpfd, MSG_ERR, "STG02 - You cannot change VID list from one structure (%s) to another (%s)\n",stgreq.u1.t.vid[ivid],stcp_input[i].u1.t.vid[ivid]);
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
							sendrep (rpfd, MSG_ERR, "STG02 - You cannot change VSN list from one structure (%s) to another (%s)\n",stgreq.u1.t.vsn[ivsn],stcp_input[i].u1.t.vsn[ivsn]);
							errflg++;
							break;
						}
					}
				}
				if (stcp_input[i].u1.t.lbl[0] != '\0') {
					if (stgreq.u1.t.lbl[0] == '\0') {
						strcpy(stgreq.u1.t.lbl,stcp_input[i].u1.t.lbl);
					} else if (strcmp(stgreq.u1.t.lbl,stcp_input[i].u1.t.lbl) != 0) {
						sendrep (rpfd, MSG_ERR, "STG02 - You cannot change label from one structure (%s) to another (%s)\n",stgreq.u1.t.lbl,stcp_input[i].u1.t.lbl);
						errflg++;
						break;
					}
				}
				if (stcp_input[i].u1.t.dgn[0] != '\0') {
					if (stgreq.u1.t.dgn[0] == '\0') {
						strcpy(stgreq.u1.t.dgn,stcp_input[i].u1.t.dgn);
					} else if (strcmp(stgreq.u1.t.dgn,stcp_input[i].u1.t.dgn) != 0) {
						sendrep (rpfd, MSG_ERR, "STG02 - You cannot change device group from one structure (%s) to another (%s)\n",stgreq.u1.t.dgn,stcp_input[i].u1.t.dgn);
						errflg++;
						break;
					}
				}
				if (stcp_input[i].u1.t.den[0] != '\0') {
					if (stgreq.u1.t.den[0] == '\0') {
						strcpy(stgreq.u1.t.den,stcp_input[i].u1.t.den);
					} else if (strcmp(stgreq.u1.t.den,stcp_input[i].u1.t.den) != 0) {
						sendrep (rpfd, MSG_ERR, "STG02 - You cannot change density from one structure (%s) to another (%s)\n",stgreq.u1.t.den,stcp_input[i].u1.t.den);
						errflg++;
						break;
					}
				}
				if (stcp_input[i].u1.t.tapesrvr[0] != '\0') {
					if (stgreq.u1.t.tapesrvr[0] == '\0') {
						strcpy(stgreq.u1.t.tapesrvr,stcp_input[i].u1.t.tapesrvr);
					} else if (strcmp(stgreq.u1.t.tapesrvr,stcp_input[i].u1.t.tapesrvr) != 0) {
						sendrep (rpfd, MSG_ERR, "STG02 - You cannot change tape server from one structure (%s) to another (%s)\n",stgreq.u1.t.tapesrvr,stcp_input[i].u1.t.tapesrvr);
						errflg++;
						break;
					}
				}
				break;
			case 'm':
			case 'h':
				if (nhsmfiles == 0) {
					if ((hsmfiles   = (char **)             malloc(sizeof(char *))) == NULL) {
						sendrep (rpfd, MSG_ERR, STG33, "malloc", strerror(errno));
						c = SYERR;
						goto reply;
					}
				} else {
					char **dummy = hsmfiles;
					if ((dummy  = (char **)             realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
						sendrep (rpfd, MSG_ERR, STG33, "realloc", strerror(errno));
						c = SYERR;
						goto reply;
					}
					hsmfiles = dummy;
				}
				if (Aflag && stcp_input[i].size != 0) {
					sendrep (rpfd, MSG_ERR, STG35, "-A deffered", "-s <size_in_MB>");
					errflg++;
				}
				hsmfiles[nhsmfiles++] = (t_or_d == 'm' ? stcp_input[i].u1.m.xfile : stcp_input[i].u1.h.xfile);
				if ((c = check_hsm_type(hsmfiles[nhsmfiles - 1],&nhpssfiles,&ncastorfiles,&nuserlevel,&nexplevel,&t_or_d)) != 0) {
					errflg++;
				}
				break;
			default:
				break;
			}
			if (errflg != 0) break;
		}
		/* Print the flags */
		stglogflags(func,flags);
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
						sendrep (rpfd, MSG_ERR, STG17, "-A deferred",
										 "stageout/stagecat");
						errflg++;
					} else
						Aflag = 1; /* deferred space allocation */
				} else if (strcmp (Coptarg, "immediate")) {
					sendrep (rpfd, MSG_ERR, STG06, "-A");
					errflg++;
				}
				break;
			case 'b':
				stgreq.blksize = strtol (Coptarg, &dp, 10);
				if (*dp != '\0' || stgreq.blksize == 0) {
					sendrep (rpfd, MSG_ERR, STG06, "-b");
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
					} else if (strcmp (p, "ascii")) {
						sendrep (rpfd, MSG_ERR, STG06, "-C");
						errflg++;
						break;
					}
					if ((p = strtok (NULL, ",")) != NULL) *(p - 1) = ',';
				}
				break;
			case 'c':	/* concatenation */
				if (strcmp (Coptarg, "off") == 0) {
					if (req_type != STAGEIN){
						sendrep (rpfd, MSG_ERR, STG17, "-c off",
										 "stageout/stagewrt/stagecat");
						errflg++;
					} else
						concat_off = 1;
				} else if (strcmp (Coptarg, "on")) {
					sendrep (rpfd, MSG_ERR, STG06, "-c");
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
					sendrep (rpfd, MSG_ERR, STG06, "-E");
					errflg++;
				}
				break;
			case 'F':
				if (strcmp (Coptarg, "F") && strcmp (Coptarg, "FB") &&
						strcmp (Coptarg, "FBS") && strcmp (Coptarg, "FS") &&
						strcmp (Coptarg, "U") && strcmp (Coptarg, "U,bin") &&
						strcmp (Coptarg, "U,f77") && strcmp (Coptarg, "F,-f77")) {
					sendrep (rpfd, MSG_ERR, STG06, "-F");
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
				stgreq.lrecl = strtol (Coptarg, &dp, 10);
				if (*dp != '\0' || stgreq.lrecl == 0) {
					sendrep (rpfd, MSG_ERR, STG06, "-L");
					errflg++;
				}
				break;
			case 'l':
				if (strcmp (Coptarg, "blp") ||
						(req_type != STAGEOUT && req_type != STAGEWRT))
					strcpy (stgreq.u1.t.lbl, Coptarg);
				else {
					sendrep (rpfd, MSG_ERR, STG17, "-l blp", "stageout/stagewrt");
					errflg++;
				}
				break;
			case 'M':
				stgreq.t_or_d = 'm';
				if (nhsmfiles == 0) {
					if ((hsmfiles   = (char **)             malloc(sizeof(char *))) == NULL) {
						sendrep (rpfd, MSG_ERR, STG33, "malloc", strerror(errno));
						c = SYERR;
						goto reply;
					}
				} else {
					char **dummy = hsmfiles;
					if ((dummy  = (char **)             realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
						sendrep (rpfd, MSG_ERR, STG33, "realloc", strerror(errno));
						c = SYERR;
						goto reply;
					}
					hsmfiles = dummy;
				}
				hsmfiles[nhsmfiles++] = Coptarg;
				if ((c = check_hsm_type(Coptarg,&nhpssfiles,&ncastorfiles,&nuserlevel,&nexplevel,&(stgreq.t_or_d))) != 0) {
					goto reply;
				}
				break;
			case 'N':
				nread = Coptarg;
				p = strtok (nread, ":");
				while (p != NULL) {
					j = strtol (p, &dp, 10);
					if (*dp != '\0') {
						sendrep (rpfd, MSG_ERR, STG06, "-N");
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
						isvalidpool (Coptarg)) {
					strcpy (stgreq.poolname, Coptarg);
				} else {
					sendrep (rpfd, MSG_ERR, STG32, Coptarg);
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
					j = strtol (p, &dp, 10);
					if (*dp != '\0' || j <= 0) {
						sendrep (rpfd, MSG_ERR, STG06, "-s");
						errflg++;
					}
					if ((p = strtok (NULL, ":")) != NULL) *(p - 1) = ':';
				}
				break;
			case 'T':
				stgreq.u1.t.E_Tflags |= NOTRLCHK;
				break;
			case 't':
				stgreq.u1.t.retentd = strtol (Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-t");
					errflg++;
				}
				break;
			case 'u':
				pool_user = Coptarg;
				break;
			case 'U':
				if (req_type == STAGECAT) {
					sendrep (rpfd, MSG_ERR, STG17, "-U", "stagecat");
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
					sendrep (rpfd, MSG_ERR, STG06, "-X");
					errflg++;
				}
			case 'z':
				copytape++;
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
			sendrep(rpfd, MSG_ERR, "### Cannot get API command-line's request equivalent of req_type=%d (!?)\n",req_type);
			c = USERR;
			goto reply;
		}
		stgreq.t_or_d = (int) t_or_d;
		nargs = nstcp_input + nstpp_input;
		Coptind = nstcp_input;
		if (concat_off != 0 && stcp_input[0].u1.t.fseq[0] != '\0') fseq = stcp_input[0].u1.t.fseq;
	}

	if (nhsmfiles > 0) {
		/* We check that there is no redundant hsm files (multiple equivalent ones) */
		for (ihsmfiles = 0; ihsmfiles < nhsmfiles; ihsmfiles++) {
			for (jhsmfiles = ihsmfiles + 1; jhsmfiles < nhsmfiles; jhsmfiles++) {
				if (strcmp(hsmfiles[ihsmfiles],hsmfiles[jhsmfiles]) == 0) {
					sendrep (rpfd, MSG_ERR, STG59, hsmfiles[ihsmfiles]);
					errflg++;
				}
			}
		}
	}

	if (Aflag && strcmp (stgreq.poolname, "NOPOOL") == 0) {
		sendrep (rpfd, MSG_ERR, STG17, "-A deferred", "-p NOPOOL");
		errflg++;
	}
	if (Aflag && req_type == STAGEWRT && ncastorfiles == 0) {
		/* If deferred allocation for stagewrt, we check for existence of -M option */
		/* The -M option values validity themselves will be checked later */
		sendrep (rpfd, MSG_ERR, STG47, "stagewrt -A deferred is valid only with CASTOR HSM files (-M /castor/...)\n");
		errflg++;
	}
#ifndef CONCAT_OFF
	if (concat_off != 0) {
		sendrep (rpfd, MSG_ERR, "STG17 - option -c off is not supported by this server\n");
		errflg++;
	}
#else
	if (concat_off != 0 && strcmp (stgreq.poolname, "NOPOOL") == 0) {
		sendrep (rpfd, MSG_ERR, STG17, "-c off", "-p NOPOOL");
		errflg++;
	}
	if (concat_off != 0 && stgreq.t_or_d != 't') {
		sendrep (rpfd, MSG_ERR, "STG17 - option -c off is only valid with -V option\n");
		errflg++;
	}
	if (concat_off != 0 && (nargs - Coptind != 0)) {
		sendrep (rpfd, MSG_ERR, "STG17 - option -c off is only valid without explicit disk files\n");
		errflg++;
	}
	if ((concat_off != 0) && (req_type != STAGEIN)) {
		sendrep (rpfd, MSG_ERR, STG17, "-c off", "any request but stage_in");
		errflg++;
	}
#endif
	if (api_out == 0) {
		/* Api checksum for this thing is done before */
		if (*stgreq.recfm == 'F' && req_type != STAGEIN && stgreq.lrecl == 0) {
			sendrep (rpfd, MSG_ERR, STG20);
			errflg++;
		}
		if (stgreq.t_or_d == '\0') {
			sendrep (rpfd, MSG_ERR, STG12);
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
				sendrep (rpfd, MSG_ERR, STG116);
				errflg++;
			} else {
				/* We check that they are all the same */
				for (i = 1; i < nstcp_input; i++) {
					if (strcmp(stcp_input[i-1].u1.h.tppool, stcp_input[i].u1.h.tppool) != 0) {
						sendrep (rpfd, MSG_ERR, STG117,
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
			sendrep (rpfd, MSG_ERR, STG17, "--tppool", (req_type == STAGEIN) ? "stagein" : "stagecat");
			errflg++;
		} else {
			/* Print the flags */
			stglogtppool(func,tppool);
		}
	}

	if (errflg != 0) {
		c = USERR;
		goto reply;
	}

	/* setting defaults */

	if (concat_off != 0)
		Aflag = 1;	/* force deferred space allocation */
	/* Api checksum for this thing is done before */
	if (req_type == STAGEIN && stgreq.t_or_d == 'm' && ! size)
		Aflag = 1;  /* force deferred policy if stagein of an hsm file without -s option */
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
	} else poolflag = 1;

	if (pool_user == NULL)
		pool_user = "stage";

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
		if (concat_off != 0 && numvid != 1) {
			sendrep (rpfd, MSG_ERR, "STG02 - option -c off is not compatible with volume spanning\n");
			errflg++;
			c = USERR;
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
				sendrep (rpfd, MSG_ERR, "STG02 - option -c off requires exactly one tapefile, ending with '-'\n");
				errflg++;
			}
			/* We want to determine which highest fseq is already staged for this vid */
			if ((maxfseq = maxfseq_per_vid(&stgreq,poolflag,stgreq.poolname,&concat_off_flag)) > 0) {
				fseq_elem *newfseq_list = NULL;

				if (maxfseq >= concat_off_fseq) {
					int new_nbtpf = maxfseq - concat_off_fseq + (concat_off_flag == 'c' ? 1 : 2);
					/* There is overlap from concat_off_fseq to maxfseq */

					if ((newfseq_list = realloc(fseq_list,new_nbtpf * sizeof(fseq_elem))) == NULL) {
						sendrep (rpfd, MSG_ERR, "STG02 - realloc() error (%s)\n", strerror(errno));
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
				}
			}
		}
	} else nbtpf = 1;

	if (errflg != 0) {
		c = USERR;
		goto reply;
	}

	/* compute number of disk files */
	nbdskf = nargs - Coptind;
	if (nbdskf == 0) {
		if (poolflag < 0) {	/* -p NOPOOL */
			sendrep (rpfd, MSG_ERR, STG07);
			c = USERR;
			goto reply;
		}
		nbdskf = nbtpf;
		no_upath = 1;
		upath[0] = '\0';
	}

	/* In case of hsm request verify the exact mapping between number of hsm files */
	/* and number of disk files.                                                   */
	if (nhsmfiles > 0) {
		if ((nargs - Coptind) > 0 && nbdskf != nhsmfiles) {
			sendrep (rpfd, MSG_ERR, STG19);
			c = USERR;
			goto reply;
		}
	} else if (concat_off != 0 || api_out == 0) {
		if ((req_type == STAGEIN &&
				 ! Uflag && nbdskf > nbtpf && trailing != '-') ||
				(req_type != STAGEIN && nbtpf > nbdskf) ||
				(Uflag && nbdskf > 2)) {
			sendrep (rpfd, MSG_ERR, STG19);
			c = USERR;
			goto reply;
		}
		if (req_type == STAGEIN && ! Uflag && nbdskf > nbtpf) {
			fseq_list = (fseq_elem *) realloc (fseq_list, nbdskf * sizeof(fseq_elem));
			j = strtol ((char *)(fseq_list + nbtpf - 1), &dp, 10) + 1;
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

	for (i = 0; i < nbdskf; i++) {
		time_t hsmmtime;
		int forced_Cns_creatx;
		int forced_rfio_stat;
		u_signed64 correct_size;
		u_signed64 hsmsize;
		u_signed64 size_to_recall;
		int stage_wrt_migration;
		struct stgcat_entry save_stcp_for_Cns_creatx;
		int have_save_stcp_for_Cns_creatx = 0;

		global_c_stagewrt = 0;
		global_c_stagewrt_SYERR = 0;

		if (api_out != 0) {
			memcpy(&stgreq,&(stcp_input[i < nstcp_input ? i : nstcp_input - 1]),sizeof(struct stgcat_entry));
			stgreq.t_or_d = stcp_input[0].t_or_d; /* Might have been overwriten ('m' -> 'h') */
			stgreq.uid = save_uid;
			stgreq.gid = save_gid;
			stgreq.mask = save_mask;
			strcpy(stgreq.user,save_user);
			strcpy(stgreq.group,save_group);
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
					sendrep (rpfd, MSG_ERR, STG21);
					c = USERR;
					goto reply;
				}
#ifdef STAGER_DEBUG
				sendrep (rpfd, MSG_ERR, "[DEBUG] Packed fseq \"%s\"\n", stgreq.u1.t.fseq);
#endif
			}
			if (concat_off_fseq > 0) {
				stgreq.filler[0] = 'c';
#ifdef STAGER_DEBUG
				sendrep (rpfd, MSG_ERR, "[DEBUG] Put 'c' in filler[0]\n");
#endif
			}
		} else if (stgreq.t_or_d == 'm') {
			strcpy(stgreq.u1.m.xfile,hsmfiles[ihsmfiles]);
		} else if (stgreq.t_or_d == 'h') {
			strcpy(stgreq.u1.h.xfile,hsmfiles[ihsmfiles]);
			stgreq.u1.h.server[0] = '\0';
			stgreq.u1.h.fileid = 0;
			stgreq.u1.h.fileclass = 0;
			if (tppool != NULL) {
				strcpy(stgreq.u1.h.tppool,tppool);
			} else {
				stgreq.u1.h.tppool[0] = '\0';
			}
		}
		if (nread) {
			/* Only set with command-line options */
			if ((p = strchr (nread, ':')) != NULL) *p = '\0';
			stgreq.nread = strtol (nread, &dp, 10);
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
				memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
				if (Cns_statx(hsmfiles[ihsmfiles], &Cnsfileid, &Cnsfilestat) != 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_statx", sstrerror(serrno));
					c = USERR;
					goto reply;
				}
				strcpy(stgreq.u1.h.server,Cnsfileid.server);
				stgreq.u1.h.fileid = Cnsfileid.fileid;
				stgreq.u1.h.fileclass = Cnsfilestat.fileclass;
				hsmmtime = Cnsfilestat.mtime;
				hsmsize = Cnsfilestat.filesize;
				break;
			case 'm':
				if (rfio_stat(hsmfiles[ihsmfiles], &filemig_stat) < 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "rfio_stat", rfio_serror());
					c = USERR;
					goto reply;
				}
				hsmmtime = filemig_stat.st_mtime;
				hsmsize = (u_signed64) filemig_stat.st_size;
				break;
			default:
				break;
			}
			setegid(start_passwd.pw_gid);
			seteuid(start_passwd.pw_uid);
			if (((api_out == 0) && (! size)) || (api_out != 0 && ! stgreq.size)) {
				/* We force size to be allocated to exactly what we need */
				stgreq.size = (int) ((hsmsize > ONE_MB) ? (((hsmsize - ((hsmsize / ONE_MB) * ONE_MB)) == 0) ? (hsmsize / ONE_MB) : ((hsmsize / ONE_MB) + 1)) : 1);
				if (ncastorfiles > 0) size_to_recall = hsmsize;
			} else {
				goto get_size_from_user;
			}
		} else if (size) {
		get_size_from_user:
			if ((p = strchr (size, ':')) != NULL) *p = '\0';
			stgreq.size = strtol (size, &dp, 10);
			if (p != NULL) {
				*p = ':';
				size = p + 1;
			}
			if (ncastorfiles > 0) {
				size_to_recall = (u_signed64) (((u_signed64) stgreq.size) * (u_signed64) ONE_MB);
				if (size_to_recall > hsmsize) {
					/* We take the minimum of -s option and real size in the nameserver */
					size_to_recall = hsmsize;
				}
			}
		}
		if (no_upath == 0) {
			if (api_out == 0) {
				strcpy(upath, argv[Coptind+i]);
			} else {
				strcpy(upath, stpp_input[i].upath);
			}
		}

		switch (req_type) {
		case STAGEIN:
			switch (isstaged (&stgreq, &stcp, poolflag, stgreq.poolname)) {
			case ISSTAGEDBUSY:
				c = EBUSY;
				goto reply;
			case ISSTAGEDSYERR:
				c = SYERR;
				goto reply;
			case STAGEIN:	/* stage in progress */
			case STAGEIN|WAITING_SPC:	/* waiting space */
				stcp->nbaccesses++;
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqid = stcp->reqid;
				stcp = newreq ();
				memcpy (stcp, &stgreq, sizeof(stgreq));
				if (i > 0)
					stcp->reqid = nextreqid();
				else
					stcp->reqid = reqid;
				stcp->status = STAGEIN;
				stcp->c_time = time(NULL);
				stcp->a_time = stcp->c_time;
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
#ifdef CONCAT_OFF
					wqp->concat_off_fseq = concat_off_fseq;
#endif
					wqp->api_out = api_out;
					wqp->uniqueid = stage_uniqueid;
					wqp->silent = silent_flag;
					if (nowait_flag != 0) {
						/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
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
				if (rfio_stat (stcp->ipath, &st) < 0) {
					stglogit (func, STG02, stcp->ipath, "rfio_stat", rfio_serror());
					if (delfile (stcp, 0, 1, 1, "not on disk", 0, 0, 0, 0) < 0) {
						sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
										 "rfio_unlink", rfio_serror());
						c = SYERR;
						goto reply;
					}
					goto notstaged;
				} else if (nhsmfiles > 0 && hsmmtime > stcp->a_time) {
					/*
					 * HSM File exists but has a modified time higher than what
					 * is known to the stager.
					 */
					if (delfile (stcp, 0, 1, 1, "mtime in nameserver > last access time", stgreq.uid, stgreq.gid, 0, 0) < 0) {
						sendrep (rpfd, MSG_ERR,
										 STG02, stcp->ipath,
										 "rfio_unlink", rfio_serror());
						c = SYERR;
						goto reply;
					}
					goto notstaged;
				} else {
					/*
					 * File exists, now check to see if it is
					 * a partial file, and replace it if current
					 * request is for a larger size.
					 */
					if ((stcp->status == (STAGEIN|STAGED|STAGED_LSZ)) &&
							(stgreq.size > stcp->size)) {
						if (delfile (stcp, 0, 1, 1, "larger req", stgreq.uid, stgreq.gid, 0, 0) < 0) {
							sendrep (rpfd, MSG_ERR,
											 STG02, stcp->ipath,
											 "rfio_unlink", rfio_serror());
							c = SYERR;
							goto reply;
						}
						goto notstaged;
					}
				}
			case STAGEWRT:
			case STAGEWRT|CAN_BE_MIGR:
			case STAGEOUT|CAN_BE_MIGR:
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
#if SACCT
				stageacct (STGFILS, stgreq.uid, stgreq.gid,
									 clienthost, reqid, req_type, 0, 0, stcp, "");
#endif
				sendrep (rpfd, RTCOPY_OUT, STG96,
								 strrchr (stcp->ipath, '/')+1,
								 stcp->actual_size,
								 (float)(stcp->actual_size)/(1024.*1024.),
								 stcp->nbaccesses);
				if (api_out != 0) sendrep(rpfd, API_STCP_OUT, stcp);
				if (copytape)
					sendinfo2cptape (rpfd, stcp);
				if (*upath && strcmp (stcp->ipath, upath))
					create_link (stcp, upath);
				if (Upluspath && ((api_out == 0) ? (argv[Coptind+1][0] != '\0') : (stpp_input[1].upath[0] != '\0')) &&
						strcmp (stcp->ipath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath))
					create_link (stcp, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
				break;
			case STAGEOUT:
			case STAGEOUT|WAITING_SPC:
			case STAGEOUT|WAITING_NS:
				sendrep (rpfd, MSG_ERR, STG37);
				c = USERR;
				goto reply;
			case STAGEOUT|CAN_BE_MIGR|WAITING_MIGR:
			case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
			case STAGEPUT|CAN_BE_MIGR:
			case STAGEWRT|CAN_BE_MIGR|BEING_MIGR:
				/* This stcp is is physically being really migrated */
				sendrep (rpfd, MSG_ERR, STG37);
				c = EBUSY;
				goto reply;
			notstaged:
			default:
				if (trailing == '-' && last_tape_file &&
						atoi (stgreq.u1.t.fseq) > last_tape_file) {
					/* requested file is not on tape */
					stglogit (func, "requested file is not on tape\n");
					nbdskf = i;
					continue;	/* exit from the loop */
				}
				stcp = newreq ();
				memcpy (stcp, &stgreq, sizeof(stgreq));
				if (i > 0)
					stcp->reqid = nextreqid();
				else
					stcp->reqid = reqid;
				stcp->status = STAGEIN;
				stcp->c_time = time(NULL);
				stcp->a_time = stcp->c_time;
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
#ifdef CONCAT_OFF
					wqp->concat_off_fseq = concat_off_fseq;
#endif
					wqp->api_out = api_out;
					wqp->uniqueid = stage_uniqueid;
					wqp->silent = silent_flag;
					if (nowait_flag != 0) {
						/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
					}
				}
				wfp->subreqid = stcp->reqid;
				/* Nota : save_subreqid is != NULL only if no '-c off' request */
				if (save_subreqid != NULL) {
					*save_subreqid = stcp->reqid;
					save_subreqid++;
				}
				wfp->size_to_recall = size_to_recall;   /* Can be zero */
				wfp->size_yet_recalled = 0;
				strcpy (wfp->upath, upath);
				strcpy (wqp->pool_user, pool_user);
				if (! Aflag) {
					if ((c = build_ipath (upath, stcp, pool_user)) < 0) {
						stcp->status |= WAITING_SPC;
						strcpy (wqp->waiting_pool, stcp->poolname);
					} else if (c) {
						updfreespace (stcp->poolname, stcp->ipath,
													(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
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
			break;
		case STAGEOUT:
			if (stgreq.t_or_d == 'h') {
				memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
				if (Cns_statx(hsmfiles[ihsmfiles], &Cnsfileid, &Cnsfilestat) == 0) {
					/* CASTOR file already exist */
					strcpy(stgreq.u1.h.server,Cnsfileid.server);
					stgreq.u1.h.fileid = Cnsfileid.fileid;
					stgreq.u1.h.fileclass = Cnsfilestat.fileclass;
				}
			}
			switch (isstaged (&stgreq, &stcp, poolflag, stgreq.poolname)) {
			case ISSTAGEDBUSY:
				c = EBUSY;
				goto reply;
			case ISSTAGEDSYERR:
				c = SYERR;
				goto reply;
			case NOTSTAGED:
				break;
			case STAGED:
				if (delfile (stcp, 0, 1, 1, user, stgreq.uid, stgreq.gid, 0, 0) < 0) {
					sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
									 "rfio_unlink", rfio_serror());
					c = SYERR;
					goto reply;
				}
				break;
			case STAGEOUT:
			case STAGEOUT|CAN_BE_MIGR:
			case STAGEOUT|WAITING_SPC:
			case STAGEOUT|WAITING_NS:
				if (stgreq.t_or_d == 't' && *stgreq.u1.t.fseq == 'n') break;
				if (strcmp (user, stcp->user)) {
					sendrep (rpfd, MSG_ERR, STG37);
					c = USERR;
					goto reply;
				}
				if (delfile (stcp, 1, 1, 1, user, stgreq.uid, stgreq.gid, 0, 0) < 0) {
					sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
									 "rfio_unlink", rfio_serror());
					c = SYERR;
					goto reply;
				}
				break;
			case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
			case STAGEOUT|CAN_BE_MIGR|WAITING_MIGR:
			case STAGEWRT|CAN_BE_MIGR:
			case STAGEWRT|CAN_BE_MIGR|BEING_MIGR:
			case STAGEPUT|CAN_BE_MIGR:
				sendrep (rpfd, MSG_ERR, STG37);
				c = EBUSY;
				goto reply;
			default:
				sendrep (rpfd, MSG_ERR, STG37);
				c = USERR;
				goto reply;
			}
			stcp = newreq ();
			memcpy (stcp, &stgreq, sizeof(stgreq));
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = STAGEOUT;
			stcp->c_time = time(NULL);
			stcp->a_time = stcp->c_time;
			stcp->nbaccesses++;
			if (stgreq.t_or_d == 'h') {
				/* Special insert before build_ipath() call because of the poissible WAITING_NS state */
				stcp->status |= WAITING_NS;
#ifdef USECDB
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs();
			}
			if ((c = build_ipath (upath, stcp, pool_user)) < 0) {
				stcp->status |= WAITING_SPC;
				if (!wqp) {
					wqp = add2wq (clienthost,
									user, stcp->uid, stcp->gid,
									user, save_group, stcp->uid, stcp->gid,
									clientpid,
									Upluspath, reqid, req_type, nbdskf, &wfp, NULL, 
									stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq, 0);
					wqp->api_out = api_out;
					wqp->openflags = openflags;
					wqp->openmode = openmode;
					wqp->uniqueid = stage_uniqueid;
					wqp->silent = silent_flag;
					if (nowait_flag != 0) {
						/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
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
				updfreespace (stcp->poolname, stcp->ipath,
							(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
				delreq(stcp,stcp->t_or_d == 'h' ? 0 : 1);
				goto reply;
			} else {
				/* Space successfully allocated - Try to get rid of WAITING_NS */
				if (stcp->t_or_d == 'h') {
					if ((c = create_hsm_entry(rpfd, stcp, api_out, openmode, 1)) != 0) {
						goto reply;
					}
				}
				if (*upath && strcmp (stcp->ipath, upath))
					create_link (stcp, upath);
				if (Upluspath && ((api_out == 0) ? (argv[Coptind+1][0] != '\0') : (stpp_input[1].upath[0] != '\0')) &&
						strcmp (stcp->ipath, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath))
					create_link (stcp, (api_out == 0) ? argv[Coptind+1] : stpp_input[1].upath);
				rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT);
			}
#ifdef USECDB
			if (stcp->t_or_d != 'h') {
				if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
					stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
				}
			}
#endif
			if (api_out != 0) sendrep(rpfd, API_STCP_OUT, stcp);
			break;
		case STAGEWRT:
			if ((p = findpoolname (upath)) != NULL) {
				if (poolflag < 0 ||
						(poolflag > 0 && strcmp (stgreq.poolname, p)))
					sendrep (rpfd, MSG_ERR, STG49, upath, p);
				actual_poolflag = 1;
				strcpy (actual_poolname, p);
			} else {
				if (Aflag) {
					/* Must have a valid poolname in deferred mode */
					sendrep (rpfd, MSG_ERR, STG33, upath, "must be in a valid pool if -A deferred mode");
                    c = USERR;
					goto reply;
				}
				if (poolflag > 0)
					sendrep (rpfd, MSG_ERR, STG50, upath);
				actual_poolflag = -1;
				actual_poolname[0] = '\0';
			}
			stage_wrt_migration = 0;
			switch (isstaged (&stgreq, &stcp, actual_poolflag, actual_poolname)) {
			case ISSTAGEDBUSY:
				global_c_stagewrt++;
				c = EBUSY;
				goto stagewrt_continue_loop;
			case ISSTAGEDSYERR:
				global_c_stagewrt++;
				global_c_stagewrt_SYERR++;
				c = SYERR;
				goto stagewrt_continue_loop;
			case NOTSTAGED:
				break;
			case STAGED:
				if (stcp->poolname[0] && strcmp (stcp->ipath, upath)) {
					if (delfile (stcp, 0, 1, 1, user, stgreq.uid, stgreq.gid, 0, 0) < 0) {
						sendrep (rpfd, MSG_ERR, STG02, stcp->ipath,
										 "rfio_unlink", rfio_serror());
						global_c_stagewrt++;
						global_c_stagewrt_SYERR++;
						c = SYERR;
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
				save_stcp_for_Cns_creatx = *stcp;
				have_save_stcp_for_Cns_creatx = 1;
				delreq(stcp,0);
				break;
			case STAGEOUT|CAN_BE_MIGR:
			case STAGEOUT|CAN_BE_MIGR|BEING_MIGR:
			case STAGEPUT|CAN_BE_MIGR:
			case STAGEWRT|CAN_BE_MIGR|BEING_MIGR:
				sendrep (rpfd, MSG_ERR, STG37);
				c = EBUSY;
				goto reply;
			case STAGEOUT|CAN_BE_MIGR|WAITING_MIGR:
				/* We accept a stagewrt request on a record waiting for it only if both tape pool matches */
				/* and are non-null */
				if (! ((stgreq.t_or_d == 'h') && 
						(stcp->u1.h.tppool[0] != '\0') && strcmp(stcp->u1.h.tppool,stgreq.u1.h.tppool) == 0)) {
					sendrep (rpfd, MSG_ERR, STG37);
					global_c_stagewrt++;
					c = USERR;
					goto stagewrt_continue_loop;
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
			case STAGEWRT:
				if (stcp->t_or_d == 't' && *stcp->u1.t.fseq == 'n') {
					save_stcp_for_Cns_creatx = *stcp;
					have_save_stcp_for_Cns_creatx = 1;
					break;
				}
			default:
				sendrep (rpfd, MSG_ERR, STG37);
				global_c_stagewrt++;
				c = USERR;
				goto stagewrt_continue_loop;
			}
			setegid(have_save_stcp_for_Cns_creatx ? save_stcp_for_Cns_creatx.gid : stgreq.gid);
			seteuid(have_save_stcp_for_Cns_creatx ? save_stcp_for_Cns_creatx.uid : stgreq.uid);
			switch (stgreq.t_or_d) {
			case 'h':
				/* If it is a stagewrt on CASTOR file and if this file have the same size we assume */
				/* that user wants to do a second copy. */
				memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
				if (Cns_statx(hsmfiles[ihsmfiles], &Cnsfileid, &Cnsfilestat) == 0) {
					/* File already exist */
					/* We already checked that nbdskf is equal to nhsmfiles */
					/* So using index ihsmfiles onto argv[Coptind + ihsmfiles] will point */
					/* immediately to the correct disk file associated with this HSM file */
					/* We compare the size of the disk file with the size in Name Server */
					if (api_out != 0 && (nstpp_input != nstcp_input)) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						/* Note - per construction u1.m.xfile and u1.h.xfile points to same string */
						sendrep (rpfd, MSG_ERR, STG119, stcp_input[ihsmfiles].u1.m.xfile);
						global_c_stagewrt++;
						c = USERR;
						goto stagewrt_continue_loop;
					}

					if (rfio_stat((api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath, &filemig_stat) < 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (rpfd, MSG_ERR, STG02, (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath, "rfio_stat", rfio_serror());
						global_c_stagewrt++;
						c = USERR;
						goto stagewrt_continue_loop;
					}
					correct_size = (u_signed64) filemig_stat.st_size;
					if (stgreq.size && ((u_signed64) (stgreq.size * ONE_MB) < correct_size)) {
						/* If user specified a maxsize of bytes to transfer and if this */
						/* maxsize is lower than physical file size, then the size of */
						/* of the migrated file will be the minimum of the twos */
						correct_size = (u_signed64) (stgreq.size * ONE_MB);
					}
					if (correct_size > 0 && correct_size == Cnsfilestat.filesize) {
						/* Same size and > 0 : we assume user asks for a new copy */
						strcpy(stgreq.u1.h.server,Cnsfileid.server);
						stgreq.u1.h.fileid = Cnsfileid.fileid;
						stgreq.u1.h.fileclass = Cnsfilestat.fileclass;
					} else if (correct_size <= 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (rpfd, MSG_OUT,
									"STG98 - %s size is of zero size - not migrated\n",
									(api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath);
						global_c_stagewrt++;
						c = USERR;
						goto stagewrt_continue_loop;
					} else {
						/* Not the same size or diskfile size is zero */
						if (Cnsfilestat.filesize > 0) {
							setegid(start_passwd.pw_gid);
							seteuid(start_passwd.pw_uid);
							sendrep (rpfd, MSG_OUT,
										"STG98 - %s renewed (size differs vs. %s)\n",
										hsmfiles[ihsmfiles],
										(api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath);
							setegid(stgreq.gid);
							seteuid(stgreq.uid);
						}
						forced_Cns_creatx = 1;
					}
				} else {
					/* It is not point to try to migrated something of zero size */
					if (rfio_stat((api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath, &filemig_stat) < 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (rpfd, MSG_ERR, STG02, (api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath, "rfio_stat", rfio_serror());
						global_c_stagewrt++;
						c = USERR;
						goto stagewrt_continue_loop;
					}
					if (filemig_stat.st_size <= 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (rpfd, MSG_OUT,
									"STG98 - %s size is of zero size - not migrated\n",
									(api_out == 0) ? argv[Coptind + ihsmfiles] : stpp_input[ihsmfiles].upath);
						global_c_stagewrt++;
						c = USERR;
						goto stagewrt_continue_loop;
					}
					/* We will have to create it */
					forced_Cns_creatx = 1;
					forced_rfio_stat = 1;
				}
				if (forced_Cns_creatx != 0) {
					mode_t okmode;

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
					if (api_out == 0) {
						okmode = ( 0777 & ~ (have_save_stcp_for_Cns_creatx ? save_stcp_for_Cns_creatx.mask : stgreq.mask));
					} else {
						okmode = (07777 & (openmode & ~ (have_save_stcp_for_Cns_creatx ? save_stcp_for_Cns_creatx.mask : stgreq.mask)));
					}
					if (have_save_stcp_for_Cns_creatx) {
						/* Makes sure we are under correct uid,gid */
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						setegid(save_stcp_for_Cns_creatx.gid);
						seteuid(save_stcp_for_Cns_creatx.uid);
					}
					if (Cns_creatx(hsmfiles[ihsmfiles], okmode, &Cnsfileid) != 0) {
						setegid(start_passwd.pw_gid);
						seteuid(start_passwd.pw_uid);
						sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_creatx", sstrerror(serrno));
						global_c_stagewrt++;
						c = USERR;
						goto stagewrt_continue_loop;
					}
					strcpy(stgreq.u1.h.server,Cnsfileid.server);
					stgreq.u1.h.fileid = Cnsfileid.fileid;
					/* The fileclass will be added if necessary by update_migpool() called below */
				}
				hsmmtime = time(NULL);
				/* Here, in any case, Cnsfileid is filled */
				break;
			case 'm':
				/* Overwriting an existing non-CASTOR HSM file is not allowed */
				if (rfio_stat(hsmfiles[ihsmfiles], &filemig_stat) == 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "rfio_stat", "file already exists");
					global_c_stagewrt++;
					c = USERR;
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
					if (rfio_stat (upath, &st) == 0) {
						correct_size = (u_signed64) st.st_size;
						if (stgreq.size && ((u_signed64) (stgreq.size * ONE_MB) < correct_size)) {
							/* If user specified a maxsize of bytes to transfer and if this */
							/* maxsize is lower than physical file size, then the size of */
							/* of the migrated file will be the minimum of the twos */
							correct_size = (u_signed64) (stgreq.size * ONE_MB);
						}
					} else {
						sendrep (rpfd, MSG_ERR, STG02, upath,
							"rfio_stat", rfio_serror());
						global_c_stagewrt++;
						c = USERR;
						goto stagewrt_continue_loop;
					}
				}
				/* We set the size in the name server */
				setegid(stgreq.gid);
				seteuid(stgreq.uid);
				if (have_save_stcp_for_Cns_creatx) {
					/* Makes sure we are under correct uid,gid */
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					setegid(save_stcp_for_Cns_creatx.gid);
					seteuid(save_stcp_for_Cns_creatx.uid);
				}
				if (Cns_setfsize(NULL,&Cnsfileid,correct_size) != 0) {
					setegid(start_passwd.pw_gid);
					seteuid(start_passwd.pw_uid);
					sendrep (rpfd, MSG_ERR, STG02, hsmfiles[ihsmfiles], "Cns_setfsize", sstrerror(serrno));
					global_c_stagewrt++;
					global_c_stagewrt_SYERR++;
					c = SYERR;
					goto stagewrt_continue_loop;
				}
				setegid(start_passwd.pw_gid);
				seteuid(start_passwd.pw_uid);
			}
			if ((stgreq.t_or_d == 'h') && (stage_wrt_migration != 0)) {
				struct stgcat_entry save_stcp = *stcp;
				/* We copy sensible part of the structure */
				stcp = newreq ();
				memcpy (stcp, &stgreq, sizeof(stgreq));
                /* Makes sure that it does not have keep thing */
				COPY_SENSIBLE_STCP(stcp,&save_stcp);
			} else {
				stcp = newreq ();
				memcpy (stcp, &stgreq, sizeof(stgreq));
			}
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = (Aflag ? (STAGEOUT|CAN_BE_MIGR) : STAGEWRT);
			strcpy (stcp->poolname, actual_poolname);
			if (stgreq.t_or_d != 'h') {
				if (rfio_stat (upath, &st) == 0) {
					stcp->actual_size = st.st_size;
					stcp->c_time = st.st_mtime;
				} else {
					stglogit(func, STG02, upath, "rfio_stat", rfio_serror());
				}
			} else {
				/* Already done before */
				stcp->actual_size = correct_size;
				stcp->c_time = hsmmtime;
			}
			stcp->a_time = time(NULL);
			strcpy (stcp->ipath, upath);
			stcp->nbaccesses = 1;
#ifdef USECDB
			if (stgdb_ins_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			if (stcp->t_or_d == 'h') {
				int ifileclass;


				if (stcp->u1.h.tppool[0] == '\0') {
					if ((ifileclass = upd_fileclass(NULL,stcp)) < 0) {
						sendrep (rpfd, MSG_ERR, STG132, stcp->u1.h.xfile, sstrerror(serrno));
						global_c_stagewrt++;
						c = USERR;
						delreq(stcp,0);
						goto stagewrt_continue_loop;
					}
					strcpy(stcp->u1.h.tppool,next_tppool(&(fileclasses[ifileclass])));
				} else if (Aflag) {        /* If Aflag is not set - check is done a little bit after */
					/* User specified a tape pool - We check the permission to access it */
					{
						struct stgcat_entry stcx = *stcp;
						stcx.uid = save_uid;
						stcx.gid = save_gid;
						/* stcx is a dummy req which inherits the uid/gid of the caller. */
						/* We use it at the first round to check that requestor's uid/gid is compatible */
						/* the uid/gid under which migration will effectively run might be different (case of stage:st) */
						if (euid_egid(&euid,&egid,stcp->u1.h.tppool,NULL,stcp,(i == 0) ? &stcx : NULL,NULL,0) != 0) {
							global_c_stagewrt++;
							c = USERR;
							delreq(stcp,0);
							goto stagewrt_continue_loop;
						}
					}
					if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
						global_c_stagewrt++;
						c = USERR;
						delreq(stcp,0);
						goto stagewrt_continue_loop;
					}
				}
				if ((stage_wrt_migration == 0) && (actual_poolname[0] != '\0')) {
					/* This entry is in the pool anyway and this is not for internal migration */
					if (update_migpool(stcp,1,Aflag ? 0 : 1) != 0) {
						global_c_stagewrt++;
						c = USERR;
						delreq(stcp,0);
						goto stagewrt_continue_loop;
					}
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				}
			}
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
							if (euid_egid(&euid,&egid,tppool,NULL,stcp,(i == 0) ? &stcx : NULL,&tppool,0) != 0) {
								global_c_stagewrt++;
								c = USERR;
								delreq(stcp,0);
								goto stagewrt_exit_loop;
							}
						}
						if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
								global_c_stagewrt++;
								c = USERR;
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
					wqp = add2wq (clienthost,
									user, stcp->uid, stcp->gid,
									user_waitq, group_waitq, uid_waitq, gid_waitq,
									clientpid, Upluspath, reqid, req_type,
									nbdskf, &wfp, NULL, 
									stcp->t_or_d == 't' ? stcp->u1.t.vid[0] : NULL, fseq, 0);
					wqp->copytape = copytape;
					wqp->silent = silent_flag;
					if (nowait_flag != 0) {
						/* User said nowait - we force silent flag anyway and reset rpfd to N/A */
						wqp->silent = 1;
						wqp->rpfd = -1;
					}
				}
				wfp->subreqid = stcp->reqid;
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
					update_migpool(stclp,-1,0);
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
					update_migpool(stclp,-1,0);
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
					sendrep (rpfd, MSG_ERR, STG49, upath, p);
				actual_poolflag = 1;
				strcpy (actual_poolname, p);
			} else {
				if (poolflag > 0)
					sendrep (rpfd, MSG_ERR, STG50, upath);
				actual_poolflag = -1;
				actual_poolname[0] = '\0';
			}
			switch (isstaged (&stgreq, &stcp, actual_poolflag, actual_poolname)) {
			case ISSTAGEDBUSY:
				c = EBUSY;
				goto reply;
			case ISSTAGEDSYERR:
				c = SYERR;
				goto reply;
			case NOTSTAGED:
				break;
			default:
				sendrep (rpfd, MSG_ERR, STG40);
				c = USERR;
				goto reply;
			}
			stcp = newreq ();
			memcpy (stcp, &stgreq, sizeof(stgreq));
			if (i > 0)
				stcp->reqid = nextreqid();
			else
				stcp->reqid = reqid;
			stcp->status = STAGEIN | STAGED;
			strcpy (stcp->poolname, actual_poolname);
			if (rfio_stat (upath, &st) < 0) {
				sendrep (rpfd, MSG_ERR, STG02, upath, "rfio_stat",
								 rfio_serror());
				delreq(stcp,1);
				goto reply;
			}
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
			if (api_out != 0) sendrep(rpfd, API_STCP_OUT, stcp);
			break;
		}
	}
	if ((req_type == STAGEWRT) && (global_c_stagewrt != 0) && (global_c_stagewrt == nbdskf)) {
		/* All entries have failed... */
		c = (global_c_stagewrt_SYERR ? SYERR : USERR);
		goto reply;
	}
	savepath ();
	savereqs ();
	c = 0;
	if (! wqp) goto reply;
	if (fseq_list!= NULL) free (fseq_list);
	if (argv != NULL) free (argv);
	if (*(wqp->waiting_pool)) {
		wqp->nb_clnreq++;
		cleanpool (wqp->waiting_pool);
	} else if (wqp->nb_subreqs > wqp->nb_waiting_on_req) {
		fork_exec_stager (wqp);
	}
	if (nowait_flag != 0) {
		sendrep (rpfd, STAGERC, req_type, c);
		close(rpfd);
		rpfd = -1;
	}
	if (hsmfiles != NULL) free(hsmfiles);
	if (stcp_input != NULL) free(stcp_input);
	if (stpp_input != NULL) free(stpp_input);
	return;
 reply:
	if (fseq_list != NULL) free (fseq_list);
	if (argv != NULL) free (argv);
	if (hsmfiles != NULL) free(hsmfiles);
	if (stcp_input != NULL) free(stcp_input);
	if (stpp_input != NULL) free(stpp_input);
#if SACCT
	stageacct (STGCMDC, stgreq.uid, stgreq.gid, clienthost,
						 reqid, req_type, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, req_type, c);
	if (c && wqp) {
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
			if (! wfp->waiting_on_req)
				updfreespace (stcp->poolname, stcp->ipath,
					(signed64) ((signed64) stcp->size * (signed64) ONE_MB));
			delreq(stcp,0);
		}
		rmfromwq (wqp);
	}
}

void procputreq(req_type, req_data, clienthost)
		 int req_type;
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	int c, i;
	int clientpid;
	char *dp;
	int errflg = 0;
	char *externfile;
	int found;
	char fseq[CA_MAXFSEQLEN + 1];
	gid_t gid;
	struct group *gr;
	int Iflag = 0;
	int Mflag = 0;
	char *name;
	int nargs;
	int nbdskf;
	int numvid = 0;
	int n1 = 0;
	int n2 = 0;
	char *q;
	char *rbp;
	struct stat st;
	struct stgcat_entry *stcp;
	int subreqid;
	int Upluspath = 0;
	uid_t uid;
	char upath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	char *user;
	char vid[7];
	struct waitf *wfp;
	struct waitq *wqp;
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
	uid_t uid_waitq;
	gid_t gid_waitq;
	char user_waitq[CA_MAXUSRNAMELEN+1];
	char group_waitq[CA_MAXGRPNAMELEN+1];
	char *tppool = NULL;
	int ifileclass;
	char save_group[CA_MAXGRPNAMELEN+1];
	char save_user[CA_MAXUSRNAMELEN+1];
	static struct Coptions longopts[] =
	{
		{"grpuser",            REQUIRED_ARGUMENT,  NULL,      'G'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"fortran_unit",       REQUIRED_ARGUMENT,  NULL,      'U'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"nowait",             NO_ARGUMENT,  &nowait_flag,      1},
		{"tppool",             REQUIRED_ARGUMENT, &tppool_flag, 1},
		{NULL,                 0,                  NULL,        0}
	};

	rbp = req_data;
	local_unmarshall_STRING (rbp, user);  /* login name */
	local_unmarshall_STRING (rbp, name);
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	unmarshall_WORD (rbp, clientpid);

	if (Cgetpwuid (uid) == NULL) {
		char uidstr[8];
		sprintf (uidstr, "%d", uid);
		sendrep (rpfd, MSG_ERR, STG11, uidstr);
		c = SYERR;
		goto reply;
	}
	if ((gr = Cgetgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
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
						 reqid, STAGEPUT, 0, 0, NULL, "");
#endif

	wqp = NULL;
	Coptind = 1;
	Copterr = 0;
	nowait_flag = 0;
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
					c = SYERR;
					goto reply;
				}
			} else {
				char **dummy = hsmfiles;
				struct stgcat_entry **dummy2 = hsmfilesstcp;
				if ((dummy = (char **) realloc(hsmfiles,(nhsmfiles+1) * sizeof(char *))) == NULL) {
					c = SYERR;
					goto reply;
				}
				hsmfiles = dummy;
				if ((dummy2 = (struct stgcat_entry **) realloc(hsmfilesstcp,(nhsmfiles+1) * sizeof(struct stgcat_entry *))) == NULL) {
					c = SYERR;
					goto reply;
				}
				hsmfilesstcp = dummy2;
			}
			hsmfiles[nhsmfiles++] = Coptarg;
			break;
		case 'q':       /* file sequence number(s) */
			if ((q = strchr (Coptarg, '-')) != NULL) {
				*q = '\0';
				n2 = strtol (q + 1, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				n1 = strtol (Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					errflg++;
				}
				*q = '-';
			} else {
				n1 = strtol (Coptarg, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
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
		c = USERR;
		goto reply;
	}

	if ((gr = Cgetgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
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
					sendrep (rpfd, MSG_ERR, STG59, hsmfiles[ihsmfiles]);
					c = USERR;
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
				sendrep (rpfd, MSG_ERR, STG22);
				c = USERR;
				goto reply;
			}
			if (stcp->status == STAGEOUT) {
				if (rfio_stat (stcp->ipath, &st) == 0) {
					stcp->actual_size = st.st_size;
				} else {
					stglogit (func, STG02, stcp->ipath, "rfio_stat", rfio_serror());
				}
				updfreespace (stcp->poolname, stcp->ipath,
					(signed64) (((signed64) stcp->size * (signed64) ONE_MB) - (signed64) stcp->actual_size));
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
						if ((stcp->u1.h.tppool[0] != '\0') && (tppool != NULL) && (strcmp(stcp->u1.h.tppool,tppool) != 0)) {
							sendrep (rpfd, MSG_ERR, STG122);
							c = USERR;
							goto reply;
						}
						if (stcp->u1.h.tppool[0] == '\0') {
							if ((ifileclass = upd_fileclass(NULL,stcp)) < 0) {
								sendrep (rpfd, MSG_ERR, STG132, stcp->u1.h.xfile, sstrerror(serrno));
								c = USERR;
								goto reply;
							}
							strcpy(stcp->u1.h.tppool,next_tppool(&(fileclasses[ifileclass])));
						}
						{
							struct stgcat_entry stcx = *stcp;
							stcx.uid = uid;
							stcx.gid = gid;
							/* stcx is a dummy req which inherits the uid/gid of the caller. */
							/* We use it at the first round to check that requestor's uid/gid is compatible */
							/* the uid/gid under which migration will effectively run might be different (case of stage:st) */
							if (euid_egid(&euid,&egid,tppool,NULL,stcp,(found == 1) ? &stcx : NULL,&tppool,0) != 0) {
								c = USERR;
								goto reply;
							}
						}
						if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
							c = USERR;
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
				if (strcmp (stcp->u1.d.xfile, externfile) != 0) continue;
				found = 1;
				break;
			}
		}
		if (Iflag) {
			if (found == 0 ||
					((stcp->status != STAGEOUT) &&
					 (stcp->status != (STAGEOUT|PUT_FAILED)))) {
				sendrep (rpfd, MSG_ERR, STG22);
				c = USERR;
				goto reply;
			}
			if (stcp->status == STAGEOUT) {
				if (rfio_stat (stcp->ipath, &st) == 0) {
					stcp->actual_size = st.st_size;
				} else {
					stglogit (func, STG02, stcp->ipath, "rfio_stat", rfio_serror());
				}
				updfreespace (stcp->poolname, stcp->ipath,
					(signed64) (((signed64) stcp->size * (signed64) ONE_MB) - (signed64) stcp->actual_size));
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
			}
			wfp->subreqid = stcp->reqid;
			wqp->nbdskf++;
			wqp->nb_subreqs++;
			wfp++;
		} else {
			if (found != nhsmfiles) {
				sendrep (rpfd, MSG_ERR, "STG02 - You requested %d hsm files while I found %d of them that you currently can migrate\n",nhsmfiles,found);
				c = USERR;
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
					hsmfilesstcp[ihsmfiles]->a_time = time(NULL);
					strcpy(hsmfilesstcp[ihsmfiles]->u1.h.tppool,tppool);
					if (update_migpool(hsmfilesstcp[ihsmfiles],1,had_put_failed ? 1 : 2) != 0) {
						c = USERR;
						goto reply;
					}
				} else {
					int save_status;

					/* We make upd_stageout believe that it is a normal stageput following a stageout */
					save_status = hsmfilesstcp[ihsmfiles]->status;
					hsmfilesstcp[ihsmfiles]->status = STAGEOUT;
					if ((c = upd_stageout (STAGEUPDC, hsmfilesstcp[ihsmfiles]->ipath, &subreqid, 0, NULL)) != 0) {
						if (c != CLEARED) {
							hsmfilesstcp[ihsmfiles]->status = save_status;
						} else {
							/* Entry has been deleted - user asks to put on tape zero-length file !? */
							c = USERR;
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
                      sendrep(rpfd, MSG_ERR, STG33, upath, "is not eligible for explicit migration");
                      c = USERR;
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
                      sendrep(rpfd, MSG_ERR, STG33, upath, "is not eligible for explicit migration");
                      c = USERR;
                      goto reply;
                    }
					/* If tppool specified in input very that it is compatible with eventual yet asisgned tppool */
					/* if tppool is NULL at first iteration, it will be set by the euid_egid() call */
					if ((found_stcp->u1.h.tppool[0] != '\0') && (tppool != NULL) && (strcmp(found_stcp->u1.h.tppool,tppool) != 0)) {
						sendrep (rpfd, MSG_ERR, STG122);
						c = USERR;
						goto reply;
					}
					if (found_stcp->u1.h.tppool[0] == '\0') {
						if ((ifileclass = upd_fileclass(NULL,found_stcp)) < 0) {
							sendrep (rpfd, MSG_ERR, STG132, found_stcp->u1.h.xfile, sstrerror(serrno));
							c = USERR;
							goto reply;
						}
						strcpy(found_stcp->u1.h.tppool,next_tppool(&(fileclasses[ifileclass])));
					}
					{
						struct stgcat_entry stcx = *found_stcp;
						stcx.uid = uid;
						stcx.gid = gid;
						/* stcx is a dummy req which inherits the uid/gid of the caller. */
						/* We use it at the first round to check that requestor's uid/gid is compatible */
						/* the uid/gid under which migration will effectively run might be different (case of stage:st) */
						if (euid_egid(&euid,&egid,tppool,NULL,found_stcp,(i == 0) ? &stcx : NULL,&tppool,0) != 0) {
							c = USERR;
							goto reply;
						}
					}
					if (verif_euid_egid(euid,egid,user_waitq,group_waitq) != 0) {
						c = USERR;
						goto reply;
					}
					break;
				}
                /* We will later remind the corresponding stcp */
				if (nhsmfiles == 0) {
					if ((hsmfilesstcp = (struct stgcat_entry **) malloc(sizeof(struct stgcat_entry *))) == NULL) {
						c = SYERR;
						goto reply;
					}
				} else {
					struct stgcat_entry **dummy2 = hsmfilesstcp;
					if ((dummy2 = (struct stgcat_entry **) realloc(hsmfilesstcp,(nhsmfiles+1) * sizeof(struct stgcat_entry *))) == NULL) {
						c = SYERR;
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
				sendrep (rpfd, MSG_ERR, STG33, upath, "unknown type");
				c = USERR;
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
			sendrep (rpfd, MSG_ERR, "STG02 - Mixing ('tape' or 'non-CASTOR') files with other types is not allowed\n");
			c = USERR;
			goto reply;
		}
		/* It is not allowed to mix 't' with 'h' type */
		if (ntapefiles > 0 && ncastorfiles > 0) {
			sendrep (rpfd, MSG_ERR, "STG02 - Mixing 'tape' files with 'CASTOR' files is not allowed\n");
			c = USERR;
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
					hsmfilesstcp[ihsmfiles]->a_time = time(NULL);
					strcpy(hsmfilesstcp[ihsmfiles]->u1.h.tppool,tppool);
					if (update_migpool(hsmfilesstcp[ihsmfiles],1,had_put_failed ? 1 : 2) != 0) {
						c = USERR;
						goto reply;
					}
				} else {
					/* We make upd_stageout believe that it is a normal stageput following a stageupdc following a stageout */
					/* This will force Cns_setfsize to be called. */
					save_status = hsmfilesstcp[ihsmfiles]->status;
					hsmfilesstcp[ihsmfiles]->status = STAGEOUT;
					if ((c = upd_stageout (STAGEUPDC, hsmfilesstcp[ihsmfiles]->ipath, &subreqid, 0, hsmfilesstcp[ihsmfiles])) != 0) {
						if (c != CLEARED) {
							hsmfilesstcp[ihsmfiles]->status = save_status;
						} else {
							/* Entry has been deleted - user asks to put on tape zero-length file !? */
							c = USERR;
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
				if ((c = upd_stageout (STAGEPUT, upath, &subreqid, 0, NULL)) != 0) {
					if (c == CLEARED) {
						/* Entry has been deleted - user asks to put on tape zero-length file !? */
						c = USERR;
					}
					goto reply;
				}
			} else {
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
	free (argv);
	fork_exec_stager (wqp);
	if (nowait_flag != 0) {
		sendrep (rpfd, STAGERC, req_type, c);
		close(rpfd);
		rpfd = -1;
	}
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmfilesstcp != NULL) free(hsmfilesstcp);
	return;
 reply:
#if SACCT
	stageacct (STGCMDC, uid, gid, clienthost,
						 reqid, STAGEPUT, 0, c, NULL, "");
#endif
	sendrep (rpfd, STAGERC, STAGEPUT, c);
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
					update_migpool(stcp,-1,0);
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
							update_migpool(stcp,-1,0);
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
	free (argv);
	if (hsmfiles != NULL) free(hsmfiles);
	if (hsmfilesstcp != NULL) free(hsmfilesstcp);
}

int
isstaged(cur, p, poolflag, poolname)
		 struct stgcat_entry *cur;
		 struct stgcat_entry **p;
		 int poolflag;
		 char *poolname;
{
	int found = 0;
	int i;
	struct stgcat_entry *stcp;
	struct stgcat_entry *stcp_castor_name = NULL;
	struct stgcat_entry *stcp_castor_invariants = NULL;

	last_tape_file = 0;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		/* if ((stcp->status & WAITING_REQ) == WAITING_REQ) continue; */
		if (cur->t_or_d != stcp->t_or_d) continue;
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
		if (cur->t_or_d == 't') {
			for (i = 0; i < MAXVSN; i++)
				if (strcmp (cur->u1.t.vid[i], stcp->u1.t.vid[i])) break;
			if (i < MAXVSN) continue;
			for (i = 0; i < MAXVSN; i++)
				if (strcmp (cur->u1.t.vsn[i], stcp->u1.t.vsn[i])) break;
			if (i < MAXVSN) continue;
			if (strcmp (cur->u1.t.lbl, stcp->u1.t.lbl)) continue;
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

			/* Remember if we found this HSM name */
			if (strcmp (cur->u1.h.xfile, stcp->u1.h.xfile) == 0) stcp_castor_name = stcp;

			/* Remember if we found this HSM invariant pair */
			if ((cur->u1.h.server[0] != '\0') && (cur->u1.h.fileid != 0) &&
				(strcmp (cur->u1.h.server, stcp->u1.h.server) == 0) &&
				(cur->u1.h.fileid == stcp->u1.h.fileid)) stcp_castor_invariants = stcp;

			if (stcp_castor_name != NULL) {
				if (stcp_castor_name == stcp_castor_invariants) {
					/* If we matched totally name + invariants, with same stcp, we got it */
					found = 1;
					break;
				} else if (stcp_castor_invariants != NULL) {
					/* If we matched totally name + invariants, with not same stcp, we neverthless get out */
					break;
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
	if (! found) {
		if (cur->t_or_d == 'h' && (stcp_castor_name != NULL || stcp_castor_invariants != NULL)) {
			if (stcp_castor_name != NULL) {
				int stcp_castor_invariants_reqid = ((stcp_castor_invariants != NULL) ?
														stcp_castor_invariants->reqid : 0);
				/* Matched name only - this stcp is always to be removed if possible */
				if (ISCASTORBEINGMIG(stcp_castor_name) || ISCASTORWAITINGMIG(stcp_castor_name)) {
					/* We cannot delete this file - it is busy */
					if (ISCASTORBEINGMIG(stcp_castor_name) && (stcp_castor_invariants == NULL) && ! ISCASTORWAITINGMIG(stcp_castor_name)) {
						sendrep(rpfd, MSG_ERR, "STG02 - Cannot delete obsolete (invariants differs) but busy file %s\n", stcp_castor_name->u1.h.xfile);
						return(ISSTAGEDBUSY);
					} else {
						stcp = stcp_castor_name;
						goto isstaged_return;
					}
				}
				if (delfile (stcp_castor_name, ((stcp_castor_name->status & 0xF0) == STAGED ||
												(stcp_castor_name->status & (STAGEOUT | PUT_FAILED)) == (STAGEOUT | PUT_FAILED)) ?
												0 : 1,
												 1, 1, "in catalog with wrong invariants", cur->uid, cur->gid, 0, 0) < 0) {
						sendrep (rpfd, MSG_ERR, STG02, stcp_castor_name->ipath, "rfio_unlink", rfio_serror());
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
						sendrep(rpfd, MSG_ERR, STG105, "isstaged", "Cannot recover stcp_castor_invariants");
						return(ISSTAGEDSYERR);
					}
				}
			}
			if (stcp_castor_invariants != NULL) {
				/* Matched invariants which is to be kept and renamed */
				sendrep(rpfd, MSG_ERR, STG101, cur->u1.h.xfile, stcp_castor_invariants->u1.h.xfile);
				strcpy(stcp_castor_invariants->u1.h.xfile,cur->u1.h.xfile);
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp_castor_invariants) != 0) {
					stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				savereqs ();
				stcp = stcp_castor_invariants;
				goto isstaged_return;
			}
			return (NOTSTAGED);
		}
		return (NOTSTAGED);
	} else {
	isstaged_return:
		*p = stcp;
		if ((stcp->status & 0xF0) == STAGED) {
			return (STAGED);
		} else {
			return (stcp->status);
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
			sendrep (rpfd, MSG_ERR, STG18);
			return (0);
		}
		*(fseq + strlen (fseq) - 1) = '\0';
	}
	switch (*fseq) {
	case 'n':
		if (req_type == STAGEIN) {
			sendrep (rpfd, MSG_ERR, STG17, "-qn", req_type == STAGEIN ? "stagein" : "stage_in");
			return (0);
		}
		++have_qn;
	case 'u':
		if (strlen (fseq) == 1) {
			nbtpf = 1;
		} else {
			nbtpf = strtol (fseq + 1, &dp, 10);
			if (*dp != '\0') {
				sendrep (rpfd, MSG_ERR, STG06, "-q");
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
				n2 = strtol (q + 1, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				n1 = strtol (p, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				*q = '-';
			} else {
				n1 = strtol (p, &dp, 10);
				if (*dp != '\0') {
					sendrep (rpfd, MSG_ERR, STG06, "-q");
					return (0);
				}
				n2 = n1;
			}
			if (n1 <= 0 || n2 < n1) {
				sendrep (rpfd, MSG_ERR, STG06, "-q");
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
				n2 = strtol (q + 1, &dp, 10);
				n1 = strtol (p, &dp, 10);
				*q = '-';
			} else {
				n1 = strtol (p, &dp, 10);
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
			sendrep (rpfd, MSG_ERR, "unpackfseq : Internal error : concat_off is ON and concat_off_fseq == NULL\n");
			return (0);
		}
		/* The fseq just before the '-' is the last element of the fseq_elem list */
		if ((*concat_off_fseq = lasttpf) <= 0) {
			sendrep (rpfd, MSG_ERR, "unpackfseq : Internal error : concat_off is ON and nbtpf > 0 and *trailing == '-' and atoi(fseq_list[nbtpf-1]) <= 0\n");
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
					sendrep (rpfd, MSG_ERR, "STG02 - calloc error (%s)\n", strerror(errno));
					free(*fseq_list);
					return(-1);
				}
				sendrep (rpfd, MSG_ERR, STG60, (char *)(*fseq_list + i));
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
	/* We check that user do not mix different hsm types (hpss and castor) */
	if (ISHPSS(arg)) {
		if (ISCASTORHOST(arg)) {
			sendrep (rpfd, MSG_ERR, STG102, "castor", "hpss", arg);
			return(USERR);
		}
		++(*nhpssfiles);
	}
	if (ISCASTOR(arg)) {
		if (ISHPSSHOST(arg)) {
			sendrep (rpfd, MSG_ERR, STG102, "hpss", "castor", arg);
			return(USERR);
		}
		++(*ncastorfiles);
		/* And we take the opportunity to decide which level of migration (user/exp) */
		if (isuserlevel(arg)) {
			(*nuserlevel)++;
		} else {
			(*nexplevel)++;
        }
	}
	/* No recognized type ? */
	if (((*nhpssfiles) == 0) && ((*ncastorfiles) == 0)) {
		sendrep (rpfd, MSG_ERR, "Cannot determine %s type (HPSS nor CASTOR)\n", arg);
		return(USERR);
	}
	/* Mixed HSM types ? */
	if (((*nhpssfiles) > 0) && ((*ncastorfiles) > 0)) {
		sendrep (rpfd, MSG_ERR, "HPSS and CASTOR files on the same command-line is not allowed\n");
		return(USERR);
	} else if (((*ncastorfiles) > 0)) {
		/* It is a CASTOR request, so stgreq.t_or_d is set to 'h' */
		if (t_or_d != NULL) *t_or_d = 'h';
	} else {
		/* It is a HPSS request, so stgreq.t_or_d is set to 'm' */
		if (t_or_d != NULL) *t_or_d = 'm';
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
			sendrep (rpfd, MSG_ERR, STG102, "castor", "hpss", arg);
			return(USERR);
		}
		++nhpssfiles;
	}
	if (ISCASTOR(arg)) {
		if (ISHPSSHOST(arg)) {
			sendrep (rpfd, MSG_ERR, STG102, "hpss", "castor", arg);
			return(USERR);
		}
		++ncastorfiles;
	}
	/* No recognized type ? */
	if ((nhpssfiles == 0) && (ncastorfiles == 0)) {
		sendrep (rpfd, MSG_ERR, "Cannot determine %s type (HPSS nor CASTOR)\n", arg);
		return(USERR);
	}
	/* Mixed HSM types ? */
	if ((nhpssfiles > 0) && (ncastorfiles > 0)) {
		sendrep (rpfd, MSG_ERR, "HPSS and CASTOR files on the same command-line is not allowed\n");
		return(USERR);
	} else if ((ncastorfiles > 0)) {
		/* It is a CASTOR request, so stgreq.t_or_d is set to 'h' */
		*t_or_d = 'h';
	} else {
		/* It is a HPSS request, so stgreq.t_or_d is set to 'm' */
		*t_or_d = 'm';
	}
	/* Ok */
	return(0);
}

int create_hsm_entry(rpfd,stcp,api_out,openmode,immediate_delete)
	int rpfd;
	struct stgcat_entry *stcp;
	int api_out;
	mode_t openmode;
	int immediate_delete;
{
	mode_t okmode;
	struct Cns_fileid Cnsfileid;
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */

	memset(&Cnsfileid,0,sizeof(struct Cns_fileid));
	setegid(stcp->gid);
	seteuid(stcp->uid);
	if (api_out == 0) {
		okmode = ( 0777 & ~ stcp->mask);
	} else {
		okmode = (07777 & (openmode & ~ stcp->mask));
	}
	if (Cns_creatx(stcp->u1.h.xfile, okmode, &Cnsfileid) != 0) {
		int save_serrno = serrno;
		setegid(start_passwd.pw_gid);
		seteuid(start_passwd.pw_uid);
		sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_creatx", sstrerror(serrno));
		if (immediate_delete != 0) {
		  if (delfile (stcp, 1, 1, 1, stcp->user, stcp->uid, stcp->gid, 0, 0) < 0) {
		    sendrep (rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_unlink", rfio_serror());
		  }
		}
		serrno = save_serrno;
		if ((serrno == SENOSHOST) || (serrno == SENOSSERV) || (serrno == SECOMERR) || (serrno == ENSNACT)) {
			return(SYERR);
		}
		return(USERR);
	}
	setegid(start_passwd.pw_gid);
	seteuid(start_passwd.pw_uid);
	strcpy(stcp->u1.h.server,Cnsfileid.server);
	stcp->u1.h.fileid = Cnsfileid.fileid;
	stcp->status &= ~WAITING_NS;
	/* The fileclass will be checked at next user call - in a stageupdc for example */
#ifdef USECDB
	if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
	stglogit (func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif
	savereqs();
	return(0);
}
