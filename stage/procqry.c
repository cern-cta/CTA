/*
 * $Id: procqry.c,v 1.87 2002/05/06 17:17:16 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procqry.c,v $ $Revision: 1.87 $ $Date: 2002/05/06 17:17:16 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

/* Enable this if you want stageqry to always run within the same process - usefull for debugging */
/* #define STAGEQRY_IN_MAIN */

/* Disable the update of the catalog in stageqry mode */
#ifdef USECDB
#undef USECDB
#endif

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <math.h>
#include <limits.h> /* For INT_MAX */
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
#include <regex.h>
#else
#define INIT       register char *sp = instring;
#define GETC()     (*sp++)
#define PEEKC()    (*sp)
#define UNGETC(c)  (--sp)
#define RETURN(c)  return(c)
#define ERROR(c)   return(0)
#include <regexp.h>
#endif
#include <string.h>
#include <netinet/in.h>
#include <time.h>
#include <sys/stat.h>
#ifndef _WIN32
#include <unistd.h>
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
#include "Cgrp.h"
#include "rfio_api.h"
#include "Cgetopt.h"
#include "Castor_limits.h"
#include "u64subr.h"

void procqryreq _PROTO((int, int, char *, char *));
void print_link_list _PROTO((char *, int, char *, int, char *, int, char (*)[7], char *, fseq_elem *, char *, char *, char *, int, int, int, int, int));
int print_sorted_list _PROTO((char *, int, char *, int, char *, int, char (*)[7], char *, fseq_elem *, char *, char *, char *, int, int, int, int, int, int));
void print_tape_info _PROTO((char *, int, char *, int, char *, int, char (*)[7], char *, fseq_elem *, int, int, int, int, int));
signed64 get_stageout_retenp _PROTO((struct stgcat_entry *));
signed64 get_put_failed_retenp _PROTO((struct stgcat_entry *));
int get_retenp _PROTO((struct stgcat_entry *, char *));
int get_mintime _PROTO((struct stgcat_entry *, char *));

extern int unpackfseq _PROTO((char *, int, char *, fseq_elem **, int, int *));
extern int req2argv _PROTO((char *, char ***));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int isvalidpool _PROTO((char *));
extern int stglogit _PROTO(());
extern char *stglogflags _PROTO((char *, char *, u_signed64));
extern void print_pool_utilization _PROTO((int, char *, char *, char *, char *, int, int, int, int));
extern int nextreqid _PROTO(());
extern int savereqs _PROTO(());
extern int cleanpool _PROTO((char *));
extern void delreq _PROTO((struct stgcat_entry *, int));
extern void sendinfo2cptape _PROTO((int, struct stgcat_entry *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern int retenp_on_disk _PROTO((int));
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *, int, int));
extern int mintime_beforemigr _PROTO((int));
extern int get_mintime _PROTO((struct stgcat_entry *, char *));
extern char *findpoolname _PROTO((char *));

#if !defined(linux)
extern char *sys_errlist[];
#endif
extern char defpoolname[];
extern char defpoolname_in[];
extern char defpoolname_out[];
extern char func[16];
extern int nbcat_ent;
extern int reqid;
extern int rpfd;
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgpath_entry *stpe;	/* end of stage path catalog */
extern struct stgpath_entry *stps;	/* start of stage path catalog */
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
static regmatch_t pmatch;
static regex_t preg;
#else
static char expbuf[256];
#endif
#ifdef USECDB
extern struct stgdb_fd dbfd;
struct stgdb_fd dbfd_in_fork;
struct stgdb_fd *dbfd_query;
#endif
extern u_signed64 stage_uniqueid;
extern struct fileclass *fileclasses;
extern int nbpool;
extern struct pool *pools;

static int nbtpf;
static int noregexp_flag;
static int reqid_flag;
static int dump_flag;
static int migrator_flag;
static int class_flag;
static int queue_flag;
static int counters_flag;
static int retenp_flag;
static int mintime_flag;
static int side_flag;
static int display_side_flag;

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

void procqryreq(req_type, magic, req_data, clienthost)
	int req_type;
	int magic;
	char *req_data;
	char *clienthost;
{
	char *afile = NULL;
	char **argv = NULL;
	int aflag = 0;
	int c, j;
	int errflg = 0;
	int fflag = 0;
	char *fseq = NULL;
	gid_t gid;
	char group[CA_MAXGRPNAMELEN + 1];
	struct group *gr;
	int hdrprinted = 0;
	int Lflag = 0;
	static char l_stat[5][12] = {"", "STAGED_LSZ", "STAGED_TPE", "", "CAN_BE_MIGR"};
	int lflag = 0;
	char *mfile = NULL;
	int nargs = 0;
	int numvid = 0;
	char *p;
	char p_lrecl[7];
	char p_recfm[6];
	char p_size[21];
	char p_stat[13];
	int Pflag = 0;
	int pid = -1;
	int poolflag = 0;
	char poolname[CA_MAXPOOLNAMELEN + 1];
	char *q;
	static char s_stat[5][9] = {"", "STAGEIN", "STAGEOUT", "STAGEWRT", "STAGEPUT"};
	char *rbp;
	int Sflag = 0;
	int sflag = 0;
	struct stat st;
	struct stgcat_entry *stcp;
	int Tflag = 0;
	static char title[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool\n";
	static char titleM[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Mintime\n";
	static char titleL[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration\n";
	static char titleLM[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration          Mintime\n";
	static char titleF[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Fileclass      Nameserver\n";
	static char titleFM[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Mintime            Fileclass      Nameserver\n";
	static char titleFL[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration          Fileclass      Nameserver\n";
	static char titleFLM[] = 
		"Vid      Fseq Lbl Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration          Mintime            Fileclass      Nameserver\n";
	static char title_A[] = 
		"File name                            State      Nbacc.     Size    Pool\n";
	static char title_AM[] = 
		"File name                            State      Nbacc.     Size    Pool           Mintime\n";
	static char title_AL[] = 
		"File name                            State      Nbacc.     Size    Pool           Expiration\n";
	static char title_ALM[] = 
		"File name                            State      Nbacc.     Size    Pool           Expiration          Mintime\n";
	static char title_AF[] = 
		"File name                            State      Nbacc.     Size    Pool           Fileclass      Nameserver\n";
	static char title_AFM[] = 
		"File name                            State      Nbacc.     Size    Pool           Mintime            Fileclass      Nameserver\n";
	static char title_AFL[] = 
		"File name                            State      Nbacc.     Size    Pool           Expiration          Fileclass      Nameserver\n";
	static char title_AFLM[] = 
		"File name                            State      Nbacc.     Size    Pool           Expiration          Mintime            Fileclass      Nameserver\n";
	static char title_I[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool\n";
	static char title_IM[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Mintime\n";
	static char title_IL[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration\n";
	static char title_ILM[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration          Mintime\n";
	static char title_IF[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Fileclass      Nameserver\n";
	static char title_IFM[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Mintime            Fileclass      Nameserver\n";
	static char title_IFL[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration          Fileclass      Nameserver\n";
	static char title_IFLM[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool           Expiration          Mintime            Fileclass      Nameserver\n";
	struct tm *tm;
	int uflag = 0;
	char *user;
	char vid[MAXVSN][7];
	static char x_stat[7][12] = {"", "WAITING_SPC", "WAITING_REQ", "STAGED","KILLED", "FAILED", "PUT_FAILED"};
	char *xfile = NULL;
	int xflag = 0;
	char trailing;
	fseq_elem *fseq_list = NULL;
	int inbtpf;
	u_signed64  flags = 0;
	char t_or_d = 0;
	int  nstcp_input, struct_status;
	struct stgcat_entry stcp_input;
	int this_reqid = -1;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
	int side = 0; /* The default */
	int have_parsed_side = 0;
	static struct Coptions longopts[] =
	{
		{"allocated",          REQUIRED_ARGUMENT,  NULL,      'A'},
		{"allgroups",          NO_ARGUMENT,        NULL,      'a'},
		{"full",               NO_ARGUMENT,        NULL,      'f'},
		{"grpuser",            NO_ARGUMENT,        NULL,      'G'},
		{"host",               REQUIRED_ARGUMENT,  NULL,      'h'},
		{"external_filename",  REQUIRED_ARGUMENT,  NULL,      'I'},
		{"link",               NO_ARGUMENT,        NULL,      'L'},
		{"long",               NO_ARGUMENT,        NULL,      'l'},
		{"migration_filename", REQUIRED_ARGUMENT,  NULL,      'M'},
		{"path",               NO_ARGUMENT,        NULL,      'P'},
		{"poolname",           REQUIRED_ARGUMENT,  NULL,      'p'},
		{"file_sequence",      REQUIRED_ARGUMENT,  NULL,      'q'},
		{"file_range",         REQUIRED_ARGUMENT,  NULL,      'Q'},
		{"sort",               NO_ARGUMENT,        NULL,      'S'},
		{"statistic",          NO_ARGUMENT,        NULL,      's'},
		{"tape_info",          NO_ARGUMENT,        NULL,      'T'},
		{"user",               NO_ARGUMENT,        NULL,      'u'},
		{"vid",                REQUIRED_ARGUMENT,  NULL,      'V'},
		{"extended",           NO_ARGUMENT,        NULL,      'x'},
		{"migration_rules",    NO_ARGUMENT,        NULL,      'X'},
		{"noregexp",           NO_ARGUMENT,  &noregexp_flag,    1},
		{"reqid",              REQUIRED_ARGUMENT,  &reqid_flag, 1},
		{"dump",               NO_ARGUMENT,  &dump_flag,        1},
		{"migrator",           NO_ARGUMENT,  &migrator_flag,    1},
		{"class",              NO_ARGUMENT,  &class_flag,       1},
		{"queue",              NO_ARGUMENT,  &queue_flag,       1},
		{"counters",           NO_ARGUMENT,  &counters_flag,    1},
		{"retenp",             NO_ARGUMENT,  &retenp_flag,      1},
		{"side",               REQUIRED_ARGUMENT, &side_flag,   1},
		{"display_side",       NO_ARGUMENT, &display_side_flag, 1},
		{"ds",                 NO_ARGUMENT, &display_side_flag, 1},
		{"mintime",            NO_ARGUMENT,  &mintime_flag,     1},
		{NULL,                 0,                  NULL,        0}
	};
	int thismintime_beforemigr = 0;
	int ifileclass = -1;
	time_t this_time = time(NULL);
	int save_rpfd;
	char timestr[64] ;    /* Time in its ASCII format             */
	char timestr2[64] ;   /* Time in its ASCII format             */
	char *dp;
	int api_out = 0;
	char tmpbuf[21];

	side_flag = 0;
	display_side_flag = 0;
	noregexp_flag = 0;
	reqid_flag = 0;
	dump_flag = 0;
	migrator_flag = 0;
	class_flag = 0;
	queue_flag = 0;
	counters_flag = 0;
	retenp_flag = 0;
	mintime_flag = 0;
	poolname[0] = '\0';
	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	if (req_type > STAGE_00) {
		api_out = 1;
		if (magic == STGMAGIC) {
			/* This is coming from the API */
			unmarshall_LONG (rbp, gid);
			unmarshall_HYPER (rbp, flags);
			unmarshall_BYTE (rbp, t_or_d);
			unmarshall_LONG(rbp, nstcp_input);
		} else if (magic >= STGMAGIC2) {
			/* Second and more version of the API (header) */
			unmarshall_LONG (rbp, gid);
			unmarshall_HYPER (rbp, flags);
			unmarshall_BYTE (rbp, t_or_d);
			unmarshall_LONG(rbp, nstcp_input);
			unmarshall_HYPER (rbp, stage_uniqueid);
		}
		stglogit (func, "STG92 - %s request by %s (,%d) from %s\n", "stage_qry", user, gid, clienthost);
    } else {
		unmarshall_WORD (rbp, gid);
		stglogit (func, "STG92 - %s request by %s (,%d) from %s\n", "stageqry", user, gid, clienthost);
		nargs = req2argv (rbp, &argv);
	}
#if SACCT
	stageacct (STGCMDR, -1, gid, clienthost,
			   reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif
	
	if ((gr = Cgetgrgid (gid)) == NULL) {
		if (errno != ENOENT) sendrep (rpfd, MSG_ERR, STG33, "Cgetgrgid", strerror(errno));
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = (api_out != 0) ? ESTGROUP : SESYSERR;
		goto reply;
	}
	strncpy (group, gr->gr_name, CA_MAXGRPNAMELEN);
	/* Makes sure null terminates */
	group[CA_MAXGRPNAMELEN] = '\0';
	
	if (req_type > STAGE_00) {
		/* This is coming from the API */
		if (nstcp_input != 1) {
			sendrep(rpfd, MSG_ERR, "STG02 - Invalid number of input structure (%d) - Should be 1\n", nstcp_input);
			c = EINVAL;
			goto reply;
		}
		memset((void *) &stcp_input, (int) 0, sizeof(struct stgcat_entry));
		{
			char logit[BUFSIZ + 1];

			struct_status = 0;
			stcp_input.reqid = -1;
			unmarshall_STAGE_CAT(magic,STGDAEMON_LEVEL,STAGE_INPUT_MODE, struct_status, rbp, &(stcp_input));
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
				stglogit("stage_qry","stcp[1/1] : %s\n",logit);
 			}
		}
		/* We mimic the getopt below */
		if (((flags & STAGE_REQID) == STAGE_REQID) && ((this_reqid = stcp_input.reqid) > 0)) {
			reqid_flag++;
		} else {
			this_reqid = 0;
        }
		if ((flags & STAGE_DUMP) == STAGE_DUMP) {
			dump_flag++;
		}
		if ((flags & STAGE_NOREGEXP) == STAGE_NOREGEXP) {
			noregexp_flag++;
		}
		if ((flags & STAGE_ALLOCED) == STAGE_ALLOCED) {
			if ((t_or_d != 'a') && (t_or_d == 'd')) {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_ALLOCED flag is valid only for t_or_d == 'a' or t_or_d == 'd'\n");
				c = EINVAL;
				goto reply;
			}
			/* t_or_d == 'a' is virtual and internally is equivalent to 'd' */
			t_or_d = 'd';
			if (stcp_input.u1.d.xfile[0] == '\0') {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_ALLOCED flag is valid only non-empty u1.d.xfile member\n");
				c = EINVAL;
				goto reply;
			}
			afile = stcp_input.u1.d.xfile;
			if (! noregexp_flag) {
				if (
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
					regcomp (&preg, afile, 0)
#else
					! compile (afile, expbuf, expbuf+sizeof(expbuf), '\0')
#endif
					)
				{
					sendrep (rpfd, MSG_ERR, STG06, "STAGE_ALLOCED");
					errflg++;
				}
			}
		}
		if ((flags & STAGE_ALL) == STAGE_ALL) {
			aflag++;
        }
		if ((flags & STAGE_FILENAME) == STAGE_FILENAME) {
			fflag++;
        }
		if ((flags & STAGE_EXTERNAL) == STAGE_EXTERNAL) {
			if ((t_or_d != 'a') && (t_or_d == 'd')) {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_EXTERNAL flag is valid only for t_or_d == 'a' or t_or_d == 'd'\n");
				c = EINVAL;
				goto reply;
			}
			/* t_or_d == 'a' is virtual and internally is equivalent to 'd' */
			t_or_d = 'd';
			if (stcp_input.u1.d.xfile[0] == '\0') {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_EXTERNAL flag is valid only non-empty u1.d.xfile member\n");
				c = EINVAL;
				goto reply;
			}
			xfile = stcp_input.u1.d.xfile;
		}
		if ((flags & STAGE_LINKNAME) == STAGE_LINKNAME) {
			Lflag++;
        }
		if ((flags & STAGE_LONG) == STAGE_LONG) {
			lflag++;
		}
		if ((t_or_d == 'm') || (t_or_d == 'h')) {
			/* This it is an HSM file, so implicit -M option */
			mfile = (t_or_d == 'm' ? stcp_input.u1.m.xfile : stcp_input.u1.h.xfile);
			if (! noregexp_flag) {
				if (
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
					regcomp (&preg, mfile, 0)
#else
					! compile (mfile, expbuf, expbuf+sizeof(expbuf), '\0')
#endif
					) {
					sendrep (rpfd, MSG_ERR, STG06, "HSM Filename not a valid regexp");
					errflg++;
				}
			}
		}
		if ((flags & STAGE_PATHNAME) == STAGE_PATHNAME) {
			Pflag++;
		}
		if (stcp_input.poolname[0] != '\0') {
			/* Implicit -p option */
			if (strcmp (stcp_input.poolname, "NOPOOL") == 0 ||
				isvalidpool (stcp_input.poolname)) {
				strcpy (poolname, stcp_input.poolname);
			} else {
				sendrep (rpfd, MSG_ERR, STG32, stcp_input.poolname);
				errflg++;
			}
		}
		if ((flags & STAGE_MULTIFSEQ) == STAGE_MULTIFSEQ) {
			if (t_or_d != 't') {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_MULTIFSEQ flag is valid only for t_or_d == 't'\n");
				c = EINVAL;
				goto reply;
			}
		}
        if ((t_or_d == 't') && stcp_input.u1.t.fseq[0] != '\0') {
			if ((flags & STAGE_MULTIFSEQ) == STAGE_MULTIFSEQ) {
				if ((nbtpf = unpackfseq (stcp_input.u1.t.fseq, STAGEQRY, &trailing, &fseq_list, 0, NULL)) == 0) {
					sendrep(rpfd, MSG_ERR, "STG02 - STAGE_MULTIFSEQ option value (u1.t.fseq) invalid\n");
					c = EINVAL;
					goto reply;
				}
			} else {
				fseq = stcp_input.u1.t.fseq;
			}
		}
		if ((flags & STAGE_SORTED) == STAGE_SORTED) {
			Sflag++;
		}
		if ((flags & STAGE_STATPOOL) == STAGE_STATPOOL) {
			sflag++;
		}
		if ((flags & STAGE_TAPEINFO) == STAGE_TAPEINFO) {
			Tflag++;
		}
		if ((flags & STAGE_USER) == STAGE_USER) {
			uflag++;
		}
		if ((t_or_d == 't') && stcp_input.u1.t.vid[0][0] != '\0') {
			int ivid;
			/* Implicit -V option */
			for (ivid = 0; ivid < MAXVSN; ivid++) {
				if (stcp_input.u1.t.vid[ivid][0] == '\0') break;
				strcpy(vid[numvid],stcp_input.u1.t.vid[ivid]);
				UPPER (vid[numvid]);
				numvid++;
			}
		}
		if ((flags & STAGE_EXTENDED) == STAGE_EXTENDED) {
			xflag++;
		}
		if ((flags & STAGE_MIGRULES) == STAGE_MIGRULES) {
			migrator_flag++;
		}
		if ((flags & STAGE_CLASS) == STAGE_CLASS) {
			class_flag++;
		}
		if ((flags & STAGE_QUEUE) == STAGE_QUEUE) {
			queue_flag++;
		}
		if ((flags & STAGE_COUNTERS) == STAGE_COUNTERS) {
			counters_flag++;
		}
		if ((flags & STAGE_RETENP) == STAGE_RETENP) {
			retenp_flag++;
		}
		if ((flags & STAGE_MINTIME) == STAGE_MINTIME) {
			mintime_flag++;
		}
		if ((flags & STAGE_DISPLAY_SIDE) == STAGE_DISPLAY_SIDE) {
			display_side_flag++;
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
		stglogflags("stage_qry",LOGFILE,(u_signed64) flags);
	} else {
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt_long (nargs, argv, "A:afGh:I:LlM:Pp:q:Q:SsTuV:x", longopts, NULL)) != -1) {
			switch (c) {
			case 'A':
				afile = Coptarg;
				break;
			case 'a':	/* all groups */
				aflag++;
				break;
			case 'f':	/* full external pathnames */
				fflag++;
				break;
			case 'G':
				break;
			case 'h':
				break;
			case 'I':
				xfile = Coptarg;
				break;
			case 'L':	/* print linkname */
				Lflag++;
				break;
			case 'l':	/* long format (more details) */
				lflag++;
				break;
			case 'M':
				mfile = Coptarg;
				break;
			case 'P':	/* print pathname */
				Pflag++;
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
				if ((nbtpf = unpackfseq (Coptarg, STAGEQRY, &trailing, &fseq_list, 0, NULL)) == 0)
					errflg++;
				break;
			case 'S':	/* sort requests for cleaner */
				Sflag++;
				break;
			case 's':	/* statistics on pool utilization */
				sflag++;
				break;
			case 'T':	/* print tape info */
				Tflag++;
				break;
			case 'u':	/* only requests belonging to user */
				uflag++;
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
			case 'x':	/* debug: reqid + internal pathname */
				xflag++;
				break;
			case 0:
				/* These are the long options */
				if ((reqid_flag != 0) && (this_reqid < 0)) {
					/* This condition happens only if it is exactly --reqid at this processing time */
					if ((this_reqid = atoi(Coptarg)) < 0) {
						this_reqid = 0;
					}
				}
				if ((side_flag != 0) && (! have_parsed_side)) {  /* Not yet done */
					stage_strtoi(&side, Coptarg, &dp, 10);
					if ((*dp != '\0') || (side < 0)) {
						sendrep (rpfd, MSG_ERR, STG06, "--side");
						errflg++;
					}
					have_parsed_side = 1;
				}
				break;
			default:
				errflg++;
				break;
			}
			if (errflg != 0) break;
		}
		/* Since the --noregexp flag can be action whenever, we compile the regular expression */
		/* after the Cgetopt_long() processing */
		if (! noregexp_flag) {
			if (afile) {
				if (
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
					regcomp (&preg, afile, 0)
#else
					! compile (afile, expbuf, expbuf+sizeof(expbuf), '\0')
#endif
					)
				{
					sendrep (rpfd, MSG_ERR, STG06, "-A");
					errflg++;
				}
			}
			if (mfile) {
				if (
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
					regcomp (&preg, mfile, 0)
#else
					! compile (mfile, expbuf, expbuf+sizeof(expbuf), '\0')
#endif
					) {
					sendrep (rpfd, MSG_ERR, STG06, "-M");
					errflg++;
				}
			}
		}
	}

	if ((fseq != NULL) && (fseq_list != NULL)) {
		sendrep (rpfd, MSG_ERR, STG35, "-q", "-Q");
		errflg++;
    }
	if (sflag && strcmp (poolname, "NOPOOL") == 0) {
		sendrep (rpfd, MSG_ERR, STG17, "-s", "-p NOPOOL");
		errflg++;
	}
	if (errflg != 0) {
		c = EINVAL;
		goto reply;
	}
	c = 0;
	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;
	if (! sflag) {
#ifndef STAGEQRY_IN_MAIN
		/* We run this procqry requests in a forked child */
		if ((pid = fork ()) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, "", "fork", sys_errlist[errno]);
			c = SESYSERR;
			goto reply;
		}
#endif
		if (pid > 0) {
			stglogit (func, "forking procqry, pid=%d\n", pid);
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
			if (afile || mfile)
				if (! noregexp_flag) regfree (&preg);
#endif
			if (argv != NULL) free (argv);
			close (rpfd);
			if (fseq_list != NULL) free(fseq_list);
			return;
		} else {
			/* We are in the child : we open a new connection to the Database Server so that   */
			/* it will not clash with current one owned by the main process.                   */

			rfio_mstat_reset();  /* Reset permanent RFIO stat connections */
			rfio_munlink_reset(); /* Reset permanent RFIO unlink connections */
#ifdef USECDB
			strcpy(dbfd_in_fork.username,dbfd.username);
			strcpy(dbfd_in_fork.password,dbfd.password);

			if (stgdb_login(&dbfd_in_fork) != 0) {
				stglogit(func, STG100, "login", sstrerror(serrno), __FILE__, __LINE__);
				stglogit(func, "Error loging to database server (%s)\n",sstrerror(serrno));
				exit(SYERR);
			}

			/* Open the database */
			if (stgdb_open(&dbfd_in_fork,"stage") != 0) {
				stglogit(func, STG100, "open", sstrerror(serrno), __FILE__, __LINE__);
				stglogit(func, "Error opening \"stage\" database (%s)\n",sstrerror(serrno));
				exit(SYERR);
			}

			/* There is no need to ask for a dump of the catalog : we use the one that is */
			/* already in memory.                                                         */

			/* We set the pointer to use to the correct dbfd structure */
			dbfd_query = &dbfd_in_fork;
#endif
		}
	}

	if (Lflag) {
		print_link_list (poolname, aflag, group, uflag, user,
						 numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid, magic,
						 side_flag, side
			);
		goto reply;
	}
	if (sflag) {
		print_pool_utilization (rpfd, poolname, defpoolname, defpoolname_in, defpoolname_out, migrator_flag, class_flag, queue_flag, counters_flag);
		goto reply;
	}
	if (Sflag) {
		if (print_sorted_list (poolname, aflag, group, uflag, user,
							   numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid, magic, class_flag,
							   side_flag, side
			) < 0)
			c = SESYSERR;
		goto reply;
	}
	if (Tflag) {
		print_tape_info (poolname, aflag, group, uflag, user,
						 numvid, vid, fseq, fseq_list, req_type, this_reqid, magic, 
						 side_flag, side
			);
		goto reply;
	}
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if ((this_reqid > 0) && (stcp->reqid != this_reqid)) continue;
		if ((stcp->status & 0xF0) == WAITING_REQ) continue;
		if (poolflag < 0) {	/* -p NOPOOL */
			if (stcp->poolname[0]) continue;
		} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
		if (!aflag && strcmp (group, stcp->group)) continue;
		if (uflag && strcmp (user, stcp->user)) continue;
		if (side_flag) {
			if (stcp->t_or_d != 't') continue;
			if (stcp->u1.t.side != side) continue;
		}
		if (numvid) {
			if (stcp->t_or_d != 't') continue;
			for (j = 0; j < numvid; j++)
				if (strcmp (stcp->u1.t.vid[0], vid[j]) == 0) break;
			if (j == numvid) continue;
		}
		if (fseq) {
			if (stcp->t_or_d != 't') continue;
			if (strcmp (fseq, stcp->u1.t.fseq)) continue;
		}
		if (fseq_list) {
			for (inbtpf = 0; inbtpf < nbtpf; inbtpf++)
				if (strcmp (stcp->u1.t.fseq, fseq_list[inbtpf]) == 0) break;
			if (inbtpf == nbtpf) continue;
		}
		if (xfile) {
			if (stcp->t_or_d != 'd') continue;
			if (strcmp (xfile, stcp->u1.d.xfile)) continue;
		}
		if (afile) {
			if (stcp->t_or_d != 'a') continue;
			if (! noregexp_flag) {
				if ((p = strrchr (stcp->u1.d.xfile, '/')) == NULL)
					p = stcp->u1.d.xfile;
				else
					p++;
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
				if (regexec (&preg, p, 1, &pmatch, 0)) continue;
#else
				if (step (p, expbuf) == 0) continue;
#endif
			} else {
				if (strcmp(stcp->u1.d.xfile, afile) != 0) continue;
			}
		}
		if (mfile) {
			if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') continue;
			p = stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile;
			if (! noregexp_flag) {
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
				if (regexec (&preg, p, 1, &pmatch, 0)) continue;
#else
				if (step (p, expbuf) == 0) continue;
#endif
			} else {
				if (strcmp(p, mfile) != 0) continue;
			}
		}
		if (Pflag) {
			sendrep (rpfd, MSG_OUT, "%s\n", stcp->ipath);
			if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp, magic);
			if (dump_flag != 0) dump_stcp(rpfd, stcp, &sendrep);
			continue;
		}
		if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp, magic);
		if (hdrprinted++ == 0) {
			if (xfile)
				if (class_flag != 0)
					if (retenp_flag != 0)
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_IFLM);
						else
							sendrep (rpfd, MSG_OUT, title_IFL);
					else
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_IFM);
						else
							sendrep (rpfd, MSG_OUT, title_IF);
				else
					if (retenp_flag != 0)
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_ILM);
						else
							sendrep (rpfd, MSG_OUT, title_IL);
					else
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_IM);
						else
							sendrep (rpfd, MSG_OUT, title_I);
			else if (afile || mfile)
				if (class_flag != 0)
					if (retenp_flag != 0)
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_AFLM);
						else
							sendrep (rpfd, MSG_OUT, title_AFL);
					else
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_AFM);
						else
							sendrep (rpfd, MSG_OUT, title_AF);
				else
					if (retenp_flag != 0)
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_ALM);
						else
							sendrep (rpfd, MSG_OUT, title_AL);
					else
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, title_AM);
						else
							sendrep (rpfd, MSG_OUT, title_A);
			else
				if (class_flag != 0)
					if (retenp_flag != 0)
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, titleFLM);
						else
							sendrep (rpfd, MSG_OUT, titleFL);
					else
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, titleFM);
						else
							sendrep (rpfd, MSG_OUT, titleF);
				else
					if (retenp_flag != 0)
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, titleLM);
						else
							sendrep (rpfd, MSG_OUT, titleL);
					else
						if (mintime_flag != 0)
							sendrep (rpfd, MSG_OUT, titleM);
						else
							sendrep (rpfd, MSG_OUT, title);
		}
		if ((stcp->t_or_d == 'h') && (! ISWAITING(stcp))) {
			if ((ifileclass = upd_fileclass(NULL,stcp,0,1)) >= 0) {
				if ((thismintime_beforemigr = stcp->u1.h.mintime_beforemigr) < 0) {
					thismintime_beforemigr = mintime_beforemigr(ifileclass);
				}
			}
		}
		if (strcmp (stcp->recfm, "U,b") == 0)
			strcpy (p_recfm, "U,bin");
		else if (strcmp (stcp->recfm, "U,f") == 0)
			strcpy (p_recfm, "U,f77");
		else
			strcpy (p_recfm, stcp->recfm);
		if (stcp->lrecl > 0)
			sprintf (p_lrecl, "%6d", stcp->lrecl);
		else
			strcpy (p_lrecl, "     *");
		if (stcp->size > 0)
			if (ISCASTORMIG(stcp))
				strcpy (p_size, "*");
			else if ((stcp->status == STAGEWRT) && (stcp->t_or_d == 'h'))
				/* a STAGEWRT stcp that is doing migration of another STAGEOUT|CAN_BE_MIGR|BEING_MIGR stcp */
				strcpy (p_size, "*");
			else
				if ((stcp->size / ONE_MB) <= (u_signed64) INT_MAX)
					sprintf (p_size, "%d", stcp->size / ONE_MB); /* Compatibility with old stageqry output */
				else
					sprintf (p_size, "%s", u64tostru(stcp->size,tmpbuf,0)); /* Cannot represent this quantity with an integer - switch to u64 mode */
		else if (stcp->status == (STAGEIN|STAGED))
			/* Case of stageing of a zero-length file */
			strcpy (p_size, "0");
		else
			strcpy (p_size, "*");
		if (stcp->status & 0xF0)
			if (((stcp->status & STAGED_LSZ) == STAGED_LSZ) ||
				((stcp->status & STAGED_TPE) == STAGED_TPE))
				strcpy (p_stat, l_stat[(stcp->status & 0xF00) >> 8]);
			else
				strcpy (p_stat, x_stat[(stcp->status & 0xF0) >> 4]);
		else if (stcp->status & 0xF00)
			if (ISCASTORBEINGMIG(stcp))
				strcpy( p_stat, "BEING_MIGR");
			else if (ISCASTORWAITINGMIG(stcp))
				strcpy( p_stat, "WAITING_MIGR");
		/* Please note that stcp->a_time + thismintime_beforemigr can go out of bounds */
		/* That's why I typecase everything to u_signed64 */
			else if ((ifileclass >= 0) && ((u_signed64) ((u_signed64) stcp->a_time + (u_signed64) thismintime_beforemigr) > (u_signed64) this_time))
				strcpy (p_stat, "DELAY_MIGR");
			else
				strcpy (p_stat, l_stat[(stcp->status & 0xF00) >> 8]);
		else if (stcp->status == STAGEALLOC)
			strcpy (p_stat, "STAGEALLOC");
		else if (ISCASTORWAITINGNS(stcp))
			strcpy( p_stat, "WAITING_NS");
		else
			strcpy (p_stat, s_stat[stcp->status]);
        /* STAGEIN                    1                        001 */
        /* STAGEOUT                   2                        010 */
        /* STAGEWRT                   3                        011 */
        /* STAGEPUT                   4                        100 */

        /* CAN_BE_MIGR                                  0x000400   */
        /* BEING_MIGR                                   0x002000   */
        /* WAITING_MIGR                                 0x020000   */
        /* WAITING_NS                                   0x040000   */
        /* STAGE_RDONLY                                 0x100000   */
		if ((lflag || ((stcp->status & 0xFFFFF0) == 0)) &&
			(! (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp))) &&
			(stcp->ipath[0] != '\0')) {
			PRE_RFIO;
			if (RFIO_STAT(stcp->ipath, &st) == 0) {
				int has_been_updated = 0;

				if (st.st_size > stcp->actual_size) {
					stcp->actual_size = st.st_size;
					has_been_updated = 1;
				}
				if (st.st_atime > stcp->a_time) {
					stcp->a_time = st.st_atime;
					has_been_updated = 1;
				}
				if (st.st_mtime > stcp->a_time) {
					stcp->a_time = st.st_mtime;
					has_been_updated = 1;
				}
				if (has_been_updated != 0) {
#ifdef USECDB
					if (stgdb_upd_stgcat(dbfd_query,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				}
			} else {
				stglogit (func, STG02, stcp->ipath, RFIO_STAT_FUNC(stcp->ipath), rfio_serror());
			}
		}
		if (stcp->t_or_d == 't') {
			char vid_and_side[CA_MAXVIDLEN+1+10+1]; /* VID[/%d].%d */

			if ((stcp->u1.t.side > 0) && (display_side_flag)) {
#if (defined(__osf__) && defined(__alpha))
				sprintf(vid_and_side, "%s/%d", stcp->u1.t.vid[0], stcp->u1.t.side);
#else
				snprintf(vid_and_side, CA_MAXVIDLEN+1+10+1, "%s/%d", stcp->u1.t.vid[0], stcp->u1.t.side);
#endif
			} else {
				strcpy(vid_and_side, stcp->u1.t.vid[0]);
			}
            vid_and_side[CA_MAXVIDLEN+1+10] = '\0';
			if (xflag) {
				if (retenp_flag != 0) {
					if (mintime_flag != 0) {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %-18s %6d %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %-18s %6d %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname,
									 get_retenp(stcp,timestr) == 0 ? timestr : "",
									 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
									 stcp->reqid, stcp->ipath)
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %6d %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %6d %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname,
									 get_retenp(stcp,timestr) == 0 ? timestr : "",
									 stcp->reqid, stcp->ipath)
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					}
				} else {
					if (mintime_flag != 0) {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-18s %6d %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-18s %6d %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname,
									 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
									 stcp->reqid, stcp->ipath)
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %6d %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %6d %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname,
									 stcp->reqid, stcp->ipath)
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					}
				}					
			} else {
				if (retenp_flag != 0) {
					if (mintime_flag != 0) {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname,
									 get_retenp(stcp,timestr) == 0 ? timestr : "",
									 get_mintime(stcp,timestr2) == 0 ? timestr2 : "")
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname, get_retenp(stcp,timestr) == 0 ? timestr : "")
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					}
				} else {
					if (mintime_flag != 0) {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname,
									 get_mintime(stcp,timestr2) == 0 ? timestr2 : "")
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if (sendrep (rpfd, MSG_OUT,
									 (display_side_flag) ?
									 "%-8s %4s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %s\n" :
									 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %s\n",
									 vid_and_side, stcp->u1.t.fseq, stcp->u1.t.lbl,
									 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									 (float)(stcp->actual_size)/(1024.*1024.), p_size,
									 stcp->poolname)
							< 0) {
							c = SESYSERR;
							goto reply;
						}
					}
				}
			}
		} else if ((stcp->t_or_d == 'd') || (stcp->t_or_d == 'a')) {
			if ((q = strrchr (stcp->u1.d.xfile, '/')) == NULL)
				q = stcp->u1.d.xfile;
			else
				q++;
			if (xflag) {
				if (retenp_flag != 0) {
					if (mintime_flag != 0) {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %-18s %6d %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "",
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
									  stcp->reqid, stcp->ipath) < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %-18s %6d %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "",
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
									  stcp->reqid, stcp->ipath)
							 < 0)) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %6d %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "",
									  stcp->reqid, stcp->ipath) < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %6d %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "",
									  stcp->reqid, stcp->ipath)
							 < 0)) {
							c = SESYSERR;
							goto reply;
						}
					}
				} else {
					if (mintime_flag != 0) {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %-18s %6d %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
									  stcp->reqid, stcp->ipath) < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %-14s %-18s %6d %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
									  stcp->reqid, stcp->ipath)
							 < 0)) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %6d %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  stcp->reqid, stcp->ipath) < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %-14s %6d %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  stcp->reqid, stcp->ipath)
							 < 0)) {
							c = SESYSERR;
							goto reply;
						}
					}
				}
			} else {
				if (retenp_flag != 0) {
					if (mintime_flag != 0) {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %-19s %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "",
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "") < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "",
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "")
							 < 0)) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "") < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %-14s %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_retenp(stcp,timestr) == 0 ? timestr : "")
							 < 0)) {
							c = SESYSERR;
							goto reply;
						}
					}
				} else {
					if (mintime_flag != 0) {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname,
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "") < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %-18s %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
									  stcp->poolname) < 0)) {
							c = SESYSERR;
							goto reply;
						}
					} else {
						if ((stcp->t_or_d == 'd' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %s\n", q,
									  stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname) < 0) ||
							(stcp->t_or_d == 'a' &&
							 sendrep (rpfd, MSG_OUT,
									  "%-36s %-11s %5d %6.1f/%-4s %s\n", q,
									  p_stat, stcp->nbaccesses,
									  (float)(stcp->actual_size)/(1024.*1024.), p_size,
									  stcp->poolname) < 0)) {
							c = SESYSERR;
							goto reply;
						}
					}
				}
			}
		} else {	/* stcp->t_or_d == 'm' or stcp->t_or_d == 'h'*/
			if ((q = strrchr (stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile, '/')) == NULL)
				q = stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile;
			else
				q++;
			save_rpfd = rpfd;
			rpfd = -1;             /* To make sure nothing does on the terminal here */
			if (! ((class_flag != 0) && (stcp->t_or_d == 'h'))) ifileclass = -1;
			rpfd = save_rpfd;
			if (xflag) {
				if (ifileclass >= 0) {
					if (retenp_flag != 0) {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %-18s %*s%*s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "",
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 -CA_MAXHOSTNAMELEN,
										 fileclasses[ifileclass].server,
										 stcp->reqid, stcp->ipath
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %*s%*s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "",
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 -CA_MAXHOSTNAMELEN,
										 fileclasses[ifileclass].server,
										 stcp->reqid, stcp->ipath
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					} else {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-18s %*s%*s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 -CA_MAXHOSTNAMELEN,
										 fileclasses[ifileclass].server,
										 stcp->reqid, stcp->ipath
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %*s%*s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 -CA_MAXHOSTNAMELEN,
										 fileclasses[ifileclass].server,
										 stcp->reqid, stcp->ipath
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					}
				} else {
					if (retenp_flag != 0) {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %-18s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "",
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
										 stcp->reqid, stcp->ipath) < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "",
										 stcp->reqid, stcp->ipath) < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					} else {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-18s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
										 stcp->reqid, stcp->ipath) < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %6d %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname, stcp->reqid, stcp->ipath) < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					}
				}
			} else {
				if (ifileclass >= 0) {
					if (retenp_flag != 0) {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %-18s %*s%s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "",
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 fileclasses[ifileclass].server
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %*s%s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "",
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 fileclasses[ifileclass].server
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					} else {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-18s %*s%s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "",
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 fileclasses[ifileclass].server
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %*s%s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 -CA_MAXCLASNAMELEN,
										 fileclasses[ifileclass].Cnsfileclass.name,
										 fileclasses[ifileclass].server
								) < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					}
				} else {
					if (retenp_flag != 0) {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %-19s %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "",
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "") < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_retenp(stcp,timestr) == 0 ? timestr : "") < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					} else {
						if (mintime_flag != 0) {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %-14s %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname,
										 get_mintime(stcp,timestr2) == 0 ? timestr2 : "") < 0) {
								c = SESYSERR;
								goto reply;
							}
						} else {
							if (sendrep (rpfd, MSG_OUT,
										 "%-36s %-11s %5d %6.1f/%-4s %s\n", q,
										 p_stat, stcp->nbaccesses,
										 (float)(stcp->actual_size)/(1024.*1024.), p_size,
										 stcp->poolname) < 0) {
								c = SESYSERR;
								goto reply;
							}
						}
					}
				}
			}
		}
		if (fflag) {
			if ((stcp->t_or_d == 'a') || (stcp->t_or_d == 'd')) {
				if (sendrep (rpfd, MSG_OUT, " %s\n",
							 stcp->u1.d.xfile) < 0) {
					c = SESYSERR;
					goto reply;
				}
			} else if (stcp->t_or_d == 'm') {
				if (sendrep (rpfd, MSG_OUT, " %s\n",
							 stcp->u1.m.xfile) < 0) {
					c = SESYSERR;
					goto reply;
				}
			} else if (stcp->t_or_d == 'h') {
				if (sendrep (rpfd, MSG_OUT, " %s\n",
							 stcp->u1.h.xfile) < 0) {
					c = SESYSERR;
					goto reply;
				}
			}
		}
		if (lflag) {
			tm = localtime (&stcp->c_time);
			if (sendrep (rpfd, MSG_OUT,
						 "\t\t\tcreated by  %-8.8s  %s  %04d/%02d/%02d %02d:%02d:%02d\n",
						 stcp->user, stcp->group,
						 tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
						 tm->tm_hour, tm->tm_min, tm->tm_sec) < 0) {
				c = SESYSERR;
				goto reply;
			}
			tm = localtime (&stcp->a_time);
			if (sendrep (rpfd, MSG_OUT,
						 "\t\t\tlast access               %04d/%02d/%02d %02d:%02d:%02d\n",
						 tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
						 tm->tm_hour, tm->tm_min, tm->tm_sec) < 0) {
				c = SESYSERR;
				goto reply;
			}
		}
		if (dump_flag != 0) dump_stcp(rpfd, stcp, &sendrep);
	}
  reply:
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	if (afile || mfile)
		if (! noregexp_flag) regfree (&preg);
#endif
	if (fseq_list != NULL) free(fseq_list);
	if (argv != NULL) free (argv);
	sendrep (rpfd, STAGERC, STAGEQRY, magic, c);
	if (pid == 0) {	/* we are in the child */
		rfio_end();
#ifdef USECDB
		if (stgdb_close(dbfd_query) != 0) {
			stglogit(func, STG100, "close", sstrerror(serrno), __FILE__, __LINE__);
		}
		if (stgdb_logout(dbfd_query) != 0) {
			stglogit(func, STG100, "logout", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		exit (c ? SYERR : 0);
	}
}

void print_link_list(poolname, aflag, group, uflag, user, numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid, magic, side_flag, side)
	char *poolname;
	int aflag;
	char *group;
	int uflag;
	char *user;
	int numvid;
	char vid[MAXVSN][7];
	char *fseq;
	fseq_elem *fseq_list;
	char *xfile;
	char *afile;
	char *mfile;		
	int req_type;
	int this_reqid;
	int magic;
	int side_flag;
	int side;
{
	int j;
	char *p;
	int poolflag = 0;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	int inbtpf;
	int loop_on_stcp = 0;

	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;

	/* We check if we have to loop on stcp */
	if (poolflag < 0) {	/* -p NOPOOL */
		++loop_on_stcp;
	} else if (*poolname) { /* -p <somepool> */
		++loop_on_stcp;
	}

	if (numvid == 0 && fseq == NULL && fseq_list == NULL && afile == NULL && mfile == NULL && xfile == NULL && loop_on_stcp == 0) {
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if ((this_reqid > 0) && (stpp->reqid != this_reqid)) continue;
			sendrep (rpfd, MSG_OUT, "%s\n", stpp->upath);
			if (req_type > STAGE_00) sendrep(rpfd, API_STPP_OUT, stpp);
			if (dump_flag != 0) dump_stpp(rpfd, stpp, &sendrep);
		}
		return;
	}

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if ((this_reqid > 0) && (stcp->reqid != this_reqid)) continue;
		if ((stcp->status & 0xF0) != STAGED) continue;
		if (poolflag < 0) {	/* -p NOPOOL */
			if (stcp->poolname[0]) continue;
		} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
		if (!aflag && strcmp (group, stcp->group)) continue;
		if (uflag && strcmp (user, stcp->user)) continue;
		if (side_flag) {
			if (stcp->t_or_d != 't') continue;
			if (side != stcp->u1.t.side) continue;
		}
		if (numvid) {
			if (stcp->t_or_d != 't') continue;
			for (j = 0; j < numvid; j++)
				if (strcmp (stcp->u1.t.vid[0], vid[j]) == 0) break;
			if (j == numvid) continue;
		}
		if (fseq) {
			if (stcp->t_or_d != 't') continue;
			if (strcmp (fseq, stcp->u1.t.fseq)) continue;
		}
		if (fseq_list) {
			for (inbtpf = 0; inbtpf < nbtpf; inbtpf++)
				if (strcmp (stcp->u1.t.fseq, fseq_list[inbtpf]) == 0) break;
			if (inbtpf == nbtpf) continue;
		}
		if (xfile) {
			if (stcp->t_or_d != 'd') continue;
			if (strcmp (xfile, stcp->u1.d.xfile)) continue;
		}
		if (afile) {
			if (stcp->t_or_d != 'a') continue;
			if (! noregexp_flag) {
				if ((p = strrchr (stcp->u1.d.xfile, '/')) == NULL)
					p = stcp->u1.d.xfile;
				else
					p++;
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
				if (regexec (&preg, p, 1, &pmatch, 0)) continue;
#else
				if (step (p, expbuf) == 0) continue;
#endif
			} else {
				if (strcmp(stcp->u1.d.xfile, afile) != 0) continue;
			}
		}
		if (mfile) {
			if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') continue;
			p = stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile;
			if (! noregexp_flag) {
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
				if (regexec (&preg, p, 1, &pmatch, 0)) continue;
#else
				if (step (p, expbuf) == 0) continue;
#endif
			} else {
				if (strcmp(p, mfile) != 0) continue;
			}
		}
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if (stcp->reqid == stpp->reqid) {
				sendrep (rpfd, MSG_OUT, "%s\n", stpp->upath);
				if (req_type > STAGE_00) sendrep(rpfd, API_STPP_OUT, stpp);
				if (dump_flag != 0) dump_stpp(rpfd, stpp, &sendrep);
			}
		}
	}
}

int print_sorted_list(poolname, aflag, group, uflag, user, numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid, magic, class_flag, side_flag, side)
	char *poolname;
	int aflag;
	char *group;
	int uflag;
	char *user;
	int numvid;
	char vid[MAXVSN][7];
	char *fseq;
	fseq_elem *fseq_list;
	char *xfile;
	char *afile;
	char *mfile;
	int req_type;
	int this_reqid;
	int magic;
	int class_flag;
	int side_flag;
	int side;
{
	/* We use the weight algorithm defined by Fabrizio Cane for DPM */
	time_t thistime = time(NULL);
	int found;
	int j;
	char *p;
	int poolflag = 0;
	struct sorted_ent *prev, *scc, *sci, *scf, *scs;
	struct stat st;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	struct tm *tm;
	int inbtpf;
	time_t this_time = time(NULL);
	int ifileclass;
	int save_rpfd;

	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;

	/* build a sorted list of stage catalog entries for the specified pool */
	scf = NULL;
	scs = (struct sorted_ent *) calloc (nbcat_ent, sizeof(struct sorted_ent));
	sci = scs;
	for (stcp = stcs; stcp < stce; stcp++) {
		int isstageout_exhausted;
		int isput_failed_exhausted;
		int isother_ok;
		signed64 put_failed_retenp;
		signed64 stageout_retenp;

		if (stcp->reqid == 0) break;
		/* Did user asked for a specific reqid ? */
		if ((this_reqid > 0) && (stcp->reqid != this_reqid)) continue;
		/* STAGE_RDONLY is a temporarly status meaning that something else is happening on this file */
		if (stcp->status == (STAGEIN|STAGED|STAGE_RDONLY)) continue;
		/* ISWAITING says that we should not touch this file, waiting for migration request */
        if (ISWAITING(stcp)) continue;
		/* We are interested in: */
		/* STAGED entries */
		/* STAGEOUT entries that have expired retention period on disk */
		/* PUT_FAILED entries that have expired retention period on disk */
		isstageout_exhausted   = (   ISSTAGEOUT  (stcp)   &&     /* A STAGEOUT file */
									 (!  ISCASTORMIG (stcp) ) &&     /* Not candidate for migration */
									 (!  ISSTAGED    (stcp) ) &&     /* And not yet STAGED */
									 (!  ISPUT_FAILED(stcp) ) &&     /* And not yet subject to a failed transfer */
									 (  (stageout_retenp = get_stageout_retenp(stcp)) >= 0) && /* And with a retention period */
									 (  (thistime - stcp->a_time) > stageout_retenp)); /* And with a exhausted retention period */
		isput_failed_exhausted = (   ISSTAGEOUT  (stcp)   &&     /* A STAGEOUT file */
								     ISPUT_FAILED(stcp)   &&     /* Subject to a failed transfer */
									 (  (put_failed_retenp = get_put_failed_retenp(stcp)) >= 0) && /* And with a ret. period */
									 (  (thistime - stcp->a_time) > put_failed_retenp)); /* That got exhausted */
		isother_ok = ISSTAGED(stcp);
		if (! (isstageout_exhausted || isput_failed_exhausted || isother_ok)) continue;
		if (poolflag < 0) {	/* -p NOPOOL */
			if (stcp->poolname[0]) continue;
		} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
		if (!aflag && strcmp (group, stcp->group)) continue;
		if (uflag && strcmp (user, stcp->user)) continue;
		if (side_flag) {
			if (stcp->t_or_d != 't') continue;
			if (side != stcp->u1.t.side) continue;
		}
		if (numvid) {
			if (stcp->t_or_d != 't') continue;
			for (j = 0; j < numvid; j++)
				if (strcmp (stcp->u1.t.vid[0], vid[j]) == 0) break;
			if (j == numvid) continue;
		}
		if (fseq) {
			if (stcp->t_or_d != 't') continue;
			if (strcmp (fseq, stcp->u1.t.fseq)) continue;
		}
		if (fseq_list) {
			for (inbtpf = 0; inbtpf < nbtpf; inbtpf++)
				if (strcmp (stcp->u1.t.fseq, fseq_list[inbtpf]) == 0) break;
			if (inbtpf == nbtpf) continue;
		}
		if (xfile) {
			if (stcp->t_or_d != 'd') continue;
			if (strcmp (xfile, stcp->u1.d.xfile)) continue;
		}
		if (afile) {
			if (stcp->t_or_d != 'a') continue;
			if (! noregexp_flag) {
				if ((p = strrchr (stcp->u1.d.xfile, '/')) == NULL)
					p = stcp->u1.d.xfile;
				else
					p++;
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
				if (regexec (&preg, p, 1, &pmatch, 0)) continue;
#else
				if (step (p, expbuf) == 0) continue;
#endif
			} else {
				if (strcmp(stcp->u1.d.xfile, afile) != 0) continue;
			}
		}
		if (mfile) {
			if (stcp->t_or_d != 'm' && stcp->t_or_d != 'h') continue;
			p = stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile;
			if (! noregexp_flag) {
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
				if (regexec (&preg, p, 1, &pmatch, 0)) continue;
#else
				if (step (p, expbuf) == 0) continue;
#endif
			} else {
				if (strcmp(p, mfile) != 0) continue;
			}
		}
		if (stcp->t_or_d == 'h') {
			/* We explicitely exclude all CASTOR HSM files that have enough retention period on disk */
			int thisretenp;

			save_rpfd = rpfd;
			rpfd = -1;             /* To make sure nothing does on the terminal here */
			if (ISWAITING(stcp) || ((ifileclass = upd_fileclass(NULL,stcp,0,1)) < 0)) {
				rpfd = save_rpfd;
				continue; /* Unknown fileclass */
			}
			rpfd = save_rpfd;
			if ((thisretenp = stcp->u1.h.retenp_on_disk) < 0) thisretenp = retenp_on_disk(ifileclass);
			if (thisretenp == INFINITE_LIFETIME) continue;
			if (thisretenp != AS_LONG_AS_POSSIBLE) {
				/* Not a default lifetime - either user defined, or group specific  - always checked */
				if (((int) (this_time - stcp->a_time)) <= thisretenp) continue; /* Lifetime not exceeded ? */
			}
		}
		if (stcp->ipath[0] != '\0') {
			PRE_RFIO;
			if (RFIO_STAT(stcp->ipath, &st) == 0) {
				int has_been_updated = 0;

				if (st.st_size > stcp->actual_size) {
					stcp->actual_size = st.st_size;
					has_been_updated = 1;
				}
				if (st.st_atime > stcp->a_time) {
					stcp->a_time = st.st_atime;
					has_been_updated = 1;
				}
				if (st.st_mtime > stcp->a_time) {
					stcp->a_time = st.st_mtime;
					has_been_updated = 1;
				}
				if (has_been_updated != 0) {
#ifdef USECDB
					if (stgdb_upd_stgcat(dbfd_query,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				}
			} else {
				stglogit (func, STG02, stcp->ipath, RFIO_STAT_FUNC(stcp->ipath), rfio_serror());
			}
		}
		sci->weight = (double)stcp->a_time;
		if (stcp->actual_size > 1024)
			sci->weight -=
				(86400.0 * log((double)stcp->actual_size/1024.0));
		if (scf == NULL) {
			scf = sci;
		} else {
			prev = NULL;
			scc = scf;
			while (scc && scc->weight <= sci->weight) {
				prev = scc;
				scc = scc->next;
			}
			if (! prev) {
				sci->next = scf;
				scf = sci;
			} else {
				prev->next = sci;
				sci->next = scc;
			}
		}
		found = 0;
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if (stcp->reqid == stpp->reqid) {
				found = 1;
				break;
			}
		}
		if (found)
			sci->stpp = stpp;
		sci->stcp = stcp;
		sci++;
	}

	/* output the sorted list */

	for (scc = scf; scc; scc = scc->next) {
		tm = localtime (&scc->stcp->a_time);
		ifileclass = -1;
		save_rpfd = rpfd;
		rpfd = -1;             /* To make sure nothing does on the terminal here */
		if ((class_flag != 0) && (scc->stcp->t_or_d == 'h')) ifileclass = upd_fileclass(NULL,scc->stcp,0,1);
		rpfd = save_rpfd;
		if (ifileclass >= 0) {
			if (sendrep (rpfd, MSG_OUT,
						 "%04d/%02d/%02d %02d:%02d:%02d %6.1f %4d %s %s %*s%s\n",
						 tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
						 tm->tm_hour, tm->tm_min, tm->tm_sec,
						 ((float)(scc->stcp->actual_size))/(1024.*1024.),
						 scc->stcp->nbaccesses, scc->stcp->ipath,
						 (scc->stpp) ? scc->stpp->upath : scc->stcp->ipath,
						 -CA_MAXCLASNAMELEN,
						 fileclasses[ifileclass].Cnsfileclass.name,
						 fileclasses[ifileclass].server
				) < 0) {
				free (scs);
				return (-1);
			}
		} else {
			if (sendrep (rpfd, MSG_OUT,
						 "%04d/%02d/%02d %02d:%02d:%02d %6.1f %4d %s %s\n",
						 tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
						 tm->tm_hour, tm->tm_min, tm->tm_sec,
						 ((float)(scc->stcp->actual_size))/(1024.*1024.),
						 scc->stcp->nbaccesses, scc->stcp->ipath,
						 (scc->stpp) ? scc->stpp->upath : scc->stcp->ipath) < 0) {
				free (scs);
				return (-1);
			}
		}
		if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp, magic);
		if (dump_flag != 0) dump_stcp(rpfd, scc->stcp, &sendrep);
	}
	free (scs);
	return (0);
}

void print_tape_info(poolname, aflag, group, uflag, user, numvid, vid, fseq, fseq_list, req_type, this_reqid, magic, side_flag, side)
	char *poolname;
	int aflag;
	char *group;
	int uflag;
	char *user;
	int numvid;
	char vid[MAXVSN][7];
	char *fseq;
	fseq_elem *fseq_list;
	int req_type;
	int this_reqid;
	int magic;
	int side_flag;
	int side;
{
	int j;
	int poolflag = 0;
	struct stgcat_entry *stcp;
	int inbtpf;

	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if ((this_reqid > 0) && (stcp->reqid != this_reqid)) continue;
		if ((stcp->status & 0xF0) != STAGED) continue;
		if (poolflag < 0) {	/* -p NOPOOL */
			if (stcp->poolname[0]) continue;
		} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
		if (!aflag && strcmp (group, stcp->group)) continue;
		if (uflag && strcmp (user, stcp->user)) continue;
		if (stcp->t_or_d != 't') continue;
		if (side_flag) {
			if (side != stcp->u1.t.side) continue;
		}
		if (numvid) {
			for (j = 0; j < numvid; j++)
				if (strcmp (stcp->u1.t.vid[0], vid[j]) == 0) break;
			if (j == numvid) continue;
			if (fseq && strcmp (fseq, stcp->u1.t.fseq)) continue;
			if (fseq_list) {
				for (inbtpf = 0; inbtpf < nbtpf; inbtpf++)
					if (strcmp (stcp->u1.t.fseq, fseq_list[inbtpf]) == 0) break;
				if (inbtpf == nbtpf) continue;
			}
		}
		if (strcmp (stcp->u1.t.lbl, "al") &&
			strcmp (stcp->u1.t.lbl, "sl")) continue;
		sendrep (rpfd, MSG_OUT, "-b %d -F %s -f %s -L %d\n",
				 stcp->blksize, stcp->recfm, stcp->u1.t.fid, stcp->lrecl);
		if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp, magic);
		if (dump_flag != 0) dump_stcp(rpfd, stcp, &sendrep);
	}
}

signed64 get_stageout_retenp(stcp)
	struct stgcat_entry *stcp;
{
	int i;
	struct pool *pool_p;

	if ((stcp->t_or_d == 'h') && (stcp->u1.h.retenp_on_disk >= 0))
		return((signed64) stcp->u1.h.retenp_on_disk);
	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (stcp->poolname, pool_p->name) == 0) 
			return((time_t) (pool_p->stageout_retenp * ONE_DAY));
	return ((signed64) -1);
}

signed64 get_put_failed_retenp(stcp)
	struct stgcat_entry *stcp;
{
	int i;
	struct pool *pool_p;

	if ((stcp->t_or_d == 'h') && (stcp->u1.h.retenp_on_disk >= 0))
		return((signed64) stcp->u1.h.retenp_on_disk);
	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (stcp->poolname, pool_p->name) == 0) 
			return((time_t) (pool_p->put_failed_retenp * ONE_DAY));
	return ((signed64) -1);
}

/* return value will be 0 or -1, where 0 means that timestr is trustable, -1 means that timestr would have has non-sense */
/* timestr, if return value is zero, contains contains a human-readable format of retention period on disk */
int get_retenp(stcp,timestr)
	struct stgcat_entry *stcp;
	char *timestr;
{
	signed64 this_retenp;
	time_t this_time = time(NULL);
	int ifileclass;

	/* Depending of the status of the stcp we will return the correct current retention period on disk */
	switch (stcp->status) {
	case STAGEOUT:
		/* stageout entry */
		if ((this_retenp = get_stageout_retenp(stcp)) >= 0) {
			/* There is a stageout retention period */
			if ((this_time - stcp->a_time) > this_retenp) {
				strcpy(timestr,"Exhausted");
			} else {
				time_t dummy_retenp;
				this_retenp += stcp->a_time;
				dummy_retenp = (time_t) this_retenp;
				/* Retention period not yet exhausted */
				stage_util_time(dummy_retenp,timestr);
			}
		} else {
			strcpy(timestr,"INFINITE_LIFETIME");
		}
		break;
	case STAGEOUT|PUT_FAILED:
	case STAGEOUT|PUT_FAILED|CAN_BE_MIGR:
		/* put_failed entry (castor or not) */
		if ((this_retenp = get_put_failed_retenp(stcp)) >= 0) {
			/* There is a put_failed retention period */
			if ((this_time - stcp->a_time) > this_retenp) {
				strcpy(timestr,"Exhausted");
			} else {
				time_t dummy_retenp;
				this_retenp += stcp->a_time;
				dummy_retenp = (time_t) this_retenp;
				/* Retention period not yet exhausted */
				stage_util_time(dummy_retenp,timestr);
			}
		} else {
			return(-1);
		}
		break;
	default:
		if ((stcp->status & (STAGEOUT|STAGED)) != (STAGEOUT|STAGED) &&
			(stcp->status & (STAGEWRT|STAGED)) != (STAGEWRT|STAGED) &&
			(stcp->status & ( STAGEIN|STAGED)) != ( STAGEIN|STAGED) &&
			(stcp->status & (STAGEPUT|STAGED)) != (STAGEPUT|STAGED)) {
			/* Not a STAGEd migrated file */
			return(-1);
		}
		/* We distinguish between CASTOR entries and non-CASTOR entry */
		switch (stcp->t_or_d) {
		case 'h':
			/* CASTOR entry */
			if ((ifileclass = upd_fileclass(NULL,stcp,0,1)) < 0) {
				return(-1);
			}
			/* If no explicit value for retention period we take the default */
			if ((this_retenp = stcp->u1.h.retenp_on_disk) < 0) this_retenp = retenp_on_disk(ifileclass);
#ifdef hpux
			/* hpux10 complaints: error 1654: Expression type is too large for switch expression.' */
			switch ((time_t) this_retenp)
#else
				switch (this_retenp)
#endif
				{
				case AS_LONG_AS_POSSIBLE:
					strcpy(timestr,"AS_LONG_AS_POSSIBLE");
					break;
				case INFINITE_LIFETIME:
					strcpy(timestr,"INFINITE_LIFETIME");
					break;
				default:
					if ((this_time - stcp->a_time) > this_retenp) {
						strcpy(timestr,"Exhausted");
					} else {
						time_t dummy_retenp;
						this_retenp += stcp->a_time;
						dummy_retenp = (time_t) this_retenp;
						/* Retention period not yet exhausted */
						stage_util_time(dummy_retenp,timestr);
					}
					break;
				}
			break;
		default:
			return(-1);
		}
	}
	/* Okay */
	return(0);
}

/* return value will be 0 or -1, where 0 means that timestr is trustable, -1 means that timestr would have has non-sense */
/* timestr, if return value is zero, contains contains a human-readable format of retention period on disk */
int get_mintime(stcp,timestr)
	struct stgcat_entry *stcp;
	char *timestr;
{
	time_t this_mintime_beforemigr;
	time_t this_time = time(NULL);
	int ifileclass;

	if (stcp->t_or_d != 'h') return(-1);

	/* Depending of the status of the stcp we will return the correct current retention period on disk */
	switch (stcp->status) {
	case STAGEOUT|CAN_BE_MIGR:
		/* CASTOR entry */
		if ((ifileclass = upd_fileclass(NULL,stcp,0,1)) < 0) {
			return(-1);
		}
		if ((this_mintime_beforemigr = stcp->u1.h.mintime_beforemigr) < 0) /* No explicit value */
			this_mintime_beforemigr = mintime_beforemigr(ifileclass); /* So we take the default */
		if ((this_time - stcp->a_time) > this_mintime_beforemigr) {
			strcpy(timestr,"Exhausted");
		} else {
			time_t dummy_mintime_beforemigr;
			this_mintime_beforemigr += stcp->a_time;
			dummy_mintime_beforemigr = (time_t) this_mintime_beforemigr;
			/* Retention period not yet exhausted */
			stage_util_time(dummy_mintime_beforemigr,timestr);
		}
		break;
	default:
		return(-1);
	}
	/* Okay */
	return(0);
}
