/*
 * $Id: procqry.c,v 1.50 2001/03/03 06:19:27 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: procqry.c,v $ $Revision: 1.50 $ $Date: 2001/03/03 06:19:27 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

/* Disable the update of the catalog in stageqry mode */
#ifdef USECDB
#undef USECDB
#endif

#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <grp.h>
#include <math.h>
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
#include "u64subr.h"

void procqryreq _PROTO((int, int, char *, char *));
void print_link_list _PROTO((char *, int, char *, int, char *, int, char (*)[7], char *, fseq_elem *, char *, char *, char *, int, int));
int print_sorted_list _PROTO((char *, int, char *, int, char *, int, char (*)[7], char *, fseq_elem *, char *, char *, char *, int, int));
void print_tape_info _PROTO((char *, int, char *, int, char *, int, char (*)[7], char *, fseq_elem *, int, int));
void dump_stpp _PROTO((int, struct stgpath_entry *));
void dump_stcp _PROTO((int, struct stgcat_entry *));

extern int unpackfseq _PROTO((char *, int, char *, fseq_elem **, int, int *));
extern int req2argv _PROTO((char *, char ***));
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern int isvalidpool _PROTO((char *));
extern int stglogit _PROTO(());
extern int stglogflags _PROTO(());
extern void print_pool_utilization _PROTO((int, char *, char *, char *, char *, int, int, int, int));
extern int nextreqid _PROTO(());
extern int savereqs _PROTO(());
extern int cleanpool _PROTO((char *));
extern void delreq _PROTO((struct stgcat_entry *, int));
extern int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int));
extern void sendinfo2cptape _PROTO((int, struct stgcat_entry *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *));

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

int nbtpf;
int noregexp_flag;
int reqid_flag;
int dump_flag;
int migrator_flag;
int class_flag;
int queue_flag;
int counters_flag;

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

#ifdef __STDC__
#define NAMEOFVAR(x) #x
#else
#define NAMEOFVAR(x) "x"
#endif

#define DUMP_VAL(rpfd,st,member) sendrep(rpfd, MSG_OUT, "%-14s : %20d\n", NAMEOFVAR(member) , (int) st->member)
#define DUMP_VALHEX(rpfd,st,member) sendrep(rpfd, MSG_OUT, "%-14s : 0x%lx\n", NAMEOFVAR(member) , (unsigned long) st->member)
#define DUMP_U64(rpfd,st,member) {                                          \
    char tmpbuf[21];                                                        \
	sendrep(rpfd, MSG_OUT, "%-14s : %20s\n", NAMEOFVAR(member) ,	            \
				 u64tostr((u_signed64) st->member, tmpbuf,0));	            \
}

#define DUMP_CHAR(rpfd,st,member) sendrep(rpfd, MSG_OUT, "%-14s : %20c\n", NAMEOFVAR(member) , st->member != '\0' ? st->member : ' ')

#define DUMP_STRING(rpfd,st,member) sendrep(rpfd, MSG_OUT, "%-14s : %20s\n", NAMEOFVAR(member) , st->member)

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
	int nargs;
	int numvid = 0;
	char *p;
	char p_lrecl[7];
	char p_recfm[6];
	char p_size[6];
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
	static char title_A[] = 
		"File name                            State      Nbacc.     Size    Pool\n";
	static char title_I[] = 
		"File name         Recfm Lrecl Blksiz State      Nbacc.     Size    Pool\n";
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
	u_signed64  flags;
	char t_or_d;
	int  nstcp_input, struct_status;
	struct stgcat_entry stcp_input;
	int this_reqid = -1;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
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
		{NULL,                 0,                  NULL,        0}
	};

	noregexp_flag = 0;
	reqid_flag = 0;
	dump_flag = 0;
	migrator_flag = 0;
	class_flag = 0;
	queue_flag = 0;
	counters_flag = 0;
	poolname[0] = '\0';
	rbp = req_data;
	local_unmarshall_STRING (rbp, user);	/* login name */
	if (req_type > STAGE_00) {
		if (magic == STGMAGIC) {
			/* This is coming from the API */
			unmarshall_LONG (rbp, gid);
			unmarshall_HYPER (rbp, flags);
			unmarshall_BYTE (rbp, t_or_d);
			unmarshall_LONG(rbp, nstcp_input);
		} else if (magic == STGMAGIC2) {
			/* This is coming from the API */
			unmarshall_LONG (rbp, gid);
			unmarshall_HYPER (rbp, flags);
			unmarshall_BYTE (rbp, t_or_d);
			unmarshall_LONG(rbp, nstcp_input);
			unmarshall_HYPER (rbp, stage_uniqueid);
		}
    } else {
		unmarshall_WORD (rbp, gid);
		nargs = req2argv (rbp, &argv);
	}
#if SACCT
	stageacct (STGCMDR, -1, gid, clienthost,
						 reqid, req_type, 0, 0, NULL, "");
#endif
	
	if ((gr = Cgetgrgid (gid)) == NULL) {
		sendrep (rpfd, MSG_ERR, STG36, gid);
		c = SYERR;
		goto reply;
	}
	strncpy (group, gr->gr_name, CA_MAXGRPNAMELEN);
	/* Makes sure null terminates */
	group[CA_MAXGRPNAMELEN] = '\0';
	
	if (req_type > STAGE_00) {
      /* This is coming from the API */
		if (nstcp_input != 1) {
			sendrep(rpfd, MSG_ERR, "STG02 - Invalid number of input structure (%d) - Should be 1\n", nstcp_input);
			c = USERR;
			goto reply;
		}
		memset((void *) &stcp_input, (int) 0, sizeof(struct stgcat_entry));
		{
			char logit[BUFSIZ + 1];

			struct_status = 0;
			stcp_input.reqid = -1;
			unmarshall_STAGE_CAT(STAGE_INPUT_MODE, struct_status, rbp, &(stcp_input));
			if (struct_status != 0) {
				sendrep(rpfd, MSG_ERR, "STG02 - Bad catalog entry input\n");
				c = SYERR;
				goto reply;
			}
			logit[0] = '\0';
			stcp_input.t_or_d = t_or_d;
			if (stage_stcp2buf(logit,BUFSIZ,&(stcp_input)) == 0 || serrno == SEUMSG2LONG) {
				logit[BUFSIZ] = '\0';
				stglogit("stage_qry","stcp : %s\n",logit);
 			}
		}
		/* We mimic the getopt below */
		if ((this_reqid = stcp_input.reqid) > 0) {
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
				c = SYERR;
				goto reply;
			}
			/* t_or_d == 'a' is virtual and internally is equivalent to 'd' */
			t_or_d = 'd';
			if (stcp_input.u1.d.xfile[0] == '\0') {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_ALLOCED flag is valid only non-empty u1.d.xfile member\n");
				c = SYERR;
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
				c = SYERR;
				goto reply;
			}
			/* t_or_d == 'a' is virtual and internally is equivalent to 'd' */
			t_or_d = 'd';
			if (stcp_input.u1.d.xfile[0] == '\0') {
				sendrep(rpfd, MSG_ERR, "STG02 - STAGE_EXTERNAL flag is valid only non-empty u1.d.xfile member\n");
				c = SYERR;
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
				c = SYERR;
				goto reply;
			}
		}
        if ((t_or_d == 't') && stcp_input.u1.t.fseq[0] != '\0') {
			if ((flags & STAGE_MULTIFSEQ) == STAGE_MULTIFSEQ) {
				if ((nbtpf = unpackfseq (Coptarg, STAGEQRY, &trailing, &fseq_list, 0, NULL)) == 0) {
					sendrep(rpfd, MSG_ERR, "STG02 - STAGE_MULTIFSEQ option value (u1.t.fseq) invalid\n");
					c = SYERR;
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
		/* Print the flags */
		stglogflags("stage_qry",flags);
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

	if (fseq != NULL && fseq_list != NULL) {
		sendrep (rpfd, MSG_ERR, STG35, "-q", "-Q");
		errflg++;
    }
	if (sflag && strcmp (poolname, "NOPOOL") == 0) {
		sendrep (rpfd, MSG_ERR, STG17, "-s", "-p NOPOOL");
		errflg++;
	}
	if (errflg != 0) {
		c = USERR;
		goto reply;
	}
	c = 0;
	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;
	if (! sflag) {
		/* We run this procqry requests in a forked child */
		if ((pid = fork ()) < 0) {
			sendrep (rpfd, MSG_ERR, STG02, "", "fork", sys_errlist[errno]);
			c = SYERR;
			goto reply;
		}
		if (pid) {
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
						numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid);
		goto reply;
	}
	if (sflag) {
		print_pool_utilization (rpfd, poolname, defpoolname, defpoolname_in, defpoolname_out, migrator_flag, class_flag, queue_flag, counters_flag);
		goto reply;
	}
	if (Sflag) {
		if (print_sorted_list (poolname, aflag, group, uflag, user,
								numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid) < 0)
			c = SYERR;
		goto reply;
	}
	if (Tflag) {
		print_tape_info (poolname, aflag, group, uflag, user,
						numvid, vid, fseq, fseq_list, req_type, this_reqid);
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
			if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp);
			if (dump_flag != 0) dump_stcp(rpfd, stcp);
			continue;
		}
		if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp);
		if (hdrprinted++ == 0) {
			if (xfile)
				sendrep (rpfd, MSG_OUT, title_I);
			else if (afile || mfile)
				sendrep (rpfd, MSG_OUT, title_A);
			else
				sendrep (rpfd, MSG_OUT, title);
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
				sprintf (p_size, "%d", stcp->size);
		else
			strcpy (p_size, "*");
		if (stcp->status & 0xF0)
			strcpy (p_stat, x_stat[(stcp->status & 0xF0) >> 4]);
		else if (stcp->status & 0xF00)
			if (ISCASTORBEINGMIG(stcp))
				strcpy( p_stat, "BEING_MIGR");
			else if (ISCASTORWAITINGMIG(stcp))
				strcpy( p_stat, "WAITING_MIGR");
			else
				strcpy (p_stat, l_stat[(stcp->status & 0xF00) >> 8]);
		else if (stcp->status == STAGEALLOC)
			strcpy (p_stat, "STAGEALLOC");
		else if (ISCASTORWAITINGNS(stcp))
			strcpy( p_stat, "WAITING_NS");
		else
			strcpy (p_stat, s_stat[stcp->status]);
		if ((lflag || ((stcp->status & 0xFF0) == 0)) && stcp->ipath[0] != '\0') {
			if (rfio_mstat(stcp->ipath, &st) == 0) {
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
				stglogit (func, STG02, stcp->ipath, "rfio_mstat", rfio_serror());
			}
		}
		if (stcp->t_or_d == 't') {
			if (xflag) {
				if (sendrep (rpfd, MSG_OUT,
							"%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %-14s %6d %s\n",
							 stcp->u1.t.vid[0], stcp->u1.t.fseq, stcp->u1.t.lbl,
							 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
							 (float)(stcp->actual_size)/(1024.*1024.), p_size,
							 stcp->poolname, stcp->reqid, stcp->ipath) < 0) {
					c = SYERR;
					goto reply;
				}
			} else {
				if (sendrep (rpfd, MSG_OUT,
							 "%-6s %6s %-3s %-5s%s %6d %-11s %5d %6.1f/%-4s %s\n",
							 stcp->u1.t.vid[0], stcp->u1.t.fseq, stcp->u1.t.lbl,
							 p_recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
							 (float)(stcp->actual_size)/(1024.*1024.), p_size,
							 stcp->poolname) < 0) {
					c = SYERR;
					goto reply;
				}
			}
		} else if ((stcp->t_or_d == 'd') || (stcp->t_or_d == 'a')) {
			if ((q = strrchr (stcp->u1.d.xfile, '/')) == NULL)
				q = stcp->u1.d.xfile;
			else
				q++;
			if (xflag) {
				if ((stcp->t_or_d == 'd' &&
						sendrep (rpfd, MSG_OUT,
								"%-17s %-3s  %s %6d %-11s %5d %6.1f/%-4s %-14s %6d %s\n", q,
								stcp->recfm, p_lrecl, stcp->blksize, p_stat, stcp->nbaccesses,
								(float)(stcp->actual_size)/(1024.*1024.), p_size,
								stcp->poolname, stcp->reqid, stcp->ipath) < 0) ||
						(stcp->t_or_d == 'a' &&
							sendrep (rpfd, MSG_OUT,
									"%-36s %-11s %5d %6.1f/%-4s %-14s %6d %s\n", q,
									p_stat, stcp->nbaccesses,
									(float)(stcp->actual_size)/(1024.*1024.), p_size,
									stcp->poolname, stcp->reqid, stcp->ipath) < 0)) {
					c = SYERR;
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
					c = SYERR;
					goto reply;
				}
			}
		} else {	/* stcp->t_or_d == 'm' or stcp->t_or_d == 'h'*/
			if ((q = strrchr (stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile, '/')) == NULL)
				q = stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile;
			else
				q++;
			if (xflag) {
				if (sendrep (rpfd, MSG_OUT,
							 "%-36s %-11s %5d %6.1f/%-4s %-14s %6d %s\n", q,
							 p_stat, stcp->nbaccesses,
							 (float)(stcp->actual_size)/(1024.*1024.), p_size,
							 stcp->poolname, stcp->reqid, stcp->ipath) < 0) {
					c = SYERR;
					goto reply;
				}
			} else {
				if (sendrep (rpfd, MSG_OUT,
							 "%-36s %-11s %5d %6.1f/%-4s %s\n", q,
							 p_stat, stcp->nbaccesses,
							 (float)(stcp->actual_size)/(1024.*1024.), p_size,
							 stcp->poolname) < 0) {
					c = SYERR;
					goto reply;
				}
			}
		}
		if (fflag) {
			if ((stcp->t_or_d == 'a') || (stcp->t_or_d == 'd')) {
				if (sendrep (rpfd, MSG_OUT, " %s\n",
										 stcp->u1.d.xfile) < 0) {
					c = SYERR;
					goto reply;
				}
			} else if (stcp->t_or_d == 'm') {
				if (sendrep (rpfd, MSG_OUT, " %s\n",
										 stcp->u1.m.xfile) < 0) {
					c = SYERR;
					goto reply;
				}
			} else if (stcp->t_or_d == 'h') {
				if (sendrep (rpfd, MSG_OUT, " %s\n",
										 stcp->u1.h.xfile) < 0) {
					c = SYERR;
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
				c = SYERR;
				goto reply;
			}
			tm = localtime (&stcp->a_time);
			if (sendrep (rpfd, MSG_OUT,
						 "\t\t\tlast access               %04d/%02d/%02d %02d:%02d:%02d\n",
						 tm->tm_year+1900, tm->tm_mon+1, tm->tm_mday,
						 tm->tm_hour, tm->tm_min, tm->tm_sec) < 0) {
				c = SYERR;
				goto reply;
			}
		}
		if (dump_flag != 0) dump_stcp(rpfd, stcp);
	}
	rfio_end();
 reply:
#if defined(_IBMR2) || defined(hpux) || (defined(__osf__) && defined(__alpha)) || defined(linux)
	if (afile || mfile)
		if (! noregexp_flag) regfree (&preg);
#endif
	if (fseq_list != NULL) free(fseq_list);
	if (argv != NULL) free (argv);
	sendrep (rpfd, STAGERC, STAGEQRY, c);
	if (pid == 0) {	/* we are in the child */
#ifdef USECDB
		if (stgdb_close(dbfd_query) != 0) {
			stglogit(func, STG100, "close", sstrerror(serrno), __FILE__, __LINE__);
		}
		if (stgdb_logout(dbfd_query) != 0) {
			stglogit(func, STG100, "logout", sstrerror(serrno), __FILE__, __LINE__);
		}
#endif
		exit (c);
	}
}

void print_link_list(poolname, aflag, group, uflag, user, numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid)
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
			if (dump_flag != 0) dump_stpp(rpfd, stpp);
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
				if (dump_flag != 0) dump_stpp(rpfd, stpp);
			}
		}
	}
}

int print_sorted_list(poolname, aflag, group, uflag, user, numvid, vid, fseq, fseq_list, xfile, afile, mfile, req_type, this_reqid)
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
{
	/* We use the weight algorithm defined by Fabrizio Cane for DPM */

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

	if (strcmp (poolname, "NOPOOL") == 0)
		poolflag = -1;

	/* build a sorted list of stage catalog entries for the specified pool */
	scf = NULL;
	scs = (struct sorted_ent *) calloc (nbcat_ent, sizeof(struct sorted_ent));
	sci = scs;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if ((this_reqid > 0) && (stcp->reqid != this_reqid)) continue;
		if ((stcp->status & 0xF0) != STAGED) continue;
		if (poolflag < 0) {	/* -p NOPOOL */
			if (stcp->poolname[0]) continue;
		} else if (*poolname && strcmp (poolname, stcp->poolname)) continue;
		if (!aflag && strcmp (group, stcp->group)) continue;
		if (uflag && strcmp (user, stcp->user)) continue;
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
		if (stcp->ipath[0] != '\0') {
			if (rfio_mstat(stcp->ipath, &st) == 0) {
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
				stglogit (func, STG02, stcp->ipath, "rfio_mstat", rfio_serror());
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
	rfio_end();

	/* output the sorted list */

	for (scc = scf; scc; scc = scc->next) {
		tm = localtime (&scc->stcp->a_time);
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
		if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp);
		if (dump_flag != 0) dump_stcp(rpfd, stcp);
	}
	free (scs);
	return (0);
}

void print_tape_info(poolname, aflag, group, uflag, user, numvid, vid, fseq, fseq_list, req_type, this_reqid)
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
		if (req_type > STAGE_00) sendrep(rpfd, API_STCP_OUT, stcp);
		if (dump_flag != 0) dump_stcp(rpfd, stcp);
	}
}

void dump_stpp(rpfd, stpp)
	int rpfd;
	struct stgpath_entry *stpp;
{
	sendrep(rpfd, MSG_OUT, "----------------------------------\n");
	sendrep(rpfd, MSG_OUT, "Path entry  -   dump of reqid %d\n", stpp->reqid);
	sendrep(rpfd, MSG_OUT, "----------------------------------\n");
	DUMP_VAL(rpfd,stpp,reqid);
	DUMP_STRING(rpfd,stpp,upath);
}

void dump_stcp(rpfd, stcp)
	int rpfd;
	struct stgcat_entry *stcp;
{
	int i;

	sendrep(rpfd, MSG_OUT, "-------------------------------------\n");
	sendrep(rpfd, MSG_OUT, "Catalog entry - dump of reqid %d\n", stcp->reqid);
	sendrep(rpfd, MSG_OUT, "-------------------------------------\n");
	DUMP_VAL(rpfd,stcp,reqid);
	DUMP_VAL(rpfd,stcp,blksize);
	DUMP_STRING(rpfd,stcp,filler);
	DUMP_CHAR(rpfd,stcp,charconv);
	DUMP_CHAR(rpfd,stcp,keep);
	DUMP_VAL(rpfd,stcp,lrecl);
	DUMP_VAL(rpfd,stcp,nread);
	DUMP_STRING(rpfd,stcp,poolname);
	DUMP_STRING(rpfd,stcp,recfm);
	DUMP_U64(rpfd,stcp,size);
	DUMP_STRING(rpfd,stcp,ipath);
	DUMP_CHAR(rpfd,stcp,t_or_d);
	DUMP_STRING(rpfd,stcp,group);
	DUMP_STRING(rpfd,stcp,user);
	DUMP_VAL(rpfd,stcp,uid);
	DUMP_VAL(rpfd,stcp,gid);
	DUMP_VAL(rpfd,stcp,mask);
	DUMP_VALHEX(rpfd,stcp,status);
	DUMP_U64(rpfd,stcp,actual_size);
	DUMP_U64(rpfd,stcp,c_time);
	DUMP_U64(rpfd,stcp,a_time);
	DUMP_VAL(rpfd,stcp,nbaccesses);
	switch (stcp->t_or_d) {
	case 't':
		DUMP_STRING(rpfd,stcp,u1.t.den);
		DUMP_STRING(rpfd,stcp,u1.t.dgn);
		DUMP_STRING(rpfd,stcp,u1.t.fid);
		DUMP_CHAR(rpfd,stcp,u1.t.filstat);
		DUMP_STRING(rpfd,stcp,u1.t.fseq);
		DUMP_STRING(rpfd,stcp,u1.t.lbl);
		DUMP_VAL(rpfd,stcp,u1.t.retentd);
		DUMP_STRING(rpfd,stcp,u1.t.tapesrvr);
		DUMP_CHAR(rpfd,stcp,u1.t.E_Tflags);
		for (i = 0; i < MAXVSN; i++) {
			DUMP_STRING(rpfd,stcp,u1.t.vid[i]);
			DUMP_STRING(rpfd,stcp,u1.t.vsn[i]);
		}
		break;
	case 'd':
	case 'a':
		DUMP_STRING(rpfd,stcp,u1.d.xfile);
		DUMP_STRING(rpfd,stcp,u1.d.Xparm);
		break;
	case 'm':
		DUMP_STRING(rpfd,stcp,u1.m.xfile);
		break;
	case 'h':
		DUMP_STRING(rpfd,stcp,u1.h.xfile);
		DUMP_STRING(rpfd,stcp,u1.h.server);
		DUMP_U64(rpfd,stcp,u1.h.fileid);
		DUMP_VAL(rpfd,stcp,u1.h.fileclass);
		DUMP_STRING(rpfd,stcp,u1.h.tppool);
		break;
	}
}
