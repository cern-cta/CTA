/*
 * stgdaemon.c,v 1.228 2002/10/30 16:23:14 jdurand Exp
 */

/*   
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * Nota : restore tab after emacs global identation:
 * perl -pi -e 'while (s/^(\t*)(  )/$1\t/) {} ' /tmp/stgdaemon.c
 *
 * HP-UX counts st_blocks in 1024-byte units
 * 
 */


#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stgdaemon.c,v $ $Revision: 1.251 $ $Date: 2004/11/02 17:47:13 $ CERN IT-ADC/CA Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <unistd.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>                     /* For INT_MIN and INT_MAX */
#include <pwd.h>
#include <sys/types.h>
#include <fcntl.h>
#if sgi || sun
#include <sys/termio.h>
#endif
#include <string.h>
#include <netdb.h>
#include <sys/socket.h>
#include <netinet/in.h>
#if ((defined(IRIX5) || defined(IRIX6)) && ! (defined(LITTLE_ENDIAN) && defined(BIG_ENDIAN) && defined(PDP_ENDIAN)))
#ifdef LITTLE_ENDIAN
#undef LITTLE_ENDIAN
#endif
#define LITTLE_ENDIAN   1234
#ifdef BIG_ENDIAN
#undef BIG_ENDIAN
#endif
#define BIG_ENDIAN      4321
#ifdef PDP_ENDIAN
#undef PDP_ENDIAN
#endif
#define PDP_ENDIAN      3412
#endif
#if (defined(IRIX5) || defined(IRIX6))
/* IRIX windowsize definition is deleted because of POSIX */
#if ! (_NO_POSIX && _NO_XOPEN4)
#include <sys/ioccom.h>
/* Windowing structure to support JWINSIZE/TIOCSWINSZ/TIOCGWINSZ */
struct winsize {
	unsigned short ws_row;       /* rows, in characters*/
	unsigned short ws_col;       /* columns, in character */
	unsigned short ws_xpixel;    /* horizontal size, pixels */
	unsigned short ws_ypixel;    /* vertical size, pixels */
};
#define TIOC    ('T'<<8)
#define TIOCGWINSZ      _IOR('t', 104, struct winsize)  /* get window size */
#define TIOCNOTTY (TIOC|113)            /* disconnect from tty & pgrp */
#endif
#endif
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/resource.h> /* For getrlimit()/setrlimit() */
#if _AIX
#include <sys/un.h>
#endif
#if _AIX && _IBMR2
#include <sys/select.h>
#endif
#include <sys/wait.h>
#include "marshall.h"
#undef  unmarshall_STRING
#define unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#ifndef _WIN32
#include <dirent.h>
#endif
#include "rfio_api.h"
#include "net.h"
#include "stage.h"
#if SACCT
#include "sacct.h"
#endif
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include "serrno.h"
#include "osdep.h"
#include "Cnetdb.h"
#include "Cns_api.h"
#include "Cupv_api.h"
#include "Cpwd.h"
#include "Cgrp.h"
#include "Cgetopt.h"
#include "u64subr.h"
#include "stage_api.h"
#include "Castor_limits.h"
#ifndef CA_MAXDOMAINNAMELEN
#define CA_MAXDOMAINNAMELEN CA_MAXHOSTNAMELEN
#endif

#ifdef STAGE_CSETPROCNAME
#define STAGE_CSETPROCNAME_FORMAT "%s PORT=%d NBCAT=%d NBPATH=%d QUEUED=%d FREE_FD=%d"
#include "Csetprocname.h"
#endif
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

/* Pages size */
#define STG_BUFSIZ 8192

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

#ifdef HAVE_SENSIBLE_STCP
#undef HAVE_SENSIBLE_STCP
#endif
/* This macro will check sensible part of the CASTOR HSM union */
#define HAVE_SENSIBLE_STCP(stcp) (((stcp)->u1.h.xfile[0] != '\0') && ((stcp)->u1.h.server[0] != '\0') && ((stcp)->u1.h.fileid != 0) && ((stcp)->u1.h.fileclass != 0))

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

char defpoolname[CA_MAXPOOLNAMELEN + 1];
char defpoolname_in[CA_MAXPOOLNAMELEN + 1];
char defpoolname_out[10*(CA_MAXPOOLNAMELEN + 1)];
char currentpool_out[CA_MAXPOOLNAMELEN + 1];
int force_init = 0;
int force_shutdown = 0;
int migr_init = 0;
char func[16];
int initreq_reqid = 0;
int initreq_req_type = 0;
int initreq_rpfd = 0;
int shutdownreq_reqid = 0;
int shutdownreq_rpfd = 0;
int last_reqid = 0;			/* last reqid used */
char logbuf[PRTBUFSZ];
int maxfds;
int nbcat_ent;
int nbcat_ent_old = -1;
int nbpath_ent;
int nbpath_ent_old = -1;
fd_set readfd, readmask;
int reqid;
int rpfd = -1;
int stg_s;
u_signed64 stage_uniqueid = 0;
#ifndef _WIN32
struct sigaction sa;
#endif
size_t stgcat_bufsz;
struct stgcat_entry *stce;	/* end of stage catalog */
struct stgcat_entry *stcs;	/* start of stage catalog */
size_t stgpath_bufsz;
struct stgpath_entry *stpe;	/* end of stage path catalog */
struct stgpath_entry *stps;	/* start of stage path catalog */
struct waitq *waitqp;
#ifdef USECDB
char *Default_db_user = "Cstg_username";
char *Default_db_pwd  = "Cstg_password";
struct stgdb_fd dbfd;
#endif
struct passwd stage_passwd;             /* Generic uid/gid stage:st */
struct passwd start_passwd;
char localdomain[CA_MAXDOMAINNAMELEN+1];  /* Local domain */
char *deflocalhost = "localhost.localdomain";
char *localhost;
unsigned long ipaddrlocal;
int have_ipaddrlocal;
time_t upd_fileclasses_int_default = 3600 * 24; /* Default update time for all CASTOR fileclasses - one day */
time_t upd_fileclasses_int;
int selectfs_ro_with_nb_stream;
int selectfs_rw_with_nb_stream;
char *selectfs_p;
time_t last_upd_fileclasses = 0;
time_t started_time;
char cns_error_buffer[512];         /* Cns error buffer */
char cupv_error_buffer[512];         /* Upv error buffer */
char *stgconfigfile = STGCONFIG;    /* Stager configuration file */

#ifdef MONITOR
/* For monitoring */
int monitorok;
time_t last_monitormsg_sent = 0;
time_t last_init_time = 0;
#endif

void prockilreq _PROTO((int, char *, char *));
void procinireq _PROTO((int, int, unsigned long, char *, char *));
void procshutdownreq _PROTO((int, int, unsigned long, char *, char *));
void check_upd_fileclasses _PROTO(());
void checkpoolstatus _PROTO(());
void checkwaitingspc _PROTO(());
void check_child_exit _PROTO(());
void checkwaitq _PROTO(());
void create_link _PROTO((int *, struct stgcat_entry *, char *));
void dellink _PROTO((struct stgpath_entry *, int));
void delreq _PROTO((struct stgcat_entry *, int));
void stgcat_shrunk_pages _PROTO(());
void stgpath_shrunk_pages _PROTO(());
void delreqid _PROTO((int, int));
void rmfromwq _PROTO((struct waitq *));
void sendinfo2cptape _PROTO((int *, struct stgcat_entry *));
void stgdaemon_usage _PROTO(());
void stgdaemon_wait4child _PROTO((int));
int req2argv _PROTO((char *, char ***));
int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *, int, int));
int ask_stageout _PROTO((int, char *, struct stgcat_entry **));
int file_modified _PROTO((int, char *, char *, uid_t, gid_t, int, char *));
int check_coff_waiting_on_req _PROTO((int, int));
int check_waiting_on_req _PROTO((int, int));
int nextreqid _PROTO(());
int savereqs _PROTO(());
#if defined(_WIN32)
int create_dir _PROTO((char *, uid_t, gid_t, int));
#else
#ifdef hpux
/* What the hell does hpux does not like this prototype ??? */
int create_dir _PROTO(());
#else
int create_dir _PROTO((char *, uid_t, gid_t, mode_t));
#endif
#endif
#ifdef hpux
/* What the hell does hpux does not like this prototype ??? */
int build_ipath _PROTO(());
#else
int build_ipath _PROTO((char *, struct stgcat_entry *, char *, int, int, mode_t));
#endif
int fork_exec_stager _PROTO((struct waitq *));
int savepath _PROTO(());
int upd_staged _PROTO((char *));
void checkovlstatus _PROTO((int, int));
void killallovl _PROTO((int));
extern void killcleanovl _PROTO((int));
extern void killmigovl _PROTO((int));
int verif_euid_egid _PROTO((uid_t, gid_t, char *, char *));
extern int get_put_failed_retenp _PROTO((char *));
size_t stg_count_digits _PROTO((int, unsigned int));

struct stgcat_entry *newreq _PROTO((int));
struct waitf *add2wf _PROTO((struct waitq *));
int add2otherwf _PROTO((struct waitq *, char *, struct waitf *, struct waitf *));
extern int update_migpool _PROTO((struct stgcat_entry **, int, int));
extern int iscleanovl _PROTO((int, int));
extern int ismigovl _PROTO((int, int));
extern int selectfs _PROTO((char *, u_signed64 *, char *, int, int));
extern int updfreespace _PROTO((char *, char *, int, u_signed64 *, signed64));
extern void procupdreq _PROTO((int, int, char *, char *));
struct waitq *add2wq _PROTO((char *, char *, uid_t, gid_t, char *, char *, uid_t, gid_t, int, int, int, int, int, struct waitf **, int **, char *, char *, int));
int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int, int, int));
extern void checkfile2mig _PROTO(());
extern int updpoolconf _PROTO((char *, char *, char *));
extern void procioreq _PROTO((int, int, char *, char *));
extern void procpingreq _PROTO((int, int, char *, char *));
extern void procputreq _PROTO((int, int, char *, char *));
extern void procqryreq _PROTO((int, int, char *, char *));
extern void procclrreq _PROTO((int, int, char *, char *));
extern void procallocreq _PROTO((int, int, char *, char *));
extern void procgetreq _PROTO((int, int, char *, char *));
extern void procfilchgreq _PROTO((int, int, char *, char *));
extern int getpoolconf _PROTO((char *, char *, char *));
extern int checkpoolcleaned _PROTO((char ***));
extern void checkpoolspace _PROTO(());
extern int cleanpool _PROTO((char *));
extern int get_create_file_option _PROTO((char *));
extern void stageacct _PROTO((int, uid_t, gid_t, char *, int, int, int, int, struct stgcat_entry *, char *, char));
extern int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *, int, int, int));
extern int upd_fileclasses _PROTO(());
extern char *getconfent();
extern void check_delaymig _PROTO(());
extern void check_expired _PROTO(());
extern int create_hsm_entry _PROTO((int *, struct stgcat_entry *, int, mode_t, int));
extern void rwcountersfs _PROTO((char *, char *, int, int));
extern u_signed64 findblocksize _PROTO((char *));
extern int stageput_check_hsm _PROTO((struct stgcat_entry *, uid_t, gid_t, int, int *, struct Cns_filestat *, int));
extern char *findpoolname _PROTO((char *));
extern int rc_shift2castor _PROTO((int,int));

/* Function with variable list of arguments - defined as in non-_STDC_ to avoir proto problem */
extern int stglogit _PROTO((char *, char *, ...));
extern char *stglogflags _PROTO((char *, char *, u_signed64));
extern int sendrep _PROTO((int *, int, ...));
EXTERN_C int DLL_DECL Cdomainname _PROTO((char *, int));

/*
 * Number of reserved/used socket connections:
 *
 * one for the next request (to be available to answer to it!)
 * one for the log
 * two for the pipe() in fork_exec_stager
 * As many as there are entries in the waitq that have a rpfd >= 0 (e.g. maintain a connection)
 * nbhost for the rfio_mstat()
 * nbhost for the rfio_munlink()
 */
extern int nbhost;
extern int nbstageout;
extern int nbstagealloc;
int nwaitq = 0;
int nwaitq_with_connection = 0;

#define RESERVED_FD (4 + nwaitq_with_connection + 2 * nbhost)
#define FREE_FD (sysconf(_SC_OPEN_MAX) - RESERVED_FD)

int stgdaemon_port = 0;
#ifdef STAGE_CSETPROCNAME
static char sav_argv0[1024+1];
#endif

#ifdef STAGE_CSETPROCNAME
int main(argc,argv,envp)
		 int argc;
		 char **argv;
		 char **envp;
#else
int main(argc,argv)
		 int argc;
		 char **argv;
#endif
{
	int c, l;
	char *clienthostptr;
	char clienthost[CA_MAXHOSTNAMELEN+1];
	int errflg = 0;
	int foreground = 0;
	int backlog = 5;
	int rlimit_nofile = 2048;
	int rlimit_nproc = 512;
	struct rlimit rlim;
	struct sockaddr_in from;
	int fromlen = sizeof(from);
	struct hostent *hp;
	int magic;
	int msglen;
	int on = 1;	/* for REUSEADDR */
	char *rbp;
	char *req_data = NULL;      /* This initialization to NULL is important */
	char req_hdr[3*LONGSIZE];
	int req_type;
	int rqfd;
	struct sockaddr_in sin;		/* internet address */
#ifndef USECDB
	int i;
	int scfd;
	int spfd;
#endif
	struct servent *sp;
	struct stat64 st;
	struct stat localst;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	struct timeval timeval;
	char tmpbuf[21];
	struct passwd *this_passwd;             /* password structure pointer */
	struct passwd root_passwd;
	char myenv[11 + CA_MAXHOSTNAMELEN + 1]; /* 11 == strlen("STAGE_HOST=") */
	char *upd_fileclasses_int_p;
	size_t read_size;
	static int rlimit_nproc_flag;
	static int rlimit_nofile_flag;
	char *stgdaemon_port_p = NULL;
	int done_rlimit_nproc;
	int done_rlimit_nofile;
	int doit_rlimit_nproc;
	int doit_rlimit_nofile;
	char thislocalhost[CA_MAXHOSTNAMELEN+1];  /* Local hostname */
	static struct Coptions longopts[] =
	{
		{"foreground",         NO_ARGUMENT,        NULL,      'f'},
		{"help",               NO_ARGUMENT,        NULL,      'h'},
		{"backlog",            REQUIRED_ARGUMENT,  NULL,      'L'},
		{"rlimit_nofile",      REQUIRED_ARGUMENT,  &rlimit_nofile_flag,     'F'},
		{"rlimit_nproc",       REQUIRED_ARGUMENT,  &rlimit_nproc_flag,      'P'},
		{"port",               REQUIRED_ARGUMENT,  NULL,      'p'},
 		{NULL,                 0,                  NULL,        0},
	};

	Coptind = 1;
	Copterr = 0;
	rlimit_nproc_flag = 0;
	rlimit_nofile_flag = 0;
	done_rlimit_nproc = 0;
	done_rlimit_nofile = 0;
	doit_rlimit_nproc = 0;
	doit_rlimit_nofile = 0;

	while ((c = Cgetopt_long (argc, argv, "fhI:L:F:P:p:", longopts, NULL)) != -1) {
		switch (c) {
		case 'f':
			foreground = 1;
			break;
		case 'h':
			stgdaemon_usage();
			exit(0);
		case 'I':
			stgconfigfile = Coptarg;
			break;
		case 'L':
			if ((backlog = atoi(Coptarg)) <= 0) {
				fprintf(stderr,"Listen queue (-L option) value '%s' : must be > 0\n",Coptarg);
				++errflg;
			}
			break;
		case 'F':
			if ((rlimit_nofile = atoi(Coptarg)) <= 0) {
				fprintf(stderr,"RLIMIT_NOFILE (-F/--rlimit_nofile option) value '%s' : must be > 0\n",Coptarg);
				++errflg;
			} else {
				doit_rlimit_nofile = 1;
			}
			break;
		case 'P':
			if ((rlimit_nproc = atoi(Coptarg)) <= 0) {
				fprintf(stderr,"RLIMIT_NPROC (-P/--rlimit_nproc option) value '%s' : must be > 0\n",Coptarg);
				++errflg;
			} else {
				doit_rlimit_nproc = 1;
			}
			break;
		case 'p':
			stgdaemon_port_p = Coptarg;
			break;
		case 0:
			if ((rlimit_nproc_flag != 0) && (! done_rlimit_nproc)) {
				if ((rlimit_nproc = atoi(Coptarg)) <= 0) {
					fprintf(stderr,"RLIMIT_NPROC (-P/--rlimit_nproc option) value '%s' : must be > 0\n",Coptarg);
					++errflg;
				} else {
					doit_rlimit_nproc = 1;
					done_rlimit_nproc = 1;
				}
			}
			if ((rlimit_nofile_flag != 0) && (! done_rlimit_nofile)) {
				if ((rlimit_nofile = atoi(Coptarg)) <= 0) {
					fprintf(stderr,"RLIMIT_NOFILE (-F/--rlimit_nofile option) value '%s' : must be > 0\n",Coptarg);
					++errflg;
				} else {
					doit_rlimit_nofile = 1;
					done_rlimit_nofile = 1;
				}
			}
			break;
		case '?':
			++errflg;
			break;
		default:
			++errflg;
			printf("?? Cgetopt returned character code 0%o (octal) 0x%lx (hex) %d (int) '%c' (char) ?\n"
						 ,c,(unsigned long) c,c,(char) c);
			break;
		}
	}

	/* Check existence of config file right now */
	if (stat (stgconfigfile, &localst) != 0) {
		fprintf(stderr, STG02, stgconfigfile, "stat", strerror(errno));
		fprintf(stderr, "Exit.\n");
		exit(1);
	}

	if (errflg != 0) {
		fprintf(stderr, "### Cgetopt error\n");
		fprintf(stderr, "Exit.\n");
		exit(1);
	}

	strcpy (func, "stgdaemon");
	stglogit (func, "==============================================\n");
	stglogit (func, "started\n");
	stglogit (func, "==============================================\n");

	/* Before getting system limit, change them right now - we do nothing if wanted value are below what system provide */
#ifdef RLIMIT_NOFILE
	if (doit_rlimit_nofile != 0) {
		/* RLIMIT_NOFILE */
		if (getrlimit(RLIMIT_NOFILE,&rlim) != 0) {
			stglogit(func, "... getrlimit(RLIMIT_NOFILE,&rlim) error : %s\n", strerror(errno));
		} else {
			if (rlim.rlim_cur > rlimit_nofile) {
				stglogit(func, "... RLIMIT_NOFILE says current limit is %d. Unchanged.\n", rlim.rlim_max);
			} else {
				rlim.rlim_cur = rlimit_nofile;
				if (rlim.rlim_max < rlim.rlim_cur) {
					stglogit(func, "... RLIMIT_NOFILE says h/w limit is %d, setting h/w and current limits to %d \n", rlim.rlim_max, rlimit_nofile);
					rlim.rlim_max = rlim.rlim_cur;
				} else {
					stglogit(func, "... RLIMIT_NOFILE says h/w limit is %d, setting only current limit to %d \n", rlim.rlim_max, rlimit_nofile);
				}
				if (setrlimit(RLIMIT_NOFILE,&rlim) != 0) {
					stglogit(func, "... setrlimit(RLIMIT_NOFILE,&rlim) error : %s\n", strerror(errno));
				}
			}
			/* Verify */
			if (getrlimit(RLIMIT_NOFILE,&rlim) != 0) {
				stglogit(func, "... getrlimit(RLIMIT_NOFILE,&rlim) error : %s\n", strerror(errno));
			} else {
				stglogit(func, "==> Running with RLIMIT_NOFILE = { cur=%d, max=%d }\n", rlim.rlim_cur, rlim.rlim_max);
			}
		}
	} else {
		if (getrlimit(RLIMIT_NOFILE,&rlim) != 0) {
			stglogit(func, "... getrlimit(RLIMIT_NOFILE,&rlim) error : %s\n", strerror(errno));
		} else {
			stglogit(func, "==> Running with RLIMIT_NOFILE = { cur=%d, max=%d }\n", rlim.rlim_cur, rlim.rlim_max);
		}
	}
#else
	stglogit(func, "... RLIMIT_NOFILE undefined on this platform\n");
#endif
#ifdef RLIMIT_NPROC
	if (doit_rlimit_nproc != 0) {
		/* RLIMIT_NPROC */
		if (getrlimit(RLIMIT_NPROC,&rlim) != 0) {
			stglogit(func, "... getrlimit(RLIMIT_NPROC,&rlim) error : %s\n", strerror(errno));
		} else {
			if (rlim.rlim_cur > rlimit_nproc) {
				stglogit(func, "... RLIMIT_NPROC says current limit is %d. Unchanged.\n", rlim.rlim_max);
			} else {
				rlim.rlim_cur = rlimit_nproc;
				if (rlim.rlim_max < rlim.rlim_cur) {
					stglogit(func, "... RLIMIT_NPROC says h/w limit is %d, setting h/w and current limits to %d\n", rlim.rlim_max, rlimit_nproc);
					rlim.rlim_max = rlim.rlim_cur;
				} else {
					stglogit(func, "... RLIMIT_NPROC says h/w limit is %d, setting only current limit to %d \n", rlim.rlim_max, rlimit_nofile);
				}
				if (setrlimit(RLIMIT_NPROC,&rlim) != 0) {
					stglogit(func, "... setrlimit(RLIMIT_NPROC,&rlim) error : %s\n", strerror(errno));
				}
			}
			/* Verify */
			if (getrlimit(RLIMIT_NPROC,&rlim) != 0) {
				stglogit(func, "... getrlimit(RLIMIT_NPROC,&rlim) error : %s\n", strerror(errno));
			} else {
				stglogit(func, "==> Running with RLIMIT_NPROC = { cur=%d, max=%d }\n", rlim.rlim_cur, rlim.rlim_max);
			}
		}
	} else {
		if (getrlimit(RLIMIT_NPROC,&rlim) != 0) {
			stglogit(func, "... getrlimit(RLIMIT_NPROC,&rlim) error : %s\n", strerror(errno));
		} else {
			stglogit(func, "==> Running with RLIMIT_NPROC = { cur=%d, max=%d }\n", rlim.rlim_cur, rlim.rlim_max);
		}
	}
#else
	stglogit(func, "... RLIMIT_NPROC undefined on this platform\n");
#endif

#if defined(SOLARIS) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(sgi)
	maxfds = getdtablesize();
#else
	maxfds = _NFILE;
#endif
#ifndef TEST
	if (foreground == 0) {
		/* Background */
		if ((c = fork()) < 0) {
			stglogit (func, STG02, "", "fork", strerror(errno));
			stglogit (func, "Exit.\n");
			exit (1);
		} else
			if (c > 0) exit (0);
#if (defined(__osf__) && defined(__alpha)) || defined(linux)
		c = setsid();
#else
#if HPUX10
		c = setpgrp3();
#else
		c = setpgrp();
#endif
#endif
		for (c = 0; c < maxfds; c++)
			close (c);
#if sun || sgi
		c = open ("/dev/tty", O_RDWR);
		if (c >= 0) {
			ioctl (c, TIOCNOTTY, 0);
			(void) close(c);
		}
#endif
	}
#endif /* not TEST */
#ifndef _WIN32
	sa.sa_handler = stgdaemon_wait4child;
	sa.sa_flags = SA_RESTART;
	sigaction (SIGCHLD, &sa, NULL);
#endif
	started_time = time(NULL);
#if SACCT
	stageacct (STGSTART, 0, 0, "", 0, 0, 0, 0, NULL, "", (char) 0);
#endif

	/* Get localhostname */
	if (gethostname(thislocalhost,sizeof(thislocalhost)) != 0)   {
		stglogit(func, "Cannot get local hostname (%s) - forcing \"localhost\"\n", strerror(errno));
		strcpy(thislocalhost,"localhost");
	}
	stglogit(func, "Local hostname is \"%s\"\n", thislocalhost);

	/* Is it a fully qualified hostname ? */
	{
		char *p;
		
		if ((p = strchr(thislocalhost,'.')) == NULL) {
			if (Cdomainname(localdomain,CA_MAXDOMAINNAMELEN) != 0) {
				stglogit(func, "Cannot get local domain (%s) - forcing \"localdomain\"\n", strerror(errno));
				strcpy(localdomain,"localdomain");
			}
			stglogit(func, "Local domain is \"%s\"\n", localdomain);
			/* Malloc area */
			if ((localhost = (char *) malloc(strlen(thislocalhost) + 1 + strlen(localdomain) + 1)) == NULL) {
				stglogit(func, "Cannot malloc (%s) - forcing \"localhost.localdomain\"\n", strerror(errno));
				localhost = deflocalhost;
			} else {
				strcpy(localhost,thislocalhost);
				strcat(localhost,".");
				strcat(localhost,localdomain);
			}
		} else {
			/* Malloc area */
			if ((localhost = (char *) malloc(strlen(thislocalhost) + 1)) == NULL) {
				stglogit(func, "Cannot malloc (%s) - forcing \"localhost.localdomain\"\n", strerror(errno));
				localhost = deflocalhost;
			} else {
				strcpy(localhost,thislocalhost);
			}
		}
	}
	stglogit(func, "Fully qualified hostname is \"%s\"\n", localhost);

	/* Get localhostname IP addr */
	if ((hp = Cgethostbyname(localhost)) == NULL) {
		stglogit (func, STG02, localhost, "Cgethostbyname", strerror(errno));
		have_ipaddrlocal = 0;
	} else {
		have_ipaddrlocal = 1;
		ipaddrlocal = (unsigned long) ((struct in_addr *)(hp->h_addr))->s_addr;
    }

	/* Force environment variable to be sure that all our callbacks, forked processes, etc... will really go there... */
	strcpy(myenv, "STAGE_HOST=");
	strcat(myenv, localhost);
	if (putenv(myenv) != 0) {
		stglogit(func, "Cannot putenv(\"%s\"), %s\n", myenv, strerror(errno));
    } else {
		stglogit(func, "Setted environment variable %s\n", myenv);
	}

#ifdef STAGE_CSETPROCNAME
#if linux
    /* The standard 'functions' in /etc/rc.d on linux will work with "stgdaemon" */
	strncpy(sav_argv0,"stgdaemon",1024);
#else
	strncpy(sav_argv0,argv[0],1024);
#endif
	sav_argv0[1024] = '\0';
	if (Cinitsetprocname(argc,argv,envp) != 0) {
		stglogit(func,"### Cinitsetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
	}
#endif

	/* We get information on current uid/gid */
	if ((this_passwd = Cgetpwuid(getuid())) == NULL) {
		stglogit(func, "### Cannot Cgetpwuid(%d) (%s)\n",(int) getuid(), strerror(errno));
		stglogit(func, "### Please check existence of current uid %d in password file\n", (int) getuid());
		stglogit (func, "Exit.\n");
 		exit (SYERR);
	}
	start_passwd = *this_passwd;
	if (Cgetgrgid(start_passwd.pw_gid) == NULL) {
		stglogit(func, "### Cannot Cgetgrgid(%d) (%s)\n",start_passwd.pw_gid,strerror(errno));
		stglogit(func, "### Please check existence of group %d (gid of account \"%s\") in group file\n", start_passwd.pw_gid, start_passwd.pw_name);
		stglogit (func, "Exit.\n");
 		exit (SYERR);
	}
	stglogit(func, "Running under \"%s\" account (uid=%d,gid=%d), pid=%d\n", start_passwd.pw_name, start_passwd.pw_uid, start_passwd.pw_gid, (int) getpid());

	/* We get information on generic stage:st uid/gid */
	if ((this_passwd = Cgetpwnam(STAGERSUPERUSER)) == NULL) {
		stglogit(func, "### Cannot Cgetpwnam(\"%s\") (%s)\n",STAGERSUPERUSER,strerror(errno));
		stglogit(func, "### Please check existence of account \"%s\" in password file\n", STAGERSUPERUSER);
		stglogit (func, "Exit.\n");
 		exit (SYERR);
	}
	stage_passwd = *this_passwd;
	if (Cgetgrgid(stage_passwd.pw_gid) == NULL) {
		stglogit(func, "### Cannot Cgetgrgid(%d) (%s)\n",stage_passwd.pw_gid,strerror(errno));
		stglogit(func, "### Please check existence of group %d (gid of account \"%s\") in group file\n", (int) stage_passwd.pw_gid, STAGERSUPERUSER);
		stglogit (func, "Exit.\n");
 		exit (SYERR);
	}

	/* We check that we currently run under root account */
	if ((this_passwd = Cgetpwnam("root")) == NULL) {
		stglogit(func, "### Cannot Cgetpwnam(\"%s\") (%s)\n","root",strerror(errno));
		stglogit(func, "### Please check existence of account \"%s\" in password file\n", "root");
	} else {
		root_passwd = *this_passwd;
		if (Cgetgrgid(root_passwd.pw_gid) == NULL) {
			stglogit(func, "### Cannot Cgetgrgid(%d) (%s)\n",root_passwd.pw_gid,strerror(errno));
			stglogit(func, "### Please check existence of group %d (gid of account \"%s\") in group file\n", (int) root_passwd.pw_gid, "root");
	 		/* If "root" group does not exist in group file (!), let's continue anyway */
		} else {
			if ((root_passwd.pw_uid != start_passwd.pw_uid) ||
				(root_passwd.pw_gid != start_passwd.pw_gid)) {
				stglogit(func, "### Warning : You, \"%s\" (uid=%d,gid=%d), are NOT running under \"root\" (uid=%d,gid=%d) account. Functionnality might very well be extremely limited.\n", start_passwd.pw_name, start_passwd.pw_uid, start_passwd.pw_gid, root_passwd.pw_uid, root_passwd.pw_gid);
			}
		}
	}


	FD_ZERO (&readmask);
	FD_ZERO (&readfd);
	signal (SIGPIPE,SIG_IGN);
#ifndef _WIN32
	signal(SIGXFSZ,SIG_IGN);
#endif
	
	/* Set daemon mask to zero */
	umask (0);

	/* Ignore NET entries in /etc/shift.conf */
	c = RFIO_NONET;
	rfiosetopt (RFIO_NETOPT, &c, 4);

	/* Set Cns error buffer */
	if (Cns_seterrbuf(cns_error_buffer,sizeof(cns_error_buffer)) != 0) {
		stglogit(func, "### Cns_seterrbuf error (%s)\n", sstrerror(serrno));
		stglogit (func, "Exit.\n");
 		exit (SYERR);
	}

	/* Set Upv error buffer */
	if (Cupv_seterrbuf(cupv_error_buffer,sizeof(cupv_error_buffer)) != 0) {
		stglogit(func, "### Cupv_seterrbuf error (%s)\n", sstrerror(serrno));
		stglogit (func, "Exit.\n");
 		exit (SYERR);
	}

	/* Open request socket */
	if ((stg_s = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		stglogit (func, STG02, "", "socket", strerror(errno));
		stglogit (func, "Exit.\n");
		exit (CONFERR);
	}
	memset ((char *)&sin, 0, sizeof(struct sockaddr_in)) ;
	if ((stgdaemon_port_p != NULL) ||            /* From command-line */
		((stgdaemon_port_p = getenv("STAGE_PORT")) != NULL) ||        /* From environment */
		((stgdaemon_port_p = getconfent("STG", "PORT", 0)) != NULL)   /* From configuration */
		) {
		int thisport = atoi(stgdaemon_port_p);
		if (thisport <= 0) {
			stglogit (func, STG09, STAGE_NAME, "service (port number) is <= 0");
			stglogit (func, "Exit.\n");
			exit (CONFERR);
		}
		if (thisport != STAGE_PORT) {
			stglogit (func, STG148, "stgdaemon port", "different than default value - Using", thisport);
		}
		sin.sin_port = htons((u_short) thisport);
		stgdaemon_port = thisport;
	} else if ((sp = Cgetservbyname (STAGE_NAME, STAGE_PROTO)) == NULL) {
		stglogit (func, STG148, STAGE_NAME, "not defined in /etc/services - Using default value", (int) STAGE_PORT);
		sin.sin_port = htons((u_short) STAGE_PORT);
		stgdaemon_port = STAGE_PORT;
	} else {
		sin.sin_port = sp->s_port;
		stgdaemon_port = ntohs(sp->s_port);
	}
	sin.sin_family = AF_INET ;
	sin.sin_addr.s_addr = htonl(INADDR_ANY);
	if (setsockopt (stg_s, SOL_SOCKET, SO_REUSEADDR, (char *)&on, sizeof(on)) < 0)
		stglogit (func, STG02, "", "setsockopt", strerror(errno));
	/*
	if (setsockopt (stg_s, IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(on)) < 0)
		stglogit (func, STG02, "", "setsockopt", strerror(errno));
	*/
	if (bind (stg_s, (struct sockaddr *) &sin, sizeof(sin)) < 0) {
		stglogit (func, STG02, "", "bind", strerror(errno));
		stglogit (func, "Exit.\n");
		exit (CONFERR);
	}
	stglogit (func, "Listen backlog set to %d on port %d\n", backlog, stgdaemon_port);
	listen (stg_s, backlog) ;
	
	FD_SET (stg_s, &readmask);

	/* Initialize current pool out */
	currentpool_out[0] = '\0';

	/* get pool configuration */

	if ((c = getpoolconf (defpoolname, defpoolname_in, defpoolname_out))) {
		stglogit (func, "Exit.\n");
		exit (CONFERR);
	}

#ifdef USECDB
	/* Get stager/database login:password */
	{
		FILE *configfd;
		char cfbuf[80];
		char *p_p, *p_u;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
		char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

		if ((configfd = fopen(STGDBCONFIG,"r")) == NULL) {
			stglogit (func, "[harmless] Cannot open Db Configuration file %s\n", STGDBCONFIG);
			stglogit (func, "[info] Using default Db Username/Password = \"%s\"/\"<not printed>\"\n",Default_db_user);
			p_u = Default_db_user;
			p_p  = Default_db_pwd;
		} else {
			if (fgets (cfbuf, sizeof(cfbuf), configfd) &&
					strlen (cfbuf) >= 5 && (p_u = strtok (cfbuf, "/\n")) != NULL &&
					(p_p = strtok (NULL, "@\n")) != NULL) {
			} else {
				stglogit(func, "Error reading Db Configuration file %s\n",STGDBCONFIG);
				stglogit(func, "Exit.\n");
				exit (CONFERR);
			}
			fclose(configfd);
		}

		if (strlen(p_u) > CA_MAXUSRNAMELEN || strlen(p_p) > CA_MAXUSRNAMELEN) {
			stglogit(func, 
					 "Database username and/or password exceeds maximum length of %d characters !\n",
					 CA_MAXUSRNAMELEN);
			stglogit (func, "Exit.\n");
			exit (CONFERR);
		}

		/* Remember it for the future */
		strcpy(dbfd.username,p_u);
		strcpy(dbfd.password,p_p);
	}

	/* Log to the database server */
	if (stgdb_login(&dbfd) != 0) {
		stglogit(func, STG100, "login", sstrerror(serrno), __FILE__, __LINE__);
		stglogit (func, "Exit.\n");
		exit(SYERR);
	}

	/* Open the database */
	if (stgdb_open(&dbfd,"stage") != 0) {
		stglogit(func, STG100, "open", sstrerror(serrno), __FILE__, __LINE__);
		stglogit (func, "Exit.\n");
		exit(SYERR);
	}

	/* Ask for a dump of the catalog */
	if (stgdb_load(&dbfd,&stcs,&stce,&stgcat_bufsz,&stps,&stpe,&stgpath_bufsz) != 0) {
		stglogit(func, STG100, "load", sstrerror(serrno), __FILE__, __LINE__);
		stglogit (func, "Exit.\n");
		exit(SYERR);
	}

	/* Temporarly patch because of rfio_serror() behaviour : we reset serrno */
	serrno = 0;

	/* We compute the number of entries as well as the last used reqid */
	nbcat_ent = 0;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) {
			break;
		}
		nbcat_ent++;
	}
	if (nbcat_ent == 0) {
		last_reqid = 0;
	} else {
		last_reqid = (stcs + nbcat_ent - 1)->reqid;
	}

	nbpath_ent = 0;
	for (stpp = stps; stpp < stpe; stpp++) {
		if (stpp->reqid == 0) {
			break;
		}
		nbpath_ent++;
	}

#else /* USECDB */
	/* read stage catalog */

	scfd = open (STGCAT, O_RDWR | O_CREAT, 0664);
	fstat (scfd, &localst);
	if (localst.st_size == 0) {
		stgcat_bufsz = STG_BUFSIZ;
		stcs = (struct stgcat_entry *) calloc (1, stgcat_bufsz);
		stce = stcs + (stgcat_bufsz/sizeof(struct stgcat_entry));
		nbcat_ent = 0;
		close (scfd);
		last_reqid = 0;
	} else {
		stgcat_bufsz = (localst.st_size + STG_BUFSIZ -1) / STG_BUFSIZ * STG_BUFSIZ;
		stcs = (struct stgcat_entry *) calloc (1, stgcat_bufsz);
		stce = stcs + (stgcat_bufsz/sizeof(struct stgcat_entry));
		nbcat_ent = localst.st_size / sizeof(struct stgcat_entry);
		read (scfd, (char *) stcs, localst.st_size);
		close (scfd);
		if (localst.st_size != nbcat_ent * sizeof(struct stgcat_entry)) {
			stglogit (func, STG48, STGCAT);
			stglogit (func, "Exit.\n");
			exit (SYERR);
		}
		for (stcp = stcs, i = 0; i < nbcat_ent; i++, stcp++)
			if (stcp->reqid == 0) {
				stglogit (func, STG48, STGCAT);
				stglogit (func, "Exit.\n");
				exit (SYERR);
			}
		last_reqid = (stcs + nbcat_ent - 1)->reqid;
	}
	spfd = open (STGPATH, O_RDWR | O_CREAT, 0664);
	fstat (spfd, &localst);
	if (localst.st_size == 0) {
		stgpath_bufsz = STG_BUFSIZ;
		stps = (struct stgpath_entry *) calloc (1, stgpath_bufsz);
		stpe = stps + (stgpath_bufsz/sizeof(struct stgpath_entry));
		nbpath_ent = 0;
		close (spfd);
	} else {
		stgpath_bufsz = (localst.st_size + STG_BUFSIZ -1) / STG_BUFSIZ * STG_BUFSIZ;
		stps = (struct stgpath_entry *) calloc (1, stgpath_bufsz);
		stpe = stps + (stgpath_bufsz/sizeof(struct stgpath_entry));
		nbpath_ent = localst.st_size / sizeof(struct stgpath_entry);
		read (spfd, (char *) stps, localst.st_size);
		close (spfd);
		if (localst.st_size != nbpath_ent * sizeof(struct stgpath_entry)) {
			stglogit (func, STG48, STGPATH);
			stglogit (func, "Exit.\n");
			exit (SYERR);
		}
		for (stpp = stps, i = 0; i < nbpath_ent; i++, stpp++)
			if (stpp->reqid == 0) {
				stglogit (func, STG48, STGPATH);
				stglogit (func, "Exit.\n");
				exit (SYERR);
			}
	}

#endif

	/* Counters on the number of entries in STAGEOUT status */
	nbstageout = nbstagealloc = 0;
	/* remove uncompleted requests */
	for (stcp = stcs; stcp < stce; ) {
		int rc_upd_fileclass;

		if (stcp->reqid == 0) {
			break;
		}
		if (stcp->ipath[0] != '\0') {
			if (stcp->poolname[0] != '\0') {
				char *p;

				/* Check that poolname from catalog match current startup configuration */
				p = findpoolname(stcp->ipath);
				if ((p != NULL) && (strcmp(p, stcp->poolname) != 0)) {
					stglogit (func, STG164, stcp->ipath, stcp->poolname, p);
					strcpy(stcp->poolname, p);
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				} else if (p == NULL) {
					stglogit (func, STG164, stcp->ipath, stcp->poolname, "");
					stcp->poolname[0] = '\0';
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				}
			} else {
				char *p;
				
				/* Would it possible that this filesystem is 'back' to stager configuration ? */
				p = findpoolname(stcp->ipath);
				if (p != NULL) {
					stglogit (func, STG164, stcp->ipath, stcp->poolname, p);
					strcpy(stcp->poolname, p);
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
				}
			}
		}
		/* Check fileclass if it is a CASTOR file */
		if (stcp->t_or_d == 'h') {
			if ((stcp->status == STAGEOUT) || (stcp->status == (STAGEOUT|CAN_BE_MIGR|PUT_FAILED))) {
				/* Force Cns_statx if it is a STAGEOUT or a STAGEOUT|CAN_BE_MIGR|PUT_FAILED file */
				/* STAGEOUT files will be moved to STAGEOUT|CAN_BE_MIGR|PUT_FAILED automatically */
				if ((rc_upd_fileclass = upd_fileclass(NULL,stcp,2,0,0)) < 0) {
					/* Try to fetch fileclass only unless record was cleared */
					if (serrno == ESTCLEARED) {
						continue;
					}
					upd_fileclass(NULL,stcp,4,0,0);
				}
			} else {
				if ((rc_upd_fileclass = upd_fileclass(NULL,stcp,0,0,0)) < 0) {
					/* Try to fetch fileclass only unless record was cleared */
					if (serrno == ESTCLEARED) {
						continue;
					}
					upd_fileclass(NULL,stcp,4,0,0);
				}
			}
		} else {
			rc_upd_fileclass = -1;
		}
		if ((((stcp->status & 0xF) == STAGEIN) && ((stcp->status & 0xF0) != STAGED)) ||
			(stcp->status == (STAGEOUT|WAITING_SPC)) ||
			(stcp->status == (STAGEOUT|WAITING_NS)) ||
			(stcp->status == (STAGEALLOC|WAITING_SPC))
			) {
			if (delfile (stcp, 0, 0, 1, "startup", 0, 0, 0, 0, 0) < 0) { /* remove incomplete file */
				stglogit (func, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath),
						  rfio_serror());
				stcp++;
			}
		} else if ((stcp->status == STAGEOUT) ||
				   (stcp->status == STAGEALLOC)) {
			u_signed64 actual_size_block;
			off_t previous_actual_size = stcp->actual_size;
			
			PRE_RFIO;
			if (RFIO_STAT64(stcp->ipath, &st) == 0) {
				stcp->actual_size = st.st_size;
				if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
					actual_size_block = stcp->actual_size;
				}
#ifdef USECDB
				if (previous_actual_size != stcp->actual_size) {
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
				}
#endif
			} else {
				stglogit (func, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
				/* No block information - assume mismatch with actual_size will be acceptable */
				actual_size_block = stcp->actual_size;
			}
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
						(signed64) ((signed64) actual_size_block - (signed64) stcp->size));
			last_upd_fileclasses = time(NULL); /* To make rwcountersfs() accept the update */
			rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, stcp->status);
			last_upd_fileclasses = 0;
			stcp++;
		} else if (stcp->status == (STAGEIN|STAGED|STAGE_RDONLY)) {
			delreq (stcp,0);
		} else if (stcp->status == STAGEWRT) {
			delreq (stcp,0);
		} else if ((stcp->status & (STAGEWRT|CAN_BE_MIGR)) == (STAGEWRT|CAN_BE_MIGR)) {
			delreq (stcp,0);
		} else if (stcp->status == STAGEPUT) {
			stcp->status = STAGEOUT|PUT_FAILED;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			stcp++;
		} else if (stcp->status == (STAGEPUT|CAN_BE_MIGR)) {
			stcp->status = STAGEOUT|CAN_BE_MIGR;
			/* This is a file for automatic migration */
			update_migpool(&stcp,1,0);
			stcp++;
		} else if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
			if ((stcp->status == (STAGEOUT|CAN_BE_MIGR|PUT_FAILED)) &&
				(rc_upd_fileclass < 0)) {
				/* We know this file cannot be migrated */
			} else {
				stcp->status = STAGEOUT|CAN_BE_MIGR;
				/* This is a file for automatic migration */
				update_migpool(&stcp,1,0);
			}
			stcp++;
		} else {
			stcp++;
		}
	}

	/* Do we take into account nb of streams when allocation a filesystem or a pool in READ_MODE ? */
	if (((selectfs_p = getconfent("STG", "SELECTFS_RO_WITH_NB_STREAM", 0)) != NULL) && (strcmp(selectfs_p,"YES") == 0)) {
		selectfs_ro_with_nb_stream = 1;
	} else {
		selectfs_ro_with_nb_stream = 0;
	}

	/* Do we take into account nb of streams when allocation a filesystem or a pool in WRITE_MODE ? */
	if (((selectfs_p = getconfent("STG", "SELECTFS_RW_WITH_NB_STREAM", 0)) != NULL) && (strcmp(selectfs_p,"YES") == 0)) {
		selectfs_rw_with_nb_stream = 1;
	} else {
		selectfs_rw_with_nb_stream = 0;
	}

	stglogit(func,"selectfs_ro_with_nb_stream=%d, selectfs_rw_with_nb_stream=%d\n", selectfs_ro_with_nb_stream, selectfs_rw_with_nb_stream);

	/* Get default upd_fileclasses_int from configuration if any (not from getenv because conf is re-checked again) */
	if ((upd_fileclasses_int_p = getconfent("STG", "UPD_FILECLASSES_INT", 0)) != NULL) {
		if ((upd_fileclasses_int = atoi(upd_fileclasses_int_p)) <= 0) {
			stglogit(func, STG02, "upd_fileclasses_int init", "upd_fileclasses_int value from config", upd_fileclasses_int_p);
			stglogit(func, STG33, "upd_fileclasses_int init value from default", u64tostr((u_signed64) upd_fileclasses_int_default, tmpbuf, 0));
			upd_fileclasses_int = upd_fileclasses_int_default;
		} else {
			stglogit(func, STG33, "upd_fileclasses_int init value from config", upd_fileclasses_int_p);
		}
	} else {
		stglogit(func, STG33, "upd_fileclasses_int init value from default", u64tostr((u_signed64) upd_fileclasses_int_default, tmpbuf, 0));
		upd_fileclasses_int = upd_fileclasses_int_default;
	}

	/* Initialize check_upd_fileclasses time */
	last_upd_fileclasses = time(NULL);

	/* Initializing Last Init time */
#ifdef MONITOR

	last_init_time = last_upd_fileclasses;

	{
		char buf[CA_MAXHOSTNAMELEN + 10]; 
		if (Cmonit_get_monitor_address(buf) == 0) {
			monitorok = 1;
			stglogit(func, "Sending monitoring information to: %s\n", buf);
		} else {
			monitorok = 0;
			stglogit(func, "### Cannot send monitoring information: %s\n", sstrerror(serrno));
		}
	}
#endif

	stglogit(func, "Starting with %d free file descriptors (system max: %d)\n", (int) FREE_FD, (int) sysconf(_SC_OPEN_MAX));

	/* main loop */
	while (1) {

#ifdef MONITOR
		/* @@@@@@@@@@@@@ MONITOR @@@@@@@@@@@@@ */
		/* Every xxx seconds call monitor API  */
		/* @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@  */
		if (monitorok) {
			check_send_monitormsg();
		}
#endif
		/* Before accept() we execute some automatic thing that are client disconnected */
	        rpfd = -1;

		stgcat_shrunk_pages();
		stgpath_shrunk_pages();
		check_upd_fileclasses (); /* update all CASTOR fileclasses regularly */
		check_delaymig (); /* move delay_migr to can_be_migr if any */
		check_expired (); /* delete expired stageout/stagealloc entries */
		check_child_exit(); /* check childs [pid,status] */
		checkpoolstatus ();	/* check if any pool just cleaned */
		checkwaitingspc ();	/* check requests that are waiting for space */
		checkpoolspace ();	/* launch gc if necessary */
		checkwaitq ();	/* scan the wait queue */
		if ((stat (NOMORESTAGE, &localst) != 0) && (stat (NOMOREMIGR, &localst) != 0)) checkfile2mig ();	/* scan the pools vs. their migration policies */
		if ((shutdownreq_reqid != 0) && (waitqp == NULL || force_shutdown)) {
			/* We send a kill to all processes we are aware about */
			killallovl(SIGINT);
		}
		if ((initreq_reqid != 0) && (waitqp == NULL || force_init)) {
			/* reread pool configuration + adjust space */
			reqid = initreq_reqid;
			rpfd = initreq_rpfd;
			c = updpoolconf (defpoolname, defpoolname_in, defpoolname_out);
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if ((stcp->status == STAGEIN) ||
					(stcp->status == STAGEOUT) ||
					(stcp->status == STAGEALLOC)) {
					u_signed64 actual_size_block;

					PRE_RFIO;
					if (RFIO_STAT64(stcp->ipath, &st) == 0) {
						stcp->actual_size = st.st_size;
						if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
							actual_size_block = stcp->actual_size;
						}
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
							stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
					} else {
						stglogit (func, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
						/* No block information - assume mismatch with actual_size will be acceptable */
						actual_size_block = stcp->actual_size;
					}
					updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
							(signed64) ((signed64) actual_size_block - (signed64) stcp->size));
				}
			}
			if (c != 0) {
				sendrep (&rpfd, MSG_ERR, STG09, stgconfigfile, "incorrect");
			} else {
#ifdef MONITOR
				/* @@@@@@@@@@@@@@@@@@@@@@@ */
				/* RESET MONITOR KNOWLEDGE */
				/* @@@@@@@@@@@@@@@@@@@@@@@ */
				if (monitorok) {
					last_init_time = time(NULL);
				}
#endif
			  stglogit(func, "Working with %d free file descriptors (system max: %d)\n", (int) FREE_FD, (int) sysconf(_SC_OPEN_MAX));
			}
			sendrep (&rpfd, STAGERC, initreq_req_type, STGMAGIC, c);
			force_init = migr_init = 0;
			initreq_reqid = 0;

			/* Re-do scan for selectfs() behaviour */
			/* Do we take into account nb of streams when allocation a filesystem or a pool in READ_MODE ? */
			if (((selectfs_p = getconfent("STG", "SELECTFS_RO_WITH_NB_STREAM", 0)) != NULL) && (strcmp(selectfs_p,"YES") == 0)) {
				selectfs_ro_with_nb_stream = 1;
			} else {
				selectfs_ro_with_nb_stream = 0;
			}
			
			/* Do we take into account nb of streams when allocation a filesystem or a pool in WRITE_MODE ? */
			if (((selectfs_p = getconfent("STG", "SELECTFS_RW_WITH_NB_STREAM", 0)) != NULL) && (strcmp(selectfs_p,"YES") == 0)) {
				selectfs_rw_with_nb_stream = 1;
			} else {
				selectfs_rw_with_nb_stream = 0;
			}
			
			stglogit(func,"selectfs_ro_with_nb_stream=%d, selectfs_rw_with_nb_stream=%d\n", selectfs_ro_with_nb_stream, selectfs_rw_with_nb_stream);
		}

		if (FD_ISSET (stg_s, &readfd)) {
			struct waitq *wqp;

			if ((rqfd = accept (stg_s, (struct sockaddr *) &from, &fromlen)) < 0) {
				stglogit (func, STG02, "", "accept",
										strerror(errno));
				goto select_continue;
			}
#if (defined(SOL_SOCKET) && defined(SO_KEEPALIVE))
			/* Set socket option */
			setsockopt(rqfd,SOL_SOCKET,SO_KEEPALIVE,(char *) &on,sizeof(on));
#endif

			/* We check that what returned accept() is not a pending file descriptor */
			/* that we keep in our waiting queue and that could be invalid because of */
			/* any connection broken in the TCP/IP layer. If we find any fd in the waitq */
			/* that is equal to what accept() have just give to us, this mean that we have */
			/* to invalidate the fd with same value inside the waitq. Putting it to value -1 */
			/* is enough : this found element in the waitq will then work in nowait mode */
			/* Of course we do not have to do close(rqfd) since we just received it! */
			for (wqp = waitqp; wqp; wqp = wqp->next) {
				if ((wqp->rpfd == rqfd) || (wqp->save_rpfd == rqfd)) {
					int sav_reqid = reqid;
					reqid = wqp->reqid;
					stglogit (func, "### STG02 - Socket fd %d taken over by new request - Moving to nowait(->silent) mode\n", (wqp->save_rpfd > 0) ? wqp->save_rpfd : wqp->rpfd);
					wqp->rpfd = -1;
					wqp->save_rpfd = -1;
					wqp->silent = 1;
					reqid = sav_reqid;
					/* In theory this cannot happen more than once - so we break now */
					break;
				}
			}
			if (getpeername (rqfd, (struct sockaddr*)&from,
										 &fromlen) < 0) {
				int sav_reqid = reqid;
				reqid = 0;
				stglogit (func, STG02, "", "getpeername",
										strerror(errno));
				reqid = sav_reqid;
			}
			hp = Cgethostbyaddr((char *)(&from.sin_addr),sizeof(struct in_addr),from.sin_family);
			if (hp == NULL)
				clienthostptr = inet_ntoa (from.sin_addr);
			else
				clienthostptr = hp->h_name ;

			if (clienthostptr != NULL) {
				strncpy(clienthost,clienthostptr,CA_MAXHOSTNAMELEN);
				clienthost[CA_MAXHOSTNAMELEN] = '\0';
			} else {
				strcpy(clienthost,"");
			}
			reqid = nextreqid();
			l = netread_timeout (rqfd, req_hdr, sizeof(req_hdr), STGTIMEOUT);
			if (l == sizeof(req_hdr)) {

				rbp = req_hdr;
				unmarshall_LONG (rbp, magic);

				if ((magic != STGMAGIC) && (magic != STGMAGIC2) && (magic != STGMAGIC3) && (magic != STGMAGIC4)) {
					stglogit(func, STG141, (unsigned long) magic);
					netclose(rqfd);
					goto endreq;
				}

				unmarshall_LONG (rbp, req_type);
				unmarshall_LONG (rbp, msglen);
				rpfd = rqfd;
				l = msglen - sizeof(req_hdr);
				if (req_data != NULL) {
					free(req_data);
					req_data = NULL;
				}
				if (l > MAX_NETDATA_SIZE) {
					stglogit(func, "STG45 - request too big (%s bytes)\n", u64tostr((u_signed64) l, tmpbuf, 0));
					netclose(rqfd);
					goto endreq;
				}
				if ((req_data = (char *) malloc((size_t) l)) == NULL) {
					stglogit(func, "STG45 - malloc of %s bytes error (%s)\n", u64tostr((u_signed64) l, tmpbuf, 0), strerror(errno));
					netclose(rqfd);
					goto endreq;
				}
				if ((read_size = netread_timeout (rqfd, req_data, l, STGTIMEOUT)) == l) {
					if (req_type == STAGEIN   || req_type == STAGEOUT  || req_type == STAGEALLOC  ||
						req_type == STAGEWRT  || req_type == STAGEPUT  ||

						req_type == STAGE_IN  || req_type == STAGE_OUT || req_type == STAGE_ALLOC ||
						req_type == STAGE_WRT || req_type == STAGE_PUT) {
						if ((initreq_reqid != 0) || (shutdownreq_reqid != 0) || (stat (NOMORESTAGE, &localst) == 0)) {
							sendrep (&rpfd, STAGERC, req_type, magic, SHIFT_ESTNACT);
							goto endreq;
						}
					}
					if ((req_type == STAGE_UPDC) && (magic != STGMAGIC2) && (magic != STGMAGIC3) && (magic != STGMAGIC4)) {
						/* The API of stage_updc only exist with magic number version >= 2 */
						stglogit(func, STG141, (unsigned long) magic);
						netclose(rqfd);
						goto endreq;
					}
					if ((req_type == STAGE_CLR) && (magic != STGMAGIC2) && (magic != STGMAGIC3) && (magic != STGMAGIC4)) {
						/* The API of stage_clr only exist with magic number version >= 2 */
						stglogit(func, STG141, (unsigned long) magic);
						netclose(rqfd);
						goto endreq;
					}
					if ((req_type == STAGE_INIT     ||
						 req_type == STAGE_SHUTDOWN ||
						 req_type == STAGE_ALLOC    ||
						 req_type == STAGE_GET) && (magic != STGMAGIC4)) {
						/* The API of stage_init/stage_shutdown/stage_alloc/stage_get only exist */
						/* with magic number version >= 4 */
						stglogit(func, STG141, (unsigned long) magic);
						netclose(rqfd);
						goto endreq;
					}

					if (req_type > STAGE_00) {
						if (magic == STGMAGIC) {
							/* Firt version of the API was only accepting a uniqueid given by stgdaemon */
							++stage_uniqueid;
							sendrep(&rpfd, UNIQUEID, stage_uniqueid);
						}
					}

					switch (req_type) {
					case STAGE_IN:
					case STAGE_OUT:
					case STAGE_WRT:
					case STAGE_CAT:
					case STAGEIN:
					case STAGEOUT:
					case STAGEWRT:
					case STAGECAT:
						procioreq (req_type, magic, req_data, clienthost);
						break;
					case STAGEPUT:
						procputreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_QRY:
					case STAGEQRY:
						procqryreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_PING:
					case STAGEPING:
						procpingreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_CLR:
					case STAGECLR:
						procclrreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_KILL:
					case STAGEKILL:
						prockilreq (req_type, req_data, clienthost);
						break;
					case STAGE_UPDC:
					case STAGEUPDC:
						procupdreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_INIT:
					case STAGEINIT:
						procinireq (req_type, magic, (unsigned long) from.sin_addr.s_addr, req_data, clienthost);
						break;
					case STAGE_ALLOC:
					case STAGEALLOC:
						procallocreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_GET:
					case STAGEGET:
						procgetreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_FILCHG:
					case STAGEFILCHG:
						procfilchgreq (req_type, magic, req_data, clienthost);
						break;
					case STAGE_SHUTDOWN:
					case STAGESHUTDOWN:
						procshutdownreq (req_type, magic, (unsigned long) from.sin_addr.s_addr, req_data, clienthost);
						break;
					default:
						sendrep (&rpfd, MSG_ERR, STG03, req_type);
						sendrep (&rpfd, STAGERC, req_type, magic, EINVAL);
					}
				} else {
					netclose (rqfd);
					if (l != 0)
						stglogit (func, STG04, "body", (int) l, (int) read_size, clienthost, errno, strerror(errno), serrno, sstrerror(serrno));
				}
			} else {
				netclose (rqfd);
				if (l != 0)
					stglogit (func, STG04, "header", (int) l, (int) sizeof(req_hdr), clienthost, errno, strerror(errno), serrno, sstrerror(serrno));
			}
		endreq:
			FD_CLR (rqfd, &readfd);
		}
	select_continue:
		memcpy (&readfd, &readmask, sizeof(readmask));
		timeval.tv_sec = CHECKI;	/* must set each time for linux */
		timeval.tv_usec = 0;
		if (select (stg_s + 1, &readfd, (fd_set *) NULL, (fd_set *) NULL, &timeval) < 0) {
			FD_ZERO (&readfd);
		}
#ifdef STAGE_CSETPROCNAME
        if (Csetprocname(STAGE_CSETPROCNAME_FORMAT, sav_argv0, stgdaemon_port, nbcat_ent, nbpath_ent, nwaitq, FREE_FD) != 0) {
			stglogit(func, "### Csetprocname error, errno=%d (%s), serrno=%d (%s)\n", errno, strerror(errno), serrno, sstrerror(serrno));
		}
#endif
	}
}

void prockilreq(req_type, req_data, clienthost)
		 int req_type;
		 char *req_data;
		 char *clienthost;
{
	int clientpid;
	uid_t uid;
	gid_t gid;
	char *rbp;
	char *user;
	struct waitq *wqp;
	u_signed64 this_uniqueid;
	int found = 0;

	rbp = req_data;
	switch (req_type) {
	case STAGEKILL:
		/* Command-line version */
		unmarshall_STRING (rbp, user);	/* login name */
		unmarshall_WORD (rbp, gid);
		unmarshall_WORD (rbp, clientpid);

		wqp = waitqp;
		while (wqp) {
			if (wqp->clientpid == clientpid && strcmp (wqp->clienthost, clienthost) == 0 &&
				wqp->req_gid == gid && strcmp (wqp->req_user, user) == 0) {
				stglogit (func, "kill received for request %d\n", wqp->reqid);
				found = 1;
				if (wqp->ovl_pid) {
					stglogit (func, "killing process %d\n", wqp->ovl_pid);
					kill (wqp->ovl_pid, SIGINT);
				}
				wqp->status = ESTKILLED;
				break;
			} else {
				wqp = wqp->next;
			}
		}
		if (found == 0) {
			stglogit(func, "kill received but ignored (no overlay), clientpid=%d, clienthost=%s, user=%s, gid=%d\n", clientpid, clienthost, user, gid);
		}
		netclose (rpfd);
		break;
	case STAGE_KILL:
		/* API version */
		unmarshall_STRING (rbp, user);	/* login name */
		unmarshall_LONG (rbp, uid);
		unmarshall_LONG (rbp, gid);
		unmarshall_HYPER (rbp, this_uniqueid);

		wqp = waitqp;
		while (wqp) {
          /* This separated line allows any root to kill any forked stager */
			if (uid != 0 || gid != 0) {
				if (strcmp(wqp->clienthost, clienthost) != 0 ||
					strcmp(wqp->req_user, user) != 0) {
					wqp = wqp->next;
					continue;
				}
			}
			if (wqp->uniqueid == this_uniqueid) {
				stglogit (func, "kill received for request %d\n", wqp->reqid);
				found = 1;
				if (wqp->ovl_pid) {
					stglogit (func, "killing process %d\n", wqp->ovl_pid);
					kill (wqp->ovl_pid, SIGINT);
				}
				wqp->status = ESTKILLED;
				/* This will close cleanly the connection from the API */
				if (wqp->save_rpfd >= 0) {
					wqp->rpfd = wqp->save_rpfd;
					wqp->save_rpfd = -1;
				}
				sendrep (&(wqp->rpfd), STAGERC, 0, wqp->magic, ESTKILLED);
				break;
			} else {
				wqp = wqp->next;
			}
		}
		if (found == 0) {
			stglogit(func, "kill received but ignored (no overlay), uniqueid gives Pid=0x%lx (%d) CthreadId+1=0x%lx (-> Tid=%d), clienthost=%s, user=%s (%d,%d)\n",
				(unsigned long) (this_uniqueid / 0xFFFFFFFF),
				(int) (this_uniqueid / 0xFFFFFFFF),
				(unsigned long) (this_uniqueid - (this_uniqueid / 0xFFFFFFFF) * 0xFFFFFFFF),
				(int) (this_uniqueid - (this_uniqueid / 0xFFFFFFFF) * 0xFFFFFFFF) - 1,
				clienthost, user, (int) uid, (int) gid);
		}
		netclose (rpfd);
		break;
	default:
		break;
	}
}

void procinireq(req_type, magic_client, ipaddr, req_data, clienthost)
		 int req_type;
		 int magic_client;
		 unsigned long ipaddr;
		 char *req_data;
		 char *clienthost;
{
	char **argv = NULL;
	int c;
	gid_t gid;
	uid_t uid = -1;
	int nargs = 0;
	char *rbp;
	char *user;
	char *p;
	int errflg = 0;
	char *func0 = "stageinit";
	char *func1 = "stage_init";
	char *func;
	struct passwd *this_passwd;             /* password structure pointer */
	u_signed64 flags = 0;

	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	if (req_type > STAGE_00) {
		func = func1;
		unmarshall_LONG (rbp, uid);
		unmarshall_LONG (rbp, gid);
		unmarshall_HYPER(rbp, flags);
		stglogit (func, "STG92 - %s request by %s (%d,%d) from %s\n", func, user, uid, gid, clienthost);
		stglogflags(func,LOGFILE,(u_signed64) flags);
	} else {
		func = func0;
		unmarshall_WORD (rbp, gid);
		stglogit (func, "STG92 - %s request by %s (,%d) from %s\n", func, user, gid, clienthost);
		nargs = req2argv (rbp, &argv);
	}
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost, reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif
	if (initreq_reqid != 0) {
		if (argv != NULL) free (argv);
		sendrep (&rpfd, MSG_ERR, STG39);
		sendrep (&rpfd, STAGERC, req_type, magic_client, EINVAL);
		return;
	}
	if (uid == -1) {
		/* Old protocol without uid - Known user ? */
		if ((this_passwd = Cgetpwnam(user)) == NULL) {
			stglogit(func, "### Cannot Cgetpwnam(%s) (%s)\n",user, strerror(errno));
			stglogit(func, "### Please check existence of \"%s\" in password file\n", user);
			serrno = SEUSERUNKN;
			sendrep (&rpfd, MSG_ERR, STG02, user, func, sstrerror(serrno));
			sendrep (&rpfd, STAGERC, req_type, magic_client, serrno);
			return;
		}
		uid = this_passwd->pw_uid;
		/* Shall I log this ? */
		/*
		  if (this_passwd->pw_gid != gid) {
		  stglogit(func, "### User \"%s\"'s gid is %d != %d from the protocol ?\n",user, this_passwd->pw_gid, gid);
		  }
		*/
	}
	/* Allowed to do so ? */
	if (Cupv_check(uid, gid, clienthost, localhost, P_ADMIN) != 0) {
		sendrep (&rpfd, MSG_ERR, STG02, "", func, sstrerror(serrno));
		sendrep (&rpfd, STAGERC, req_type, magic_client, serrno);
		return;
	}
	/* - Do we want to force it to come from local host ? */
	if ((p = getconfent("STG", "STAGEINIT_FROM_LOCALHOST", 0)) != NULL) {
		if (atoi(p) != 0) {
			if (! have_ipaddrlocal) {
				stglogit (func, "[STAGEINIT_FROM_LOCALHOST] Localhost's %s IP address unavailable\n", localhost);
			} else {
				/* Verify remote IP address */
				if (ipaddr != ipaddrlocal) {
					unsigned char *s_client = (unsigned char *) &ipaddr;
					unsigned char *s_local  = (unsigned char *) &ipaddrlocal;

					if (argv != NULL) free (argv);
					stglogit (func, "[STAGEINIT_FROM_LOCALHOST] Requestor's %s IP address (%d.%d.%d.%d) != Localhost's %s IP address (%d.%d.%d.%d)\n", clienthost, s_client[0] & 0xFF, s_client[1] & 0xFF, s_client[2] & 0xFF, s_client[3] & 0xFF, localhost, s_local[0] & 0xFF, s_local[1] & 0xFF, s_local[2] & 0xFF, s_local[3] & 0xFF);
					serrno = EACCES;
					sendrep (&rpfd, MSG_ERR, STG02, clienthost, func, sstrerror(serrno));
					sendrep (&rpfd, STAGERC, req_type, magic_client, serrno);
					return;
				}
			}
		}
	}
	if (req_type < STAGE_00) {
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt (nargs, argv, "Fh:X")) != -1) {
			switch (c) {
			case 'F':
				force_init = 1;
				break;
			case 'h':
				break;
			case 'X':
				migr_init = 1;
				break;
			case '?':
				errflg++;
				break;
			default:
				errflg++;
				break;
			}
		}
		if (! errflg) {
			initreq_reqid = reqid;
			initreq_rpfd = rpfd;
		} else {
			sendrep (&rpfd, MSG_ERR, "usage: stageinit [-F] [-h stage_host] [-X]\n");
			sendrep (&rpfd, STAGERC, req_type, STGMAGIC, EINVAL);
		}
		if (argv != NULL) free (argv);
	} else {
		if ((flags & STAGE_FORCE) == STAGE_FORCE) force_init = 1;
		if ((flags & STAGE_MIGRINIT) == STAGE_MIGRINIT) migr_init = 1;
		initreq_reqid = reqid;
		initreq_rpfd = rpfd;
	}
}

void procshutdownreq(req_type, magic_client, ipaddr, req_data, clienthost)
	int req_type;
	int magic_client;
	unsigned long ipaddr;
	char *req_data;
	char *clienthost;
{
	char **argv = NULL;
	int c;
	uid_t uid = -1;
	gid_t gid;
	int nargs = 0;
	char *rbp;
	char *user;
	char *stghost = NULL;
	int errflg = 0;
	int force_shutdown = 0;
	char *func0 = "stageshutdown";
	char *func1 = "stage_shutdown";
	char *func;
	char *p;
	struct passwd *this_passwd;             /* password structure pointer */
	u_signed64 flags = 0;

	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	if (req_type > STAGE_00) {
		func = func1;
		unmarshall_LONG (rbp, uid);
		unmarshall_LONG (rbp, gid);
		unmarshall_HYPER(rbp, flags);
		stglogit (func, "STG92 - %s request by %s (%d,%d) from %s\n", func, user, uid, gid, clienthost);
		stglogflags(func,LOGFILE,(u_signed64) flags);
	} else {
		func = func0;
		unmarshall_WORD (rbp, gid);
		stglogit (func, "STG92 - %s request by %s (,%d) from %s\n", func, user, gid, clienthost);
		nargs = req2argv (rbp, &argv);
	}
#if SACCT
	stageacct (STGCMDR, uid, gid, clienthost, reqid, req_type, 0, 0, NULL, "", (char) 0);
#endif
	if (shutdownreq_reqid != 0) {
		if (argv != NULL) free (argv);
		sendrep (&rpfd, MSG_ERR, STG58);
		sendrep (&rpfd, STAGERC, req_type, magic_client, EINVAL);
		return;
	}
	if (uid == -1) {
		/* Old protocol without uid - Known user ? */
		if ((this_passwd = Cgetpwnam(user)) == NULL) {
			stglogit(func, "### Cannot Cgetpwnam(%s) (%s)\n",user, strerror(errno));
			stglogit(func, "### Please check existence of \"%s\" in password file\n", user);
			serrno = SEUSERUNKN;
			sendrep (&rpfd, MSG_ERR, STG02, user, func, sstrerror(serrno));
			sendrep (&rpfd, STAGERC, req_type, magic_client, serrno);
			return;
		}
		uid = this_passwd->pw_uid;
		/* Shall I log this ? */
		/*
		  if (this_passwd->pw_gid != gid) {
		  stglogit(func, "### User \"%s\"'s gid is %d != %d from the protocol ?\n",user, this_passwd->pw_gid, gid);
		  }
		*/
	}
	/* Allowed to do so ? */
	if (Cupv_check(uid, gid, clienthost, localhost, P_ADMIN) != 0) {
		sendrep (&rpfd, MSG_ERR, STG02, "", func, sstrerror(serrno));
		sendrep (&rpfd, STAGERC, req_type, magic_client, serrno);
		return;
	}
	/* - Do we want to force it to come from local host ? */
	if ((p = getconfent("STG", "STAGESHUTDOWN_FROM_LOCALHOST", 0)) != NULL) {
		if (atoi(p) != 0) {
			if (! have_ipaddrlocal) {
				stglogit (func, "[STAGESHUTDOWN_FROM_LOCALHOST] Localhost's %s IP address unavailable\n", localhost);
			} else {
				/* Verify remote IP address */
				if (ipaddr != ipaddrlocal) {
					unsigned char *s_client = (unsigned char *) &ipaddr;
					unsigned char *s_local  = (unsigned char *) &ipaddrlocal;

					if (argv != NULL) free (argv);
					stglogit (func, "[STAGESHUTDOWN_FROM_LOCALHOST] Requestor's %s IP address (%d.%d.%d.%d) != Localhost's %s IP address (%d.%d.%d.%d)\n", clienthost, s_client[0] & 0xFF, s_client[1] & 0xFF, s_client[2] & 0xFF, s_client[3] & 0xFF, localhost, s_local[0] & 0xFF, s_local[1] & 0xFF, s_local[2] & 0xFF, s_local[3] & 0xFF);
					serrno = EACCES;
					sendrep (&rpfd, MSG_ERR, STG02, clienthost, func, sstrerror(serrno));
					sendrep (&rpfd, STAGERC, req_type, magic_client, serrno);
					return;
				}
			}
		}
	}

	if (req_type < STAGE_00) {
		Coptind = 1;
		Copterr = 0;
		while ((c = Cgetopt (nargs, argv, "Fh:")) != -1) {
			switch (c) {
			case 'F':
				force_shutdown = 1;
				break;
			case 'h':
				stghost = Coptarg;
				break;
			case '?':
				errflg++;
				break;
			default:
				errflg++;
				break;
			}
		}
		/* We force -h parameter to appear in the parameters */
		if (stghost == NULL) {
			errflg++;
		}

		if (errflg != 0) {
			free (argv);
			sendrep (&rpfd, MSG_ERR, STG33, "stageshutdown", "invalid option(s)");
			sendrep (&rpfd, STAGERC, STAGESHUTDOWN, STGMAGIC, EINVAL);
			return;
		}

		shutdownreq_reqid = reqid;
		shutdownreq_rpfd = rpfd;
		if (argv != NULL) free (argv);
	} else {
		if ((flags & STAGE_FORCE) == STAGE_FORCE) force_shutdown = 1;
		shutdownreq_reqid = reqid;
		shutdownreq_rpfd = rpfd;
	}
}

struct waitq *
add2wq (clienthost, req_user, req_uid, req_gid, rtcp_user, rtcp_group, rtcp_uid, rtcp_gid, clientpid, Upluspath, reqid, req_type, nbwf, wfp, save_subreqid, vid, fseq, use_subreqid)
		 char *clienthost;
		 char *req_user;
		 uid_t req_uid;
		 gid_t req_gid;
		 char *rtcp_user;
		 char *rtcp_group;
		 uid_t rtcp_uid;
		 gid_t rtcp_gid;
		 int clientpid;
		 int Upluspath;
		 int reqid;
		 int req_type;
		 int nbwf;
		 struct waitf **wfp;
		 int **save_subreqid;
		 char *vid;
		 char *fseq;
		 int use_subreqid;
{
	/* add request to the wait queue */
	struct waitq *prev = NULL, *wqp;

	wqp = waitqp;
	while (wqp) {
		prev = wqp;
		wqp = wqp->next;
	}
	wqp = (struct waitq *) calloc (1, sizeof(struct waitq));
	if (!waitqp) {	/* queue is empty */
		waitqp = wqp;
	} else {
		prev->next = wqp;
		wqp->prev = prev;
	}
	strcpy (wqp->clienthost, clienthost);
	strcpy (wqp->req_user, req_user);
	wqp->req_uid = req_uid;
	wqp->req_gid = req_gid;
	strcpy (wqp->rtcp_user, rtcp_user);
	strcpy (wqp->rtcp_group, rtcp_group);
	wqp->rtcp_uid = rtcp_uid;
	wqp->rtcp_gid = rtcp_gid;
	wqp->clientpid = clientpid;
	wqp->Upluspath = Upluspath;
	wqp->reqid = reqid;
	wqp->req_type = req_type;
	wqp->key = time(NULL) & 0xFFFF;
	wqp->rpfd = rpfd;
	wqp->save_rpfd = -1;
	wqp->use_subreqid = use_subreqid;
	wqp->wf = (struct waitf *) calloc (nbwf, sizeof(struct waitf));
	wqp->last_rwcounterfs_vs_R = 0;
	wqp->last_rwcounterfs_index = -1;
	wqp->flags = (u_signed64) 0;
	if (wqp->use_subreqid != 0) {
		if ((wqp->save_subreqid = *save_subreqid = (int *) calloc (nbwf, sizeof(int))) == NULL) {
			sendrep (&rpfd, MSG_ERR, STG33, "malloc", strerror(errno));
			wqp->use_subreqid = 0;
		} else {
			wqp->save_nbsubreqid = nbwf;
		}
	}
	*wfp = wqp->wf;
    ++nwaitq;
	return (wqp);
}

int
add2otherwf(wqp_orig,fseq_orig,wfp_orig,wfp_new)
	struct waitq *wqp_orig;
	char *fseq_orig;
	struct waitf *wfp_orig;
	struct waitf *wfp_new;
{
	struct waitq *wqp;
	struct waitf *newwaitf, *wfp;
	struct stgcat_entry *stcp;
	struct stgcat_entry *stcp_ok;
	struct waitf *wfp_ok;
	int i, found, index_found;
	int save_reqid, save_nextreqid;
	int rc = 0;

	save_reqid = reqid;
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		if (wqp == wqp_orig) continue;
		reqid = wqp->reqid;
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			if (wfp->waiting_on_req != wfp_orig->subreqid) continue;
			found = index_found = 0;
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if (stcp->reqid == wfp->subreqid) {
					found = 1;
					break;
				}
				index_found++;
			}
			if (! found) {
				sendrep(&(wqp->rpfd), MSG_ERR, "STG02 - Internal error : Your are waiting on a \"-c off\" another request, but corresponding member is not in the in-memory catalog\n");
				rc = -1;
				goto add2otherwf_return;
			}
			if (wqp->concat_off_fseq > 0) {
				/* We change this "-c off" waiting request only if the found index */
				/* is the very last one in the queue. */
				if (wfp != &(wqp->wf[wqp->nbdskf - 1])) {
					/* This is not the last one in this queue but it neverthless waits  */
					/* This handles the case : a "-q 3- -c off" has been splitted onto  */
					/* "-q 2" + "-q 3- -c off" because there has been a "-q 2" before.  */
					/* Then, here, all the wfp's are all waiting on an original "-q 1-" */
					/* From logical point of view this is equivalent to a normal request */
					/* waiting on another "-c off" one.                                  */
					goto normal_waiting_on_c_off;
				}
				/* If the file sequence of this "-c off" callback is higher or equal to the one */
				/* we are waiting for, then we extended the structure. */
				/* Otherwise we only update it. */
				/* This handles the case where a "-q 3- -c off", for example, is waiting on */
				/* "-q 1- -c off" one. Until the callback for file sequence 3 is reached we */
				/* only update the structure */
				if (atoi(fseq_orig) < atoi(stcp->u1.t.fseq)) {
					wfp->waiting_on_req = wfp_new->subreqid;
#ifdef STAGER_DEBUG
					sendrep(&(wqp->rpfd), MSG_ERR, "STG33 - Updated wfp for eventual tape_fseq %d-\n",atoi(fseq_orig) + 1);
#else
					stglogit(func, "add2otherwf : Updated wfp for eventual tape_fseq %d-\n",atoi(fseq_orig) + 1);
#endif
				} else {
					if ((newwaitf = (struct waitf *) realloc(wqp->wf, (wqp->nbdskf + 1) * sizeof(struct waitf))) == NULL) {
 						sendrep(&(wqp->rpfd), MSG_ERR, "STG02 - System error : realloc failed at %s:%d (%s)\n",__FILE__,__LINE__,strerror(errno));
						rc = -1;
						goto add2otherwf_return;
					}
					wqp->wf = wfp = newwaitf;
					wqp->nbdskf++;
					wqp->nb_subreqs++;
					wqp->nb_waiting_on_req++;
					stcp_ok = newreq((int) 't');
					/* The memory may have been realloced, so we follow this eventual reallocation */
					/* by reassigning stcp pointer using the found index */
					stcp = &(stcs[index_found]);
					save_nextreqid = nextreqid();
					memcpy(stcp_ok,stcp,sizeof(struct stgcat_entry));
					stcp_ok->reqid = save_nextreqid;
					/* Current stcp is "<X>-", new one is "<X+1>-" */
					sprintf(stcp_ok->u1.t.fseq,"%d",atoi(stcp->u1.t.fseq) + 1);
					strcat(stcp_ok->u1.t.fseq,"-");
					/* And it is a deffered allocation */
					stcp_ok->ipath[0] = '\0';
					wfp_ok = &(wqp->wf[wqp->nbdskf - 1]);
					memset((char *) wfp_ok,0,sizeof(struct waitf));
					wfp_ok->subreqid = stcp_ok->reqid;
					wfp_ok->waiting_on_req = wfp_new->subreqid;
					/* Current stcp become "<X>" */
					sprintf(stcp->u1.t.fseq,"%d",atoi(fseq_orig));
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
					if (stgdb_ins_stgcat(&dbfd,stcp_ok) != 0) {
						stglogit(func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					savereqs ();
#ifdef STAGER_DEBUG
					sendrep(&(wqp->rpfd), MSG_ERR, "STG33 - Extended wfp for eventual tape_fseq %s\n",stcp_ok->u1.t.fseq);
#else
					stglogit(func, "add2otherwf : Extended wfp for eventual tape_fseq %s\n",stcp_ok->u1.t.fseq);
#endif
				}
			} else {
	normal_waiting_on_c_off:
				/* This request is NOT a "-c off" one, but is neverthless waiting on another "-c off" one */
				/* We will simply update the wfp->waiting_on_req to wfp_new->subreqid, only if the fseq */
				/* given on the command is NOT what we are waiting for. */
				if (strcmp(stcp->u1.t.fseq, fseq_orig) == 0) continue;
				wfp->waiting_on_req = wfp_new->subreqid;
#ifdef STAGER_DEBUG
				sendrep(&(wqp->rpfd), MSG_ERR, "STG33 - Updated wfp for eventual tape_fseq %d\n",atoi(fseq_orig) + 1);
#else
				stglogit(func, "add2otherwf : Updated wfp for eventual tape_fseq %d\n",atoi(fseq_orig) + 1);
#endif
			}
		}
	}
 add2otherwf_return:
	reqid = save_reqid;
	return(rc);
}

struct waitf *
add2wf (wqp)
	struct waitq *wqp;
{
	/* add a file to the wait queue */
	struct waitf *newwaitf;

	if ((newwaitf = (struct waitf *) realloc(wqp->wf, (wqp->nbdskf + 1) * sizeof(struct waitf))) == NULL) {
		sendrep(&(wqp->rpfd), MSG_ERR, "STG02 - System error : realloc failed at %s:%d (%s)\n",__FILE__,__LINE__,strerror(errno));
		return(NULL);
	}
	wqp->wf = newwaitf;
	wqp->nbdskf++;
	wqp->nb_subreqs++;
	return(newwaitf);
}

int build_ipath(upath, stcp, pool_user, noallocation, api_out, openmode)
		 char *upath;
		 struct stgcat_entry *stcp;
		 char *pool_user;
		 int noallocation;
		 int api_out;
		 mode_t openmode;
{
	int c;
	int fd;
	char *p;
	char *p_f, *p_u;
	struct passwd *pw;
	mode_t okmode;

	if ((api_out == 0) || (openmode == 0)) {
		okmode = ( 0777 & ~ stcp->mask);
	} else {
		okmode = (07777 & (openmode & ~ stcp->mask));
	}

	if (ISSTAGEOUT(stcp) || ISSTAGEALLOC(stcp)) {
		/* We are in write-mode */
		if ((okmode & S_IWUSR) != S_IWUSR) {
			stglogit (func, STG33, "owner will not be able to write the file", "Forcing S_IWUSR (00200) in open mode");
			okmode |= S_IWUSR;
		}
	}

	if (*stcp->poolname == '\0' && upath != NULL) {
		if (strlen(upath) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
			stcp->ipath[0] = '\0';
			serrno = SENAMETOOLONG;
			sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(serrno));
			return(serrno);
		}
		strcpy (stcp->ipath, upath);
		return (0);
	}

	/* allocate space */

	STAGE_TIME_START("build_ipath : selectfs");
	if (selectfs (stcp->poolname, &(stcp->size), stcp->ipath, stcp->status, noallocation) < 0) {
		stcp->ipath[0] = '\0';
		return (-1);	/* not enough space */
	}
	STAGE_TIME_END;

	/* build full internal path name */

	if ((strlen(stcp->ipath) + 1 + strlen(stcp->group)) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
		serrno = SENAMETOOLONG;
		sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(serrno));
		updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
		stcp->ipath[0] = '\0';
		return(serrno);
	}
	sprintf (stcp->ipath+strlen(stcp->ipath), "/%s", stcp->group);

	p_u = stcp->ipath + strlen (stcp->ipath);
	if ((strlen(stcp->ipath) + 1 + strlen(pool_user)) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
		serrno = SENAMETOOLONG;
		sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(serrno));
		updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
		stcp->ipath[0] = '\0';
		return(serrno);
	}
	sprintf (p_u, "/%s", pool_user);

	p_f = stcp->ipath + strlen (stcp->ipath);
	if (stcp->t_or_d == 't') {
		if (*(stcp->u1.t.fseq) != 'n') {
			if (stcp->u1.t.side > 0) {
				if ((strlen(stcp->ipath) + 1 + strlen(stcp->u1.t.vid[0]) + 1 + stg_count_digits(stcp->u1.t.side,10) + 1 + strlen(stcp->u1.t.fseq) + 1 + strlen(stcp->u1.t.lbl)) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
					serrno = SENAMETOOLONG;
					sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(serrno));
					updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
					stcp->ipath[0] = '\0';
					return(serrno);
				}
				sprintf (p_f, "/%s.%d.%s.%s", stcp->u1.t.vid[0], stcp->u1.t.side, stcp->u1.t.fseq, stcp->u1.t.lbl);
			} else {
				/* Backward compatiblity */
				if ((strlen(stcp->ipath) + 1 + strlen(stcp->u1.t.vid[0]) + 1 + strlen(stcp->u1.t.fseq) + 1 + strlen(stcp->u1.t.lbl)) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
					serrno = SENAMETOOLONG;
					sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(serrno));
					updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
					stcp->ipath[0] = '\0';
					return(serrno);
				}
				sprintf (p_f, "/%s.%s.%s", stcp->u1.t.vid[0], stcp->u1.t.fseq, stcp->u1.t.lbl);
			}
		} else {
			if ((strlen(stcp->ipath) + 1 + strlen(stcp->u1.t.vid[0]) + 1 + strlen(stcp->u1.t.fseq) + 1 + strlen(stcp->u1.t.lbl) + 1 + stg_count_digits(stcp->reqid,10)) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
				serrno = SENAMETOOLONG;
				sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(serrno));
				updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
				stcp->ipath[0] = '\0';
				return(serrno);
			}
			sprintf (p_f, "/%s.%s.%s.%d", stcp->u1.t.vid[0], stcp->u1.t.fseq, stcp->u1.t.lbl, stcp->reqid);
		}
	} else {
		if ((p = strrchr (stcp->u1.d.xfile, '/')) == NULL) {
			p = stcp->u1.d.xfile;
		} else {
			p++;
		}
		if ((strlen(stcp->ipath) + 1 + strlen(p) + 1 + stg_count_digits(stcp->reqid,10)) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
			serrno = SENAMETOOLONG;
			sendrep (&rpfd, MSG_ERR, STG33, "build_ipath", sstrerror(serrno));
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return(serrno);
		}
		sprintf (p_f, "/%s.%d", p, stcp->reqid);
	}

	/* Do not create empty file on stagein/out (for Objectivity DB) */
	/* But create subdirectories if needed */

	if (get_create_file_option (stcp->poolname)) {
		/* Create group directory if needed */
		*p_u = '\0';
		STAGE_TIME_START("build_ipath : create_dir");
		c = create_dir (stcp->ipath, (uid_t) 0, stcp->gid, (mode_t) 0755);
		STAGE_TIME_END;
		*p_u = '/';
		if (c) {
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return (c);
		}
		if ((pw = Cgetpwnam (pool_user)) == NULL) {
			if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetpwnam", strerror(errno));
			sendrep (&rpfd, MSG_ERR, STG11, pool_user);
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return ((errno == ENOENT) ? SEUSERUNKN : SESYSERR);
		}
		*p_f = '\0';
		STAGE_TIME_START("build_ipath : create_dir");
		c = create_dir (stcp->ipath, pw->pw_uid, stcp->gid, (mode_t) 0775);
		STAGE_TIME_END;
		*p_f = '/';
		if (c) {
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return (c);
		}
		/* Okay */
		return(0);
	}

	/* try to create file */

	PRE_RFIO;
	STAGE_TIME_START("build_ipath : rfio_open");
	fd = rfio_open (stcp->ipath, O_WRONLY | O_CREAT, (int) okmode);
	STAGE_TIME_END;
	if (fd < 0) {
		if (errno != ENOENT && rfio_errno != ENOENT) {
			/* If rfio_open fails, it can be a real open error or a client/or/server error */
			/* We reset our internal path */
			sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_open",
							 rfio_serror());
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return (SESYSERR);
		}

		/* create group and user directories */

		*p_u = '\0';
		STAGE_TIME_START("build_ipath : create_dir");
		c = create_dir (stcp->ipath, (uid_t) 0, stcp->gid, (mode_t) 0755);
		STAGE_TIME_END;
		*p_u = '/';
		if (c) {
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return (c);
		}
		if ((pw = Cgetpwnam (pool_user)) == NULL) {
			if (errno != ENOENT) sendrep (&rpfd, MSG_ERR, STG33, "Cgetpwnam", strerror(errno));
			sendrep (&rpfd, MSG_ERR, STG11, pool_user);
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return (SESYSERR);
		}
		*p_f = '\0';
		STAGE_TIME_START("build_ipath : create_dir");
		c = create_dir (stcp->ipath, pw->pw_uid, stcp->gid, (mode_t) 0775);
		STAGE_TIME_END;
		*p_f = '/';
		if (c) {
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return (c);
        }

		/* try again to create file */

		PRE_RFIO;
		STAGE_TIME_START("build_ipath : rfio_open");
		fd = rfio_open (stcp->ipath, O_WRONLY | O_CREAT, (int) okmode);
		STAGE_TIME_END;
		if (fd < 0) {
			sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_open", rfio_serror());
			updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
			stcp->ipath[0] = '\0';
			return (SESYSERR);
		}
	}
	PRE_RFIO;
	STAGE_TIME_START("build_ipath : rfio_fchown");
	if (rfio_fchown (fd, (int) stcp->uid, (int) stcp->gid) < 0) {
		sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath, "rfio_fchown", rfio_serror());
		PRE_RFIO;
		STAGE_TIME_START("build_ipath : rfio_close");
		if (rfio_close(fd) != 0) {
			stglogit (func, STG02, stcp->ipath, "rfio_close", rfio_serror());
		}
		STAGE_TIME_END;
		/* Here file has been created but we cannot chown... We erase the file */
		PRE_RFIO;
        if (RFIO_UNLINK(stcp->ipath) != 0 && (errno != ENOENT && rfio_errno != ENOENT)) {
			stglogit (func, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
        }
		updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) stcp->size);
		stcp->ipath[0] = '\0';
		return (SESYSERR);
	}
	PRE_RFIO;
	STAGE_TIME_START("build_ipath : rfio_close");
	if (rfio_close(fd) != 0) {
		stglogit (func, STG02, stcp->ipath, "rfio_close", rfio_serror());
	}
	STAGE_TIME_END;
	STAGE_TIME_END;
	return (0);
}

#define STG_LOG_OVL_STATUS(type,pid,status) { \
	if (WIFEXITED(status)) { \
		stglogit (func, "%s process %d exiting with status %d\n", type, pid, (status = WEXITSTATUS(status))); \
	} else if (WIFSIGNALED(status)) { \
		stglogit (func, "%s process %d exiting due to uncaught signal %d\n", type, pid, (status = WTERMSIG(status))); \
	} else { \
		stglogit (func, "%s process %d was stopped\n", pid); \
        status = REQKILD; \
	} \
}

void
checkovlstatus(pid, status)
		 int pid;
		 int status;
{
	int found;
	int savereqid;
	struct waitq *wqp;

	savereqid = reqid;

	/* was it a "cleaner" overlay ? */
	if (iscleanovl (pid, status)) {
		reqid = 0;
		STG_LOG_OVL_STATUS("cleaner",pid,status);
	/* was it a "migrator" overlay ? */
	} else if (ismigovl (pid, status)) {
		reqid = 0;
		STG_LOG_OVL_STATUS("migration",pid,status);
	} else {	/* it was a "stager" or a "stageqry" overlay */
		found =  0;
		wqp = waitqp;
		while (wqp) {
			if (wqp->ovl_pid == pid) {
				found = 1;
				break;
			}
			wqp = wqp->next;
		}
		if (! found) {		/* entry already discarded from waitq or stageqry */
			reqid = 0;
			STG_LOG_OVL_STATUS("forked",pid,status);
		} else {
			reqid = wqp->reqid;
			STG_LOG_OVL_STATUS("stager",pid,status);
			wqp->ovl_pid = 0;
			if (wqp->status == 0) {
				/* Status was not yet set */
				wqp->status = rc_shift2castor(STGMAGIC,status);
			}
			wqp->nretry++;
		}
	}
	reqid = savereqid;
}

void
killallovl(sig)
		 int sig;
{
	struct waitq *wqp;
	char *func = "killallovl";
	int c = 0;

    /* kill the "stager"s */
	wqp = waitqp;
	while (wqp) {
		if (wqp->ovl_pid != 0) {
			stglogit (func, "killing stager process %d\n", wqp->ovl_pid);
			kill (wqp->ovl_pid, sig);
		}
	}

    /* Check current child exits */
	check_child_exit();

	/* Do safety things without launching anything - but return ESTNACT to clients */
	checkpoolstatus ();	/* check if any pool just cleaned */
	checkwaitingspc ();	/* check requests that are waiting for space */
	checkwaitq ();	/* scan the wait queue */

	/* Protect against the signal we are going to send */
	signal (SIGINT,SIG_IGN);

	/* kill every process in our process group */
	stglogit(func, STG124);

    /*
      ### Warning ### This call is NOT standard v.s. __STDC__, but neverthless behaves the
      same on all UNIX platforms we officially support:

      SGI claims:
      If pid is 0, sig will be sent to all processes excluding proc0 and proc1
      whose process group ID is equal to the process group ID of the sender.
      
      Tru64 claims:
      If the process parameter is equal to 0 (zero), the signal specified by the
      signal parameter is sent to all of the processes (other than system
      processes) whose process group ID is equal to the process group ID of the
      sender.
      
      SunOS claims:
      If pid is 0, sig will be sent  to  all  processes  excluding
      special  processes  (see intro(2)) whose process group ID is
      equal to the process group ID of the sender.
      
      HP-UX claims:
      If pid is 0, sig is sent to all processes excluding special system
      processes whose process group ID is equal to the process group ID of
      the sender.
      
      Linux claims:
      If pid equals 0, then sig is sent to every process in  the
      process group of the current process.
      
      Lynx claims (Nota : Lynx is not an officially supported platform, though)
      If pid is 0, the signal is sent to all other processes in the process
      group of the caller. If pid is -1, the signal is sent to all
      processes on the system except processes 0 and 1.

      AIX claims:
      If the Process parameter is 0, the signal specified by the Signal parameter
      is sent to all processes, excluding proc0 and proc1, whose process group
      ID matches the process group ID of the sender.
    */

	kill(0,SIGINT);

	/* We reply shutdown is ok */
	sendrep (&shutdownreq_rpfd, STAGERC, STAGESHUTDOWN, STGMAGIC, c);

	/* Reset the permanent RFIO connections */
	rfio_end();
	rfio_unend();

	/* Global exit */
	stglogit (func, "Exit.\n");
	exit(0);
}

void checkpoolstatus()
{
	int c, i, j, n;
	char **poolc;
	struct stgcat_entry *stcp;
	struct waitf *wfp;
	struct waitq *wqp;
	int have_waiting_spc;
	int have_found_spc;

	n = checkpoolcleaned (&poolc);
	for (j = 0; j < n; j++) {
		for (wqp = waitqp; wqp; wqp = wqp->next) {
			if (strcmp (wqp->waiting_pool, poolc[j]) == 0) {
				have_waiting_spc = 0;
				have_found_spc = 0;
				reqid = wqp->reqid;
				if ((shutdownreq_reqid != 0) && (wqp->status != STAGESHUTDOWN)) {
					/* There is a coming shutdown */
					sendrep (&(wqp->rpfd), MSG_ERR, STG162);
					if (wqp->save_rpfd >= 0) {
						wqp->rpfd = wqp->save_rpfd;
						wqp->save_rpfd = -1;
					}
					sendrep (&(wqp->rpfd), STAGERC, STAGESHUTDOWN, wqp->magic, SHIFT_ESTNACT);
					wqp->status = STAGESHUTDOWN;
					continue;
				}
				for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
					if (wfp->waiting_on_req > 0) continue;
					for (stcp = stcs; stcp < stce; stcp++) {
						if (wfp->subreqid == stcp->reqid)
							break;
					}
					if ((stcp->status & 0xF0) == WAITING_SPC) {
						int has_trailing = 0;
						++have_waiting_spc;       /* Count the number of entries in WAITING_SPC */
						if ((wqp->concat_off_fseq > 0) && (i == (wqp->nbdskf - 1))) {
							/* We remove the trailing '-' */
							if (stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] == '-') {
								has_trailing = 1;
								stcp->u1.t.fseq[strlen(stcp->u1.t.fseq) - 1] = '\0';
							}
						}
						c = build_ipath (wfp->upath, stcp, wqp->pool_user, 0, wqp->api_out, wqp->openmode);
						if (has_trailing != 0) {
							/* We restore the trailing '-' */
							strcat(stcp->u1.t.fseq,"-");
						}
						if (c < 0) {
							if (wqp->nb_clnreq++ > (wqp->noretry ? 0 : MAXRETRY)) {
								sendrep (&(wqp->rpfd), MSG_ERR, STG45,
										(stcp->poolname[0] != '\0' ? stcp->poolname : "<none>"));
								wqp->status = ENOSPC;
							} else {
								strcpy (wqp->waiting_pool, stcp->poolname);
								cleanpool (stcp->poolname);
							}
							break;
						} else if (c) {
							wqp->status = c;
						} else {
							int hsm_ns_error = 0;
							stcp->c_time = time(NULL);
							stcp->a_time = stcp->c_time;
							stcp->nbaccesses = 1;
							stcp->status &= 0xF;
							if (ISSTAGEOUT(stcp)) {
								if (stcp->t_or_d == 'h') {
									stcp->status |= WAITING_NS;
                                }
#ifdef USECDB
								if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
									stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
								}
#endif
								if (stcp->t_or_d == 'h') {
									int this_status;
									/* Try now to create entry in the HSM name server */
									if ((this_status = create_hsm_entry(&(wqp->rpfd), stcp, wqp->api_out, wqp->openmode, 0)) != 0) {
										/* Too bad - finally this request fails because of the name server */
										wqp->status = this_status;
										hsm_ns_error = 1;
										wqp->forced_exit = 1;
									}
								}
							} else {
#ifdef USECDB
								if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
									stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
								}
#endif
							}
							if (hsm_ns_error == 0) {
								++have_found_spc;           /* Count the number of entries WAITING_SPC recovered */
								if ((stcp->status == STAGEOUT) ||
									(stcp->status == STAGEALLOC)) {
									if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
									if (wqp->Pflag)
										sendrep (&(wqp->rpfd), MSG_OUT,
														 "%s\n", stcp->ipath);
									if (*(wfp->upath) && strcmp (stcp->ipath, wfp->upath))
										create_link (&(wqp->rpfd), stcp, wfp->upath);
									if (wqp->Upluspath && *((wfp+1)->upath) &&
											strcmp (stcp->ipath, (wfp+1)->upath))
										create_link (&(wqp->rpfd), stcp, (wfp+1)->upath);
									rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, stcp->status);
								}
							}
						}
					}
				}
				if ((have_waiting_spc > 0) && (have_waiting_spc == have_found_spc)) {
					/* All entries in WAITING_SPC state were recovered - we can remove the waiting_pool global entry */
					wqp->waiting_pool[0] = '\0';
				}
			}
		}
	}
}

void checkwaitingspc()
{
	int c, i;
	struct stgcat_entry *stcp;
	struct waitf *wfp;
	struct waitq *wqp;
	int has_trailing;
	int hsm_ns_error;

	/* Look if there any waitq in WAITING_SPC mode */
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		if (wqp->waiting_pool[0] == '\0') continue;
		if ((shutdownreq_reqid != 0) && (wqp->status != STAGESHUTDOWN)) {
			/* There is a coming shutdown */
			reqid = wqp->reqid;
			sendrep (&(wqp->rpfd), MSG_ERR, STG162);
			if (wqp->save_rpfd >= 0) {
				wqp->rpfd = wqp->save_rpfd;
				wqp->save_rpfd = -1;
			}
			sendrep (&(wqp->rpfd), STAGERC, STAGESHUTDOWN, wqp->magic, SHIFT_ESTNACT);
			wqp->status = STAGESHUTDOWN;
			continue;
		}
		/* Got one */
		goto is_waiting_spc;
	}
	/* None */
	return;

	is_waiting_spc:
	/* Initialize all waitq counters */
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		if (wqp->waiting_pool[0] == '\0') continue;
		wqp->nb_waiting_spc = 0;
		wqp->nb_found_spc = 0;
    }

	/* We know that there is at least one entry in waiting space mode */
	for (stcp = stcs; stcp < stce; stcp++) {
		if ((stcp->status & 0xF0) != WAITING_SPC) continue;
		/* We need to know to which waiting queue this entry belong */
		for (wqp = waitqp; wqp; wqp = wqp->next) {
			if (wqp->waiting_pool[0] == '\0') continue;
			for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
				if (wfp->subreqid == stcp->reqid) goto have_found_wfp;
			}
		}
		reqid = wqp->reqid;
		/* Not found... */
		stglogit(func, "STG02 - Reqid %d in WAITING_SPC not found in wait queue\n", stcp->reqid);
		continue;

	have_found_wfp:
		/* Increment the number of entries in WAITING_SPC inside this waitq */
		++(wqp->nb_waiting_spc);
		has_trailing = 0;
		if ((wqp->concat_off_fseq > 0) && (i == (wqp->nbdskf - 1))) {
			size_t thislen = strlen(stcp->u1.t.fseq) - 1;
			/* We remove the trailing '-' */
			if (stcp->u1.t.fseq[thislen] == '-') {
				has_trailing = 1;
				stcp->u1.t.fseq[thislen] = '\0';
			}
		}
		c = build_ipath (wfp->upath, stcp, wqp->pool_user, 0, wqp->api_out, wqp->openmode);
		if (has_trailing != 0) {
			/* We restore the trailing '-' */
			strcat(stcp->u1.t.fseq,"-");
		}
		if (c == 0) {
			hsm_ns_error = 0;
			stcp->c_time = time(NULL);
			stcp->a_time = stcp->c_time;
			stcp->nbaccesses = 1;

			/* We succeeded to allocate space for this WAITING_SPC request */
			/* regardless of an existing gc or not */
			stcp->status &= 0xF;
			if (ISSTAGEOUT(stcp)) {
				if (stcp->t_or_d == 'h') {
					stcp->status |= WAITING_NS;
				}
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
				if (stcp->t_or_d == 'h') {
					int this_status;
					/* Try now to create entry in the HSM name server */
					if ((this_status = create_hsm_entry(&(wqp->rpfd), stcp, wqp->api_out, wqp->openmode, 0)) != 0) {
						/* Too bad - finally this request fails because of the name server */
						wqp->status = this_status;
						hsm_ns_error = 1;
						wqp->forced_exit = 1;
					}
				}
			} else {
#ifdef USECDB
				if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
					stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
#endif
			}
			if (hsm_ns_error == 0) {
				/* Count the number of entries WAITING_SPC recovered */
				++(wqp->nb_found_spc);
				if ((stcp->status == STAGEOUT) ||
					(stcp->status == STAGEALLOC)) {
					if (wqp->api_out)
						sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
					if (wqp->Pflag)
						sendrep (&(wqp->rpfd), MSG_OUT, "%s\n", stcp->ipath);
					if (*(wfp->upath) && strcmp (stcp->ipath, wfp->upath))
						create_link (&(wqp->rpfd), stcp, wfp->upath);
					if (wqp->Upluspath && *((wfp+1)->upath) && strcmp (stcp->ipath, (wfp+1)->upath))
						create_link (&(wqp->rpfd), stcp, (wfp+1)->upath);
					rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, stcp->status);
				}
			}
		}
    }
	/* Look if we recovered a whole entry in the waitq */
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		if (wqp->waiting_pool[0] == '\0') continue;
		if ((wqp->nb_waiting_spc > 0) && (wqp->nb_waiting_spc == wqp->nb_found_spc)) {
			/* All entries in WAITING_SPC state were recovered - we can remove the waiting_pool global entry */
			wqp->waiting_pool[0] = '\0';
		}
    }
}

int
check_waiting_on_req(subreqid, state)
		 int subreqid;
		 int state;
{
	int c;
	int firstreqid;
	int found;
	int i;
	int savereqid;
	struct stgcat_entry *stcp, *stcp_check;
	struct waitf *wfp;
	struct waitq *wqp;
	char tmpbuf[21];

	found = 0;
	firstreqid = 0;
	savereqid = reqid;
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		if (! wqp->nb_waiting_on_req) continue;
		/* It is a STAGEIN request */
		reqid = wqp->reqid;
		for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
			if (wfp->waiting_on_req != subreqid) continue;
			found++;
			if (state == STAGED) {
				for (stcp = stcs; stcp < stce; stcp++) {
					if (stcp->reqid == 0) break;
					if (stcp->reqid == subreqid) break;
				}
				if (wqp->concat_off_fseq <= 0 && stcp->filler[0] == 'c') {
					/* If this request is NOT a "-c off" one and if the request to which belongs */
					/* this callback IS a "-c off", then we react to the callback ONLY if the */
					/* fseq is exactly the same. */
					for (stcp_check = stcs; stcp_check < stce; stcp_check++) {
						if (stcp_check->reqid == 0) break;
						if (stcp_check->reqid == wfp->subreqid) break;
					}
#ifdef STAGER_DEBUG
                    sendrep(&(wqp->rpfd), MSG_ERR, "stcp_check->u1.t.fseq=%s\n", stcp_check->u1.t.fseq);
#endif
					if (strcmp(stcp->u1.t.fseq,stcp_check->u1.t.fseq) != 0) {
						/* The file sequence of this "-c off" callback does not */
						/* match the file sequence we are waiting for. */
						continue;
					}
				}
#if SACCT
				stageacct (STGFILS, wqp->req_uid, wqp->req_gid, wqp->clienthost,
									 wqp->reqid, wqp->req_type, 0, 0, stcp, "", (char) 0);
#endif
				sendrep (&(wqp->rpfd), RTCOPY_OUT, STG96,
								 strrchr (stcp->ipath, '/')+1,
								 u64tostr(stcp->actual_size,tmpbuf,0),
								 (float)(stcp->actual_size)/(1024.*1024.),
								 stcp->nbaccesses);
				if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
				if (wqp->copytape)
					sendinfo2cptape (&(wqp->rpfd), stcp);
				if (*(wfp->upath) && strcmp (stcp->ipath, wfp->upath))
					create_link (&(wqp->rpfd), stcp, wfp->upath);
				if (wqp->Upluspath && *((wfp+1)->upath) &&
						strcmp (stcp->ipath, (wfp+1)->upath))
					create_link (&(wqp->rpfd), stcp, (wfp+1)->upath);
				for (stcp = stcs; stcp < stce; stcp++) {
					if (wfp->subreqid == stcp->reqid)
						break;
				}
				delreq (stcp,0);
				wqp->nb_waiting_on_req--;
				wqp->nb_subreqs--;
				wqp->nbdskf--;
				for ( ; i < wqp->nb_subreqs; i++, wfp++) {
					memcpy(wfp,wfp+1,sizeof(struct waitf));
					/*
					wfp->subreqid = (wfp+1)->subreqid;
					wfp->waiting_on_req = (wfp+1)->waiting_on_req;
					strcpy (wfp->upath, (wfp+1)->upath);
					*/
				}
				if (wqp->save_subreqid != NULL) {
					int isubreqid;
					/* This waiting queue is supporting the async callback */
					/* By construction we already had allocated enough space */
					/* in wqp->save_subreqid (an automatic retry cannot be done with more than previous try) */
					for (isubreqid = 0, wfp = wqp->wf; isubreqid < wqp->nbdskf; isubreqid++, wfp++) {
						wqp->save_subreqid[isubreqid] = wfp->subreqid;
					}
					wqp->save_nbsubreqid = wqp->nbdskf;
				}
			} else {	/* FAILED or KILLED */
				if (firstreqid == 0) {
					for (stcp = stcs; stcp < stce; stcp++) {
						if (wfp->subreqid == stcp->reqid)
							break;
					}
					stcp->status &= 0xF;
					if (! wqp->Aflag) {
                      /* Note that "-c off" request always have a Aflag */
						if ((c = build_ipath (wfp->upath, stcp, wqp->pool_user, 0, wqp->api_out, wqp->openmode)) < 0) {
							stcp->status |= WAITING_SPC;
#ifdef USECDB
							if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
								stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
							}
#endif
							strcpy (wqp->waiting_pool, stcp->poolname);
							wqp->nb_clnreq++;
							cleanpool (stcp->poolname);
						} else if (c) {
							wqp->status = c;
						} else {
#ifdef USECDB
							if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
								stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
							}
#endif
						}
					} else {
#ifdef USECDB
						if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
							stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
						}
#endif
					}
			   		wqp->nb_waiting_on_req--;
				   	wfp->waiting_on_req = -1;
					firstreqid = wfp->subreqid;
				} else {
					wfp->waiting_on_req = firstreqid;
				}
			}
			break;
		}
	}
	reqid = savereqid;
#ifdef STAGER_DEBUG
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waiting_on_req : Waitq->wf : \n");
		for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid) {
					switch (stcp->t_or_d) {
					case 't':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waiting_on_req : No %3d    : VID.FSEQ=%s.%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.t.vid[0], stcp->u1.t.fseq,wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'd':
					case 'a':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waiting_on_req : No %3d    : u1.d.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.d.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'm':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waiting_on_req : No %3d    : u1.m.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.m.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'h':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waiting_on_req : No %3d    : u1.h.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.h.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					default:
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waiting_on_req : No %3d    : <unknown to_or_d=%c>\n",i,stcp->t_or_d);
						break;
                    }
					break;
				}
			}
		}
	}
#endif
	return (found);
}

int
check_coff_waiting_on_req(subreqid, state)
		 int subreqid;
		 int state;
{
	int firstreqid;
	int found;
	int i;
	int savereqid;
	struct stgcat_entry *stcp;
	struct waitf *wfp;
	struct waitq *wqp;

	/* For the moment this routine is meaningful ONLY if state is LAST_TPFILE */
	if (state != LAST_TPFILE) {
		stglogit("check_coff_waiting_on_req","STG02 - Called with state=0x%lx != LAST_TPFILE=0x%lx\n", (unsigned long) state, (unsigned long) LAST_TPFILE);
		return(-1);
	}

	found = 0;
	firstreqid = 0;
	savereqid = reqid;
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		/* It HAS to be a "-c off" request */
		if (wqp->concat_off_fseq <= 0) continue;
		if (! wqp->nb_waiting_on_req) continue;
		reqid = wqp->reqid;
		for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
			if (wfp->waiting_on_req != subreqid) continue;
			found++;
			if (state == LAST_TPFILE) {
				/* Equivalent as a successful check - we mimic STAGED behaviour of check_waiting_on_req */
				for (stcp = stcs; stcp < stce; stcp++) {
					if (wfp->subreqid == stcp->reqid)
						break;
				}
				delreq (stcp,0);
				wqp->nb_waiting_on_req--;
				wqp->nb_subreqs--;
				wqp->nbdskf--;
				for ( ; i < wqp->nb_subreqs; i++, wfp++) {
					memcpy(wfp,wfp+1,sizeof(struct waitf));
					/*
					wfp->subreqid = (wfp+1)->subreqid;
					wfp->waiting_on_req = (wfp+1)->waiting_on_req;
					strcpy (wfp->upath, (wfp+1)->upath);
					*/
				}
				if (wqp->save_subreqid != NULL) {
					int isubreqid;
					/* This waiting queue is supporting the async callback */
					/* By construction we already had allocated enough space */
					/* in wqp->save_subreqid (an automatic retry cannot be done with more than previous try) */
					for (isubreqid = 0, wfp = wqp->wf; isubreqid < wqp->nbdskf; isubreqid++, wfp++) {
						wqp->save_subreqid[isubreqid] = wfp->subreqid;
					}
					wqp->save_nbsubreqid = wqp->nbdskf;
				}
			}
			break;
		}
	}
	reqid = savereqid;
#ifdef STAGER_DEBUG
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_coff_waiting_on_req : Waitq->wf : \n");
		for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid) {
					switch (stcp->t_or_d) {
					case 't':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_coff_waiting_on_req : No %3d    : VID.FSEQ=%s.%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.t.vid[0], stcp->u1.t.fseq,wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'd':
					case 'a':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_coff_waiting_on_req : No %3d    : u1.d.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.d.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'm':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_coff_waiting_on_req : No %3d    : u1.m.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.m.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'h':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_coff_waiting_on_req : No %3d    : u1.h.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.h.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					default:
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waiting_on_req : No %3d    : <unknown to_or_d=%c>\n",i,stcp->t_or_d);
						break;
                    }
					break;
				}
			}
		}
	}
#endif
	return (found);
}

void checkwaitq()
{
	int i;
	struct stgcat_entry *stcp;
	struct waitf *wfp;
	struct waitq *wqp, *wqp1;
	int save_reqid;

	nwaitq_with_connection = 0;
	save_reqid = reqid;
	wqp = waitqp;
	while (wqp) {
		if ((wqp->rpfd >= 0) || (wqp->save_rpfd >= 0)) nwaitq_with_connection++;
		reqid = wqp->reqid;
		if ((shutdownreq_reqid != 0) && (wqp->status != STAGESHUTDOWN)) {
			/* There is a coming shutdown */
			sendrep (&(wqp->rpfd), MSG_ERR, STG162);
			if (wqp->save_rpfd >= 0) {
				wqp->rpfd = wqp->save_rpfd;
				wqp->save_rpfd = -1;
			}
			sendrep (&(wqp->rpfd), STAGERC, STAGESHUTDOWN, wqp->magic, SHIFT_ESTNACT);
			wqp->status = STAGESHUTDOWN;
			wqp1 = wqp;
			wqp = wqp->next;
			rmfromwq (wqp1);
			continue;
		}
#ifdef STAGER_DEBUG
		sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] nb_subreqs=%d, concat_off_fseq=%d, nb_waiting_on_req=%d, waiting_pool=%s, ovl_pid=%d\n", wqp->nb_subreqs, wqp->concat_off_fseq, wqp->nb_waiting_on_req, wqp->waiting_pool, wqp->ovl_pid);
#endif
		if (wqp->nb_subreqs == 0 && wqp->nb_waiting_on_req == 0 &&
				! *(wqp->waiting_pool) && ! wqp->ovl_pid) {
			/* stage request completed successfully */
#if SACCT
			stageacct (STGCMDC, wqp->req_uid, wqp->req_gid, wqp->clienthost,
								 reqid, wqp->req_type, wqp->nretry, wqp->status,
								 NULL, "", (char) 0);
#endif
			if (wqp->save_rpfd >= 0) {
				wqp->rpfd = wqp->save_rpfd;
				wqp->save_rpfd = -1;
			}
			sendrep (&(wqp->rpfd), STAGERC, wqp->req_type, wqp->magic, wqp->status);
			wqp1 = wqp;
			wqp = wqp->next;
			rmfromwq (wqp1);
		} else if (wqp->nb_subreqs > wqp->nb_waiting_on_req &&
				   ! wqp->ovl_pid &&
				   ! *(wqp->waiting_pool) && wqp->status != ENOSPC &&
				   wqp->status != USERR   && wqp->status != EINVAL &&
				   wqp->status != CLEARED && wqp->status != ESTCLEARED &&
				   wqp->status != REQKILD && wqp->status != ESTKILLED &&
				   wqp->status != CHECKSUMERR && wqp->status != SECHECKSUM &&
				   wqp->nretry < (wqp->noretry ? 0 : MAXRETRY)) {
			if (wqp->nretry)
				sendrep (&(wqp->rpfd), MSG_ERR, STG43, wqp->nretry);
			if (wqp->save_subreqid != NULL) {
				int isubreqid;
				/* This waiting queue is supporting the async callback */
				/* By construction we already had allocated enough space */
				/* in wqp->save_subreqid (an automatic retry cannot be done with more than previous try) */
				for (isubreqid = 0, wfp = wqp->wf; isubreqid < wqp->nbdskf; isubreqid++, wfp++) {
					wqp->save_subreqid[isubreqid] = wfp->subreqid;
				}
				wqp->save_nbsubreqid = wqp->nbdskf;
			}
			{
				int c;
				if ((c = fork_exec_stager (wqp)) != 0) {
					/* If this fails it will be retried at next loop */
					/* wqp->nretry is automatically updated only when a forked */
					/* process exits. In this case we are even not able to */
					/* execute fork_exec_stager ... */
					wqp->nretry++;
					if (! (wqp->nretry < (wqp->noretry ? 0 : MAXRETRY))) {
						/* Has reached the maximum number of retries : there is a system error in here... */
						wqp->status = c;
					}
				}
			}
			wqp = wqp->next;
		} else if (wqp->forced_exit) {
			goto wqp_forced_exit;
		} else if ((wqp->clnreq_reqid != 0) &&	/* space requested by rtcopy */
							 (! *(wqp->waiting_pool) ||	/* has been freed or */
								wqp->status)) {		/* could not be found */
			reqid = wqp->clnreq_reqid;
			/* wqp->clnreq_waitingreqid contains the reqid that is WAITING_SPC */
			/*
			for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++)
				if (! wfp->waiting_on_req) break;
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid)
					break;
			}
			*/
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wqp->clnreq_waitingreqid == stcp->reqid)
					break;
			}
			sendrep (&(wqp->clnreq_rpfd), MSG_OUT, "%s", stcp->ipath);
			sendrep (&(wqp->clnreq_rpfd), STAGERC, STAGEUPDC, wqp->magic, wqp->status);
			wqp->clnreq_reqid = 0;
			wqp->clnreq_rpfd = 0;
			wqp->status = 0;
			wqp->clnreq_waitingreqid = 0;
			wqp = wqp->next;
		} else if (wqp->status) {
		  wqp_forced_exit:
			/* request failed */
#if SACCT
			stageacct (STGCMDC, wqp->req_uid, wqp->req_gid, wqp->clienthost,
								 reqid, wqp->req_type, wqp->nretry, wqp->status,
								 NULL, "", (char) 0);
#endif
			sendrep (&(wqp->rpfd), MSG_ERR, STG98, sstrerror(wqp->status));
			/* We close cleanly the connection */
			if (wqp->save_rpfd >= 0) {
				wqp->rpfd = wqp->save_rpfd;
				wqp->save_rpfd = -1;
			}
			sendrep (&(wqp->rpfd), STAGERC, wqp->req_type, wqp->magic, wqp->status);

			for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
				for (stcp = stcs; stcp < stce; stcp++) {
					if (wfp->subreqid == stcp->reqid)
						break;
				}
				switch (stcp->status & 0xF) {
				case STAGEIN:
					if (wfp->waiting_on_req > 0) {
						delreq (stcp,0);
						continue;
					}
					if (delfile (stcp, 1, 0, 1, ((wqp->status == REQKILD) || (wqp->status == ESTKILLED)) ?
											 "req killed" : "failing req", wqp->req_uid, wqp->req_gid, 0, 0, 0) < 0)
						stglogit (func, STG02, stcp->ipath,
											RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
					check_waiting_on_req (wfp->subreqid,
							((wqp->status == REQKILD) || (wqp->status == ESTKILLED)) ? KILLED : STG_FAILED);
					break;
				case STAGEWRT:
					if (stcp->t_or_d == 'h') {
						/* This is a file coming from migration */
						if (HAVE_SENSIBLE_STCP(stcp)) {
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
					}
					/* There is intentionnaly no 'break' here */
				case STAGEOUT:
				case STAGEALLOC:
					delreq (stcp,0);
					break;
				case STAGEPUT:
					if ((stcp->status & (CAN_BE_MIGR)) == CAN_BE_MIGR) {
						/* This is a file coming from (automatic or not) migration */
						update_migpool(&stcp,-1,0);
						stcp->status = STAGEOUT | PUT_FAILED | CAN_BE_MIGR;
					} else {
						stcp->status = STAGEOUT | PUT_FAILED;
					}
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					if (wqp->api_out) sendrep(&(wqp->rpfd), API_STCP_OUT, stcp, wqp->magic);
				}
			}
			wqp1 = wqp;
			wqp = wqp->next;
			rmfromwq (wqp1);
		} else {
			wqp = wqp->next;
		}
	}
#ifdef STAGER_DEBUG
	for (wqp = waitqp; wqp; wqp = wqp->next) {
		sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waitq : Waitq->wf : \n");
		for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (wfp->subreqid == stcp->reqid) {
					switch (stcp->t_or_d) {
					case 't':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waitq : No %3d    : VID.FSEQ=%s.%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.t.vid[0], stcp->u1.t.fseq,wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'd':
					case 'a':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waitq : No %3d    : u1.d.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.d.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'm':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waitq : No %3d    : u1.m.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.m.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					case 'h':
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waitq : No %3d    : u1.h.xfile=%s, subreqid=%d, waiting_on_req=%d, upath=%s\n",i,stcp->u1.h.xfile, wfp->subreqid, wfp->waiting_on_req, wfp->upath);
						break;
					default:
						sendrep(&(wqp->rpfd), MSG_ERR, "[DEBUG] check_waitq : No %3d    : <unknown to_or_d=%c>\n",i,stcp->t_or_d);
						break;
					}
					break;
				}
			}
		}
	}
#endif
	reqid = save_reqid;
}

int create_dir(dirname, uid, gid, mask)
		 char *dirname;
		 uid_t uid;
		 gid_t gid;
#if defined(_WIN32)
		 int mask;
#else
		 mode_t mask;
#endif
{
	PRE_RFIO;
	if (rfio_mkdir (dirname, mask) < 0) {
		if (errno == EEXIST || rfio_errno == EEXIST) {
			return (0);
		} else {
			sendrep (&rpfd, MSG_ERR, STG02, dirname, "rfio_mkdir",
							 rfio_serror());
			return (SESYSERR);
		}
	}
	PRE_RFIO;
	if (rfio_chown (dirname, uid, gid) < 0) {
		sendrep (&rpfd, MSG_ERR, STG02, dirname, "rfio_chown",
						 rfio_serror());
		return (SESYSERR);
	}
	return (0);
}

void create_link(rpfd, stcp, upath)
	int *rpfd;
	struct stgcat_entry *stcp;
	char *upath;
{
	int c;
	int found;
	char lpath[CA_MAXHOSTNAMELEN + 1 + MAXPATH];
	struct stgpath_entry *newpath();
	struct stgpath_entry *stpp;
	int found_reqid = 0;

	found = 0;
	for (stpp = stps; stpp < stpe; stpp++) {
		if (stpp->reqid == 0) break;
		if (strcmp (upath, stpp->upath)) continue;
		found = 1;
		found_reqid = stpp->reqid;
		break;
	}
	if (! found) {
		stpp = newpath ();
		strcpy (stpp->upath, upath);
	}
	stpp->reqid = stcp->reqid;
	PRE_RFIO;
	if ((c = rfio_readlink (upath, lpath, sizeof(lpath))) > 0) {
		if (c > (CA_MAXHOSTNAMELEN + MAXPATH)) {
			stglogit (func, "### rfio_readlink returned %d\n", c);
			c = CA_MAXHOSTNAMELEN + MAXPATH;
		}
		lpath[c] = '\0';
		if (strcmp (lpath, stcp->ipath) == 0) goto create_link_return;
	}
	if (c > 0 || errno == EINVAL || rfio_errno == EINVAL) {
		stglogit (func, STG93, upath);
		sendrep (rpfd, RMSYMLINK, upath);
	}
	stglogit (func, STG94, upath);
	sendrep (rpfd, SYMLINK, stcp->ipath, upath);
 create_link_return:
	savepath ();
#ifdef USECDB
	if (! found) {
		if (stgdb_ins_stgpath(&dbfd,stpp) != 0) {
			stglogit(func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
			if (serrno == EDB_D_UNIQUE) {
				/* Entry yet exist with this pathname in the DB */
				if (stgdb_upd_stgpath(&dbfd,stpp) != 0) {
					stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
				}
			}
		}
	} else if (found_reqid != stcp->reqid) {
		/* The link name is the same, but the reqid is not ! */
		if (stgdb_upd_stgpath(&dbfd,stpp) != 0) {
			stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
		}
	}
#endif
}

int delfile(stcp, freersv, dellinks, delreqflg, by, byuid, bygid, remove_hsm, allow_one_stagein, nodisk_flag)
		 struct stgcat_entry *stcp;
		 int freersv;
		 int dellinks;
		 int delreqflg;
		 char *by;
		 uid_t byuid;
		 gid_t bygid;
		 int remove_hsm;
		 int allow_one_stagein;
		 int nodisk_flag;
{
	u_signed64 actual_size = 0;
	struct stat64 st;
	struct stgpath_entry *stpp;
	int found = 0;
	struct stgcat_entry *stcp_perhaps_stagein = NULL;
	u_signed64 actual_size_block = 0;

	if (stcp->ipath[0]) {
		/* Is there any other entry pointing at the same disk file?
			 It could be the case if stagein+stagewrt or several stagewrt */
		/* We accept to remove the entry also if there is another entry pointing */
		/* to the same disk file under one unique condition : there is only ONE */
		/* other entry matching the disk file, it is a STAGEIN|STAGED one and its */
		/* nbaccesses is <= 1 and it is an automatic delfile() call from procupd */
		/* (.e.g at a STAGEWRT successful callback) and it is another 'h' type */
		struct stgcat_entry *stclp;

		for (stclp = stcs; stclp < stce; stclp++) {
			if (stclp->reqid == 0) break;
			if (stclp == stcp) continue;
			if (strcmp (stcp->ipath, stclp->ipath) == 0) {
				stcp_perhaps_stagein = stclp;
				if (allow_one_stagein != 0) {
                	if (++found > 1) {
						/* More than two entries found */
						break;
					}
				} else {
					/* We accept only one entry */
					found = 1;
					break;
				}
			}
		}
		/* It can be that the entry we have in input is a STAGEIN|STAGED one and that what we found is a STAGEOUT|CAN_BE_MIGR one */
		/* In such a case it is also legal to remove the file */
		if ((allow_one_stagein != 0) && (found == 1) && ISSTAGEIN(stcp) && ((stcp->status & STAGED) == STAGED) && (stcp_perhaps_stagein->status == (STAGEOUT|CAN_BE_MIGR))) {
			struct stgcat_entry *dummystcp;
			dummystcp = stcp;
			stcp = stcp_perhaps_stagein;
			stcp_perhaps_stagein = dummystcp;
		}
		if ((found == 0) ||
			((allow_one_stagein != 0) && (found == 1) && ISSTAGEIN(stcp_perhaps_stagein) && (stcp_perhaps_stagein->nbaccesses == 1) &&
				(
				 ((stcp_perhaps_stagein->status & STAGED) == STAGED) ||
				 ((stcp_perhaps_stagein->status & STAGED_TPE) == STAGED_TPE) ||
				 ((stcp_perhaps_stagein->status & STAGED_LSZ) == STAGED_LSZ)
				)
             )
			) {
			/* it's the last entry - we can remove the file and release the space */
			/* or it's the last entry but another STAGED + STAGEIN */
			if (! nodisk_flag) {
				if (! freersv) {
					PRE_RFIO;
					if (RFIO_STAT64(stcp->ipath, &st) == 0) {
						actual_size = (u_signed64) st.st_size;
						if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < actual_size) {
							actual_size_block = actual_size;
						}
					} else if (rfio_serrno() == ENOENT) {
						/* What are we going to use as size information !? */
						stglogit(func, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
						actual_size = (u_signed64) (stcp->actual_size ? stcp->actual_size : stcp->size);
						actual_size_block = actual_size;
					} else {
						return(-1);
					}
					/* It is okay to delete a file even if it is in ENOENT */
				}
				PRE_RFIO;
				if (RFIO_UNLINK(stcp->ipath) == 0) {
#if SACCT
					stageacct (STGFILC, byuid, bygid, "",
							   reqid, 0, 0, 0, stcp, "", (char) 0);
#endif
					stglogit (func, STG95, stcp->ipath, by);
				} else if ((errno != ENOENT) && (rfio_errno != ENOENT)) {
					if ((errno != EIO) && (rfio_errno != EIO))
						return (-1);
					else
						stglogit (func, STG02, stcp->ipath, RFIO_UNLINK_FUNC(stcp->ipath),
								  rfio_serror());
				}
			}
			if ((((stcp->status & (STAGEPUT|CAN_BE_MIGR)) == (STAGEPUT|CAN_BE_MIGR)) && ((stcp->status & PUT_FAILED) != PUT_FAILED)) ||
				((stcp->status & (STAGEWRT|CAN_BE_MIGR)) == (STAGEWRT|CAN_BE_MIGR)) ||
				(((stcp->status & (STAGEOUT|CAN_BE_MIGR)) == (STAGEOUT|CAN_BE_MIGR)) && ((stcp->status & PUT_FAILED) != PUT_FAILED))
				) {
				/* This is a file coming from (automatic or not) migration */
				update_migpool(&stcp,-1,0);
			}
			if (! nodisk_flag) {
				updfreespace (stcp->poolname, stcp->ipath, 0, NULL, (signed64) ((freersv) ? (signed64) (stcp->size) : ((signed64) actual_size_block)));
			}
		} else {
			/* We neverthless take into account the migration counters */
			if ((((stcp->status & (STAGEPUT|CAN_BE_MIGR)) == (STAGEPUT|CAN_BE_MIGR)) && ((stcp->status & PUT_FAILED) != PUT_FAILED)) ||
				((stcp->status & (STAGEWRT|CAN_BE_MIGR)) == (STAGEWRT|CAN_BE_MIGR)) ||
				(((stcp->status & (STAGEOUT|CAN_BE_MIGR)) == (STAGEOUT|CAN_BE_MIGR)) && ((stcp->status & PUT_FAILED) != PUT_FAILED))
				) {
				/* This is a file coming from (automatic or not) migration */
				update_migpool(&stcp,-1,0);
			}
		}
	}
	if (remove_hsm) {
		setegid(bygid);
		seteuid(byuid);
		if (stcp->t_or_d == 'm') {
			PRE_RFIO;
			if (rfio_unlink (stcp->u1.m.xfile) == 0) {
				stglogit (func, STG95, stcp->u1.m.xfile, by);
			} else {
				if (rfio_serrno() == ENOENT) {
					/* ENOENT is acceptable, nothing else */
					stglogit (func, STG02, stcp->u1.m.xfile, "rfio_unlink", rfio_serror());
				} else {
					sendrep (&rpfd, RTCOPY_OUT, STG02, stcp->u1.m.xfile,
							 "rfio_unlink", rfio_serror());
				}
			}
		} else if (stcp->t_or_d == 'h') {
			if (Cns_unlink (stcp->u1.h.xfile) == 0) {
				stglogit (func, STG95, stcp->u1.h.xfile, by);
			} else {
				if (serrno == ENOENT) {
					/* ENOENT is acceptable, nothing else */
					stglogit (func, STG02, stcp->u1.h.xfile, "Cns_unlink", sstrerror(serrno));
				} else {
					sendrep (&rpfd, RTCOPY_OUT, STG02, stcp->u1.h.xfile,
							 "Cns_unlink", sstrerror(serrno));
				}
			}
		}
		setegid(start_passwd.pw_gid);
		seteuid(start_passwd.pw_uid);
	}
	if (dellinks) {
		for (stpp = stps; stpp < stpe; ) {
			if (stpp->reqid == 0) break;
			if (stcp->reqid == stpp->reqid)
				dellink (stpp, nodisk_flag);
			else
				stpp++;
		}
	}
	if (delreqflg) {
		if ((allow_one_stagein != 0) && (found == 1) && ISSTAGEIN(stcp_perhaps_stagein) && (stcp_perhaps_stagein->nbaccesses == 1) &&
				(
				 ((stcp_perhaps_stagein->status & STAGED) == STAGED) ||
				 ((stcp_perhaps_stagein->status & STAGED_TPE) == STAGED_TPE) ||
				 ((stcp_perhaps_stagein->status & STAGED_LSZ) == STAGED_LSZ)
				)
			) {
			/* We have not only stcp to delete - but also stcp_perhaps_stagein */
			struct stgcat_entry *stclp;
			struct stgcat_entry save_stcp = *stcp_perhaps_stagein;
			delreq (stcp,0);
			/* And we have to rescan to find save_stcp.reqid */
			for (stclp = stcs; stclp < stce; stclp++) {
				if (stclp->reqid == 0) break;
				if (stclp->reqid != save_stcp.reqid) continue;
				delreq (stclp,0);
				break;
			}
		} else {
			delreq (stcp,0);
		}
	}
	savepath ();
	return (0);
}

void dellink(stpp, nodisk_flag)
		 struct stgpath_entry *stpp;
		 int nodisk_flag;
{
	int n;
	char *p1, *p2;

#ifdef USECDB
	if (stgdb_del_stgpath(&dbfd,stpp) != 0) {
		stglogit(func, STG100, "delete", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif
	if (nodisk_flag) {
		stglogit (func, "STG93 - removing silently link %s (--nodisk option) \n", stpp->upath);
	} else if (rpfd >= 0) {
		stglogit (func, STG93, stpp->upath);
		sendrep (&rpfd, RMSYMLINK, stpp->upath);
	} else {
		stglogit (func, "STG93 - removing silently link %s (no client to do it)\n", stpp->upath);
	}
	nbpath_ent--;
	p2 = (char *)stps + (nbpath_ent * sizeof(struct stgpath_entry));
	if ((char *)stpp < p2) {	/* not last path in the list */
		p1 = (char *)stpp + sizeof(struct stgpath_entry);
		n = p2 + sizeof(struct stgpath_entry) - p1;
		memmove ((char *)stpp, p1, n);
	}
	memset (p2, 0, sizeof(struct stgpath_entry));
}

void stgcat_shrunk_pages()
{
	char *func = "stgcat_shrunk_pages";
	int save_reqid = reqid;

	reqid = 0;

	if (nbcat_ent_old != nbcat_ent) {
		size_t stgcat_bufsz_new;
		int npages2free;

		stgcat_bufsz_new = (nbcat_ent * sizeof(struct stgcat_entry) + STG_BUFSIZ -1) / STG_BUFSIZ * STG_BUFSIZ;
		if (stgcat_bufsz_new == 0) stgcat_bufsz_new = STG_BUFSIZ;

		if (stgcat_bufsz_new < stgcat_bufsz) {
			/* Something was deleted - check if we can shrunk pages */

			if ((npages2free = ((stgcat_bufsz - stgcat_bufsz_new) / STG_BUFSIZ)) > 0) {
				/* This is giving the number of pages we can free() */
				struct stgcat_entry *stcs_new;
				char tmpbuf1[21];
				char tmpbuf2[21];

				if ((stcs_new = (struct stgcat_entry *) realloc(stcs, stgcat_bufsz_new)) != NULL) {
					stcs = stcs_new;
					stce = stcs + (stgcat_bufsz_new/sizeof(struct stgcat_entry));      
					stglogit(func, "... Shrunked %d page%s of stgcat (%s to %s bytes)\n", npages2free, npages2free > 1 ? "s" : "", u64tostr((u_signed64) stgcat_bufsz, tmpbuf1, 0), u64tostr((u_signed64) stgcat_bufsz_new, tmpbuf2, 0));
					stgcat_bufsz = stgcat_bufsz_new;
				} else {
					stglogit(func, "STG45 - realloc from %s to %s bytes error (%s)\n", u64tostr((u_signed64) stgcat_bufsz, tmpbuf1, 0), u64tostr((u_signed64) stgcat_bufsz_new, tmpbuf2, 0), strerror(errno));
				}
			}
		}
		nbcat_ent_old = nbcat_ent;
	}
	reqid = save_reqid;
}


void stgpath_shrunk_pages()
{
	char *func = "stgpath_shrunk_pages";
	int save_reqid = reqid;

	reqid = 0;

	if (nbpath_ent_old != nbpath_ent) {
		size_t stgpath_bufsz_new;
		int npages2free;

		stgpath_bufsz_new = (nbpath_ent * sizeof(struct stgpath_entry) + STG_BUFSIZ -1) / STG_BUFSIZ * STG_BUFSIZ;
		if (stgpath_bufsz_new == 0) stgpath_bufsz_new = STG_BUFSIZ;

		if (stgpath_bufsz_new < stgpath_bufsz) {
			/* Something was deleted - check if we can shrunk pages */

			if ((npages2free = ((stgpath_bufsz - stgpath_bufsz_new) / STG_BUFSIZ)) > 0) {
				/* This is giving the number of pages we can free() */
				struct stgpath_entry *stps_new;
				char tmpbuf1[21];
				char tmpbuf2[21];

				if ((stps_new = (struct stgpath_entry *) realloc(stps, stgpath_bufsz_new)) != NULL) {
					stps = stps_new;
					stpe = stps + (stgpath_bufsz_new/sizeof(struct stgpath_entry));      
					stglogit(func, "... Shrunked %d page%s of stgpath (%s to %s bytes)\n", npages2free, npages2free > 1 ? "s" : "", u64tostr((u_signed64) stgpath_bufsz, tmpbuf1, 0), u64tostr((u_signed64) stgpath_bufsz_new, tmpbuf2, 0));
					stgpath_bufsz = stgpath_bufsz_new;
				} else {
					stglogit(func, "STG45 - realloc from %s to %s bytes error (%s)\n", u64tostr((u_signed64) stgpath_bufsz, tmpbuf1, 0), u64tostr((u_signed64) stgpath_bufsz_new, tmpbuf2, 0), strerror(errno));
				}
			}
		}
		nbpath_ent_old = nbpath_ent;
	}
	reqid = save_reqid;
}


/* If nodb_delete_flag is != 0 then only update in memory is performed */
void delreq(stcp,nodb_delete_flag)
		 struct stgcat_entry *stcp;
		 int nodb_delete_flag;
{
	int n;
	char *p1, *p2;

#ifdef USECDB
	if (nodb_delete_flag == 0) {
		if (stgdb_del_stgcat(&dbfd,stcp) != 0) {
			stglogit(func, STG100, "delete", sstrerror(serrno), __FILE__, __LINE__);
		}
	}
#endif

	nbcat_ent--;
	p2 = (char *)stcs + (nbcat_ent * sizeof(struct stgcat_entry));
	if ((char *)stcp < p2) {	/* not last request in the list */
		p1 = (char *)stcp + sizeof(struct stgcat_entry);
		n = p2 + sizeof(struct stgcat_entry) - p1;
		memmove ((char *)stcp, p1, n);
	}
	memset (p2, 0, sizeof(struct stgcat_entry));
	savereqs ();
}

/* If nodb_delete_flag is != 0 then only update in memory is performed */
void delreqid(thisreqid,nodb_delete_flag)
		 int thisreqid;
		 int nodb_delete_flag;
{
	struct stgcat_entry *stcp;
	int found = 0;

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if (stcp->reqid == thisreqid) {
			found = 1;
			break;
		}
	}

	if (! found) return;
	delreq(stcp,nodb_delete_flag);
}

int fork_exec_stager(wqp)
		 struct waitq *wqp;
{
	char arg_Aflag[11], arg_silent[11], arg_use_subreqid[11], t_or_d = 0;
	char arg_key[11], arg_nbsubreqs[11], arg_reqid[11], arg_nretry[11], arg_rpfd[11], arg_concat_off_fseq[11];
	char arg_api_flag[11];      /* Flag telling if this is an API call or not */
	char arg_api_flags[21];    /* Flag giving the API flags themselves */
	char arg_rtcp_uid[11];     /* Explicit uid under which the process will run */
	char arg_rtcp_gid[11];     /* Explicit uid under which the process will run */
	int c, i, sav_ovl_pid;
	static int pfd[2];
	int pid;
	char progfullpath[MAXPATH];
	char progname[MAXPATH];
	struct stgcat_entry *stcp;
	struct stgcat_entry lying_stcp;
	char changed_t_or_d = '\0';
	struct waitf *wfp;
#ifdef __INSURE__
	/* INSURE DOES NOT WORK WITH THIS PIPE... I've spent three hours trying to make work */
	char tmpfile[L_tmpnam];
	FILE *f;
#endif

	/* We look if we can really fork a stager. The macro FREE_FD() already */
	/* takes into account the two file descriptors needed for the pipe. But we */
	/* have to make sure that there will always be at least one available file descriptor to serve the next request */
	/* Current requests count as one, and is a candidate for the waitq */
	/* So we need two free file descriptors left: one for the current one, one for the next */

	/* This logic apply only on fresh new connections, and not on internal retries */
	if (! wqp->nretry) {
		if (FREE_FD <= 2) {
			/* We are at the limit - Sorry we have to reject */
			/* We count on the client retry some time later saying him we are not */
			/* available for the moment - with ESTNACT */
			sendrep (&(wqp->rpfd), MSG_ERR, STG160, sysconf(_SC_OPEN_MAX));
			return(SHIFT_ESTNACT);
		}
	}

	/* We determine in advance the t_or_d to know which stager to fork_exec */
	for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
		if (wfp->waiting_on_req > 0) continue;
		if (wfp->waiting_on_req < 0) wfp->waiting_on_req = 0;
		for (stcp = stcs; stcp < stce; stcp++)
			if (stcp->reqid == wfp->subreqid) break;
		t_or_d = stcp->t_or_d;
		break;
	}
	/* We verify if this is a correct t_or_d */
	switch (t_or_d) {
	case 't':
	case 'd':
	case 'm':
	case 'h':
		break;
	default:
		sendrep (&(wqp->rpfd), MSG_ERR, "STG02 - Unknown request type '%c'\n", t_or_d);
		return (SEINTERNAL);
	}

	if (pipe (pfd) < 0) {
		sendrep (&(wqp->rpfd), MSG_ERR, STG02, "", "pipe", strerror(errno));
		return (SESYSERR);
	}

#ifdef __INSURE__
	if (tmpnam(tmpfile) == NULL) {
		close (pfd[0]);
		close (pfd[1]);
		return (SESYSERR);
	}
	if ((f = fopen(tmpfile,"w+b")) == NULL) {
		close (pfd[0]);
		close (pfd[1]);
		return (SESYSERR);
	}
	for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
		if (wfp->waiting_on_req > 0) continue;
		if (wfp->waiting_on_req < 0) wfp->waiting_on_req = 0;
		for (stcp = stcs; stcp < stce; stcp++)
			if (stcp->reqid == wfp->subreqid) break;
		if (wfp->ipath[0] != '\0') {
			/* We lye to the system making it believing it is a disk copy */
			/* This apply only for internal copy of CASTOR HSM files */
			memset((void *) &lying_stcp, 0, sizeof(struct stgcat_entry));
			lying_stcp.t_or_d = 'd';
			changed_t_or_d = '3';
			strcpy(lying_stcp.u1.d.xfile,wfp->ipath);
			lying_stcp.u1.d.Xparm[0] = '\0';
			lying_stcp.actual_size = stcp->actual_size;
			lying_stcp.size = stcp->size;
			lying_stcp.mask = stcp->mask;
			lying_stcp.uid = stcp->uid;
			lying_stcp.gid = stcp->gid;
			lying_stcp.reqid = stcp->reqid;
			strcpy(lying_stcp.ipath, stcp->ipath);
			fwrite (&lying_stcp, sizeof(struct stgcat_entry), 1, f);
		} else {
			fwrite (stcp, sizeof(struct stgcat_entry), 1, f);
		}
	}
	fclose(f);
#endif

	sav_ovl_pid = wqp->ovl_pid;
	wqp->ovl_pid = fork ();
	pid = wqp->ovl_pid;
	if (pid < 0) {
		sendrep (&(wqp->rpfd), MSG_ERR, STG02, "", "fork", strerror(errno));
#ifdef __INSURE__
		remove(tmpfile);
#endif
		/* Close the fds opened by the pipe */
		close (pfd[0]);
		close (pfd[1]);
		wqp->ovl_pid = sav_ovl_pid;
		return (SESYSERR);
	} else if (pid == 0) {	/* we are in the child */
		rfio_mstat_reset();  /* Reset permanent RFIO stat connections */
		rfio_munlink_reset(); /* Reset permanent RFIO unlink connections */
		/* In case of --silent mode only: wqp->save_rpfd will be closed by the following. */
		/* No pb, we want nothing from forked stager in client's output, and the connection */
		/* will still be alive from parent (stgdaemon point of view) */
#ifdef __INSURE__
		c = 0;
		/* if (c != pfd[0] && c != wqp->rpfd) close (c); */
#else
		for (c = 0; c < maxfds; c++)
			if (c != pfd[0] && c != wqp->rpfd) close (c);
#endif
		dup2 (pfd[0], 0);
		switch (t_or_d) {
		case 'h': /* CASTOR HSM */
			sprintf (progfullpath, "%s/stager_castor", BIN);
			sprintf (progname, "%s", "stager_castor");
			break;
		case 't': /* Tape */
			sprintf (progfullpath, "%s/stager_tape", BIN);
			sprintf (progname, "%s", "stager_tape");
			break;
		case 'd': /* Disk */
			sprintf (progfullpath, "%s/stager_disk", BIN);
			sprintf (progname, "%s", "stager_disk");
			break;
		case 'm': /* Non-CASTOR HSM */
			sprintf (progfullpath, "%s/stager_disk", BIN);
			sprintf (progname, "%s", "stager_disk");
			break;
		}
		sprintf (arg_reqid, "%d", wqp->reqid);
		sprintf (arg_key, "%d", wqp->key);
		sprintf (arg_rpfd, "%d", wqp->rpfd);
		sprintf (arg_nbsubreqs, "%d", wqp->nbdskf - wqp->nb_waiting_on_req);
		sprintf (arg_nretry, "%d", wqp->nretry);
		sprintf (arg_Aflag, "%d", wqp->Aflag);
		sprintf (arg_concat_off_fseq, "%d", wqp->concat_off_fseq);
		sprintf (arg_silent, "%d", wqp->silent);
		if ((wqp->nb_waiting_on_req > 0) && (wqp->use_subreqid != 0)) {
			stglogit(func, "### wqp->nb_waiting_on_req=%d and wqp->use_subreqid=%d are not compatible, wqp->use_subreqid forced to 0\n", wqp->nb_waiting_on_req, wqp->use_subreqid);
			wqp->use_subreqid = 0;
		}
		sprintf (arg_use_subreqid, "%d", wqp->use_subreqid);
		sprintf (arg_api_flag, "%d", wqp->api_out);
		u64tostr(wqp->flags, arg_api_flags, 0);
		sprintf(arg_rtcp_uid,"%d",wqp->rtcp_uid);
		sprintf(arg_rtcp_gid,"%d",wqp->rtcp_gid);
#ifdef __INSURE__
		stglogit (func, "execing %s reqid=%s key=%s rpfd=%s nbsubreqs=%s nretry=%s Aflag=%s concat_off_fseq=%s silent=%s use_subreqid=%s api_flag=%s flags=%s, rtcp_uid=%s, rtcp_gid=%s, tmpfile=%s, pid=%d\n",
				  progname,
				  arg_reqid,
				  arg_key,
				  arg_rpfd,
				  arg_nbsubreqs,
				  arg_nretry,
				  arg_Aflag,
				  arg_concat_off_fseq,
				  arg_silent,
				  arg_use_subreqid,
				  arg_api_flag,
				  stglogflags(NULL,NULL,(u_signed64) wqp->flags),
				  arg_rtcp_uid,
				  arg_rtcp_gid,
				  tmpfile,
				  (int) getpid());
		execl (progfullpath, progname,
			   arg_reqid,
			   arg_key, arg_rpfd,
			   arg_nbsubreqs,
			   arg_nretry,
			   arg_Aflag,
			   arg_concat_off_fseq,
			   arg_silent,
			   arg_use_subreqid,
			   arg_api_flag,
			   arg_api_flags,
			   arg_rtcp_uid,
			   arg_rtcp_gid,
			   tmpfile,
			   NULL);
#else
		stglogit (func, "execing %s reqid=%s key=%s rpfd=%s nbsubreqs=%s nretry=%s Aflag=%s concat_off_fseq=%s silent=%s use_subreqid=%s api_flag=%s flags=%s rtcp_uid=%s rtcp_gid=%s, pid=%d\n",
				  progname,
				  arg_reqid,
				  arg_key,
				  arg_rpfd,
				  arg_nbsubreqs,
				  arg_nretry,
				  arg_Aflag,
				  arg_concat_off_fseq,
				  arg_silent,
				  arg_use_subreqid,
				  arg_api_flag,
				  stglogflags(NULL,NULL,(u_signed64) wqp->flags),
				  arg_rtcp_uid,
				  arg_rtcp_gid,
				  (int) getpid());
		execl (progfullpath, progname,
			   arg_reqid,
			   arg_key,
			   arg_rpfd,
			   arg_nbsubreqs,
			   arg_nretry,
			   arg_Aflag,
			   arg_concat_off_fseq,
			   arg_silent,
			   arg_use_subreqid,
			   arg_api_flag,
			   arg_api_flags,
			   arg_rtcp_uid,
			   arg_rtcp_gid,
			   NULL);
#endif
		stglogit (func, STG02, "stager", "execl", strerror(errno));
		exit (SYERR);
	} else {
		wqp->status = 0;
		close (pfd[0]);
		for (i = 0, wfp = wqp->wf; i < wqp->nbdskf; i++, wfp++) {
			if (wfp->waiting_on_req > 0) continue;
			if (wfp->waiting_on_req < 0) wfp->waiting_on_req = 0;
			for (stcp = stcs; stcp < stce; stcp++)
				if (stcp->reqid == wfp->subreqid) break;
#ifndef __INSURE__
			if (wfp->ipath[0] != '\0') {
				/* We lye to the system making it believing it is a disk copy */
				/* This apply only for internal copy of CASTOR HSM files */
				/* and by definition there is only ONE entry here */
				memset((void *) &lying_stcp, 0, sizeof(struct stgcat_entry));
				lying_stcp.t_or_d = 'd';
				changed_t_or_d = '3';
				strcpy(lying_stcp.u1.d.xfile,wfp->ipath);
				lying_stcp.u1.d.Xparm[0] = '\0';
				lying_stcp.actual_size = stcp->actual_size;
				lying_stcp.size = stcp->size;
				lying_stcp.mask = stcp->mask;
				lying_stcp.uid = stcp->uid;
				lying_stcp.gid = stcp->gid;
				lying_stcp.reqid = stcp->reqid;
				strcpy(lying_stcp.ipath, stcp->ipath);
				write (pfd[1], (char *) &lying_stcp, sizeof(struct stgcat_entry));
			} else {
				write (pfd[1], (char *) stcp, sizeof(struct stgcat_entry));
			}
#endif
		}
		close (pfd[1]);
#if SACCT
		switch (t_or_d) {
		case 'h': /* CASTOR HSM */
			if (changed_t_or_d == '\0') {
				if ((wqp->req_type == STAGEIN) || (wqp->req_type == STAGE_IN)) {
					changed_t_or_d = '1';
				} else {
					changed_t_or_d = '2';
				}
			}
			break;
		case 't': /* Tape */
			if ((wqp->req_type == STAGEIN) || (wqp->req_type == STAGE_IN)) {
				changed_t_or_d = '4';
			} else {
				changed_t_or_d = '5';
			}
			break;
		case 'd': /* Disk */
			if ((wqp->req_type == STAGEIN) || (wqp->req_type == STAGE_IN)) {
 				changed_t_or_d = '6';
			} else {
				changed_t_or_d = '7';
			}
			break;
		case 'm': /* Non-CASTOR HSM */
			if ((wqp->req_type == STAGEIN) || (wqp->req_type == STAGE_IN)) {
				changed_t_or_d = '8';
			} else {
				changed_t_or_d = '9';
			}
			break;
		}
		/* When stagestat processes accounting files, previous version was IGNORING any member but the subtype */
		/* when subtype == STGCMDS. Exitcode is now used to store additional information */
		/* Previous stagestat will continue to ignore it. New stagestat will use it. */
		stageacct (STGCMDS, wqp->req_uid, wqp->req_gid, wqp->clienthost,
				   wqp->reqid, wqp->req_type, wqp->nretry, (int) ((changed_t_or_d != '\0') ? changed_t_or_d : t_or_d),
				   NULL, "", changed_t_or_d != '\0' ? changed_t_or_d : t_or_d);
#endif
	}
	return (0);
}

struct stgpath_entry *
newpath()
{
	struct stgpath_entry *stpp;

	nbpath_ent++;
	if (nbpath_ent > stgpath_bufsz/sizeof(struct stgpath_entry)) {
		stps = (struct stgpath_entry *) realloc (stps, stgpath_bufsz + STG_BUFSIZ);
		memset ((char *)stps+stgpath_bufsz, 0, STG_BUFSIZ);
		stgpath_bufsz += STG_BUFSIZ;
		stpe = stps + (stgpath_bufsz/sizeof(struct stgpath_entry));
	}
	stpp = stps + (nbpath_ent - 1);
	return (stpp);
}

struct stgcat_entry *
newreq(int_t_or_d)
		int int_t_or_d;
{
	char t_or_d = (char) int_t_or_d;
	struct stgcat_entry *stcp;

	nbcat_ent++;
	if (nbcat_ent > stgcat_bufsz/sizeof(struct stgcat_entry)) {
		stcs = (struct stgcat_entry *) realloc (stcs, stgcat_bufsz + STG_BUFSIZ);
		memset ((char *)stcs+stgcat_bufsz, 0, STG_BUFSIZ);
		stgcat_bufsz += STG_BUFSIZ;
		stce = stcs + (stgcat_bufsz/sizeof(struct stgcat_entry));
	}
	stcp = stcs + (nbcat_ent - 1);
	switch (t_or_d) {
	case 'h':
		/* We know we have to initialize some sensible things that calloc does not properly do */
		stcp->u1.h.retenp_on_disk = -1;
		stcp->u1.h.mintime_beforemigr = -1;
		break;
	default:
		break;
	}
	return (stcp);
}

int nextreqid()
{
	int found;
	struct stgcat_entry *stcp;

	while (1) {
		found = 0;
		if (++last_reqid > CA_MAXSTGREQID) last_reqid = 1;
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (stcp->reqid == last_reqid) {
				found = 1;
				break;
			}
		}
		if (! found) return (last_reqid);
	}
}

int req2argv(rbp, argvp)
		 char *rbp;
		 char ***argvp;
{
	char **argv;
	int i, n;
	int nargs;
	char *p;

	unmarshall_WORD (rbp, nargs);
	argv = (char **) malloc ((nargs + 1) * sizeof(char *));
	unmarshall_STRING (rbp, argv[0]);
	strcpy (logbuf, argv[0]);
	for (i = 1; i < nargs; i++) {
		unmarshall_STRING (rbp, argv[i]);
		strcat (logbuf, " ");
		n = strlen (argv[i]);
		p = argv[i];
		if ((int) (strlen (logbuf) + n) > (PRTBUFSZ - 43)) {
			strcat (logbuf, "\\");
			stglogit (func, STG98, logbuf);
			strcpy (logbuf, "+ ");
			while (n > (PRTBUFSZ - 45)) {
				strncat (logbuf, p, PRTBUFSZ - 45);
				strcat (logbuf, "\\");
				stglogit (func, STG98, logbuf);
				strcpy (logbuf, "+ ");
				n -= PRTBUFSZ - 45;
				p += PRTBUFSZ - 45;
			}
		}
		strcat (logbuf, p);
	}
	argv[nargs] = NULL;
	stglogit (func, STG98, logbuf);
	*argvp = argv;
	return (nargs);
}

void rmfromwq(wqp)
		 struct waitq *wqp;
{
	char *func = "rmfromwq";

	if (wqp->prev) (wqp->prev)->next = wqp->next;
	else waitqp = wqp->next;
	if (wqp->next) (wqp->next)->prev = wqp->prev;
	free (wqp->wf);
	if (wqp->save_subreqid != NULL) free (wqp->save_subreqid);
	free (wqp);
	if (--nwaitq < 0) {
		stglogit (func, "### nwaitq < 0 - reset to zero\n");
		nwaitq = 0;
	}
}

int savepath()
{
#ifndef USECDB
	int c, n;
	int spfd;
#endif

#ifdef USECDB
	/* This function is now dummy with the DB interface */
#else
	if ((spfd = open (STGPATH, O_WRONLY)) < 0) {
		stglogit (func, STG02, STGPATH, "open", strerror(errno));
		return (-1);
	}
	n = nbpath_ent * sizeof(struct stgpath_entry);
	if (nbpath_ent != 0) {
		if ((c = write (spfd, stps, n)) != n) {
			stglogit (func, STG02, STGPATH, "write", strerror(errno));
			close (spfd);
			return (-1);
		}
	}
	ftruncate (spfd, n);
	close (spfd);
#endif

	return (0);
}

int savereqs()
{
#ifndef USECDB
	int c, n;
	int scfd;
#endif

#ifdef USECDB
	/* This function is now dummy with the DB interface */
#else
	if ((scfd = open (STGCAT, O_WRONLY)) < 0) {
		stglogit (func, STG02, STGCAT, "open", strerror(errno));
		return (-1);
	}
	n = nbcat_ent * sizeof(struct stgcat_entry);
	if (nbcat_ent != 0) {
		if ((c = write (scfd, stcs, n)) != n) {
			stglogit (func, STG02, STGCAT, "write", strerror(errno));
			close (scfd);
			return (-1);
		}
	}
	ftruncate (scfd, n);
	close (scfd);
#endif

	return (0);
}

void sendinfo2cptape(rpfd, stcp)
		 int *rpfd;
		 struct stgcat_entry *stcp;
{
	char buf[PRTBUFSZ];

	switch (stcp->t_or_d) {
	case 't':
		if (strcmp(stcp->u1.t.lbl, "sl") == 0 || strcmp(stcp->u1.t.lbl, "al") == 0) {
			sprintf(buf, "-P %s -q %s -b %d -F %s -f %s -L %d",
							 stcp->ipath, stcp->u1.t.fseq, stcp->blksize,
							 stcp->recfm, stcp->u1.t.fid, stcp->lrecl);
		} else {
			sprintf(buf, "-P %s -q %s", stcp->ipath, stcp->u1.t.fseq);
		}
		break;
	default:
		sprintf(buf, "-P %s", stcp->ipath);
		break;
	}
	sendrep(rpfd, MSG_ERR, STG47, buf);
}

int ask_stageout(req_type, upath, found_stcp)
		 int req_type;
		 char *upath;
		 struct stgcat_entry **found_stcp;
{
	int found;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;

	found = 0;
	/* first lets assume that internal and user path are different */
	for (stpp = stps; stpp < stpe; stpp++) {
		if (stpp->reqid == 0) break;
		if (strcmp (upath, stpp->upath)) continue;
		found = 1;
		break;
	}
	if (found) {
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stpp->reqid == stcp->reqid) break;
		}
	} else {
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (strcmp (upath, stcp->ipath)) continue;
			found = 1;
			break;
		}
	}
	if (found == 0 ||
			(((req_type == STAGEUPDC) || (req_type == STAGE_UPDC)) &&
			 (stcp->status != STAGEOUT) && (stcp->status != STAGEALLOC)) ||
			((req_type == STAGEPUT) &&
			 (! ISSTAGEOUT(stcp)) && ((stcp->status & (STAGEOUT|PUT_FAILED)) != (STAGEOUT|PUT_FAILED)))) {
		sendrep (&rpfd, MSG_ERR, STG22);
		return (ENOENT);
	}
	*found_stcp = stcp;
	return (0);
}

int upd_stageout(req_type, upath, subreqid, can_be_migr_flag, forced_stcp, was_put_failed, nohsmcreat_flag)
		 int req_type;
		 char *upath;
		 int *subreqid;
		 int can_be_migr_flag;
		 struct stgcat_entry *forced_stcp;
		 int was_put_failed;
		 int nohsmcreat_flag;
{
	int found;
	struct stat64 st;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	int done_a_time = 0;

	found = 0;
	if (forced_stcp == NULL) {
		/* first lets assume that internal and user path are different */
		for (stpp = stps; stpp < stpe; stpp++) {
			if (stpp->reqid == 0) break;
			if (strcmp (upath, stpp->upath)) continue;
			found = 1;
			break;
		}
		if (found) {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stpp->reqid == stcp->reqid) break;
			}
		} else {
			for (stcp = stcs; stcp < stce; stcp++) {
				if (stcp->reqid == 0) break;
				if (strcmp (upath, stcp->ipath)) continue;
				/* Warning: Here the status have to be exactly STAGEOUT or STAGEALLOC */
				/* That's why we do not use the macros ISSTAGEOUT(stcp) nor ISSTAGEALLOC(stcp) */
				if (((req_type == STAGEUPDC) || (req_type == STAGE_UPDC)) && (stcp->status != STAGEOUT) && (stcp->status != STAGEALLOC)) continue;
				found = 1;
				break;
			}
		}
	} else {
		found = 1;
		stcp = forced_stcp;
	}
	if (found == 0 ||
			(((req_type == STAGEUPDC) || (req_type == STAGE_UPDC)) &&
			 (stcp->status != STAGEOUT) && (stcp->status != STAGEALLOC)) ||
			((req_type == STAGEPUT) &&
			 (! ISSTAGEOUT(stcp)) && ((stcp->status & (STAGEOUT|PUT_FAILED)) != (STAGEOUT|PUT_FAILED)))) {
		sendrep (&rpfd, MSG_ERR, STG22);
		return (ENOENT);
	}
	if ((stcp->status == STAGEOUT) || (stcp->status == STAGEALLOC)) {
		u_signed64 actual_size_block;

		PRE_RFIO;
		if (RFIO_STAT64(stcp->ipath, &st) == 0) {
			stcp->actual_size = st.st_size;
			if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
				actual_size_block = stcp->actual_size;
			}
		} else {
			sendrep(&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
			return(rfio_serrno());
		}
		updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
						(signed64) ((signed64) stcp->size - (signed64) actual_size_block));
		rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, STAGEUPDC);
	}
	if (req_type == STAGEPUT) {
		stcp->status = STAGEPUT;
	} else if ((req_type == STAGEUPDC) || (req_type == STAGE_UPDC)) {
		if ((stcp->status == STAGEOUT) && ((stcp->t_or_d == 'm') || (stcp->t_or_d == 'h'))) {
			if (stcp->actual_size <= 0) {
				/* We cannot put a to-be-migrated file in status CAN_BE_MIGR if its size is zero */
				sendrep (&rpfd, MSG_ERR, STG02, (stcp->t_or_d == 'm') ? stcp->u1.m.xfile : stcp->u1.h.xfile, "size", "zero-length file on disk - removed");
				if (delfile(stcp, 0, 1, 1,
							"upd_stageout update on zero-length to-be-migrated file ok",
							stcp->uid, stcp->gid, 0, 0, 0) < 0) {
					sendrep (&rpfd, MSG_ERR, STG02, stcp->ipath,
									 RFIO_UNLINK_FUNC(stcp->ipath), rfio_serror());
				}
				return((req_type > STAGE_00) ? ESTCLEARED : CLEARED);
			} else {
				if (stcp->t_or_d == 'h') {
					int thisrc;
					int yetdone_Cns_statx_flag = 0;
					struct Cns_filestat Cnsfilestat;

					if (last_upd_fileclasses == 0) {
					  /* This routine is executed at startup on STAGEOUT CASTOR files only, and we know */
					  /* by construction that it is because file is ENOENT in the Name Server */
					  /* No need to call stageput_check_hsm */
					  return(ENOENT);
					}

					/* This is a CASTOR HSM file */
					if ((thisrc = stageput_check_hsm(stcp,stcp->uid,stcp->gid,was_put_failed,&yetdone_Cns_statx_flag,&Cnsfilestat,nohsmcreat_flag)) != 0) {
						return(thisrc);
					}
					if (can_be_migr_flag) {
						stcp->status |= CAN_BE_MIGR; /* Now status is STAGEOUT | CAN_BE_MIGR */
						/* This is a file for automatic migration */
						done_a_time = 1;
						stcp->a_time = time(NULL);
						if ((thisrc = update_migpool(&stcp,1,0)) != 0) {
							return(serrno);
						}
					}
				}
			}
		} else {
			stcp->status |= STAGED;
		}
	}
	if (! done_a_time) stcp->a_time = time(NULL);
	if (subreqid != NULL) *subreqid = stcp->reqid;

#ifdef USECDB
	if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
		stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif

	return (0);
}

int upd_staged(upath)
		 char *upath;
{
	int found;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	u_signed64 actual_size_block;
	u_signed64 elemp_free;
	struct stat64 st;

	found = 0;
	/* first lets assume that internal and user path are different */
	for (stpp = stps; stpp < stpe; stpp++) {
		if (stpp->reqid == 0) break;
		if (strcmp (upath, stpp->upath)) continue;
		found = 1;
		break;
	}
	if (found) {
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stpp->reqid == stcp->reqid) break;
		}
	} else {
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if ((stcp->t_or_d != 'm') && (stcp->t_or_d != 'h')) continue;
			if (strcmp (upath, stcp->ipath)) continue;
			found = 1;
			break;
		}
	}
	if ((found == 0) || ((stcp->t_or_d != 'm') && (stcp->t_or_d != 'h')) || ((stcp->status & BEING_MIGR) == BEING_MIGR) || ((stcp->status & WAITING_MIGR) == WAITING_MIGR) || (! (((stcp->status & 0xF0) == STAGED) || ((stcp->status & (STAGEOUT|CAN_BE_MIGR)) == (STAGEOUT|CAN_BE_MIGR))))) {
		if (found == 0) {
			sendrep (&rpfd, MSG_ERR, STG22);
		} else if ((stcp->t_or_d != 'm') && (stcp->t_or_d != 'h')) {
			sendrep (&rpfd, MSG_ERR, "STG02 - Request should be on an HSM file\n");
		} else if ((stcp->status & BEING_MIGR) == BEING_MIGR) {
			sendrep (&rpfd, MSG_ERR, "STG02 - Request should not be on a BEING_MIGR HSM file\n");
		} else {
			sendrep (&rpfd, MSG_ERR, "STG02 - %s : Request should be on a STAGED or CAN_BE_MIGR HSM file\n", stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile);
		}
		return (ENOENT);
	}
	if ((stcp->status & (STAGEOUT|CAN_BE_MIGR)) == (STAGEOUT|CAN_BE_MIGR)) {
		/* There are the migration counters to update */
		update_migpool(&stcp,-1,0);
	}
	stcp->status = STAGEOUT;
	rwcountersfs(stcp->poolname, stcp->ipath, STAGEOUT, STAGEOUT);
	stcp->a_time = time(NULL);
	stcp->nbaccesses++;
	if (*upath && strcmp (stcp->ipath, upath))
		create_link (&rpfd, stcp, upath);

	savereqs();
	PRE_RFIO;
	if (RFIO_STAT64(stcp->ipath, &st) == 0) {
		stcp->actual_size = st.st_size;
		if ((actual_size_block = BLOCKS_TO_SIZE(st.st_blocks,stcp->ipath)) < stcp->actual_size) {
			actual_size_block = stcp->actual_size;
		}
	} else {
		sendrep(&rpfd, MSG_ERR, STG02, stcp->ipath, RFIO_STAT64_FUNC(stcp->ipath), rfio_serror());
		return(rfio_serrno());
    }
#ifdef USECDB
	if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
		stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
	}
#endif
	/* We instruct updfreespace() that we reject overflow */
	if (updfreespace (stcp->poolname, stcp->ipath, 1, (u_signed64 *) &elemp_free, 
					  (signed64) ((signed64) actual_size_block - (signed64) stcp->size)) != 0) {
		u_signed64 elemp_free_plus_blocks;
		int sav_stcp_size = stcp->size;
		char tmpbuf1[21], tmpbuf2[21];

		/* There is overflow - this mean that incrementing elemp->free and pool_p->free by */
		/* (actual_size_block - stcp->size) would have make free space > capacity... */
		/* We give it another try by reajusting stcp->size to the exact value v.s. elemp_free */
		/* We have to make sure that elemp_free + (actual_size_block - stcp->size) >= 0 */
		/* e.g. stcp->size <= elemp_free + actual_size_block */
		elemp_free_plus_blocks = elemp_free + actual_size_block;

		/* The following have to underestimate as less as possible the size */
		/* Note that in rfio/rfcp.c there is almost the same line, but then there is a +1 to overestimate */
		/* as less as possible the size...! */
		stcp->size = elemp_free_plus_blocks;
		sendrep (&rpfd, MSG_ERR, "STG02 - %s : Filesystem too low in space, underestimating initial allocated size from %s to %s\n",  stcp->t_or_d == 'm' ? stcp->u1.m.xfile : stcp->u1.h.xfile, u64tostru(sav_stcp_size,tmpbuf1,0), u64tostru(stcp->size,tmpbuf2,0));
		/* Give it another try, then allowing overflow in order to allow user to continue */
		updfreespace (stcp->poolname, stcp->ipath, 0, NULL, 
					  (signed64) ((signed64) actual_size_block - (signed64) stcp->size));
	}
	return (0);
}

#if ! defined(_WIN32)
void stgdaemon_wait4child(signo)
	int signo;
{
	return;
}

void check_child_exit()
{
	int pid;
	int status;

	while ((pid = waitpid (-1, &status, WNOHANG)) > 0) {
		checkovlstatus(pid,status);
	}
}
#endif

void stgdaemon_usage() {
	printf("\nUsage : stgdaemon [options]\n"
		   "  where options can be\n"
		   "  -f                   Foreground\n"
		   "  -h                   This help\n"
		   "  -L <%%d>              Listen backlog\n"
		   "  --rlimit_nofile <%%d> Max number of open file (or -F <%%d>)\n"
		   "  --rlimit_nproc  <%%d> Max number of processes (or -P <%%d>)\n"
		   "  -p <%%d>              Port number\n"
 		   "\n"
		);
}

int verif_euid_egid(euid,egid,username,group)
	uid_t euid;
	gid_t egid;
	char *username;
	char *group;
{
	struct passwd *this_passwd;             /* password structure pointer */
	struct group *this_gr;

	if (stage_util_validuser(NULL,euid, egid) != 0) {
		stglogit(func, "### stage_util_validuser(NULL,%d,%d) error, %s\n",(int) euid, (int) egid, sstrerror(serrno));
 		return(-1);
	}

	if ((this_passwd = Cgetpwuid(euid)) == NULL) {
		stglogit(func, "### Cannot Cgetpwuid(%d) (%s)\n",(int) euid, strerror(errno));
		stglogit(func, "### Please check existence of uid %d in password file\n", (int) euid);
        serrno = SEUSERUNKN;
 		return(-1);
	}

	/* We get group name */
	if ((this_gr = Cgetgrgid(egid)) == NULL) {
		stglogit(func, "### Cannot Cgetgrgid(%d) (%s)\n",egid,strerror(errno));
		stglogit(func, "### Please check existence of group %d (gid of account \"%s\") in group file\n", egid, this_passwd->pw_name);
        serrno = SEUSERUNKN;
 		return(-1);
	}
	if (username != NULL) strcpy(username,this_passwd->pw_name);
	if (group != NULL) strcpy(group,this_gr->gr_name);
	/* Ok */
	return(0);
}

void check_upd_fileclasses() {
	time_t this_time = time(NULL);
	char *func = "check_upd_fileclasses";

	if ((this_time - last_upd_fileclasses) > upd_fileclasses_int) {
		stglogit(func, "Automatic update of CASTOR fileclasses\n");
		upd_fileclasses();
		/* Update check_upd_fileclasses time */
		last_upd_fileclasses = time(NULL);
	}
}

#ifdef MONITOR
/*
 * Function that sends the monitoring information to the Monitoring daemon when necessary
 *
 */
int check_send_monitormsg() {
  time_t this_time = time(NULL);

  if (monitorok) {
	  if ((this_time - last_monitormsg_sent) > CMONIT_STAGER_SEND_PERIOD) {
		  /* Using the Monitoring API to send the data */
		  Cmonit_send_stager_status(last_init_time);
		  last_monitormsg_sent = time(NULL);
	  }
  }
  return(0);
}
#endif

/* ----------------------------------------------------------- */
/* Subroutine: stg_count_digits                                */
/* ----------------------------------------------------------- */
/* Utility routine to count the number of digits of an integer */
/* ----------------------------------------------------------- */
/* Input: integer                                              */
/*        base                                                 */
/* Output: number of digits                                    */
/* ----------------------------------------------------------- */
size_t stg_count_digits(number, base)
	int number;
	unsigned int base;
{
	int digits = 1;
	size_t rc;

	if ( number < 0 ) {
		++digits;
		if ( number == INT_MIN ) {
			number = -(number / (int)base) - (number % (int)base > 0);
			++digits;
		} else
			number = -number;
	}
  
	while ( number /= base )
		++digits;
  
	rc = (size_t) digits;
	return(rc);
}
