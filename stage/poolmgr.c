/*
 * $Id: poolmgr.c,v 1.256 2004/09/16 19:07:34 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: poolmgr.c,v $ $Revision: 1.256 $ $Date: 2004/09/16 19:07:34 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <signal.h>
#ifndef _WIN32
#include <unistd.h>
#else
#define R_OK 4
#define W_OK 2
#define X_OK 1
#define F_OK 0
#endif
#include "osdep.h"
#ifndef _WIN32
#include <netinet/in.h>
#endif
#include <errno.h>
#include <limits.h> /* For INT_MAX */
#include "rfio_api.h"
#include "stage.h"
#include "stage_api.h"
#include "u64subr.h"
#include "serrno.h"
#include "marshall.h"
#include "osdep.h"
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include "Cns_api.h"
#include "Cupv_api.h"
#include "Castor_limits.h"
#ifndef CA_MAXDOMAINNAMELEN
#define CA_MAXDOMAINNAMELEN CA_MAXHOSTNAMELEN
#endif
#include "Cpwd.h"
#include "Cgrp.h"
#include "Csnprintf.h"

#ifndef _WIN32
/* Signal handler - Simplify the POSIX sigaction calls */
#ifdef __STDC__
typedef void    Sigfunc(int);
Sigfunc *_stage_signal(int, Sigfunc *);
#else
typedef void    Sigfunc();
Sigfunc *_stage_signal();
#endif
#else
#define _stage_signal(a,b) signal(a,b)
#endif
void stage_stgcleanup _PROTO((int));

#if (defined(IRIX5) || defined(IRIX6) || defined(IRIX64))
/* Surpringly, on Silicon Graphics, strdup declaration depends on non-obvious macros */
extern char *strdup _PROTO((CONST char *));
#endif

extern char *getconfent();
extern char defpoolname[];
extern char defpoolname_in[];
extern char defpoolname_out[];
extern char currentpool_out[];
extern int rpfd;
extern int req2argv _PROTO((char *, char ***));
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern struct stgpath_entry *stps;	/* start of stage path catalog */
extern int maxfds;
extern int reqid;
extern int stglogit _PROTO((char *, char *, ...));
extern int stgmiglogit _PROTO((char *, char *, ...));
extern char *stglogflags _PROTO((char *, char *, u_signed64));
extern int sendrep _PROTO((int *, int, ...));
extern struct stgcat_entry *newreq _PROTO((int));
extern int nextreqid _PROTO(());
EXTERN_C int DLL_DECL rfio_parseln _PROTO((char *, char **, char **, int));

struct migrator *migrators;
struct fileclass *fileclasses;
int nbmigrator;
int nbpool;
int nbhost;
int nbstageout;
int nbstagealloc;
static char *nfsroot;
static char **poolc;
struct pool *pools;
#ifndef _WIN32
struct sigaction sa_poolmgr;
#endif
static int nbfileclasses;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
extern int savereqs _PROTO(());
extern char *localhost; /* Fully qualified hostname */
extern char localdomain[];  /* Local domain */
extern int selectfs_ro_with_nb_stream;
extern int selectfs_rw_with_nb_stream;
static int redomigpool_in_action = 0;
static struct pool *sav_pools = NULL;
static int sav_nbpool = 0;

struct files_per_stream {
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp;
	int nstcp;
	u_signed64 size;
	uid_t euid;
	gid_t egid;
	char tppool[CA_MAXPOOLNAMELEN+1];
	int nb_substreams;
};

/* This structure is used for the qsort on pool elements */
struct pool_element_ext {
	u_signed64 free;
	u_signed64 capacity;
	int nbreadaccess;
	int nbwriteaccess;
	int mode;
	int nbreadserver;
	int nbwriteserver;
	int poolmigrating;
	char server[CA_MAXHOSTNAMELEN+1];
	char dirpath[MAXPATH];
	time_t last_allocation;
	time_t server_last_allocation;
	u_signed64 defsize;
};

void print_pool_utilization _PROTO((int *, char *, char *, char *, char *, int, int, int, int));
int update_migpool _PROTO((struct stgcat_entry **, int, int));
void incr_migpool_counters _PROTO((struct stgcat_entry *, struct pool *, int, int, time_t));
int insert_in_migpool _PROTO((struct stgcat_entry *));
void checkfile2mig _PROTO(());
int migrate_files _PROTO((struct pool *));
int migpoolfiles _PROTO((struct pool *));
int iscleanovl _PROTO((int, int));
void killcleanovl _PROTO((int));
int ismigovl _PROTO((int, int));
void killmigovl _PROTO((int));
int isvalidpool _PROTO((char *));
int ismetapool _PROTO((char *));
int havemetapool _PROTO((char *, char *));
int max_setretenp _PROTO((char *));
void migpoolfiles_log_callback _PROTO((int, char *));
char migpoolfiles_migrname[CA_MAXMIGRNAMELEN+1024+1];
int isuserlevel _PROTO((char *));
void poolmgr_wait4child _PROTO(());
int selectfs _PROTO((char *, u_signed64 *, char *, int, int));
void rwcountersfs _PROTO((char *, char *, int, int));
void getdefsize _PROTO((char *, u_signed64 *));
void getmindefsize _PROTO((char *, u_signed64 *));
int updfreespace _PROTO((char *, char *, int, u_signed64 *, signed64));
void redomigpool _PROTO(());
int updpoolconf _PROTO((char *, char *, char *));
int getpoolconf _PROTO((char *, char *, char *));
int checkpoolcleaned _PROTO((char ***));
void checkpoolspace _PROTO(());
int cleanpool _PROTO((char *));
int get_create_file_option _PROTO((char *));
int export_hsm_option _PROTO((char *));
int have_another_pool_with_export_hsm_option _PROTO((char *));
int poolalloc _PROTO((struct pool *, int));
int checklastaccess _PROTO((char *, time_t));
int enoughfreespace _PROTO((char *, int));
int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *, int, int, int));
int upd_fileclasses _PROTO(());
void upd_last_tppool_used _PROTO((struct fileclass *));
char *next_tppool _PROTO((struct fileclass *));
int euid_egid _PROTO((uid_t *, gid_t *, char *, struct migrator *, struct stgcat_entry *, struct stgcat_entry *, char **, int, int));
extern int verif_euid_egid _PROTO((uid_t, gid_t, char *, char *));
extern int upd_stageout _PROTO((int, char *, int *, int, struct stgcat_entry *, int, int));
void stglogfileclass _PROTO((struct Cns_fileclass *));
void printfileclass _PROTO((int *, struct fileclass *));
int retenp_on_disk _PROTO((int));
int mintime_beforemigr _PROTO((int));
int mintime_beforemigr_vs_pool _PROTO((struct pool *, int));
void check_delaymig _PROTO(());
void check_expired _PROTO(());
int ispool_out _PROTO((char *));
void nextpool_out _PROTO((char *));
void bestnextpool_out _PROTO((char *, int));
void bestnextpool_from_metapool _PROTO((char *, char *, int));
int ispoolmigrating _PROTO((char *));
time_t server_last_allocation _PROTO((char *));
time_t pool_last_allocation _PROTO((char *));
time_t elemp_last_allocation _PROTO((char *, char *));
struct pool_element *betterfs_vs_pool _PROTO((char *, int, u_signed64, int *));
int pool_elements_cmp _PROTO((CONST void *, CONST void *));
int pool_elements_cmp_simple _PROTO((CONST void *, CONST void *));
void get_global_stream_count _PROTO((char *, int *, int *));
char *findpoolname _PROTO((char *));
int findfs _PROTO((char *, char **, char **));
u_signed64 findblocksize _PROTO((char *));
#if defined(_WIN32)
extern int create_dir _PROTO((char *, uid_t, gid_t, int));
#else
#ifdef hpux
/* What the hell does hpux does not like this prototype ??? */
extern int create_dir _PROTO(());
#else
extern int create_dir _PROTO((char *, uid_t, gid_t, mode_t));
#endif
#endif
signed64 get_stageout_retenp _PROTO((struct stgcat_entry *));
signed64 get_stagealloc_retenp _PROTO((struct stgcat_entry *));
signed64 get_put_failed_retenp _PROTO((struct stgcat_entry *));

signed64 get_stageout_retenp_raw _PROTO((struct stgcat_entry *));
signed64 get_stagealloc_retenp_raw _PROTO((struct stgcat_entry *));
signed64 get_put_failed_retenp_raw _PROTO((struct stgcat_entry *));

int have_at_least_one_stageout_retenp_raw _PROTO(());
int have_at_least_one_stagealloc_retenp_raw _PROTO(());
int have_at_least_one_put_failed_retenp_raw _PROTO(());
void update_last_allocation _PROTO((char *));
void update_last_pool_allocation _PROTO((char *));

#if hpux
/* On HP-UX seteuid() and setegid() do not exist and have to be wrapped */
/* calls to setresuid().                                                */
#define seteuid(euid) setresuid(-1,euid,-1)
#define setegid(egid) setresgid(-1,egid,-1)
#endif

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

int getpoolconf(defpoolname,defpoolname_in,defpoolname_out)
	char *defpoolname;
	char *defpoolname_in;
	char *defpoolname_out;
{
	char buf[4096];
	u_signed64 defsize;
	u_signed64 mindefsize;
	char *dp;
	struct pool_element *elemp = NULL;
	int errflg = 0;
	FILE *fopen(), *s;
	char func[16];
	int i, j;
	struct migrator *migr_p;
	int nbpool_ent;
	char *p;
	char path[MAXPATH];
	struct pool *pool_p;
	struct rfstatfs st;
	time_t thistime;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
	int nbmigrator_real;
	extern char *stgconfigfile;
	int nblocal;

	strcpy (func, "getpoolconf");
	if ((s = fopen (stgconfigfile, "r")) == NULL) {
		sendrep(&rpfd, MSG_ERR, STG23, stgconfigfile);
		return (ESTCONF);
	}
	
	/* 1st pass: count number of pools and migrators  */

	migrators = NULL;
	nbmigrator_real = 0;
	nbmigrator = 0;
	nbpool = 0;
	nbhost = 0;
	nblocal = 0;
	*defpoolname = '\0';
	*defpoolname_in = '\0';
	*defpoolname_out = '\0';
	defsize = 0;
	mindefsize = 0;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		int gotit;
		char *pdash;

		if ((pdash = strchr(buf,'#')) != NULL) {
			*pdash = '\0';
		}
		if ((p = strtok (buf, " \t\n")) == NULL) continue; 
		if (strcmp (p, "POOL") == 0) {
			nbpool++;
		}
		gotit = 0;
		while ((p = strtok (NULL, " \t\n")) != NULL) {
			if (strcmp (p, "MIGRATOR") == 0) {
				if (gotit != 0) {
					sendrep(&rpfd, MSG_ERR, "STG29 - Bad MIGRATOR construct\n");
					errflg++;
					goto reply;
				}
				nbmigrator++;
				gotit = 1;
			}
		}
	}
	if (nbpool == 0) {
		sendrep (&rpfd, MSG_ERR, STG29);
		errflg++;
		goto reply;
	}
	pools = (struct pool *) calloc (nbpool, sizeof(struct pool));
	if (nbmigrator > 0)
		migrators = (struct migrator *)
			calloc (nbmigrator, sizeof(struct migrator));
	/* Although there is no pid for any migration yet, the element migreqtime_last_start */
	/* has to be initialized to current time... */
	thistime = time(NULL);
	for (i = 0; i < nbmigrator; i++) {
		migrators[i].migreqtime_last_start = thistime;
	}

	/* 2nd pass: count number of members in each pool and
	   store migration parameters */

	rewind (s);
	nbpool_ent = -1;
	pool_p = pools - 1;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		char *pdash;

		if ((pdash = strchr(buf,'#')) != NULL) {
			*pdash = '\0';
		}
		if ((p = strtok (buf, " \t\n")) == NULL) continue;
		if (strcmp (p, "POOL") == 0) {
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = 0;
			pool_p++;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG25, "pool");	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > CA_MAXPOOLNAMELEN) {
				sendrep (&rpfd, MSG_ERR, STG27, "pool", p);
				errflg++;
				goto reply;
			}
			strcpy (pool_p->name, p);
			/* Initialize some critical values */
			pool_p->max_setretenp = -1;
			pool_p->put_failed_retenp = -1;
			pool_p->stageout_retenp = -1;
			pool_p->stagealloc_retenp = -1;
			while ((p = strtok (NULL, " \t\n")) != NULL) {
				if (strcmp (p, "DEFSIZE") == 0) {
					int checkrc;
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "DEFSIZE in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					if ((checkrc = stage_util_check_for_strutou64(p)) < 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "DEFSIZE in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->defsize = strutou64(p);
					if (pool_p->defsize <= 0) {
						/* Well, it can not be < 0, strictly speaking */
						sendrep (&rpfd, MSG_ERR, STG26, "DEFSIZE in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					if (checkrc == 0) pool_p->defsize *= ONE_MB; /* Not unit : default MB */
				} else if (strcmp (p, "MINDEFSIZE") == 0) {
					int checkrc;
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "MINDEFSIZE in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					if ((checkrc = stage_util_check_for_strutou64(p)) < 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "MINDEFSIZE in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->mindefsize = strutou64(p);
					if (pool_p->mindefsize <= 0) {
						/* Well, it can not be < 0, strictly speaking */
						sendrep (&rpfd, MSG_ERR, STG26, "MINDEFSIZE in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					if (checkrc == 0) pool_p->mindefsize *= ONE_MB; /* Not unit : default MB */
				} else if ((strcmp (p, "MINFREE") == 0) || (strcmp (p, "GC_STOP_THRESH") == 0)) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "GC_STOP_THRESH (or MINFREE) in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					stage_strtoi(&(pool_p->gc_stop_thresh), p, &dp, 10);
					if (*dp != '\0' || pool_p->gc_stop_thresh < 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "GC_STOP_THRESH (or MINFREE) pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "GC_START_THRESH") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "GC_START_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					stage_strtoi(&(pool_p->gc_start_thresh), p, &dp, 10);
					if (*dp != '\0' || pool_p->gc_start_thresh < 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "GC_START_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "MAX_SETRETENP") == 0) {
					int thisunit = ONE_SECOND; /* Default is second */
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "MAX_SETRETENP in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					switch (p[strlen(p)-1]) {
					case 'S':
					case 's':
						thisunit = ONE_SECOND; /* Second */
						p[strlen(p) - 1] = '\0';
						break;
					case 'M':
					case 'm':
						thisunit = ONE_MINUTE; /* Minute */
						p[strlen(p) - 1] = '\0';
						break;
					case 'H':
					case 'h':
						thisunit = ONE_HOUR; /* Hour */
						p[strlen(p) - 1] = '\0';
						break;
					case 'D':
					case 'd':
						thisunit = ONE_DAY; /* Day */
						p[strlen(p) - 1] = '\0';
						break;
					case 'Y':
					case 'y':
						thisunit = ONE_YEAR; /* Year */
						p[strlen(p) - 1] = '\0';
						break;
					default:
						/* If there is another non-digit character the stage_strtoi() call will catch it */
						break;
					}
					if (stage_strtoi(&(pool_p->max_setretenp), p, &dp, 0) != 0) {
						if (serrno != ERANGE) {
							sendrep (&rpfd, MSG_ERR, STG26, "MAX_SETRETENP in pool", pool_p->name);
						} else {
							sendrep (&rpfd, MSG_ERR, STG26, "MAX_SETRETENP (out of range) in pool", pool_p->name);
						}
						errflg++;
						goto reply;
					}
					if (pool_p->max_setretenp < 0) {
						pool_p->max_setretenp = -1;
					} else {
						if (thisunit != ONE_SECOND) {
							/* Again out of range ? */
							if (pool_p->max_setretenp > (INT_MAX / thisunit)) {
								sendrep (&rpfd, MSG_ERR, STG26, "MAX_SETRETENP (out of range) in pool", pool_p->name);
								errflg++;
								goto reply;
							} else {
								pool_p->max_setretenp *= thisunit;
							}
						}
					}
					if (pool_p->max_setretenp == 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "MAX_SETRETENP (should be <0 xor >0) in pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "PUT_FAILED_RETENP") == 0) {
					int thisunit = ONE_SECOND; /* Default is second */
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "PUT_FAILED_RETENP in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					switch (p[strlen(p)-1]) {
					case 'S':
					case 's':
						thisunit = ONE_SECOND; /* Second */
						p[strlen(p) - 1] = '\0';
						break;
					case 'M':
					case 'm':
						thisunit = ONE_MINUTE; /* Minute */
						p[strlen(p) - 1] = '\0';
						break;
					case 'H':
					case 'h':
						thisunit = ONE_HOUR; /* Hour */
						p[strlen(p) - 1] = '\0';
						break;
					case 'D':
					case 'd':
						thisunit = ONE_DAY; /* Day */
						p[strlen(p) - 1] = '\0';
						break;
					case 'Y':
					case 'y':
						thisunit = ONE_YEAR; /* Year */
						p[strlen(p) - 1] = '\0';
						break;
					default:
						/* If there is another non-digit character the stage_strtoi() call will catch it */
						break;
					}
					if (stage_strtoi(&(pool_p->put_failed_retenp), p, &dp, 0) != 0) {
						if (serrno != ERANGE) {
							sendrep (&rpfd, MSG_ERR, STG26, "PUT_FAILED_RETENP in pool", pool_p->name);
						} else {
							sendrep (&rpfd, MSG_ERR, STG26, "PUT_FAILED_RETENP (out of range) in pool", pool_p->name);
						}
						errflg++;
						goto reply;
					}
					if (pool_p->put_failed_retenp < 0) {
						pool_p->put_failed_retenp = -1;
					} else {
						if (thisunit != ONE_SECOND) {
							/* Again out of range ? */
							if (pool_p->put_failed_retenp > (INT_MAX / thisunit)) {
								sendrep (&rpfd, MSG_ERR, STG26, "PUT_FAILED_RETENP (out of range) in pool", pool_p->name);
								errflg++;
								goto reply;
							} else {
								pool_p->put_failed_retenp *= thisunit;
							}
						}
					}
					if (pool_p->put_failed_retenp == 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "PUT_FAILED_RETENP (should be <0 xor >0) in pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "STAGEOUT_RETENP") == 0) {
					int thisunit = ONE_SECOND; /* Default is second */
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "STAGEOUT_RETENP in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					switch (p[strlen(p)-1]) {
					case 'S':
					case 's':
						thisunit = ONE_SECOND; /* Second */
						p[strlen(p) - 1] = '\0';
						break;
					case 'M':
					case 'm':
						thisunit = ONE_MINUTE; /* Minute */
						p[strlen(p) - 1] = '\0';
						break;
					case 'H':
					case 'h':
						thisunit = ONE_HOUR; /* Hour */
						p[strlen(p) - 1] = '\0';
						break;
					case 'D':
					case 'd':
						thisunit = ONE_DAY; /* Day */
						p[strlen(p) - 1] = '\0';
						break;
					case 'Y':
					case 'y':
						thisunit = ONE_YEAR; /* Year */
						p[strlen(p) - 1] = '\0';
						break;
					default:
						/* If there is another non-digit character the stage_strtoi() call will catch it */
						break;
					}
					if (stage_strtoi(&(pool_p->stageout_retenp), p, &dp, 0) != 0) {
						if (serrno != ERANGE) {
							sendrep (&rpfd, MSG_ERR, STG26, "STAGEOUT_RETENP in pool", pool_p->name);
						} else {
							sendrep (&rpfd, MSG_ERR, STG26, "STAGEOUT_RETENP (out of range) in pool", pool_p->name);
						}
						errflg++;
						goto reply;
					}
					if (pool_p->stageout_retenp < 0) {
						pool_p->stageout_retenp = -1;
					} else {
						if (thisunit != ONE_SECOND) {
							/* Again out of range ? */
							if (pool_p->stageout_retenp > (INT_MAX / thisunit)) {
								sendrep (&rpfd, MSG_ERR, STG26, "STAGEOUT_RETENP (out of range) in pool", pool_p->name);
								errflg++;
								goto reply;
							} else {
								pool_p->stageout_retenp *= thisunit;
							}
						}
					}
					if (pool_p->stageout_retenp == 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "STAGEOUT_RETENP (should be <0 xor >0) in pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "STAGEALLOC_RETENP") == 0) {
					int thisunit = ONE_SECOND; /* Default is second */
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "STAGEALLOC_RETENP in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					switch (p[strlen(p)-1]) {
					case 'S':
					case 's':
						thisunit = ONE_SECOND; /* Second */
						p[strlen(p) - 1] = '\0';
						break;
					case 'M':
					case 'm':
						thisunit = ONE_MINUTE; /* Minute */
						p[strlen(p) - 1] = '\0';
						break;
					case 'H':
					case 'h':
						thisunit = ONE_HOUR; /* Hour */
						p[strlen(p) - 1] = '\0';
						break;
					case 'D':
					case 'd':
						thisunit = ONE_DAY; /* Day */
						p[strlen(p) - 1] = '\0';
						break;
					case 'Y':
					case 'y':
						thisunit = ONE_YEAR; /* Year */
						p[strlen(p) - 1] = '\0';
						break;
					default:
						/* If there is another non-digit character the stage_strtoi() call will catch it */
						break;
					}
					if (stage_strtoi(&(pool_p->stagealloc_retenp), p, &dp, 0) != 0) {
						if (serrno != ERANGE) {
							sendrep (&rpfd, MSG_ERR, STG26, "STAGEALLOC_RETENP in pool", pool_p->name);
						} else {
							sendrep (&rpfd, MSG_ERR, STG26, "STAGEALLOC_RETENP (out of range) in pool", pool_p->name);
						}
						errflg++;
						goto reply;
					}
					if (pool_p->stagealloc_retenp < 0) {
						pool_p->stagealloc_retenp = -1;
					} else {
						if (thisunit != ONE_SECOND) {
							/* Again out of range ? */
							if (pool_p->stagealloc_retenp > (INT_MAX / thisunit)) {
								sendrep (&rpfd, MSG_ERR, STG26, "STAGEALLOC_RETENP (out of range) in pool", pool_p->name);
								errflg++;
								goto reply;
							} else {
								pool_p->stagealloc_retenp *= thisunit;
							}
						}
					}
					if (pool_p->stagealloc_retenp == 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "STAGEALLOC_RETENP (should be <0 xor >0) in pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp(p, "GC") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL ||
						strchr (p, ':') == NULL ) {
						sendrep (&rpfd, MSG_ERR, STG26, "GC [RFIO syntax, with hostname, required] in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					if ((int) strlen(p) > (CA_MAXHOSTNAMELEN+MAXPATH)) {
						sendrep (&rpfd, MSG_ERR, STG27, "garbage collector", p);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->gc, p);
					/* Check if GC exist and is executable */
					if (rfio_access(pool_p->gc,X_OK) < 0) {
						sendrep (&rpfd, MSG_ERR, STG02, pool_p->gc, "rfio_access (X_OK)", rfio_serror());
						errflg++;
						goto reply;
					}
				} else if (strcmp(p, "NO_FILE_CREATION") == 0) {
					pool_p->no_file_creation = 1;
				} else if (strcmp(p, "EXPORT_HSM") == 0) {
					pool_p->export_hsm = 1;
				} else if (strcmp (p, "MIGRATOR") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG25, "migrator");	/* name missing */
						errflg++;
						goto reply;
					}
					if ((int) strlen (p) > CA_MAXMIGRNAMELEN) {
						sendrep (&rpfd, MSG_ERR, STG27, "migrator", p);
						errflg++;
						goto reply;
					}
					if (pool_p->migr_name[0] != '\0') {
						/* Yet defined !? */
						sendrep (&rpfd, MSG_ERR, STG26, "MIGRATOR in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->migr_name, p);
					for (j = 0, migr_p = migrators; j < nbmigrator; j++, migr_p++) {
						if (strcmp (pool_p->migr_name, migr_p->name) == 0) {
							/* Yet defined */
							pool_p->migr = migr_p;
							break;
						}
						if (migr_p->name[0] != '\0') continue; /* Place yet taken by another migrator name... */
						/* migrator name was not yet registered */
						strcpy(migr_p->name, p);
						/* Got a new one */
						pool_p->migr = migr_p;
						nbmigrator_real++;
						break;
					}
				} else if (strcmp (p, "METAPOOL") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG25, "metapool");	/* name missing */
						errflg++;
						goto reply;
					}
					if ((int) strlen (p) > CA_MAXPOOLNAMELEN) {
						sendrep (&rpfd, MSG_ERR, STG27, "metapool", p);
						errflg++;
						goto reply;
					}
					if (pool_p->metapool[0] != '\0') {
						/* Yet defined !? */
						sendrep (&rpfd, MSG_ERR, STG26, "METAPOOL in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->metapool, p);
				} else if (strcmp (p, "MIG_START_THRESH") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "MIG_START_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					stage_strtoi(&(pool_p->mig_start_thresh), p, &dp, 10);
					if (*dp != '\0' || pool_p->mig_start_thresh < 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "MIG_START_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "MIG_STOP_THRESH") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "MIG_STOP_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					stage_strtoi(&(pool_p->mig_stop_thresh), p, &dp, 10);
					if (*dp != '\0' || pool_p->mig_stop_thresh < 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "MIG_STOP_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "MIG_DATA_THRESH") == 0) {
					int checkrc;
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG26, "MIG_DATA_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					if ((checkrc = stage_util_check_for_strutou64(p)) < 0) {
						sendrep (&rpfd, MSG_ERR, STG26, "MIG_DATA_THRESH in pool", pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->mig_data_thresh = strutou64 (p);
					if (checkrc == 0) pool_p->mig_data_thresh *= ONE_MB; /* Not unit : default MB */
					/* It is allowed to have pool_p->mig_data_thresh == 0 */
				} else {
					sendrep (&rpfd, MSG_ERR, STG26, "pool", pool_p->name);
					errflg++;
					goto reply;
				}
			}
		} else if (strcmp (p, "DEFPOOL") == 0) {
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = -1;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG30);	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > CA_MAXPOOLNAMELEN) {
				sendrep (&rpfd, MSG_ERR, STG27, "pool", p);
				errflg++;
				goto reply;
			}
			strcpy (defpoolname, p);
		} else if (strcmp (p, "DEFPOOL_IN") == 0) {
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = -1;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG30);	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > CA_MAXPOOLNAMELEN) {
				sendrep (&rpfd, MSG_ERR, STG27, "pool", p);
				errflg++;
				goto reply;
			}
			strcpy (defpoolname_in, p);
		} else if (strcmp (p, "DEFPOOL_OUT") == 0) {
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = -1;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG30);	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > ((10*(CA_MAXPOOLNAMELEN + 1)) - 1)) {
				sendrep (&rpfd, MSG_ERR, STG27, "pool", p);
				errflg++;
				goto reply;
			}
			strcpy (defpoolname_out, p);
			/* Please note that defpoolname_out can contain multiple entries, separated with a ':' */
		} else if (strcmp (p, "DEFSIZE") == 0) {
			int checkrc;
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = -1;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG28);
				errflg++;
				goto reply;
			}
			if ((checkrc = stage_util_check_for_strutou64(p)) < 0) {
				sendrep (&rpfd, MSG_ERR, STG28);
				errflg++;
				goto reply;
			}
			defsize = strutou64(p);
			if (defsize <= 0) {
				/* Cannot be < 0, strictly speaking */
				sendrep (&rpfd, MSG_ERR, STG28);
				errflg++;
				goto reply;
			}
			if (checkrc == 0) defsize *= ONE_MB; /* Not unit : default MB */
		} else if (strcmp (p, "MINDEFSIZE") == 0) {
			int checkrc;
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = -1;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG28);
				errflg++;
				goto reply;
			}
			if ((checkrc = stage_util_check_for_strutou64(p)) < 0) {
				sendrep (&rpfd, MSG_ERR, STG28);
				errflg++;
				goto reply;
			}
			mindefsize = strutou64(p);
			if (mindefsize <= 0) {
				/* Cannot be < 0, strictly speaking */
				sendrep (&rpfd, MSG_ERR, STG28);
				errflg++;
				goto reply;
			}
			if (checkrc == 0) mindefsize *= ONE_MB; /* Not unit : default MB */
		} else {
			if (nbpool_ent < 0) {
				sendrep (&rpfd, MSG_ERR, STG26, "pool", "");
				errflg++;
				goto reply;
			}
			nbpool_ent++;
		}
	}
	if (poolalloc (pool_p, nbpool_ent) < 0) {
		errflg++;
		goto reply;
	}

	/* Verify metapool names - it is not allowed a metapool name matches a pool name */
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (pool_p->metapool[0] != '\0') {
			if (isvalidpool(pool_p->metapool)) {
				sendrep (&rpfd, MSG_ERR, STG178, pool_p->name, pool_p->metapool);
				errflg++;
				goto reply;
			}
		}
	}

	/* Verify DEFPOOL */
	if (*defpoolname == '\0') {
		if (nbpool == 1) {
			strcpy (defpoolname, pools->name);
		} else {
			sendrep (&rpfd, MSG_ERR, STG30);
			errflg++;
			goto reply;
		}
	} else {
		if (! isvalidpool (defpoolname)) {
			/* We neverthless support a defpoolname that would be a metapool */
			if (! ismetapool (defpoolname)) {
				sendrep (&rpfd, MSG_ERR, STG32, defpoolname);
				errflg++;
				goto reply;
			}
		}
	}

	/* Verify DEFPOOL_IN */
	if (*defpoolname_in == '\0') {
		strcpy (defpoolname_in, defpoolname);
	} else {
		if (! isvalidpool (defpoolname_in)) {
			/* We neverthless support a defpoolname_in that would be a metapool */
			if (! ismetapool (defpoolname_in)) {
				sendrep (&rpfd, MSG_ERR, STG32, defpoolname_in);
				errflg++;
				goto reply;
			}
		}
	}

	/* Verify DEFPOOL_OUT */
	if (*defpoolname_out == '\0') {
		strcpy (defpoolname_out, defpoolname);
	} else {
		/* Either DEFPOOL_OUT is totally equal to a metapool, either it contains */
		/* only a list of valid pools */
		/* We loop all entried separated by the ':' to verify poolnames validity */
		if (! ismetapool(defpoolname_out)) {
			char savdefpoolname_out[10*(CA_MAXPOOLNAMELEN + 1)];
			strcpy(savdefpoolname_out, defpoolname_out);
			if ((p = strtok(defpoolname_out, ":")) != NULL) {
				while (1) {
					if (! isvalidpool (p)) {
						sendrep (&rpfd, MSG_ERR, STG32, p);
						errflg++;
						strcpy(defpoolname_out, savdefpoolname_out);
						goto reply;
					}
					if ((p = strtok(NULL, ":")) == NULL) break;
				}
			}
			strcpy(defpoolname_out, savdefpoolname_out);
		}
	}

	/* Reduce number of migrator in case of pools sharing the same one */
	if ((nbmigrator_real > 0) && (nbmigrator > 0) && (nbmigrator_real < nbmigrator)) {
		struct migrator *dummy;
    
		dummy = (struct migrator *) realloc(migrators, nbmigrator_real * sizeof(struct migrator));
		migrators = dummy;
		nbmigrator = nbmigrator_real;
	}

	/* Verify garbage collector constants : start thresh should be higher than stop thresh */
	/* otherwise this will loop : when gc will start stager will always answer there is */
	/* nothing to delete because enough free space */
	/* The only accepted exception is when both a equal to zero. In such a case GC is */
	/* guaranteed to never start, so it does not hurt if gc_stop_thresh == gc_start_thresh == 0 */
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (pool_p->gc_start_thresh >= pool_p->gc_stop_thresh) {
			if ((pool_p->gc_start_thresh != 0) || (pool_p->gc_stop_thresh != 0)) {
				sendrep (&rpfd, MSG_ERR, STG163, pool_p->name, pool_p->gc_start_thresh, pool_p->gc_stop_thresh);
				errflg++;
				goto reply;
			}
		}
	}

	/* associate pools with migrators */

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (! *pool_p->migr_name) continue;
		for (j = 0, migr_p = migrators; j < nbmigrator; j++, migr_p++)
			if (strcmp (pool_p->migr_name, migr_p->name) == 0) break;
		if (nbmigrator == 0 || j >= nbmigrator) {
			sendrep (&rpfd, MSG_ERR, STG55, pool_p->migr_name);
			errflg++;
			goto reply;
		}
		pool_p->migr = migr_p;
	}

	/* 3rd pass: store pool elements */

	rewind (s);
	pool_p = pools - 1;
	migr_p = migrators - 1;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		char *pdash;

		if ((pdash = strchr(buf,'#')) != NULL) {
			*pdash = '\0';
		}
		if ((p = strtok (buf, " \t\n")) == NULL) continue;
		if (strcmp (p, "POOL") == 0) {
			char tmpbuf[21];
			char tmpbuf2[21];
			pool_p++;
			elemp = pool_p->elemp;
			if (pool_p->defsize == 0) pool_p->defsize = defsize;
			if (pool_p->mindefsize == 0) pool_p->mindefsize = mindefsize;
			if (pool_p->defsize <= 0) {
				sendrep(&rpfd, MSG_ERR,
						 "### CONFIGURATION ERROR : POOL %s with DEFSIZE <= 0\n",
						 pool_p->name);
				errflg++;
				goto reply;
			}
			if (pool_p->mindefsize > pool_p->defsize) {
				sendrep(&rpfd, MSG_ERR,
						 "### CONFIGURATION ERROR : POOL %s with MINDEFSIZE > DEFSIZE\n",
						 pool_p->name);
				errflg++;
				goto reply;
			}
			/* It is allowed to have mindefsize <= 0 */
			stglogit (func,"POOL %s DEFSIZE %s MINDEFSIZE %s GC_STOP_THRESH %d GC_START_THRESH %d\n",
					  pool_p->name, u64tostru(pool_p->defsize, tmpbuf, 0), u64tostru(pool_p->mindefsize, tmpbuf2, 0), pool_p->gc_stop_thresh, pool_p->gc_start_thresh);
			stglogit (func,".... GC %s\n",
					  *(pool_p->gc) != '\0' ? pool_p->gc : "<none>");
			if (*(pool_p->gc) == '\0') {
				sendrep (&rpfd, MSG_ERR, STG26, "pool [no garbage collector]", pool_p->name);
				errflg++;
				goto reply;
			}
			stglogit (func,".... NO_FILE_CREATION %s\n",
					  (pool_p->no_file_creation ? "YES (!= 0)" : "NO (== 0)"));
			stglogit (func,".... EXPORT_HSM %s\n",
					  (pool_p->export_hsm ? "YES (!= 0)" : "NO (== 0)"));
			if (pool_p->max_setretenp < 0) {
				stglogit (func,".... MAX_SETRETENP <none>\n");
			} else {
				stglogit (func,".... MAX_SETRETENP %d\n", pool_p->max_setretenp);
			}
			if (pool_p->put_failed_retenp < 0) {
				stglogit (func,".... PUT_FAILED_RETENP <infinite>\n");
			} else {
				stglogit (func,".... PUT_FAILED_RETENP %d\n", pool_p->put_failed_retenp);
			}
			if (pool_p->stageout_retenp < 0) {
				stglogit (func,".... STAGEOUT_RETENP <infinite>\n");
			} else {
				stglogit (func,".... STAGEOUT_RETENP %d\n", pool_p->stageout_retenp);
			}
			if (pool_p->stagealloc_retenp < 0) {
				stglogit (func,".... STAGEALLOC_RETENP <infinite>\n");
			} else {
				stglogit (func,".... STAGEALLOC_RETENP %d\n", pool_p->stagealloc_retenp);
			}
			if (pool_p->migr_name[0] != '\0') {
				stglogit (func,".... MIGRATOR %s\n", pool_p->migr_name);
				stglogit (func,".... MIG_STOP_THRESH %d MIG_START_THRESH %d\n",
						  pool_p->mig_stop_thresh, pool_p->mig_start_thresh);
			}
		} else if (strcmp (p, "DEFPOOL") == 0) continue;
		else if (strcmp (p, "DEFPOOL_IN") == 0) continue;
		else if (strcmp (p, "DEFPOOL_OUT") == 0) continue;
		else if (strcmp (p, "DEFSIZE") == 0) continue;
		else if (strcmp (p, "MINDEFSIZE") == 0) continue;
		else {
			if ((int) strlen (p) > CA_MAXHOSTNAMELEN) {
				sendrep (&rpfd, MSG_ERR, STG26, "pool element [with hostname too long]", p);
				errflg++;
				goto reply;
			}
			strcpy (elemp->server, p);
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG26, "pool element [with pathname]", elemp->server);
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) >= MAXPATH) {
				sendrep (&rpfd, MSG_ERR, STG26, "pool element [with pathname too long]", p);
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) < 0) {
				sendrep (&rpfd, MSG_ERR, STG26, "pool element [with pathname empty !?]", p);
				errflg++;
				goto reply;
			}
			strcpy (elemp->dirpath, p);
			nfsroot = getconfent ("RFIO", "NFS_ROOT", 0);
#ifdef NFSROOT
			if (nfsroot == NULL) nfsroot = NFSROOT;
#endif
			if (nfsroot != NULL &&
				strncmp (elemp->dirpath, nfsroot, strlen (nfsroot)) == 0 &&
				*(elemp->dirpath + strlen(nfsroot)) == '/')	/* /shift syntax */
				strcpy (path, elemp->dirpath);
			else
				sprintf (path, "%s:%s", elemp->server, elemp->dirpath);
			PRE_RFIO;
			if (rfio_access (path, W_OK|F_OK|X_OK|R_OK) < 0) {
				sendrep (&rpfd, MSG_ERR, STG02, path, "rfio_access (W_OK|F_OK|X_OK|R_OK)", rfio_serror());
			  check_fs_yet_known:
				if (rfio_serrno() == SETIMEDOUT) {
					/* Check if we already know about that filesystem. If yes, we take */
					/* old values (capacity,free,blocksize) */
					if ((sav_pools != NULL) && (sav_nbpool > 0)) {
						int i, j;
						struct pool *sav_pool_p;
						struct pool_element *sav_elemp = NULL;

						for (i = 0, sav_pool_p = sav_pools; i < sav_nbpool; i++, sav_pool_p++) {
							for (j = 0, sav_elemp = sav_pool_p->elemp; j < sav_pool_p->nbelem; j++, sav_elemp++) {
								if ((strcmp(sav_elemp->server,elemp->server) == 0) &&
									(strcmp(sav_elemp->dirpath,elemp->dirpath) == 0)) {
									sendrep(&rpfd, MSG_ERR, "%s capacity, blocksize and free space kept as before\n", path);
									elemp->bsize = sav_elemp->bsize;
									elemp->capacity = sav_elemp->capacity;
									elemp->free = sav_elemp->free;
									goto forced_fs_ok;
								}
							}
						}
					}
				}
			} else {
				PRE_RFIO;
				if (rfio_statfs (path, &st) < 0) {
					sendrep (&rpfd, MSG_ERR, STG02, path, "rfio_statfs", rfio_serror());
					goto check_fs_yet_known;
				} else {
					char tmpbuf1[21];
					char tmpbuf2[21];
					char tmpbuf3[21];
					elemp->bsize = (u_signed64) st.bsize;
					elemp->capacity = (u_signed64) ((u_signed64) st.totblks * elemp->bsize);
					elemp->free = (u_signed64) ((u_signed64) st.freeblks * elemp->bsize);
				  forced_fs_ok:
					pool_p->capacity += elemp->capacity;
					pool_p->free += elemp->free;
					stglogit (func,
							  ".... %s capacity=%s, free=%s, blocksize=%s\n",
							  path, u64tostru(elemp->capacity, tmpbuf1, 0),
							  u64tostru(elemp->free, tmpbuf2, 0),
							  u64tostru(elemp->bsize, tmpbuf3, 0)
						);
				}
			}
			elemp++;
		}
	}
  reply:
	fclose (s);
	if (errflg) {
		if (pools != NULL) {
			for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
				if (pool_p->elemp != NULL) free (pool_p->elemp);
			}
			free (pools);
		}
		if (migrators != NULL) free (migrators);
		return (ESTCONF);
	} else {
		/* This pointer will maintain a list of pools on which a gc have finished since last call */
		poolc = (char **) calloc (nbpool, sizeof(char *));
		/* Count the number of different machines between all the pools */
		for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
			struct pool_element *elemp;
			for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
				/* elemp->flag is always zero there because it is result of a calloc() */
				if (elemp->flag == 0) {
					int i2, j2;
					struct pool *pool_p2;
					struct pool_element *elemp2;
          
					elemp->flag = 1;
					nbhost++;
					/* Remove all elemps that share the same host [does not work if you refer same machine with different names...] */
					for (i2 = 0, pool_p2 = pools; i2 < nbpool; i2++, pool_p2++) {
						for (j2 = 0, elemp2 = pool_p2->elemp; j2 < pool_p2->nbelem; j2++, elemp2++) {
							if ((elemp2->flag == 0) && (strcmp(elemp->server,elemp2->server) == 0)) {
								elemp2->flag = 1;
							}
						}
					}
					/* If (elemp->server == localhost) we do not need the permanent rfio connections ! */

					/* Is elemp->server a fully qualified hostname ? */
					if (strchr(elemp->server,'.') != NULL) {
						/* Yes : compare with localhost, that is also a FQ-hostname (c.f. startup in stgdaemon.c) */
						if (strcmp(elemp->server,localhost) == 0) {
							nblocal++;
						}
					} else {
						/* No  : this mean that hostnames are equal only if localhost is exactly equal to "elemp->server" + "." + "localdomain" */
						if ((strncmp(elemp->server,localhost,strlen(elemp->server)) == 0) &&
							(localhost[strlen(elemp->server)] == '.') &&                        /* Executed only if upper is true, so cannot be null */
							(strcmp(localdomain,localhost + strlen(elemp->server) + 1) == 0)) { /* Idem */
							nblocal++;
						}
					}
				}
			}
		}
		if (nblocal == 0) {
			stglogit (func, "==> Detected %d different machine name(s), all remote, spreaded over %d pool(s)\n", nbhost, nbpool);
		} else {
			stglogit (func, "==> Detected %d different machine name(s), %d remote, spreaded over %d pool(s)\n", nbhost, nbhost - 1, nbpool);
			if (--nbhost < 0) {
				stglogit (func, "### Internal error, nbhost < 0 - Resetted to zero\n");
				nbhost = 0;
			}
		}
		/* Check the location of garbage collectors : they will issue stageclr and we can detect if they will */
		/* get Permission Denied (EACCES) with current Cupv database */
		stglogit (func, "==> Checking garbage collectors and privileges\n");
		for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
			if (pool_p->gc[0] != '\0') {
				char save_gc[CA_MAXHOSTNAMELEN+MAXPATH+1];
				char *gc_host = NULL;
				char *gc_host2 = NULL;
				char *gc_path = NULL;
			
				strcpy(save_gc,pool_p->gc);
				rfio_parseln (save_gc, &gc_host, &gc_path, NORDLINKS);
				if (gc_host != NULL) {
					if (strchr(gc_host,'.') == NULL) {
						/* This is not yet fully qualified hostname */
						if ((gc_host2 = (char *) malloc(strlen(gc_host) + 1 + strlen(localdomain) + 1)) == NULL) {
							sendrep (&rpfd, MSG_ERR, "... Pool %s : malloc error building fully qualified GC %s hostname: %s\n", pool_p->name, pool_p->gc, strerror(errno));
						} else {
							strcpy(gc_host2,gc_host);
							strcat(gc_host2,".");
							strcat(gc_host2,localdomain);
							gc_host = gc_host2;
						}
					}
				} else {
					/* Note : at startup we make sure 'localhost' is a FQ-hostname */
					gc_host = localhost;
				}
				/* Note : gc_host can be NULL - this is valid in the call to Cupv_check() */
				if (Cupv_check((uid_t) 0, (gid_t) 0, gc_host, localhost, P_ADMIN) != 0) {
					sendrep (&rpfd, MSG_ERR, "### Pool %s : GC %s : Rule [uid=0,gid=0,src=%s,tgt=%s,ADMIN] is probably needed in Cupv (%s)\n", pool_p->name, pool_p->gc, gc_host, localhost, sstrerror(serrno));
				} else {
					stglogit (func, "... Pool %s : GC %s : Rule [uid=0,gid=0,src=%s,tgt=%s,ADMIN] ok\n", pool_p->name, pool_p->gc, gc_host, localhost);
				}
				if (gc_host2 != NULL) free(gc_host2);
			} else {
				stglogit (func, "... Pool %s have no garbage collector\n", pool_p->name);
			}
		}
		return (0);
	}
}

int checklastaccess(poolname, atime)
	char *poolname;
	time_t atime;
{
	int i;
	struct pool *pool_p;

	/*	return -1 if the file has been accessed after garbage collector startup,
	 *		  the file should not be removed
	 *	return 0  otherwise
	 */
	if (*poolname == '\0')
		return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (i == nbpool) return (0);    /* old entry; pool does not exist */
	if (pool_p->cleanreqtime == 0) return (0);	/* cleaner not active */
	if (atime < pool_p->cleanreqtime) return (0);
	return (-1);
}

int checkpoolcleaned(pool_list)
	char ***pool_list;
{
	int i, n;
	struct pool *pool_p;

	n = 0;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (pool_p->cleanstatus == 0) continue;
		poolc[n++] = pool_p->name;
		pool_p->cleanstatus = 0;
	}
	if (n)
		*pool_list = poolc;
	return (n);	/* number of pools cleaned since last call */
}

void checkpoolspace()
{
	int i;
	struct pool *pool_p;
	extern int shutdownreq_reqid;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if ((pool_p->free * 100) < (pool_p->capacity * pool_p->gc_start_thresh)) {
			/* Not enough minimum space */
			/* Note that cleanpool() not launch a cleaner if another is already running */
			if (shutdownreq_reqid == 0) cleanpool(pool_p->name);
		}
	}
}

int cleanpool(poolname)
	char *poolname;
{
	char func[16];
	int i;
	int pid;
	struct pool *pool_p;
	char progfullpath[MAXPATH];
	char hostname[CA_MAXHOSTNAMELEN + 1];

	strcpy (func, "cleanpool");
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (pool_p->ovl_pid != 0) return (0);   /* Another cleaner on same pool is already running */
	pool_p->ovl_pid = fork ();
	pid = pool_p->ovl_pid;
	if (pid < 0) {
		stglogit (func, STG02, "", "fork", strerror(errno));
		pool_p->ovl_pid = 0;
		return (SESYSERR);
	} else if (pid == 0) {  /* we are in the child */
		rfio_mstat_reset();  /* Reset permanent RFIO stat connections */
		rfio_munlink_reset(); /* Reset permanent RFIO unlink connections */
		gethostname (hostname, CA_MAXHOSTNAMELEN + 1);
		sprintf (progfullpath, "%s/cleaner", BIN);
		stglogit (func, "execing cleaner, pid=%d\n", getpid());
		execl (progfullpath, "cleaner", pool_p->gc, poolname, hostname, 0);
		stglogit (func, STG02, "cleaner", "execl", strerror(errno));
		exit (SYERR); /* Warning: this is an exit code of a process, not a return code */
	} else {
		pool_p->cleanreqtime = time(NULL);
	}
	return (0);
}

int enoughfreespace(poolname, minf)
	char *poolname;
	int minf;
{
	int i;
	int gc_stop_thresh;
	struct pool *pool_p;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (i == nbpool) return (0);	/* old entry; pool does not exist */
	if (minf)
		gc_stop_thresh = minf;
	else
		gc_stop_thresh = pool_p->gc_stop_thresh;
	return((pool_p->free * 100) > (pool_p->capacity * gc_stop_thresh));
}

char *
findpoolname(path)
	char *path;
{
	struct pool_element *elemp;
	int i, j;
	char *p;
	struct pool *pool_p;
	char server[CA_MAXHOSTNAMELEN + 1];

	server[0] = '\0';
	/* If we find a ":/" set - it is a hostname specification if there is no '/' before */
	if (((p = strstr (path, ":/")) != NULL) && (strchr(path, '/') > p)) {
		/* Note that per construction strchr() returns != NULL, because we know there is a '/' */
		strncpy (server, path, (size_t) (p - path));
		server[p - path] = '\0';
	} else {
		p = NULL;
	}
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if (p) {
				if (strcmp (server, elemp->server) == 0 &&
					strncmp (p + 1, elemp->dirpath, strlen (elemp->dirpath)) == 0 &&
					*(p + 1 + strlen (elemp->dirpath)) == '/')
					return (pool_p->name);
			} else {
				if (strncmp (path, elemp->dirpath, strlen (elemp->dirpath)) == 0 &&
					*(path + strlen (elemp->dirpath)) == '/')
					return (pool_p->name);
			}
		}
	}
	return (NULL);
}

void update_last_allocation(path)
	char *path;
{
	struct pool_element *elemp;
	int i, j;
	char *p;
	struct pool *pool_p;
	char server[CA_MAXHOSTNAMELEN + 1];

	server[0] = '\0';
	/* If we find a ":/" set - it is a hostname specification if there is no '/' before */
	if (((p = strstr (path, ":/")) != NULL) && (strchr(path, '/') > p)) {
		/* Note that per construction strchr() returns != NULL, because we know there is a '/' */
		strncpy (server, path, (size_t) (p - path));
		server[p - path] = '\0';
	} else {
		p = NULL;
	}
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if (p) {
				if (strcmp (server, elemp->server) == 0 &&
					strncmp (p + 1, elemp->dirpath, strlen (elemp->dirpath)) == 0 &&
					*(p + 1 + strlen (elemp->dirpath)) == '/') {
					pool_p->last_allocation = elemp->last_allocation = time(NULL);
					return;
				}
			} else {
				if (strncmp (path, elemp->dirpath, strlen (elemp->dirpath)) == 0 &&
					*(path + strlen (elemp->dirpath)) == '/') {
					pool_p->last_allocation = elemp->last_allocation = time(NULL);
					return;
				}
			}
		}
	}
}

void update_last_pool_allocation(poolname)
	char *poolname;
{
	int i;
	struct pool *pool_p;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp(pool_p->name,poolname) == 0) {
			pool_p->last_allocation = time(NULL);
			return;
		}
	}
}

int
findfs(path,found_server,found_dirpath)
	char *path;
	char **found_server;
	char **found_dirpath;
{
	struct pool_element *elemp;
	int i, j;
	char *p;
	struct pool *pool_p;
	char server[CA_MAXHOSTNAMELEN + 1];

	server[0] = '\0';
	/* If we find a ":/" set - it is a hostname specification if there is no '/' before */
	if (((p = strstr (path, ":/")) != NULL) && (strchr(path, '/') > p)) {
		/* Note that per construction strchr() returns != NULL, because we know there is a '/' */
		strncpy (server, path, (size_t) (p - path));
		server[p - path] = '\0';
	} else {
		p = NULL;
	}
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if (p) {
				if (strcmp (server, elemp->server) == 0 &&
					strncmp (p + 1, elemp->dirpath, strlen (elemp->dirpath)) == 0 &&
					*(p + 1 + strlen (elemp->dirpath)) == '/') {
					if (found_server != NULL) *found_server = elemp->server;
					if (found_dirpath != NULL) *found_dirpath = elemp->dirpath;
					return (0);
				}
			} else {
				if (strncmp (path, elemp->dirpath, strlen (elemp->dirpath)) == 0 &&
					*(path + strlen (elemp->dirpath)) == '/') {
					if (found_server != NULL) *found_server = elemp->server;
					if (found_dirpath != NULL) *found_dirpath = elemp->dirpath;
					return (0);
				}
			}
		}
	}
	if (found_server != NULL) *found_server = NULL;
	if (found_dirpath != NULL) *found_dirpath = NULL;
	return(-1);
}

u_signed64
findblocksize(path)
	char *path;
{
	/* Not really predictable, 512 almost everywhere, but not always (hpux, some aix...) */
	/* Taken from fileutils commentaries:
	   "A warning about du for HP-UX users: GNU du (and I'm sure BSD-derived
	   versions) counts the st_blocks field of the `struct stat' for each
	   file.  (It's best to use st_blocks where available, instead of
	   st_size, because otherwise you get wildly wrong answers for sparse
	   files like coredumps, and it counts indirect blocks.)  Chris Torek in
	   a comp.unix.wizards posting stated that in 4BSD st_blocks is always
	   counted in 512 byte blocks.  On HP-UX filesystems, however, st_blocks
	   is counted in 1024 byte blocks.  When GNU du is compiled on HP-UX, it
	   assumes that st_blocks counts 1024-byte blocks, because locally
	   mounted filesystems do; so to get the number of 512-byte blocks, it
	   doubles the st_blocks value.  (The HP-UX du seems to do the same
	   thing.)  This gives the correct numbers on HP-UX filesystems.  But for
	   4BSD filesystems mounted on HP-UX machines, it gives twice the correct
	   numbers; similarly, for HP-UX filesystems, du on 4BSD machines gives
	   half the correct numbers.  GNU ls with the -s option has the same
	   problem.  I know of no way to determine for a given filesystem or file
	   what units st_blocks is measured in.  The f_bsize element of `struct
	   statfs' does not work, because its meaning varies between different
	   versions of Unix."
	*/
	return ((u_signed64) 512);
}

int iscleanovl(pid, status)
	int pid;
	int status;
{
	int found;
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	found = 0;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (pool_p->ovl_pid == pid) {
			found = 1;
			break;
		}
	if (! found) return (0);
	pool_p->ovl_pid = 0;
	pool_p->cleanreqtime_previous = pool_p->cleanreqtime;
	pool_p->cleanreqtime_previous_end = time(NULL);
	pool_p->cleanreqtime = 0;
	if (status == 0)
		pool_p->cleanstatus = 1;
	return (1);
}

void killcleanovl(sig)
	int sig;
{
	int i;
	struct pool *pool_p;
	char *func = "killcleanovl";

	if (nbpool == 0) return;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (pool_p->ovl_pid != 0) {
			stglogit (func, "killing process %d\n", pool_p->ovl_pid);
			kill (pool_p->ovl_pid, sig);
		}
}

int ismigovl(pid, status)
	int pid;
	int status;
{
	int found, found2;
	int i;
	struct pool *pool_p;
	struct pool *pool_p_ok = NULL;
	struct stgcat_entry *stcp;
	char *func = "ismigovl";
	struct pool **pool_migovl;

	if (nbpool == 0) return (0);

	/* Several pools can share the SAME migrator - so we have to find all the pools */
	/* for which the migrator pid is the same */
	if ((pool_migovl = (struct pool **) calloc(nbpool, sizeof(struct pool *))) == NULL) {
		stglogit(func, "### calloc error (%s)\n",strerror(errno));
		stglogit(func, "### processing will more CPU intensive\n");
	}

	found = 0;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (pool_p->migr == NULL) continue;
		if (pool_p->migr->mig_pid == pid) {
			pool_p_ok = pool_p;
			if (pool_migovl != NULL) {
				pool_migovl[found++] = pool_p;
			} else {
				found++;
			}
		}
	}
	if (! found) {
		if (pool_migovl != NULL) free(pool_migovl);
		return (0);
	}

	pool_p_ok->migr->mig_pid = 0;
	pool_p_ok->migr->migreqtime_previous = pool_p_ok->migr->migreqtime;
	pool_p_ok->migr->migreqtime_previous_end = time(NULL);
	pool_p_ok->migr->migreqtime = 0;
	/* We check if there are remaining entries in WAITING_MIG status in this pool */
	/* If so, this is the migrator that failed. */
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		found2 = 0;
		if (pool_migovl != NULL) {
			/* We look if the poolname of this stcp matches one of the pools that shares the same migrator (pid) */

			for (i = 0; i < found; i++) {
				if (strcmp(stcp->poolname, pool_migovl[i]->name) == 0) {
					found2 = 1;
					break;
				}
			}
		} else {
			/* We were not able to calloc() memory for searching in the pool structures - so we scan all */
			/* pool structures, this is a very little bit more CPU intensive... */
			found2 = 0;
			for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
				if (pool_p->migr == NULL) continue;
				if ((pool_p->migr->mig_pid == pid) && (strcmp(stcp->poolname, pool_p->name) == 0)) {
					found2 = 1;
					break;
				}
			}
		}
		if (! found2) continue;
		if ((stcp->status & WAITING_MIGR) == WAITING_MIGR) {
			/* This entry had a problem */
			stglogit(func, "STG02 - %s still in WAITING_MIGR, changed to PUT_FAILED\n", stcp->u1.h.xfile);
			stcp->status &= ~WAITING_MIGR;
			/* The following will force update_migpool() to correctly update the being_migr counters */
			stcp->status |= BEING_MIGR;
			update_migpool(&stcp,-1,0);
			stcp->status |= PUT_FAILED;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs ();
		}
	}
	if (pool_migovl != NULL) free(pool_migovl);
	return (1);
}

void killmigovl(sig)
	int sig;
{
	int i;
	struct pool *pool_p;
	char *func = "killmigovl";

	if (nbpool == 0) return;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (pool_p->migr == NULL) continue;
		if (pool_p->migr->mig_pid != 0) {
			stglogit (func, "killing process %d\n", pool_p->migr->mig_pid);
			kill (pool_p->migr->mig_pid, sig);
		}
	}
}

int isvalidpool(poolname)
	char *poolname;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	return (i == nbpool ? 0 : 1);
}

int havemetapool(poolname,metapool)
	char *poolname;
	char *metapool;
{
	int i;
	int found = 0;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp (poolname, pool_p->name) == 0) {
			if (strcmp (metapool, pool_p->metapool) == 0) {
				found = 1;
			}
			break;
		}
	}
	return (found);
}

int ismetapool(metapool)
	char *metapool;
{
	int i;
	struct pool *pool_p;
	
	if ((nbpool == 0) || (metapool[0] == '\0')) return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp (metapool, pool_p->metapool) == 0) break;
	}
	return (i == nbpool ? 0 : 1);
}

int max_setretenp(poolname)
	char *poolname;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	return (i == nbpool ? -1 : pool_p->max_setretenp);
}

int poolalloc(pool_p, nbpool_ent)
	struct pool *pool_p;
	int nbpool_ent;
{
	char func[16];

	strcpy (func, "poolalloc");
	switch (nbpool_ent) {
	case -1:
		break;
	case 0:		/* pool is empty */
		stglogit (func, STG24, pool_p->name);
		return (-1);
	default:
		pool_p->elemp = (struct pool_element *)
			calloc (nbpool_ent, sizeof(struct pool_element));
		pool_p->nbelem = nbpool_ent;
	}
	return (0);
}

void print_pool_utilization(rpfd, poolname, defpoolname, defpoolname_in, defpoolname_out, migrator_flag, class_flag, queue_flag, counters_flag)
	int *rpfd;
	char *poolname, *defpoolname, *defpoolname_in, *defpoolname_out;
	int migrator_flag;
	int class_flag;
	int queue_flag;
{
	struct pool_element *elemp;
	int i, j;
	int migr_newline = 1;
	struct pool *pool_p;
	struct migrator *migr_p;
	struct migrator *pool_p_migr = NULL;
	char tmpbuf[21];
	char timestr[64] ;   /* Time in its ASCII format             */
	char max_setretenp_timestr[64] ;
	char put_failed_retenp_timestr[64] ;
	char stageout_retenp_timestr[64] ;
	char stagealloc_retenp_timestr[64] ;
	char tmpbuf0[21];
	char tmpbuf1[21];
	char tmpbuf2[21];
	char tmpbuf3[21];
	char tmpbuf4[21];
	char tmpbuf5[21];
	char tmpbuf6[21];
	char tmpbuf7[21];
	u_signed64 before_fraction, after_fraction;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (*poolname && strcmp (poolname, pool_p->name)) continue;
		if (*poolname) pool_p_migr = pool_p->migr;
		stage_util_retenp(pool_p->max_setretenp,max_setretenp_timestr);
		stage_util_retenp(pool_p->stageout_retenp,stageout_retenp_timestr);
		stage_util_retenp(pool_p->stagealloc_retenp,stagealloc_retenp_timestr);
		stage_util_retenp(pool_p->put_failed_retenp,put_failed_retenp_timestr);
		sendrep (rpfd, MSG_OUT, "POOL %s%s%s%s%s DEFSIZE %s MINDEFSIZE %s GC_START_THRESH %d GC_STOP_THRESH %d GC %s%s%s%s%s%s%s%s%s MAX_SETRETENP %s PUT_FAILED_RETENP %s STAGEOUT_RETENP %s STAGEALLOC_RETENP %s\n",
				 pool_p->name,
				 pool_p->metapool[0] != '\0' ? " METAPOOL " : "",
				 pool_p->metapool[0] != '\0' ? pool_p->metapool : "",
				 pool_p->no_file_creation ? " NO_FILE_CREATION" : "",
				 pool_p->export_hsm ? " EXPORT_HSM" : "",
				 u64tostru(pool_p->defsize,tmpbuf0,0),
				 u64tostru(pool_p->mindefsize,tmpbuf7,0),
				 pool_p->gc_start_thresh,
				 pool_p->gc_stop_thresh,
				 pool_p->gc,
				 (pool_p->migr_name[0] != '\0') ? " MIGRATOR " : "",
				 (pool_p->migr_name[0] != '\0') ? pool_p->migr_name : "",
				 (pool_p->migr_name[0] != '\0') ? " MIG_START_THRESH " : "",
				 (pool_p->migr_name[0] != '\0') ? u64tostr(pool_p->mig_start_thresh, tmpbuf1, 0) : "",
				 (pool_p->migr_name[0] != '\0') ? " MIG_STOP_THRESH " : "",
				 (pool_p->migr_name[0] != '\0') ? u64tostr(pool_p->mig_stop_thresh, tmpbuf2, 0) : "",
				 (pool_p->migr_name[0] != '\0') ? " MIG_DATA_THRESH " : "",
				 (pool_p->migr_name[0] != '\0') ? u64tostru(pool_p->mig_data_thresh, tmpbuf3, 0) : "",
				 max_setretenp_timestr,
				 put_failed_retenp_timestr,
				 stageout_retenp_timestr,
				 stagealloc_retenp_timestr
			);
		if (pool_p->cleanreqtime > 0) {
			stage_util_time(pool_p->cleanreqtime,timestr);
			sendrep (rpfd, MSG_OUT, "\tLAST GARBAGE COLLECTION STARTED %s%s\n", timestr, pool_p->ovl_pid > 0 ? " STILL ACTIVE" : "");
		} else {
			sendrep (rpfd, MSG_OUT, "\tLAST GARBAGE COLLECTION STARTED <none>\n");
		}
		if (pool_p->cleanreqtime_previous > 0) {
			stage_util_time(pool_p->cleanreqtime_previous,timestr);
			sendrep (rpfd, MSG_OUT, "\tPREVIOUS GARBAGE COLLECTION STARTED %s\n", timestr);
		} else {
			sendrep (rpfd, MSG_OUT, "\tPREVIOUS GARBAGE COLLECTION STARTED <none>\n");
		}
		if (pool_p->cleanreqtime_previous_end > 0) {
			stage_util_time(pool_p->cleanreqtime_previous_end,timestr);
			sendrep (rpfd, MSG_OUT, "\tPREVIOUS GARBAGE COLLECTION ENDED %s\n", timestr);
		} else {
			sendrep (rpfd, MSG_OUT, "\tPREVIOUS GARBAGE COLLECTION ENDED <none>\n");
		}
		before_fraction = pool_p->capacity ? (100 * pool_p->free) / pool_p->capacity : 0;
		after_fraction = pool_p->capacity ?
			(10 * (pool_p->free * 100 - pool_p->capacity * before_fraction)) / pool_p->capacity :
			0;
		sendrep (rpfd, MSG_OUT,"                              CAPACITY %s FREE %s (%s.%s%%)\n",
				 u64tostru(pool_p->capacity, tmpbuf1, 0),
				 u64tostru(pool_p->free, tmpbuf2, 0),
				 u64tostr(before_fraction, tmpbuf3, 0),
				 u64tostr(after_fraction, tmpbuf4, 0));
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			before_fraction = elemp->capacity ? (100 * elemp->free) / elemp->capacity : 0;
			after_fraction = elemp->capacity ?
				(10 * (elemp->free * 100 - elemp->capacity * before_fraction)) / elemp->capacity :
				0;
			sendrep (rpfd, MSG_OUT, "  %s %s CAPACITY %s FREE %s (%s.%s%%)%s%s%s%s%s%s\n",
					 elemp->server,
					 elemp->dirpath,
					 u64tostru(elemp->capacity, tmpbuf1, 0),
					 u64tostru(elemp->free, tmpbuf2, 0),
					 u64tostr(before_fraction, tmpbuf3, 0),
					 u64tostr(after_fraction, tmpbuf4, 0),
					 counters_flag ?       " " : "",
					 counters_flag ?  "nread " : "",
					 counters_flag ? u64tostr((u_signed64) elemp->nbreadaccess, tmpbuf5, 0) : "",
					 counters_flag ?       " " : "",
					 counters_flag ? "nwrite " : "",
					 counters_flag ? u64tostr((u_signed64) elemp->nbwriteaccess, tmpbuf6, 0) : ""
				);
		}
	}
	if (*poolname == '\0') {
		sendrep (rpfd, MSG_OUT, "DEFPOOL     %s\n", defpoolname);
		sendrep (rpfd, MSG_OUT, "DEFPOOL_IN  %s\n", defpoolname_in);
		sendrep (rpfd, MSG_OUT, "DEFPOOL_OUT %s\n", defpoolname_out);
	}
	if ((migrator_flag != 0) && (migrators != NULL)) {
		for (i = 0, migr_p = migrators; i < nbmigrator; i++, migr_p++) {
			if (*poolname && pool_p_migr != migr_p) continue;
			if (migr_newline) {
				sendrep (rpfd, MSG_OUT, "\n");
				migr_newline = 0;
			}
			sendrep (rpfd, MSG_OUT, "MIGRATOR %s\n",migr_p->name);
			sendrep (rpfd, MSG_OUT, "\tNBFILES_CAN_BE_MIGR    %d\n", migr_p->global_predicates.nbfiles_canbemig);
			sendrep (rpfd, MSG_OUT, "\tSPACE_CAN_BE_MIGR      %s\n", u64tostru(migr_p->global_predicates.space_canbemig, tmpbuf, 0));
			sendrep (rpfd, MSG_OUT, "\tNBFILES_DELAY_MIGR     %d\n", migr_p->global_predicates.nbfiles_delaymig);
			sendrep (rpfd, MSG_OUT, "\tSPACE_DELAY_MIGR       %s\n", u64tostru(migr_p->global_predicates.space_delaymig, tmpbuf, 0));
			sendrep (rpfd, MSG_OUT, "\tNBFILES_BEING_MIGR     %d\n", migr_p->global_predicates.nbfiles_beingmig);
			sendrep (rpfd, MSG_OUT, "\tSPACE_BEING_MIGR       %s\n", u64tostru(migr_p->global_predicates.space_beingmig, tmpbuf, 0));
			if (migr_p->migreqtime > 0) {
				stage_util_time(migr_p->migreqtime,timestr);
				sendrep (rpfd, MSG_OUT, "\tLAST MIGRATION STARTED %s%s\n", timestr, migr_p->mig_pid > 0 ? " STILL ACTIVE" : "");
			} else {
				sendrep (rpfd, MSG_OUT, "\tLAST MIGRATION STARTED <none>\n");
			}
			if (migr_p->migreqtime_previous > 0) {
				stage_util_time(migr_p->migreqtime_previous,timestr);
				sendrep (rpfd, MSG_OUT, "\tPREVIOUS MIGRATION STARTED %s\n", timestr);
			} else {
				sendrep (rpfd, MSG_OUT, "\tPREVIOUS MIGRATION STARTED <none>\n");
			}
			if (migr_p->migreqtime_previous_end > 0) {
				stage_util_time(migr_p->migreqtime_previous_end,timestr);
				sendrep (rpfd, MSG_OUT, "\tPREVIOUS MIGRATION ENDED %s\n", timestr);
			} else {
				sendrep (rpfd, MSG_OUT, "\tPREVIOUS MIGRATION ENDED <none>\n");
			}
			for (j = 0; j < migr_p->nfileclass; j++) {
				sendrep (rpfd, MSG_OUT, "\tFILECLASS %s@%s (classid=%d)\n",
						 migr_p->fileclass[j]->Cnsfileclass.name,
						 migr_p->fileclass[j]->server,
						 migr_p->fileclass[j]->Cnsfileclass.classid);
				sendrep (rpfd, MSG_OUT, "\t\tNBFILES_CAN_BE_MIGR    %d\n", migr_p->fileclass_predicates[j].nbfiles_canbemig);
				sendrep (rpfd, MSG_OUT, "\t\tSPACE_CAN_BE_MIGR      %s\n", u64tostru(migr_p->fileclass_predicates[j].space_canbemig, tmpbuf, 0));
				sendrep (rpfd, MSG_OUT, "\t\tNBFILES_DELAY_MIGR     %d\n", migr_p->fileclass_predicates[j].nbfiles_delaymig);
				sendrep (rpfd, MSG_OUT, "\t\tSPACE_DELAY_MIGR       %s\n", u64tostru(migr_p->fileclass_predicates[j].space_delaymig, tmpbuf, 0));
				sendrep (rpfd, MSG_OUT, "\t\tNBFILES_BEING_MIGR     %d\n", migr_p->fileclass_predicates[j].nbfiles_beingmig);
				sendrep (rpfd, MSG_OUT, "\t\tSPACE_BEING_MIGR       %s\n", u64tostru(migr_p->fileclass_predicates[j].space_beingmig, tmpbuf, 0));
			}
		}
	}
	if ((class_flag != 0) && (fileclasses != NULL)) {
		sendrep (rpfd, MSG_OUT, "\n");
		for (i = 0; i < nbfileclasses; i++) {
			printfileclass(rpfd, &(fileclasses[i]));
		}
	}
	if (queue_flag != 0) {
		struct waitq *wqp = NULL;
		struct waitf *wfp;
		extern struct waitq *waitqp;
		int iwaitq;

		for (wqp = waitqp, iwaitq = 1; wqp; wqp = wqp->next, iwaitq++) {
			struct stgcat_entry *stcp;
			sendrep (rpfd, MSG_OUT, "\n--------------------\nWaiting Queue No %d\n--------------------\n", iwaitq);
			sendrep (rpfd, MSG_OUT, "pool_user                %s\n", wqp->pool_user);
			sendrep (rpfd, MSG_OUT, "clienthost               %s\n", wqp->clienthost);
			sendrep (rpfd, MSG_OUT, "req_user                 %s\n", wqp->req_user);
			sendrep (rpfd, MSG_OUT, "req_uid                  %ld\n", (unsigned long) wqp->req_uid);
			sendrep (rpfd, MSG_OUT, "req_gid                  %ld\n", (unsigned long) wqp->req_gid);
			sendrep (rpfd, MSG_OUT, "rtcp_user                %s\n", wqp->rtcp_user);
			sendrep (rpfd, MSG_OUT, "rtcp_group               %s\n", wqp->rtcp_group);
			sendrep (rpfd, MSG_OUT, "rtcp_uid                 %ld\n", (unsigned long) wqp->rtcp_uid);
			sendrep (rpfd, MSG_OUT, "rtcp_gid                 %ld\n", (unsigned long) wqp->rtcp_gid);
			sendrep (rpfd, MSG_OUT, "clientpid                %ld\n", (unsigned long) wqp->clientpid);
			sendrep (rpfd, MSG_OUT, "uniqueid gives           Pid=0x%lx (%d) CthreadId+1=0x%lx (-> Tid=%d)\n",
					 (unsigned long) (wqp->uniqueid / 0xFFFFFFFF),
					 (int) (wqp->uniqueid / 0xFFFFFFFF),
					 (unsigned long) (wqp->uniqueid - (wqp->uniqueid / 0xFFFFFFFF) * 0xFFFFFFFF),
					 (int) (wqp->uniqueid - (wqp->uniqueid / 0xFFFFFFFF) * 0xFFFFFFFF) - 1
				);
			sendrep (rpfd, MSG_OUT, "copytape                 %d\n", wqp->copytape);
			sendrep (rpfd, MSG_OUT, "Pflag                    %d\n", wqp->Pflag);
			sendrep (rpfd, MSG_OUT, "Upluspath                %d\n", wqp->Upluspath);
			sendrep (rpfd, MSG_OUT, "reqid                    %d\n", wqp->reqid);
			sendrep (rpfd, MSG_OUT, "key                      %d\n", wqp->key);
			sendrep (rpfd, MSG_OUT, "rpfd                     %d\n", wqp->rpfd);
			sendrep (rpfd, MSG_OUT, "save_rpfd                %d\n", wqp->save_rpfd);
			sendrep (rpfd, MSG_OUT, "ovl_pid                  %d\n", wqp->ovl_pid);
			sendrep (rpfd, MSG_OUT, "nb_subreqs               %d\n", wqp->nb_subreqs);
			sendrep (rpfd, MSG_OUT, "nbdskf                   %d\n", wqp->nbdskf);
			sendrep (rpfd, MSG_OUT, "nb_waiting_on_req        %d\n", wqp->nb_waiting_on_req);
			sendrep (rpfd, MSG_OUT, "nb_clnreq                %d\n", wqp->nb_clnreq);
			sendrep (rpfd, MSG_OUT, "waiting_pool             %s\n", wqp->waiting_pool);
			sendrep (rpfd, MSG_OUT, "clnreq_reqid             %d\n", wqp->clnreq_reqid);
			sendrep (rpfd, MSG_OUT, "clnreq_rpfd              %d\n", wqp->clnreq_rpfd);
			sendrep (rpfd, MSG_OUT, "wqp->clnreq_waitingreqid %d\n", wqp->clnreq_waitingreqid);
			sendrep (rpfd, MSG_OUT, "status                   %d\n", wqp->status);
			sendrep (rpfd, MSG_OUT, "nretry                   %d\n", wqp->nretry);
			sendrep (rpfd, MSG_OUT, "noretry flag             %d\n", wqp->noretry);
			sendrep (rpfd, MSG_OUT, "Aflag                    %d\n", wqp->Aflag);
			sendrep (rpfd, MSG_OUT, "concat_off_fseq          %d\n", wqp->concat_off_fseq);
			sendrep (rpfd, MSG_OUT, "magic                    0x%lx\n", (unsigned long) wqp->magic);
			sendrep (rpfd, MSG_OUT, "api_out                  %d\n", wqp->api_out);
			sendrep (rpfd, MSG_OUT, "openmode                 %d\n", (int) wqp->openmode);
			sendrep (rpfd, MSG_OUT, "openflags                0x%lx\n", (unsigned long) wqp->openflags);
			sendrep (rpfd, MSG_OUT, "silent                   %d\n", wqp->silent);
			sendrep (rpfd, MSG_OUT, "use_subreqid             %d\n", wqp->use_subreqid);
			sendrep (rpfd, MSG_OUT, "save_nbsubreqid          %d\n", wqp->save_nbsubreqid);
			sendrep (rpfd, MSG_OUT, "(API) flags              %s\n", stglogflags(NULL,NULL,(u_signed64) wqp->flags));
			if (wqp->save_subreqid != NULL) {
				for (i = 0, wfp = wqp->wf; i < wqp->save_nbsubreqid; i++, wfp++) {
					sendrep (rpfd, MSG_OUT, "\tsave_subreqid[%d] = %d\n", i, wqp->save_subreqid[i]);
				}
			}
			sendrep (rpfd, MSG_OUT, "last_rwcounterfs_vs_R     %d\n", wqp->last_rwcounterfs_vs_R);
			sendrep (rpfd, MSG_OUT, "last_rwcounterfs_index    %d\n", wqp->last_rwcounterfs_index);
			for (i = 0, wfp = wqp->wf; i < wqp->nb_subreqs; i++, wfp++) {
				for (stcp = stcs; stcp < stce; stcp++) {
					if (wfp->subreqid == stcp->reqid) {
						char tmpbuf1[21];
						char tmpbuf2[22];

						sendrep(rpfd, MSG_OUT, "\tWaiting File No %3d\n", i);
						sendrep(rpfd, MSG_OUT, "\t\tsubreqid=%d\n", wfp->subreqid);
						sendrep(rpfd, MSG_OUT, "\t\twaiting_on_req=%d\n", wfp->waiting_on_req);
						sendrep(rpfd, MSG_OUT, "\t\tupath=%s\n", wfp->upath);
						sendrep(rpfd, MSG_OUT, "\t\tsize_to_recall=%s\n", u64tostr(wfp->size_to_recall, tmpbuf1, 0));
						sendrep(rpfd, MSG_OUT, "\t\thsmsize=%s\n", u64tostr(wfp->hsmsize, tmpbuf1, 0));
						sendrep(rpfd, MSG_OUT, "\t\tnb_segments=%d\n", wfp->nb_segments);
						sendrep(rpfd, MSG_OUT, "\t\tsize_yet_recalled=%s\n", u64tostr(wfp->size_yet_recalled, tmpbuf2, 0));

						switch (stcp->t_or_d) {
						case 't':
							sendrep(rpfd, MSG_OUT, "\t\tVID.FSEQ=%s.%s\n", stcp->u1.t.vid[0], stcp->u1.t.fseq);
							break;
						case 'd':
						case 'a':
							sendrep(rpfd, MSG_OUT, "\t\tu1.d.xfile=%s\n", stcp->u1.d.xfile);
							break;
						case 'm':
							sendrep(rpfd, MSG_OUT, "\t\tu1.m.xfile=%s\n", stcp->u1.m.xfile);
							break;
						case 'h':
							sendrep(rpfd, MSG_OUT, "\t\tu1.h.xfile=%s\n", stcp->u1.h.xfile);
							if (wfp->ipath[0] != '\0') {
								sendrep(rpfd, MSG_OUT, "\t\tipath=%s (copy between pools)\n", wfp->ipath);
							}
							break;
						default:
							sendrep(rpfd, MSG_OUT, "\t\t<unknown_type>\n");
							break;
						}
						break;
					}
				}
			}
		}
	}
}

int
selectfs(poolname, size, path, status, noallocation)
	char *poolname;
	u_signed64 *size;
	char *path;
	int status;
	int noallocation;
{
	struct pool_element *elemp;
	int found = 0;
	int i;
	struct pool *pool_p;
	u_signed64 reqsize;
	u_signed64 reqsize_orig;
	char tmpbuf0[21];
	char tmpbuf1[21];
	char tmpbuf2[21];
	struct pool_element *this_element;
	struct stgcat_entry stcx;
	struct stgcat_entry *stcp;

	stcx.status = status;
	stcp = &stcx;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (! noallocation) {
		if (*size == 0) {
			*size = pool_p->defsize;
		}
		reqsize = *size;	/* size in bytes */
		reqsize_orig = reqsize;
		if (reqsize < pool_p->mindefsize) {
			/* We guarantee at minimum MINDEFSIZE bytes when allocation */
			/* even if user asked for less */
			reqsize = pool_p->mindefsize;
		}
	} else {
		*size = 0;
		reqsize = 0;
		reqsize_orig = 0;
	}

	/* If this is an OUT pool we do qsort() anyway */
	if (ISSTAGEOUT(stcp) || ISSTAGEALLOC(stcp)) {
		if ((this_element = betterfs_vs_pool(poolname,WRITE_MODE,reqsize,&i)) == NULL) {
			/* If pool was automatically selected using bestnextpool_from_metapool() or bestnextpool_out() */
			/* We signal that this pool was anyway subject to allocation */
			update_last_pool_allocation(poolname);
			/* Oups... Tant-pis, return what will provocate the ENOENT */
			return(-1);
		} else {
			elemp = this_element;
			found = 1;
		}
	} else {
		i = pool_p->next_pool_elem;
		do {
			elemp = pool_p->elemp + i;
			/* Because reqsize can be zero if open/rd_only of zero-length CASTOR file */
			if (elemp->free <= 0) goto selectfs_continue;
			if (elemp->free >= reqsize) {
				found = 1;
				break;
			}
		  selectfs_continue:
			i++;
			if (i >= pool_p->nbelem) i = 0;
		} while (i != pool_p->next_pool_elem);
	}

	/* We can already reply now if we have found NO candidate ! */
	if (!found) {
		/* If pool was automatically selected using bestnextpool_from_metapool() or bestnextpool_out() */
		/* We signal that this pool was anyway subject to allocation */
		update_last_pool_allocation(poolname);
		return (-1);
	}

	pool_p->next_pool_elem = i + 1;
	if (pool_p->next_pool_elem >= pool_p->nbelem) pool_p->next_pool_elem = 0;
	pool_p->free -= reqsize_orig;
	elemp->free -= reqsize_orig;
	if (pool_p->free > pool_p->capacity) {
		/* This is impossible. In theory it can happen only if reqsize_orig > previous pool_p->free and */
		/* if pool_p->capacity < max_u_signed64 */
		stglogit ("selectfs", "### Warning, pool_p->free > pool_p->capacity. pool_p->free set to 0\n");
		pool_p->free = 0;
	}
	if (elemp->free > elemp->capacity) {
		/* This is impossible. In theory it can happen only if reqsize_orig > previous elemp->free and */
		/* if elemp->capacity < max_u_signed64 */
		stglogit ("selectfs", "### Warning, elemp->free > elemp->capacity. elemp->free set to 0\n");
		elemp->free = 0;
	}
	nfsroot = getconfent ("RFIO", "NFS_ROOT", 0);
#ifdef NFSROOT
	if (nfsroot == NULL) nfsroot = NFSROOT;
#endif
	if (nfsroot != NULL &&
		strncmp (elemp->dirpath, nfsroot, strlen (nfsroot)) == 0 &&
		*(elemp->dirpath + strlen(nfsroot)) == '/')	/* /shift syntax */
		strcpy (path, elemp->dirpath);
	else
		sprintf (path, "%s:%s", elemp->server, elemp->dirpath);
	stglogit ("selectfs", "%s reqsize=%s, elemp->free=%s, pool_p->free=%s\n",
			  path, u64tostr(reqsize_orig, tmpbuf0, 0), u64tostr(elemp->free, tmpbuf1, 0), u64tostr(pool_p->free, tmpbuf2, 0));
	/* We udpate known allocation timestamp */
	pool_p->last_allocation = elemp->last_allocation = time(NULL);
	return (1);
}

void
rwcountersfs(poolname, ipath, status, req_type)
	char *poolname;
	char *ipath;
	int status;
	int req_type;
{
	struct pool_element *elemp;
	int i, j;
	char *p;
	char path[MAXPATH];
	struct pool *pool_p;
	char server[CA_MAXHOSTNAMELEN + 1];
	int read_incr = 0;
	int write_incr = 0;
	extern time_t last_upd_fileclasses; /* == 0 only at the initial startup or when not wanted */

	if ((poolname == NULL) || (*poolname == '\0') || (ipath == NULL) || (*ipath == '\0') ||
	    ((last_upd_fileclasses == 0) && (redomigpool_in_action == 0))) {
		return;
	}

	/* Not supported on pool other but defpoolname_out */
	/*
	if (ispool_out(poolname) != 0) {
		return;
	}
	*/
	
	switch (req_type) {
	case STAGEOUT:
	case STAGEALLOC:
		write_incr = 1;
		break;
	case STAGEWRT:
	case STAGEPUT:
		read_incr = 1;
		break;
	case STAGEUPDC:
		if (((status & 0xF) == STAGEOUT) ||
			((status & 0xF) == STAGEALLOC)) {
			write_incr = -1;
		} else if (((status & 0xF) == STAGEWRT) ||
				   ((status & 0xf) == STAGEPUT)) {
			read_incr = -1;
		} else if (((status & 0xF) == STAGEIN)) {
			/* Not supported for the moment */
			/* write_incr = -1; */
		} else {
			stglogit ("rwcountersfs", "### Warning - called in STAGEUPDC mode for an unsupported stcp->status = 0x%lx\n", (unsigned long) status);
			return;      
		}
		break;
	default:
		/*
		stglogit ("rwcountersfs", "### Warning, called in unsupported mode %d (%s)\n", req_type,
		  req_type == STAGEIN ? "STAGEIN" :
		  (req_type == STAGEQRY ? "STAGEQRY" :
		  (req_type == STAGECLR ? "STAGECLR" :
		  (req_type == STAGEKILL ? "STAGEKILL" :
		  (req_type == STAGEINIT ? "STAGEINIT" :
		  (req_type == STAGECAT ? "STAGECAT" :
		  (req_type == STAGEGET ? "STAGEGET" :
		  (req_type == STAGEFILCHG ? "STAGEFILCHG" :
		  (req_type == STAGESHUTDOWN ? "STAGESHUTDOWN" : "<unknown>"
		  )
		  )
		  )
		  )
		  )
		  )
		  )
		  )
		  );
		*/
		return;      
	}
	if ((p = strchr (ipath, ':')) != NULL) {
		strncpy (server, ipath, (size_t) (p - ipath));
		*(server + (p - ipath)) = '\0';
	}
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (i == nbpool) return;	/* old entry; pool does not exist */
	for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++)
		if (p) {
			if (strcmp (server, elemp->server) ||
				strncmp (p + 1, elemp->dirpath, strlen (elemp->dirpath)) ||
				*(p + 1 + strlen (elemp->dirpath)) != '/') continue;
			sprintf (path, "%s:%s", elemp->server, elemp->dirpath);
			break;
		} else {
			if (strncmp (ipath, elemp->dirpath, strlen (elemp->dirpath)) ||
				*(ipath + strlen (elemp->dirpath)) != '/') continue;
			strcpy (path, elemp->dirpath);
			break;
		}
	if (j < pool_p->nbelem) {
		if ((write_incr < 0) && (elemp->nbwriteaccess <= 0)) {
		    stglogit ("rwcountersfs", "### %s Warning, write_incr=-1 on nbwriteaccess <= 0. All reseted to 0.\n", path);
		    write_incr = 0;
		    elemp->nbwriteaccess = 0;
		}
		if ((read_incr < 0) && (elemp->nbreadaccess <= 0)) {
		    stglogit ("rwcountersfs", "### %s Warning, read_incr=-1 on nbreadaccess <= 0. All reseted to 0.\n", path);
		    read_incr = 0;
		    elemp->nbreadaccess = 0;
		}
		if (read_incr != 0 || write_incr != 0) {
			elemp->nbreadaccess += read_incr;
			elemp->nbwriteaccess += write_incr;
			stglogit ("rwcountersfs", "%s read[%s%d]/write[%s%d]=%2d/%2d\n",
					  path,
					  read_incr >= 0 ? "+" : "-",
					  read_incr >= 0 ? read_incr : -read_incr,
					  write_incr >= 0 ? "+" : "-",
					  write_incr >= 0 ? write_incr : -write_incr,
					  elemp->nbreadaccess,
					  elemp->nbwriteaccess);
			/* We changed the counters - keep in mind exactly how many stageouts we have */
			switch (req_type) {
			case STAGEOUT:
				nbstageout++;
				break;
			case STAGEALLOC:
				nbstagealloc++;
				break;
			case STAGEUPDC:
				switch (status & 0xF) {
				case STAGEOUT:
					nbstageout--;
					break;
				case STAGEALLOC:
					nbstagealloc--;
					break;
				default:
					break;
				}
				break;
			default:
				break;
			}
		}
	}
}

void
getdefsize(poolname, size)
	char *poolname;
	u_signed64 *size;
{
	int i;
	struct pool *pool_p;
	int found = 0;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp (poolname, pool_p->name) == 0) {
			found = 1;
			break;
		}
	}
	if (found != 0) {
		*size = pool_p->defsize;
	} else {
		*size = 0;
	}
}

void
getmindefsize(poolname, size)
	char *poolname;
	u_signed64 *size;
{
	int i;
	struct pool *pool_p;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	*size = pool_p->mindefsize;
}

int
updfreespace(poolname, ipath, reject_overflow, elemp_free, incr)
	char *poolname;
	char *ipath;
	int reject_overflow;
	u_signed64 *elemp_free;
	signed64 incr;
{
	struct pool_element *elemp;
	int i, j;
	char *p;
	char path[MAXPATH];
	struct pool *pool_p;
	char server[CA_MAXHOSTNAMELEN + 1];
	char tmpbuf1[21];
	char tmpbuf2[21];
	char tmpbuf3[21];

	if ((poolname == NULL) || (*poolname == '\0'))
		return (0);
	if ((ipath == NULL) || (*ipath == '\0'))
		return(0);
	if ((p = strchr (ipath, ':')) != NULL) {
		strncpy (server, ipath, (size_t) (p - ipath));
		*(server + (p - ipath)) = '\0';
	}
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (i == nbpool) return (0);	/* old entry; pool does not exist */
	for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
		if (p) {
			if (strcmp (server, elemp->server) ||
				strncmp (p + 1, elemp->dirpath, strlen (elemp->dirpath)) ||
				*(p + 1 + strlen (elemp->dirpath)) != '/') continue;
			sprintf (path, "%s:%s", elemp->server, elemp->dirpath);
			break;
		} else {
			if (strncmp (ipath, elemp->dirpath, strlen (elemp->dirpath)) ||
				*(ipath + strlen (elemp->dirpath)) != '/') continue;
			strcpy (path, elemp->dirpath);
			break;
		}
	}
	if (j < pool_p->nbelem) {
		u_signed64 elemp_old_free = elemp->free;
		u_signed64 pool_p_old_free = pool_p->free;

		elemp->free += incr;
		pool_p->free += incr;
		if (((pool_p->free > pool_p->capacity) || ((elemp->free > elemp->capacity))) && (reject_overflow)) goto rejected_overflow;
		if (pool_p->free > pool_p->capacity) {
			if (incr >= 0) {
				stglogit ("selectfs", "### Warning, pool_p->free > pool_p->capacity. pool_p->free set to pool_p->capacity\n");
				pool_p->free = pool_p->capacity;
			} else {
				stglogit ("selectfs", "### Warning, pool_p->free > pool_p->capacity. pool_p->free set to 0\n");
				pool_p->free = 0;
			}
		}
		if (elemp->free > elemp->capacity) {
			if (incr >= 0) {
				stglogit ("selectfs", "### Warning, elemp->free > elemp->capacity. elemp->free set to elemp->capacity\n");
				elemp->free = elemp->capacity;
			} else {
				stglogit ("selectfs", "### Warning, elemp->free > elemp->capacity. elemp->free set to 0\n");
				elemp->free = 0;
			}
		}
		stglogit ("updfreespace", "%s incr=%s%s, elemp->free=%s, pool_p->free=%s\n",
				  path, (incr < 0 ? "-" : ""),
				  u64tostr((u_signed64) (incr < 0 ? -incr : incr), tmpbuf1, 0),
				  u64tostr(elemp->free, tmpbuf2, 0),
				  u64tostr(pool_p->free, tmpbuf3, 0));
		goto selectfs_ok;
	  rejected_overflow:
		elemp->free = elemp_old_free;
		pool_p->free = pool_p_old_free;
		stglogit ("updfreespace", "### %s incr=%s%s but elemp->free=%s : overflow rejected\n",
				  path, (incr < 0 ? "-" : ""),
				  u64tostr((u_signed64) (incr < 0 ? -incr : incr), tmpbuf1, 0),
				  u64tostr(elemp->free, tmpbuf2, 0));
		if (elemp_free != NULL) *elemp_free = elemp->free;
		return(-1);
	}
  selectfs_ok:
	return (0);
}

int updpoolconf(defpoolname,defpoolname_in,defpoolname_out)
	char *defpoolname;
	char *defpoolname_in;
	char *defpoolname_out;
{
	int c, i, j;
	struct pool *pool_n, *pool_p;
	char sav_defpoolname[CA_MAXPOOLNAMELEN + 1];
	char sav_defpoolname_in[CA_MAXPOOLNAMELEN + 1];
	char sav_defpoolname_out[10 * (CA_MAXPOOLNAMELEN + 1)];
	struct migrator *sav_migrators;
	int sav_nbmigrator;
	int sav_nbhost;
	char **sav_poolc;
	extern int migr_init;
	struct stgcat_entry *stcp;
	struct waitq *wqp = NULL;
	struct waitf *wfp;
	extern struct waitq *waitqp;
	char *func = "updpoolconf";

	/* save the current configuration */
	strcpy (sav_defpoolname, defpoolname);
	strcpy (sav_defpoolname_in, defpoolname_in);
	strcpy (sav_defpoolname_out, defpoolname_out);
	sav_migrators = migrators;
	sav_nbmigrator = nbmigrator;
	sav_nbpool = nbpool;
	sav_nbhost = nbhost;
	sav_poolc = poolc;
	sav_pools = pools;

	if (migr_init == 0) {
		/* We check if we have to force migr_init value or not */
		/* If there is any MIGR counter different than zero, so we do */
		for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
			if (pool_n->migr != NULL) {
				int k;

				if ((pool_n->migr->global_predicates.nbfiles_canbemig  != 0) ||
					(pool_n->migr->global_predicates.space_canbemig    != 0) ||
					(pool_n->migr->global_predicates.nbfiles_delaymig  != 0) ||
					(pool_n->migr->global_predicates.space_delaymig    != 0) ||
					(pool_n->migr->global_predicates.nbfiles_beingmig  != 0) ||
					(pool_n->migr->global_predicates.space_beingmig    != 0)) {
					if (migr_init == 0) {
					  sendrep(&rpfd, MSG_ERR, "STG02 - Forcing -X option\n");
					  migr_init = 1;
					}
					break;
				}
				for (k = 0; k < pool_n->migr->nfileclass; k++) {
					if ((pool_n->migr->fileclass_predicates[k].nbfiles_canbemig != 0) ||
						(pool_n->migr->fileclass_predicates[k].space_canbemig   != 0) ||
						(pool_n->migr->fileclass_predicates[k].nbfiles_delaymig != 0) ||
						(pool_n->migr->fileclass_predicates[k].space_delaymig   != 0) ||
						(pool_n->migr->fileclass_predicates[k].nbfiles_beingmig != 0) ||
						(pool_n->migr->fileclass_predicates[k].space_beingmig   != 0)) {
					  if (migr_init == 0) {
					    sendrep(&rpfd, MSG_ERR, "STG02 - Forcing -X option\n");
					    migr_init = 1;
					  }
						break;
					}
				}
				if (migr_init != 0) break;
			}
		}
	}

	if ((c = getpoolconf (defpoolname,defpoolname_in,defpoolname_out))) {	/* new configuration is incorrect */
		/* restore the previous configuration */
		strcpy (defpoolname, sav_defpoolname);
		strcpy (defpoolname_in, sav_defpoolname_in);
		strcpy (defpoolname_out, sav_defpoolname_out);
		migrators = sav_migrators;
		nbmigrator = sav_nbmigrator;
		nbpool = sav_nbpool;
		nbhost = sav_nbhost;
		pools = sav_pools;
		sav_pools = NULL;
	} else {			/* free the old configuration */
		/* but keep pids of cleaner/migrator as well as started time if any */
		free (sav_poolc);
		for (i = 0, pool_p = sav_pools; i < sav_nbpool; i++, pool_p++) {
			for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
				if (strcmp (pool_n->name, pool_p->name) == 0) {
					pool_n->ovl_pid = pool_p->ovl_pid;
					pool_n->cleanreqtime = pool_p->cleanreqtime;
					pool_n->cleanreqtime_previous = pool_p->cleanreqtime_previous;
					pool_n->cleanreqtime_previous_end = pool_p->cleanreqtime_previous_end;
					if (pool_n->migr != NULL && pool_p->migr != NULL) {
						pool_n->migr->mig_pid = pool_p->migr->mig_pid;
						pool_n->migr->migreqtime = pool_p->migr->migreqtime;
						pool_n->migr->migreqtime_last_start = pool_p->migr->migreqtime_last_start;
						pool_n->migr->migreqtime_previous = pool_p->migr->migreqtime_previous;
						pool_n->migr->migreqtime_previous_end = pool_p->migr->migreqtime_previous_end;
					}
					break;
				}
			}
			free (pool_p->elemp);
		}
		free (sav_pools);
		sav_pools = NULL;
		if (sav_migrators) {
			for (i = 0; i < sav_nbmigrator; i++) {
				if (sav_migrators[i].fileclass != NULL) free (sav_migrators[i].fileclass);
				if (sav_migrators[i].fileclass_predicates != NULL) free (sav_migrators[i].fileclass_predicates);
			}
			free (sav_migrators);
		}
		/* Counters on the number of entries in STAGEOUT/STAGEALLOC status */
		nbstageout = nbstagealloc = 0;
		/* And restore rw counters + check poolname definition */
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (stcp->ipath[0] != '\0') {
				if (stcp->poolname[0] != '\0') {
					char *p;

					/* Check that poolname from catalog match current configuration */
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
			if ((stcp->status == STAGEOUT) || (stcp->status == STAGEALLOC)) {
			  if (stcp->t_or_d == 'h') {
			    if (migr_init == 0) {
			      sendrep(&rpfd, MSG_ERR, "STG02 - Forcing -X option\n");
			      migr_init = 1;
			    }
			  }
			  rwcountersfs(stcp->poolname, stcp->ipath, stcp->status, stcp->status);
			}
		}
		for (wqp = waitqp; wqp; wqp = wqp->next) {
			if (wqp->last_rwcounterfs_vs_R == LAST_RWCOUNTERSFS_TPPOS) {
				wfp = wqp->wf;
				for (stcp = stcs; stcp < stce; stcp++) {
					if (stcp->reqid == 0) break;
					if (stcp->reqid == wfp->subreqid) {
						if (ISSTAGEWRT(stcp) || ISSTAGEPUT(stcp)) {
							/* We can loose some opening notifications doing so */
							/* Progressively situation will come back to normal */
							/* because rwcountersfs(), when decreasing counters */
							/* will say there is internal error and let numbers */
							/* to zero */
							rwcountersfs(stcp->poolname, stcp->ipath, STAGEWRT, STAGEWRT); /* Could be STAGEPUT */
						}
						break;
					}
				}
			}
		}
	}
	if (migr_init != 0) {
		redomigpool();
	}
	/* Reset the permanent RFIO connections */
	/* [Needed in particular if pools have changed] */
	rfio_end();
	rfio_unend();
	return (c);
}

void redomigpool()
{
	struct stgcat_entry *stcp;
	int j;
	struct pool *pool_n;
	extern time_t last_upd_fileclasses; /* == 0 only at the initial startup or when not wanted */
	time_t save_last_upd_fileclasses;
  
	redomigpool_in_action = 1;

	/* Update the fileclasses */
	upd_fileclasses();

	/* Update the migrators */
	for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
		if (pool_n->migr != NULL) {
			int k;
		  
			pool_n->migr->global_predicates.nbfiles_canbemig = 0;
			pool_n->migr->global_predicates.space_canbemig = 0;
			pool_n->migr->global_predicates.nbfiles_beingmig = 0;
			pool_n->migr->global_predicates.space_beingmig = 0;
			pool_n->migr->global_predicates.nbfiles_delaymig = 0;
			pool_n->migr->global_predicates.space_delaymig = 0;
			for (k = 0; k < pool_n->migr->nfileclass; k++) {
				pool_n->migr->fileclass_predicates[k].nbfiles_canbemig = 0;
				pool_n->migr->fileclass_predicates[k].space_canbemig = 0;
				pool_n->migr->fileclass_predicates[k].nbfiles_beingmig = 0;
				pool_n->migr->fileclass_predicates[k].space_beingmig = 0;
				pool_n->migr->fileclass_predicates[k].nbfiles_delaymig = 0;
				pool_n->migr->fileclass_predicates[k].space_delaymig = 0;
			}
		}
	}

	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if ((stcp->t_or_d == 'h') && (stcp->status == STAGEOUT)) {
			/* Force a Cns_statx call for a STAGEOUT file */
			/* upd_fileclass() will move to PUT_FAILED if necessary */
			save_last_upd_fileclasses = last_upd_fileclasses;
			last_upd_fileclasses = 0;
			if (upd_fileclass(NULL,stcp,2,0,0) < 0) {
				/* Try to fetch fileclass only unless record was cleared */
				if (serrno == ESTCLEARED) {
					last_upd_fileclasses = save_last_upd_fileclasses;
					continue;
				}
				upd_fileclass(NULL,stcp,4,0,0);
			}
			last_upd_fileclasses = save_last_upd_fileclasses;
		}
		if ((stcp->status & CAN_BE_MIGR) != CAN_BE_MIGR) continue;
		if ((stcp->status & PUT_FAILED) == PUT_FAILED) continue;
		insert_in_migpool(stcp);
	}
	redomigpool_in_action = 0;
}

int get_create_file_option(poolname)
	char *poolname;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) 
			return(pool_p->no_file_creation);
	return (0);
}


int export_hsm_option(poolname)
	char *poolname;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) 
			return(pool_p->export_hsm);
	return (0);
}

int have_another_pool_with_export_hsm_option(poolname)
	char *poolname;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp (poolname, pool_p->name) == 0) continue;
		if (pool_p->export_hsm) return(pool_p->export_hsm);
	}
	return (0);
}


/* immediate parameter: used only if flag == 1;                */
/* If 0           set only canbemigr or delaymigr stack        */
/* If 1           set both canbemigr and beingmigr stacks      */
/* If 2           set only being migr stack                    */
/* If 3           move from delaymigr to canbemigr             */
/* If 4           conditional move from delaymigr to canbemigr */
/* this routine is called by update_migpool() and cannot fail  */
/* (everything is checked upper by update_migpool())           */
void incr_migpool_counters(stcp,pool_p,immediate,ifileclass,thistime)
	struct stgcat_entry *stcp;
	struct pool *pool_p;
	int immediate;
	int ifileclass;
	time_t thistime;
{
	char *func = "incr_migpool_counters";
	int thismintime_beforemigr;

	if ((thismintime_beforemigr = stcp->u1.h.mintime_beforemigr) < 0) {
		thismintime_beforemigr = mintime_beforemigr_vs_pool(pool_p,ifileclass);
	}

	switch (immediate) {
	case 0:
		/* We check stcp->a_time and default mintime_beforemigr */
		/* If there is a delay in migration for this stcp, we increment correspondingly the */
		/* correct counters */
		/* We maintain an interval and non-persistent flag so that we remember what action to do at */
		/* next iteration */
		/* Default value for this non-persistent flag is always 0 */
		/* Please note that if thismintime_beforemigr is high enough the operation */
		/* stcp->a_time + thismintime_beforemigr will go out of bounds...! */
		/* That's why we typecase to whole operation into 64bits quantities */
		if ((stcp->filler[0] == 0) && ((u_signed64) ((u_signed64) stcp->a_time + (u_signed64) thismintime_beforemigr) > (u_signed64) thistime)) {
			/* We udpate global migrator variables (wait mode) */
			pool_p->migr->global_predicates.nbfiles_delaymig++;
			pool_p->migr->global_predicates.space_delaymig += stcp->actual_size;
			/* We udpate global fileclass variables (wait mode) */
			pool_p->migr->fileclass_predicates[ifileclass].nbfiles_delaymig++;
			pool_p->migr->fileclass_predicates[ifileclass].space_delaymig += stcp->actual_size;
			stcp->filler[0] = 'd'; /* 'd' for 'putted in delayed mode' */
		} else if ((stcp->filler[0] != 'm') && ((u_signed64) ((u_signed64) stcp->a_time + (u_signed64) thismintime_beforemigr) <= (u_signed64) thistime)) {
			if (stcp->filler[0] == 'd') {
				char tmpbuf[21];
				/* We log the fact that we moved this entry from delayed mode to non-delayed mode */
				stglogit (func, STG155,
						  stcp->u1.h.xfile,
						  stcp->poolname,
						  u64tostr((u_signed64) stcp->u1.h.fileid, tmpbuf, 0),
						  stcp->u1.h.server);
			}
			/* We udpate global migrator variables */
			pool_p->migr->global_predicates.nbfiles_canbemig++;
			pool_p->migr->global_predicates.space_canbemig += stcp->actual_size;
			/* We udpate global fileclass variables */
			pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig++;
			pool_p->migr->fileclass_predicates[ifileclass].space_canbemig += stcp->actual_size;
			stcp->filler[0] = 'm'; /* 'm' for 'putted in canbemigr (non-delayed) mode' */
		}
		break;
	case 1:
		/* We udpate global migrator variables */
		pool_p->migr->global_predicates.nbfiles_canbemig++;
		pool_p->migr->global_predicates.space_canbemig += stcp->actual_size;
		/* We udpate global fileclass variables */
		pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig++;
		pool_p->migr->fileclass_predicates[ifileclass].space_canbemig += stcp->actual_size;
		/* No break here - intentionnally */
	case 2:
		/* We udpate global migrator variables */
		pool_p->migr->global_predicates.nbfiles_beingmig++;
		pool_p->migr->global_predicates.space_beingmig += stcp->actual_size;
		/* We udpate global fileclass variables */
		pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig++;
		pool_p->migr->fileclass_predicates[ifileclass].space_beingmig += stcp->actual_size;
		stcp->filler[0] = 'm'; /* 'm' for 'putted in canbemigr or beingmigr (=> non-delayed) mode' */
		break;
	case 3:
	  case_3:
	  /* We udpate global migrator variables (wait mode) */
	  if (--pool_p->migr->global_predicates.nbfiles_delaymig < 0) {
		  stglogit (func, STG106, func,
					stcp->poolname,
					"nbfiles_delaymig < 0 (resetted to 0)");
		  pool_p->migr->global_predicates.nbfiles_delaymig = 0;
	  }
	  if (pool_p->migr->global_predicates.space_delaymig < stcp->actual_size) {
		  stglogit (func, STG106, func,
					stcp->poolname,
					"space_delaymig predicted to be < 0 (resetted to 0)");
		  pool_p->migr->global_predicates.space_delaymig = 0;
	  } else {
		  pool_p->migr->global_predicates.space_delaymig -= stcp->actual_size;
	  }
	  /* We udpate global fileclass variables (wait mode) */
	  if (--pool_p->migr->fileclass_predicates[ifileclass].nbfiles_delaymig < 0) {
		  stglogit (func, STG110,
					func,
					stcp->poolname,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
					pool_p->migr->fileclass[ifileclass]->server,
					"nbfiles_delaymig < 0 (resetted to 0)");
		  pool_p->migr->fileclass_predicates[ifileclass].nbfiles_delaymig = 0;
      }
	  if (pool_p->migr->fileclass_predicates[ifileclass].space_delaymig < stcp->actual_size) {
		  stglogit (func, STG110,
					func,
					stcp->poolname,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
					pool_p->migr->fileclass[ifileclass]->server,
					"space_delaymig predicted to be < 0 (resetted to 0)");
		  pool_p->migr->fileclass_predicates[ifileclass].space_delaymig = 0;
	  } else {
		  pool_p->migr->fileclass_predicates[ifileclass].space_delaymig -= stcp->actual_size;
	  }
	  /* We udpate global migrator variables */
	  pool_p->migr->global_predicates.nbfiles_canbemig++;
	  pool_p->migr->global_predicates.space_canbemig += stcp->actual_size;
	  /* We udpate global fileclass variables */
	  pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig++;
	  pool_p->migr->fileclass_predicates[ifileclass].space_canbemig += stcp->actual_size;
	  stcp->filler[0] = 'm'; /* 'm' for 'putted in canbemigr (non-delayed) mode' */
	  break;
	case 4:
		/* We udpate global migrator variables (wait mode) if necessary */
		if (stcp->filler[0] == 'd') {
			/* Latency time has not yet expired but we move neverthless DELAY counters to CAN_BE_MIGR ones */
			stcp->u1.h.mintime_beforemigr = 0;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
			goto case_3;
		}
		break;
	default:
		break;
	}
	return;
}

/* flag means :                                                */
/*  1             file put in stack can_be_migr                */
/* -1             file removed from stack can_be_migr          */
/* immediate parameter: used only if flag == 1;                */
/* If 0           set only canbemigr or delaymigr stack        */
/* If 1           set both canbemigr and beingmigr stacks      */
/* If 2           set only being migr stack                    */
/* If 3           move from delaymigr to canbemigr             */
/* If 4           conditional move from delaymigr to canbemigr */
/* Returns: 0 (OK) or -1 (NOT OK)                              */
int update_migpool(stcp,flag,immediate)
	struct stgcat_entry **stcp;
	int flag;
	int immediate;
{
	int i, ipool;
	struct pool *pool_p;
	int ifileclass;
	struct stgcat_entry savestcp = **stcp;
	char *func = "update_migpool";
	int savereqid = reqid;
	int rc = 0;
	int ngen = 0;
	time_t thistime = time(NULL); /* To have exactly the same time within this routine */

	if (((flag != 1) && (flag != -1)) || ((immediate != 0) && (immediate != 1) && (immediate != 2) && (immediate != 3) && (immediate != 4))) {
		stglogit (func, STG105, func,  "flag should be 1 or -1, and immediate should be 0, 1, 2, 3 or 4");
		serrno = EINVAL;
		rc = -1;
		goto update_migpool_return;
	}

	/* We check that this poolname exist */
	ipool = -1;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp(pool_p->name,(*stcp)->poolname) == 0) {
			ipool = i;
			break;
		}
	}
	if (ipool < 0) {
		stglogit (func, STG32, (*stcp)->poolname);
		serrno = EINVAL;
		rc = -1;
		goto update_migpool_return;
	}
	/* We check that this pool have a migrator */
	if (pool_p->migr == NULL) {
		sendrep(&rpfd, MSG_ERR, STG149, (*stcp)->u1.h.xfile, (*stcp)->poolname);
		rc = -1;
		goto update_migpool_return;
	}
	/* We check that this stcp have a known fileclass v.s. its pool migrator */
	if ((ifileclass = upd_fileclass(pool_p,*stcp,0,0,0)) < 0) {
		rc = -1;
		goto update_migpool_return;
	}

	/* In case flag is 1, we check that the file can be put in the can_be_migr stack */
	if (flag == 1) {
		/* If this fileclass specifies nbcopies == 0 or nbtppools == 0 (in practice, both) */
		/* [it is protected to restrict to this - see routine upd_fileclass() in this source] */
		/* this stcp cannot be a candidate for migration - we put it is STAGED status */
		/* immediately */
		if ((pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbcopies == 0) || 
			(pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbtppools == 0)) {
			/*
			sendrep(&rpfd, MSG_ERR, STG139,
					(*stcp)->u1.h.xfile, 
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
					pool_p->migr->fileclass[ifileclass]->server,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.classid,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbcopies,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbtppools);
			*/
			/* We mark it as staged */
			(*stcp)->status |= STAGED;
			if (((*stcp)->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
				/* And not as a migration candidate */
				(*stcp)->status &= ~CAN_BE_MIGR;
		}
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,*stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
			/* But we still returns ok... */
			return(0);
		}
		
		/* If this fileclass specifies a minimum filesize we check it */
		if ((pool_p->migr->fileclass[ifileclass]->Cnsfileclass.min_filesize > 0) &&
			((u_signed64) ((*stcp)->actual_size) < (u_signed64) (((u_signed64) pool_p->migr->fileclass[ifileclass]->Cnsfileclass.min_filesize) * ((u_signed64) ONE_MB)))) {
			char tmpbuf1[21];
			char tmpbuf2[21];
			sendrep(&rpfd, MSG_ERR, STG168,
					(*stcp)->u1.h.xfile, 
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
					pool_p->migr->fileclass[ifileclass]->server,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.classid,
					"minimum",
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.min_filesize,
					u64tostr ((*stcp)->actual_size , tmpbuf1, 0),
					u64tostru((*stcp)->actual_size , tmpbuf2, 0));
			/* We mark it as put_failed */
			(*stcp)->status |= PUT_FAILED;
			if (((*stcp)->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
				/* And not as a migration candidate */
				(*stcp)->status &= ~CAN_BE_MIGR;
			}
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,*stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
			/* But we still returns ok... */
			return(0);
		}
		
		/* If this fileclass specifies a maximum filesize we check it */
		if ((pool_p->migr->fileclass[ifileclass]->Cnsfileclass.max_filesize > 0) &&
			((u_signed64) (*stcp)->actual_size > (u_signed64) (((u_signed64) pool_p->migr->fileclass[ifileclass]->Cnsfileclass.max_filesize) * ((u_signed64) ONE_MB)))) {
			char tmpbuf1[21];
			char tmpbuf2[21];
			sendrep(&rpfd, MSG_ERR, STG168,
					(*stcp)->u1.h.xfile, 
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
					pool_p->migr->fileclass[ifileclass]->server,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.classid,
					"maximum",
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.max_filesize,
					u64tostr ((*stcp)->actual_size , tmpbuf1, 0),
					u64tostru((*stcp)->actual_size , tmpbuf2, 0));
			/* We mark it as put_failed */
			(*stcp)->status |= PUT_FAILED;
			if (((*stcp)->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
				/* And not as a migration candidate */
				(*stcp)->status &= ~CAN_BE_MIGR;
			}
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,*stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
			/* But we still returns ok... */
			return(0);
		}

		/* If the fileclass specifies (uid,gid) filters we verify that user does match them */
		if (((pool_p->migr->fileclass[ifileclass]->Cnsfileclass.uid != 0) && ((*stcp)->uid != pool_p->migr->fileclass[ifileclass]->Cnsfileclass.uid)) ||
			((pool_p->migr->fileclass[ifileclass]->Cnsfileclass.gid != 0) && ((*stcp)->gid != pool_p->migr->fileclass[ifileclass]->Cnsfileclass.gid))) {
			sendrep(&rpfd, MSG_ERR, STG176,
					(*stcp)->u1.h.xfile, 
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
					pool_p->migr->fileclass[ifileclass]->server,
					pool_p->migr->fileclass[ifileclass]->Cnsfileclass.classid,
					(unsigned long) pool_p->migr->fileclass[ifileclass]->Cnsfileclass.uid,
					(unsigned long) pool_p->migr->fileclass[ifileclass]->Cnsfileclass.gid,
					(unsigned long) (*stcp)->uid,
					(unsigned long) (*stcp)->gid);
			/* We mark it as put_failed */
			(*stcp)->status |= PUT_FAILED;
			if (((*stcp)->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
				/* And not as a migration candidate */
				(*stcp)->status &= ~CAN_BE_MIGR;
			}
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,*stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
			serrno = EACCES;
			rc = -1;
			goto update_migpool_return;
		}
			
	}

	switch (flag) {
	case -1:
		if (((*stcp)->status & CAN_BE_MIGR) != CAN_BE_MIGR) {
			/* This is a not a return from automatic migration */
			(*stcp)->filler[0] = '\0'; /* To be sure */
			return(0);
		}
		if ((*stcp)->filler[0] != 'd') {
			/* This is a return from automatic or explicit migration */
			/* We update global migrator variables */
			if (--pool_p->migr->global_predicates.nbfiles_canbemig < 0) {
				stglogit (func, STG106, func,
						  (*stcp)->poolname,
						  "nbfiles_canbemig < 0 after automatic migration OK (resetted to 0)");
				pool_p->migr->global_predicates.nbfiles_canbemig = 0;
			}
			if (pool_p->migr->global_predicates.space_canbemig < (*stcp)->actual_size) {
				stglogit (func, STG106,
						  func,
						  (*stcp)->poolname,
						  "space_canbemig < (*stcp)->actual_size after automatic migration OK (resetted to 0)");
				pool_p->migr->global_predicates.space_canbemig = 0;
			} else {
				pool_p->migr->global_predicates.space_canbemig -= (*stcp)->actual_size;
			}
			if (((*stcp)->status == (STAGEPUT|CAN_BE_MIGR)) || (((*stcp)->status & BEING_MIGR) == BEING_MIGR)) {
				if (--pool_p->migr->global_predicates.nbfiles_beingmig < 0) {
					stglogit (func, STG106,
							  func,
							  (*stcp)->poolname,
							  "nbfiles_beingmig < 0 after automatic migration OK (resetted to 0)");
					pool_p->migr->global_predicates.nbfiles_beingmig = 0;
				}
				if (pool_p->migr->global_predicates.space_beingmig < (*stcp)->actual_size) {
					stglogit (func, STG106,
							  func,
							  (*stcp)->poolname,
							  "space_beingmig < (*stcp)->actual_size after automatic migration OK (resetted to 0)");
					pool_p->migr->global_predicates.space_beingmig = 0;
				} else {
					pool_p->migr->global_predicates.space_beingmig -= (*stcp)->actual_size;
				}
			}
			/* We update fileclass_vs_migrator variables */
			if (--pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig < 0) {
				stglogit (func, STG110,
						  func,
						  (*stcp)->poolname,
						  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
						  pool_p->migr->fileclass[ifileclass]->server,
						  "nbfiles_canbemig < 0 after automatic migration OK (resetted to 0)");
				pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig = 0;
			}
			if (pool_p->migr->fileclass_predicates[ifileclass].space_canbemig < (*stcp)->actual_size) {
				stglogit (func, STG110,
						  func,
						  (*stcp)->poolname,
						  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
						  pool_p->migr->fileclass[ifileclass]->server,
						  "space_canbemig < (*stcp)->actual_size after automatic migration OK (resetted to 0)");
				pool_p->migr->fileclass_predicates[ifileclass].space_canbemig = 0;
			} else {
				pool_p->migr->fileclass_predicates[ifileclass].space_canbemig -= (*stcp)->actual_size;
			}
			if (((*stcp)->status == (STAGEPUT|CAN_BE_MIGR)) || (((*stcp)->status & BEING_MIGR) == BEING_MIGR)) {
				if (--pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig < 0) {
					stglogit (func, STG110,
							  func,
							  (*stcp)->poolname,
							  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
							  pool_p->migr->fileclass[ifileclass]->server,
							  "nbfiles_beingmig < 0 after automatic migration OK (resetted to 0)");
					pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig = 0;
				}
				if (pool_p->migr->fileclass_predicates[ifileclass].space_beingmig < (*stcp)->actual_size) {
					stglogit (func, STG110,
							  func,
							  (*stcp)->poolname,
							  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
							  pool_p->migr->fileclass[ifileclass]->server,
							  "space_beingmig < (*stcp)->actual_size after automatic migration OK (resetted to 0)");
					pool_p->migr->fileclass_predicates[ifileclass].space_beingmig = 0;
				} else {
					pool_p->migr->fileclass_predicates[ifileclass].space_beingmig -= (*stcp)->actual_size;
				}
			}
		} else {
			/* This is a deletion of a delayed candidate for migration */
			/* We update global migrator variables */
			if (--pool_p->migr->global_predicates.nbfiles_delaymig < 0) {
				stglogit (func, STG106, func,
						  (*stcp)->poolname,
						  "nbfiles_delaymig < 0 (resetted to 0)");
				pool_p->migr->global_predicates.nbfiles_delaymig = 0;
			}
			if (pool_p->migr->global_predicates.space_delaymig < (*stcp)->actual_size) {
				stglogit (func, STG106,
						  func,
						  (*stcp)->poolname,
						  "space_delaymig < (*stcp)->actual_size (resetted to 0)");
				pool_p->migr->global_predicates.space_delaymig = 0;
			} else {
				pool_p->migr->global_predicates.space_delaymig -= (*stcp)->actual_size;
			}
			/* We update fileclass_vs_migrator variables */
			if (--pool_p->migr->fileclass_predicates[ifileclass].nbfiles_delaymig < 0) {
				stglogit (func, STG110,
						  func,
						  (*stcp)->poolname,
						  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
						  pool_p->migr->fileclass[ifileclass]->server,
						  "nbfiles_delaymig < 0 (resetted to 0)");
				pool_p->migr->fileclass_predicates[ifileclass].nbfiles_delaymig = 0;
			}
			if (pool_p->migr->fileclass_predicates[ifileclass].space_delaymig < (*stcp)->actual_size) {
				stglogit (func, STG110,
						  func,
						  (*stcp)->poolname,
						  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
						  pool_p->migr->fileclass[ifileclass]->server,
						  "space_delaymig < (*stcp)->actual_size (resetted to 0)");
				pool_p->migr->fileclass_predicates[ifileclass].space_delaymig = 0;
			} else {
				pool_p->migr->fileclass_predicates[ifileclass].space_delaymig -= (*stcp)->actual_size;
			}
		}
		(*stcp)->filler[0] = '\0'; /* Not in delayed, not a migration candidate, not being migrated anymore */
		if (((*stcp)->status & BEING_MIGR) == BEING_MIGR) {
			(*stcp)->status &= ~BEING_MIGR;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,*stcp) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
		}
		break;
	case 1:
		/* This is to add an entry for the next automatic migration */

		if ((*stcp)->actual_size <= 0) {
			sendrep(&rpfd, MSG_ERR, STG105, "update_migpool", "(*stcp)->actual_size <= 0");
			/* No point */
			serrno = EINVAL;
			rc = -1;
			break;
		}

		/* If this entry has an empty member (*stcp)->u1.h.tppool this means that this */
		/* is a new entry that we will have to migrate in conformity with its fileclass */
		/* Otherwise this means that this is an entry that is a survivor of a previous */
		/* migration that failed - this can happen in particular at a start of the stgdaemon */
		/* when it loads and check the full catalog before starting processing requests... */

		if ((*stcp)->u1.h.tppool[0] == '\0') {
			struct stgcat_entry *stcp_ok;
			int index_found;

			for (i = 0; i < pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbcopies; i++) {
				/* We decide right now to which tape pool this (*stcp) will go */
				/* We have the list of available tape pools managed by function next_tppool() */
				if (i > 0) {
					/* We need to create a new entry in the catalog for this file */
					/* Because this may generate a realloc we need to restore afterwhile */
					/* the correct original *stcp */
					index_found = (*stcp) - stcs;
					stcp_ok = newreq((int) 'h');
					memcpy(stcp_ok, &savestcp, sizeof(struct stgcat_entry));
					stcp_ok->reqid = nextreqid();
					reqid = savereqid;
					/* We flag this stcp_ok as a candidate for migration */
					stcp_ok->status |= CAN_BE_MIGR;
					strcpy(stcp_ok->u1.h.tppool,next_tppool(pool_p->migr->fileclass[ifileclass]));
					stglogit(func, "STG98 - %s %s (copy No %d) with tape pool %s\n",
							 i > 0 ? "Extended" : "Modified",
							 stcp_ok->u1.h.xfile,
							 i,
							 stcp_ok->u1.h.tppool);
					/* We prepare the 'immediate' switch below because we can do this now and only now */
					if ((immediate >= 1) && (immediate <= 2)) {
						if (stcp_ok->status != (STAGEPUT|CAN_BE_MIGR)) stcp_ok->status |= BEING_MIGR;
					}
					incr_migpool_counters(stcp_ok,pool_p,immediate,ifileclass,thistime);
#ifdef USECDB
					if (stgdb_ins_stgcat(&dbfd,stcp_ok) != 0) {
						stglogit(func, STG100, "insert", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					/* We restore original stcp */
					*stcp = stcs + index_found;
					savereqs();
				} else {
					/* We flag this (*stcp) as a candidate for migration */
					(*stcp)->status |= CAN_BE_MIGR;
					strcpy((*stcp)->u1.h.tppool,next_tppool(pool_p->migr->fileclass[ifileclass]));
					stglogit(func, "STG98 - %s %s (copy No %d) with tape pool %s\n",
							 i > 0 ? "Extended" : "Modified",
							 (*stcp)->u1.h.xfile,
							 i,
							 (*stcp)->u1.h.tppool);
					/* We prepare the 'immediate' switch below because we can do this now and only now */
					if ((immediate >= 1) && (immediate <= 2)) {
						if ((*stcp)->status != (STAGEPUT|CAN_BE_MIGR)) (*stcp)->status |= BEING_MIGR;
					}
					incr_migpool_counters(*stcp,pool_p,immediate,ifileclass,thistime);
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,(*stcp)) != 0) {
						stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					savereqs();
				}
			}
			ngen = pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbcopies;
		} else {
			/*
			  stglogit(func, STG120,
			  (*stcp)->u1.h.xfile,
			  (*stcp)->u1.h.tppool,
			  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
			  pool_p->migr->fileclass[ifileclass]->server,
			  pool_p->migr->fileclass[ifileclass]->Cnsfileclass.classid);
			*/
			/* We flag this (*stcp) as a candidate for migration */
			(*stcp)->status |= CAN_BE_MIGR;
			/* We prepare the 'immediate' switch below because we can do this now and only now */
			if ((immediate >= 1) && (immediate <= 2)) {
				if ((*stcp)->status != (STAGEPUT|CAN_BE_MIGR)) (*stcp)->status |= BEING_MIGR;
			}
			incr_migpool_counters(*stcp,pool_p,immediate,ifileclass,thistime);
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,(*stcp)) != 0) {
				stglogit(func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
			ngen = 1;
		}
		break;
	default:
		sendrep(&rpfd, MSG_ERR, STG105, "update_migpool", "flag != 1 && flag != -1");
		serrno = EINVAL;
		rc = -1;
		break;
	}
  update_migpool_return:
	if (rc != 0) {
		/* Oups... Error... We add PUT_FAILED to the the eventual CAN_BE_MIGR state */
		if (((*stcp)->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
			(*stcp)->status |= PUT_FAILED;
#ifdef USECDB
			if (stgdb_upd_stgcat(&dbfd,*stcp) != 0) {
				stglogit (func, STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
			}
#endif
			savereqs();
		}
	}

	return(rc);
}

/* Returns: 0 (OK) or -1 (NOT OK) */
int insert_in_migpool(stcp)
	struct stgcat_entry *stcp;
{
	int i, ifileclass, ipool;
	struct pool *pool_p;
	char *func = "insert_in_migpool";
	time_t thistime = time(NULL);         /* To have same time baseline for all of this routine */
	int thismintime_beforemigr;

	/* We check that this poolname exist */
	ipool = -1;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp(pool_p->name,stcp->poolname) == 0) {
			ipool = i;
			break;
		}
	}
	if (ipool < 0) {
		stglogit(func, STG32, stcp->poolname); /* Not in the list of pools */
		serrno = EINVAL;
		return(-1);
	}
	/* We check that this pool have a migrator */
	if (pool_p->migr == NULL) {
		stglogit(func, STG33, stcp->ipath, "New configuration makes it orphan of migrator");
		return(0);
	}
	if ((ifileclass = upd_fileclass(pool_p,stcp,0,0,0)) < 0) {
		/* If the file does not exist anymore it will be ignored when executing migration stream */
		/* e.g. once for all in the life of the stager daemon (unless stagechng on it). This is */
		/* better than calling upd_fileclass() here with a flag that is forcing a call to */
		/* Cns_statx() : this would always fail, doing always the same result, until migration */
		/* stream is happening */
		return(-1);
	}

	if ((thismintime_beforemigr = stcp->u1.h.mintime_beforemigr) < 0) {
		thismintime_beforemigr = mintime_beforemigr_vs_pool(pool_p,ifileclass);
	}

	/* Please note that stcp->a_time + thismintime_beforemigr can go out of bounds */
	/* That's why I typecast everything to u_signed64 */
	if ((u_signed64) ((u_signed64) stcp->a_time + (u_signed64) thismintime_beforemigr) > (u_signed64) thistime) {
		pool_p->migr->global_predicates.nbfiles_delaymig++;
		pool_p->migr->global_predicates.space_delaymig += stcp->actual_size;
		/* Makes sure it is flagged as delayed */
		stcp->filler[0] = 'd';
	} else {
		pool_p->migr->global_predicates.nbfiles_canbemig++;
		pool_p->migr->global_predicates.space_canbemig += stcp->actual_size;
		/* Makes sure it is flagged as non-delayed */
		stcp->filler[0] = 'm';
	}
	if ((stcp->status == (STAGEPUT|CAN_BE_MIGR)) || ((stcp->status & BEING_MIGR) == BEING_MIGR) || ((stcp->status & WAITING_MIGR) == WAITING_MIGR)) {
		pool_p->migr->global_predicates.nbfiles_beingmig++;
		pool_p->migr->global_predicates.space_beingmig += stcp->actual_size;
	}
	if ((ifileclass = upd_fileclass(pool_p,stcp,0,0,0)) >= 0) {
		if ((u_signed64) ((u_signed64) stcp->a_time + (u_signed64) thismintime_beforemigr) > (u_signed64) thistime) {
			pool_p->migr->fileclass_predicates[ifileclass].nbfiles_delaymig++;
			pool_p->migr->fileclass_predicates[ifileclass].space_delaymig += stcp->actual_size;
		} else {
			pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig++;
			pool_p->migr->fileclass_predicates[ifileclass].space_canbemig += stcp->actual_size;
		}
		if ((stcp->status == (STAGEPUT|CAN_BE_MIGR)) || ((stcp->status & BEING_MIGR) == BEING_MIGR) || ((stcp->status & WAITING_MIGR) == WAITING_MIGR)) {
			pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig++;
			pool_p->migr->fileclass_predicates[ifileclass].space_beingmig += stcp->actual_size;
		}
	}
	return(0);
}

void checkfile2mig()
{
	int i, j;
	struct pool *pool_p;

	/* The migration will be started if one of the 4 following criteria is met:
	   - space occupied by files in state CAN_BE_MIGR execeeds a given value
	   - amount of free space in the disk pool is below a given threshold
	   - time elapsed since last migration is bigger than a given value.

	   The number of parallel migration streams is set for each pool
	*/

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		int global_or;

		global_or = 0;
		if (pool_p->migr == NULL)	/* migration not wanted in this pool */
			continue;
		if (pool_p->migr->mig_pid != 0)	/* migration already running */
			continue;

		if ((pool_p->migr->global_predicates.nbfiles_canbemig - pool_p->migr->global_predicates.nbfiles_beingmig) <= 0)	/* No point anyway */
			continue;

		/* (Note that because of explicit migration there can be files in BEING_MIGR without going through this routine) */

		/* We check global predicates that are the sum of every fileclasses predicates */
		if (((pool_p->mig_data_thresh > 0) && ((pool_p->migr->global_predicates.space_canbemig - pool_p->migr->global_predicates.space_beingmig) >= pool_p->mig_data_thresh)) ||
			((pool_p->mig_start_thresh > 0) && ((pool_p->free * 100) <= (pool_p->capacity * pool_p->mig_start_thresh)))) {
			global_or = 1;
		}
		if (global_or != 0) {
			char tmpbuf1[21];
			char tmpbuf2[21];
			char tmpbuf3[21];
			char tmpbuf4[21];
			char tmpbuf5[21];
			int save_reqid = reqid;

			reqid = 0;
			stglogit("checkfile2mig", "STG98 - Migrator %s@%s - Global Predicate returns true:\n",
					 pool_p->migr->name,
					 pool_p->name);
			stglogit("checkfile2mig", "STG98 - 1) (%s) (space_canbemig=%s - space_beingmig=%s)=%s > data_mig_threshold=%s ?\n",
					 pool_p->mig_data_thresh > 0 ? "ON" : "OFF",
					 u64tostru(pool_p->migr->global_predicates.space_canbemig, tmpbuf1, 0),
					 u64tostru(pool_p->migr->global_predicates.space_beingmig, tmpbuf2, 0),
					 u64tostru(pool_p->migr->global_predicates.space_canbemig - pool_p->migr->global_predicates.space_beingmig, tmpbuf4, 0),
					 u64tostru(pool_p->mig_data_thresh, tmpbuf5, 0)
				);
			stglogit("checkfile2mig", "STG98 - 2) (%s) free=%s < (capacity=%s * mig_start_thresh=%d%%)=%s ?\n",
					 pool_p->mig_start_thresh > 0 ? "ON" : "OFF",
					 u64tostru(pool_p->free, tmpbuf1, 0),
					 u64tostru(pool_p->capacity, tmpbuf2, 0),
					 pool_p->mig_start_thresh,
					 u64tostru((pool_p->capacity * pool_p->mig_start_thresh) /
							   ((u_signed64) 100), tmpbuf3, 0)
				);
			reqid = save_reqid;
		}

		if (global_or == 0) {
			/* Global predicate is not enough - we go through each fileclass predicate */
      
			for (j = 0; j < pool_p->migr->nfileclass; j++) {
				if ((pool_p->migr->fileclass_predicates[j].nbfiles_canbemig - pool_p->migr->fileclass_predicates[j].nbfiles_beingmig) <= 0)	/* No point anyway */
					continue;
				/* Preventive action in case of timing problem : the last startup time of a migration stream could not */
				/* be higher than current time */
				if (pool_p->migr->migreqtime_last_start > time(NULL)) {
					stglogit("checkfile2mig", "### pool_p->migr->migreqtime_last_start > time(NULL) - Reset to zero\n");
					pool_p->migr->migreqtime_last_start = 0;
				}
				if (((pool_p->mig_data_thresh > 0) && ((pool_p->migr->fileclass_predicates[j].space_canbemig - pool_p->migr->fileclass_predicates[j].space_beingmig) >= pool_p->mig_data_thresh)) ||
					(((time(NULL) - pool_p->migr->migreqtime_last_start) >= pool_p->migr->fileclass[j]->Cnsfileclass.migr_time_interval))) {
					global_or = 1;
				}
				if (global_or != 0) {
					char tmpbuf1[21];
					char tmpbuf2[21];
					char tmpbuf3[21];
					char tmpbuf4[21];
					char tmpbuf5[21];
					int save_reqid = reqid;

					reqid = 0;
					stglogit("checkfile2mig", "STG98 - Migrator %s@%s - Fileclass %s@%s (classid %d) - %d cop%s - Predicates returns true:\n",
							 pool_p->migr->name,
							 pool_p->name,
							 pool_p->migr->fileclass[j]->Cnsfileclass.name,
							 pool_p->migr->fileclass[j]->server,
							 pool_p->migr->fileclass[j]->Cnsfileclass.classid,
							 pool_p->migr->fileclass[j]->Cnsfileclass.nbcopies,
							 pool_p->migr->fileclass[j]->Cnsfileclass.nbcopies > 1 ? "ies" : "y");
					stglogit("checkfile2mig", "STG98 - 1) (%s) (space_canbemig=%s - space_beingmig=%s)=%s > data_mig_threshold=%s ?\n",
							 pool_p->mig_data_thresh > 0 ? "ON" : "OFF",
							 u64tostru(pool_p->migr->fileclass_predicates[j].space_canbemig, tmpbuf1, 0),
							 u64tostru(pool_p->migr->fileclass_predicates[j].space_beingmig, tmpbuf2, 0),
							 u64tostru(pool_p->migr->fileclass_predicates[j].space_canbemig - pool_p->migr->fileclass_predicates[j].space_beingmig, tmpbuf4, 0),
							 u64tostru(pool_p->mig_data_thresh, tmpbuf5, 0)
						);
					stglogit("checkfile2mig", "STG98 - 2) (%s) last migrator started %d seconds ago > %d seconds ?\n",
							 "ON",
							 (int) (time(NULL) - pool_p->migr->migreqtime_last_start),
							 pool_p->migr->fileclass[j]->Cnsfileclass.migr_time_interval);
					reqid = save_reqid;
					break;
				}
			}
		}
		if (global_or != 0) {
			migrate_files(pool_p);
			return;
		}
	}
}


int migrate_files(pool_p)
	struct pool *pool_p;
{
	char func[16];
	struct stgcat_entry *stcp;
	int pid, ifileclass;
	int save_reqid = reqid;

	strcpy (func, "migrate_files");
	pool_p->migr->mig_pid = fork ();
	pid = pool_p->migr->mig_pid;

	if (pid < 0) {
		reqid = 0;
		stglogit (func, STG02, "", "fork", strerror(errno));
		/* We do not retry here because we would have to sleep and stop service until sleep is over */
		pool_p->migr->mig_pid = 0;
		reqid = save_reqid;
		return (SESYSERR);
	} else if (pid == 0) {  /* we are in the child */
		/* @@@@ EXCEPTIONNAL @@@@ */
		/* exit(2); */
		/* @@@@ END OF EXCEPTIONNAL @@@@ */
		rfio_mstat_reset();  /* Reset permanent RFIO stat connections */
		rfio_munlink_reset(); /* Reset permanent RFIO unlink connections */
		strcpy(migpoolfiles_migrname,pool_p->migr->name); /* To have migrator name in mig_log file */
		exit(rc_castor2shift(migpoolfiles(pool_p)));
	} else {  /* we are in the parent */
		struct pool *pool_n;
		int okpoolname;
		int ipoolname;
		int j;
		char tmpbuf[21];

		reqid = 0;
		/* For logging purpose we reset these variables, used only in this routine */
		pool_p->migr->global_predicates.nbfiles_to_mig = 0;
		pool_p->migr->global_predicates.space_to_mig = 0;
		for (j = 0; j < pool_p->migr->nfileclass; j++) {
			pool_p->migr->fileclass_predicates[j].nbfiles_to_mig = 0;
			pool_p->migr->fileclass_predicates[j].space_to_mig = 0;
		}

		/* We remember all the entries that will be treated in this migration */
		for (stcp = stcs; stcp < stce; stcp++) {
			if (stcp->reqid == 0) break;
			if (! ISCASTORCANBEMIG(stcp)) continue;       /* Not a candidate for migration */
			if (ISCASTORWAITINGMIG(stcp)) continue;       /* This cannot already have the flag we are going to put it to */
			if (ISCASTORBEINGMIG(stcp)) continue;         /* Cannot have this status at this step - this is too early */
			if (stcp->filler[0] == 'd') continue;         /* This is a delayed candidate */
			okpoolname = 0;
			/* Does it belong to a pool managed by this migrator ? */
			for (ipoolname = 0, pool_n = pools; ipoolname < nbpool; ipoolname++, pool_n++) {
				/* We search the pool structure for this stcp */
				if (strcmp(stcp->poolname,pool_n->name) != 0) continue;
				/* And we check if it has the same migrator pointer */
				if (pool_n->migr != pool_p->migr) continue;
				/* We found it : this stcp is a candidate for migration */
				okpoolname = 1;
				break;
			}
			if (okpoolname == 0) continue;    /* This stcp is not a candidate for migration */
			if ((ifileclass = upd_fileclass(pool_p,stcp,0,0,0)) < 0) continue;
			/* We update the fileclass within this migrator watch variables */
			pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig++;
			pool_p->migr->fileclass_predicates[ifileclass].nbfiles_to_mig++;
			pool_p->migr->fileclass_predicates[ifileclass].space_beingmig += stcp->actual_size;
			pool_p->migr->fileclass_predicates[ifileclass].space_to_mig += stcp->actual_size;
			/* We flag this stcp a WAITING_MIGR (before being in STAGEOUT|CAN_BE_MIGR|BEING_MIGR) */
			stcp->status |= WAITING_MIGR;
			/* We update the migrator global watch variables */
			pool_p->migr->global_predicates.nbfiles_beingmig++;
			pool_p->migr->global_predicates.nbfiles_to_mig++;
			pool_p->migr->global_predicates.space_beingmig += stcp->actual_size;
			pool_p->migr->global_predicates.space_to_mig += stcp->actual_size;
		}
      
		stglogit (func, "execing migrator %s@%s for %d HSM files (total of %s), pid=%d\n",
				  pool_p->migr->name,
				  pool_p->name,
				  pool_p->migr->global_predicates.nbfiles_to_mig,
				  u64tostru(pool_p->migr->global_predicates.space_to_mig, tmpbuf, 0),
				  pid);

		for (j = 0; j < pool_p->migr->nfileclass; j++) {
			if (pool_p->migr->fileclass_predicates[j].nbfiles_to_mig <= 0) continue;
			/* We log how many files per fileclass are concerned for this migration */
			stglogit (func, "detail> migrator %s@%s, Fileclass %s@%s (classid %d) : %d HSM files (total of %s)\n",
					  pool_p->migr->name,
					  pool_p->name,
					  pool_p->migr->fileclass[j]->Cnsfileclass.name,
					  pool_p->migr->fileclass[j]->server,
					  pool_p->migr->fileclass[j]->Cnsfileclass.classid,
					  pool_p->migr->fileclass_predicates[j].nbfiles_to_mig,
					  u64tostru(pool_p->migr->fileclass_predicates[j].space_to_mig, tmpbuf, 0));
		}
		/* We keep track of last fork time with the associated pid */
		pool_p->migr->migreqtime = pool_p->migr->migreqtime_last_start = time(NULL);
		if ((pool_p->migr->global_predicates.nbfiles_beingmig == 0) ||
			(pool_p->migr->global_predicates.space_beingmig == 0)) {
			stglogit (func, "### Executing recovery procedure - migrator should not have been executed\n");
			redomigpool();
		}
	}
	reqid = save_reqid;
	return (0);
}

int migpoolfiles(pool_p)
	struct pool *pool_p;
{
	/* We use the weight algorithm defined by Fabrizio Cane for DPM */

	int c;
	struct sorted_ent *prev, *scc, *sci, *scf, *scs, *scc_found = NULL;
	struct stgcat_entry *stcp;
	struct stgpath_entry *stpp = NULL;
	int found_nbfiles = 0;
	int found_nbfiles_max = 0;
	char func[16];
	int okpoolname;
	int ipoolname;
	int term_status;
	pid_t child_pid;
	int i, j, k, l, nfiles_per_tppool;
	struct pool *pool_n;
	int found_not_scanned = -1;
	int fork_pid;
	extern struct passwd start_passwd;         /* Generic uid/gid at startup (admin) */
	extern struct passwd stage_passwd;             /* Generic uid/gid stage:st */
	char *minsize_per_stream;
	u_signed64 minsize;
	u_signed64 defminsize = (u_signed64) ((u_signed64) 2 * (u_signed64) ONE_GB);
	struct files_per_stream *tppool_vs_stcp = NULL;
	int ntppool_vs_stcp = 0;
	char tmpbuf[21];
	char remember_tppool[CA_MAXPOOLNAMELEN+1];
	u_signed64 ideal_minsize;
	int nideal_minsize;
	int itppool, found_tppool;
	int npoolname_out;
	int original_nb_of_stream;
	int nstreams_to_fork;
	int nstreams_forked;

	/* We get the minimum size to be transfered */
	minsize = defminsize;
	if ((minsize_per_stream = getconfent ("STG", "MINSIZE_PER_STREAM", 0)) != NULL) {
		int checkrc;
		minsize = defminsize;
		if ((checkrc = stage_util_check_for_strutou64(minsize_per_stream)) >= 0) {
			if ((minsize = strutou64(minsize_per_stream)) <= 0) {
				minsize = defminsize;
			} else {
				if (checkrc == 0) minsize *= ONE_MB; /* Not unit : default MB */
			}
		}
	}


	strcpy (func, "migpoolfiles");
	/* We reset the reqid (we are in a child) */
	reqid = 0;

	stglogit(func, STG33, "minsize per stream current value", u64tostr((u_signed64) minsize, tmpbuf, 0));

#ifdef STAGER_DEBUG
	stglogit(func, "### Please gdb /usr/local/bin/stgdaemon %d, then break %d\n",getpid(),__LINE__+2);
	sleep(9);
	sleep(1);
#endif

	if (stage_setlog((void (*) _PROTO((int, char *))) &migpoolfiles_log_callback) != 0) {
		stglogit(func, "### stage_setlog error (%s)\n",sstrerror(serrno));
		return(SESYSERR);
	}

#ifndef __INSURE__
	/* We do not want any connection but the one to the stgdaemon */
	for (c = 0; c < maxfds; c++)
		close (c);
#endif

#ifndef _WIN32
	sa_poolmgr.sa_handler = poolmgr_wait4child;
	sa_poolmgr.sa_flags = SA_RESTART;
	sigaction (SIGCHLD, &sa_poolmgr, NULL);
#endif

	/* Protect ourself agains something impossible unless a problem in the counters */
	if ((pool_p == NULL) || (pool_p->migr == NULL)) {
		stglogit(func, "### (pool_p == NULL) || (pool_p->migr == NULL)\n");
		return(SESYSERR);
	}
	if ((found_nbfiles_max = (pool_p->migr->global_predicates.nbfiles_canbemig - pool_p->migr->global_predicates.nbfiles_beingmig)) <= 0) {
		stglogit(func, "### pool_p->migr->global_predicates.nbfiles_canbemig - pool_p->migr->global_predicates.nbfiles_beingmig = %d - %d = %d\n",
				 pool_p->migr->global_predicates.nbfiles_canbemig,
				 pool_p->migr->global_predicates.nbfiles_beingmig,
				 pool_p->migr->global_predicates.nbfiles_canbemig - pool_p->migr->global_predicates.nbfiles_beingmig);
		return(SESYSERR);
	}

	/* build a sorted list of stage catalog entries for the specified pool */
	scf = NULL;
	if ((scs = (struct sorted_ent *) calloc (found_nbfiles_max, sizeof(struct sorted_ent))) == NULL) {
		stglogit(func, "### calloc error (%s)\n",strerror(errno));
		return(SESYSERR);
	}

	for (i = 0; i < pool_p->migr->nfileclass; i++) {
		pool_p->migr->fileclass[i]->streams = 0;
		pool_p->migr->fileclass[i]->orig_streams = 0;
	}

	sci = scs;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if (! ISCASTORCANBEMIG(stcp)) continue;       /* Not a being migrated candidate */
		if (ISCASTORWAITINGMIG(stcp)) continue;       /* Cannot have this because it is setted only after the fork */
		if (ISCASTORBEINGMIG(stcp)) continue;         /* Cannot have this status at this step - this is too early */
		if (stcp->filler[0] == 'd') continue;         /* This is a delayed candidate */
		okpoolname = 0;
		/* Does it belong to a pool managed by this migrator ? */
		for (ipoolname = 0, pool_n = pools; ipoolname < nbpool; ipoolname++, pool_n++) {
			/* We search the pool structure for this stcp */
			if (strcmp(stcp->poolname,pool_n->name) != 0) continue;
			/* And we check if it has the same migrator pointer */
			if (pool_n->migr != pool_p->migr) continue;
			/* We found it : this stcp is a candidate for migration */
			okpoolname = 1;
			break;
		}
		if (okpoolname == 0) continue;    /* This stcp is not one of our candidates for migration */
		if (stcp->u1.h.tppool[0] == '\0') {
			/* This should never be */
			stglogit(func, STG115, stcp->reqid, stcp->u1.h.xfile);
			free(scs);
			return(SEINTERNAL);
		}
		sci->weight = (double)stcp->a_time;
		if (stcp->actual_size > ONE_KB)
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
		sci->stcp = stcp;
		sci++;
		if (++found_nbfiles >= found_nbfiles_max) {
			/* We should have find already all the candidates - no need to go further */
			/* And if we would ever have find other candidates further we cannot */
			/* treat them otherwise this would cause a memory corruption */
			break;
		}
	}

	if (found_nbfiles == 0) {
		stglogit(func, "### Internal error - Found 0 files to migrate !??\n");
		free(scs);
		return(0);
	}

	/* All entries that are to be migrated have a not-empty tape pool in member  */
	/* stcp->u1.h.tppool.                                                        */

	/* Concerning fileclasses uid/gid filters :                                  */
	/* If (uid/gid) filter is NOT set : migration is done under stage:st account */
	/* If (   /gid) filter is     set : migration is done under the first stcp   */
	/* If (uid/gid) filter is     set : migration is done under this account     */
	/* If (uid/   ) filter is     set : migration is done under this account     */

	/* We reset the internal counter on streams per fileclass */
	/* Note that we are already in a forked process, so this cannot clash with another */

	/* We group the stcp entries per tppool */
	while (1) {
		/* We search at least one entry not yet scanned */
		found_not_scanned = 0;
		for (scc = scf; scc; scc = scc->next) {
			if (scc->scanned == 0) {
				found_not_scanned = 1;
				scc_found = scc;
				break;
			}
		}
		if (found_not_scanned == 0) {
			break;
		}
		/* We want to know all migration requests before we execute them */

		/* Is it a known poolname ? */
		found_tppool = 0;
		for (itppool = 0; itppool < ntppool_vs_stcp; itppool++) {
			if (strcmp(scc_found->stcp->u1.h.tppool, tppool_vs_stcp[itppool].tppool) == 0) {
				found_tppool = 1;
				break;
			}
		}
		if (found_tppool == 0) {
			if (ntppool_vs_stcp == 0) {
				if ((tppool_vs_stcp = (struct files_per_stream *) malloc(sizeof(struct files_per_stream))) == NULL) {
					stglogit(func, "### malloc error (%s)\n",strerror(errno));
					free(scs);
					return(SESYSERR);
				}
			} else {
				struct files_per_stream *dummy;
        
				if ((dummy = (struct files_per_stream *) realloc(tppool_vs_stcp,(ntppool_vs_stcp + 1) * sizeof(struct files_per_stream))) == NULL) {
					stglogit(func, "### realloc error (%s)\n",strerror(errno));
					free(scs);
					for (i = 0; i < ntppool_vs_stcp; i++) {
						if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
						if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
					}
					free(tppool_vs_stcp);
					return(SESYSERR);
				}
				tppool_vs_stcp = dummy;
			}
			found_tppool = ntppool_vs_stcp++;
			tppool_vs_stcp[ntppool_vs_stcp - 1].stcp = NULL;
			tppool_vs_stcp[ntppool_vs_stcp - 1].stpp = NULL;
			tppool_vs_stcp[ntppool_vs_stcp - 1].nstcp = 0;
			tppool_vs_stcp[ntppool_vs_stcp - 1].size = 0;       /* In BYTES - not MBytes */
			tppool_vs_stcp[ntppool_vs_stcp - 1].euid = 0;
			tppool_vs_stcp[ntppool_vs_stcp - 1].egid = 0;
			tppool_vs_stcp[ntppool_vs_stcp - 1].nb_substreams = 0;
			strcpy(tppool_vs_stcp[ntppool_vs_stcp - 1].tppool,scc_found->stcp->u1.h.tppool);
		}

		/* We grab the most restrictive (euid/egid) pair based on our known fileclasses */
		if (euid_egid(&(tppool_vs_stcp[found_tppool].euid),
					  &(tppool_vs_stcp[found_tppool].egid),
					  scc_found->stcp->u1.h.tppool,
					  pool_p->migr, scc_found->stcp, NULL, NULL, 1, 1) != 0) {
			free(scs);
			for (i = 0; i < ntppool_vs_stcp; i++) {
				if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
				if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
			}
			free(tppool_vs_stcp);
			return(SESYSERR);
		}
		if (tppool_vs_stcp[found_tppool].egid == 0) {      /* No gid filter */
			tppool_vs_stcp[found_tppool].egid = stage_passwd.pw_gid; /* Put default gid */
			if (tppool_vs_stcp[found_tppool].euid == 0) {   /* No uid filter */
				tppool_vs_stcp[found_tppool].euid = stage_passwd.pw_uid; /* Put default uid */
			}
		} else {
			if (tppool_vs_stcp[found_tppool].euid == 0) {   /* No uid filter */
				tppool_vs_stcp[found_tppool].euid = scc_found->stcp->uid; /* Put current uid */
			}
		}
		verif_euid_egid(tppool_vs_stcp[found_tppool].euid,tppool_vs_stcp[found_tppool].egid, NULL, NULL);

		/* We search all other entries sharing the same tape pool */
		j = -1;
		nfiles_per_tppool = 0;
		/* We reset internal flags to not do double count */
		for (i = 0; i < pool_p->migr->nfileclass; i++) {
			pool_p->migr->fileclass[i]->flag = 0;
		}
		for (scc = scf; scc; scc = scc->next) {
			if (scc->scanned != 0) continue;
			if ((scc == scc_found) || (strcmp(scc->stcp->u1.h.tppool,scc_found->stcp->u1.h.tppool) == 0)) {
				int ifileclass;

				/* And we count how of them we got */
				++nfiles_per_tppool;
				if ((ifileclass = upd_fileclass(pool_p,scc->stcp,0,1,1)) >= 0) {
					if (pool_p->migr->fileclass[ifileclass]->flag == 0) {
						/* This fileclass [ifileclass] was not yet counted for this tape pool [scc_found->stcp->u1.h.tpool] */
						pool_p->migr->fileclass[ifileclass]->streams++;
						pool_p->migr->fileclass[ifileclass]->orig_streams++;
						pool_p->migr->fileclass[ifileclass]->flag = 1;
					}
				}
			}
		}
		/* We group them into a single entity for API compliance */
		if (((stcp = tppool_vs_stcp[found_tppool].stcp = (struct stgcat_entry *) calloc(nfiles_per_tppool,sizeof(struct stgcat_entry))) == NULL) ||
			((stpp = tppool_vs_stcp[found_tppool].stpp = (struct stgpath_entry *) calloc(nfiles_per_tppool,sizeof(struct stgpath_entry))) == NULL)) {
			stglogit(func, "### calloc error (%s)\n",strerror(errno));
			free(scs);
			for (i = 0; i < ntppool_vs_stcp; i++) {
				if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
				if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
			}
			free(tppool_vs_stcp);
			return(SESYSERR);
		}
		tppool_vs_stcp[found_tppool].nstcp = nfiles_per_tppool;
		/* We store them in this area */
		j = -1;
		for (scc = scf; scc; scc = scc->next) {
			if (scc->scanned != 0) continue;
			if ((scc == scc_found) || (strcmp(scc->stcp->u1.h.tppool,scc_found->stcp->u1.h.tppool) == 0)) {
				++j;
				memcpy(stcp+j,scc->stcp,sizeof(struct stgcat_entry));
				/* We makes sure that the -K option is disabled... This means that when this copy will be migrated */
				/* The corresponding STAGEWRT entry in the catalog will disappear magically !... */
				(stcp+j)->keep = 0;
				/* We also makes sure that the -s parameter is disabled as well */
				(stcp+j)->size = 0;
				/* We remove the poolname argument that we will sent via stage_wrt_hsm */
				(stcp+j)->poolname[0] = '\0';
				/* We remove other arguments - not needed - will be logged for nothing */
				/* (stcp+j)->u1.h.fileclass = 0; */
				(stcp+j)->u1.h.retenp_on_disk = -1;
				(stcp+j)->u1.h.mintime_beforemigr = -1;
				/* Please note that the (stcp+j)->ipath will be our user path (stpp) */
				strcpy((stpp+j)->upath, (stcp+j)->ipath);
				scc->scanned = 1;
				tppool_vs_stcp[found_tppool].size += scc->stcp->actual_size;
			}
		}
		/* Next round */
	}

	/* Here we have all the ntppool_vs_stcp streams to launch, each of them composed of */
	/* tppool_vs_stcp[0..(ntppool_vs_stcp - 1)].nstcp entries */

	/* We check if the size of the migration does fit the minimum size per stream - default to 2GB */
	if (minsize > 0) {
		for (i = 0; i < ntppool_vs_stcp; i++) {
			if (tppool_vs_stcp[i].size < minsize) {
				char tmpbuf1[21];
				char tmpbuf2[21];
        
				/* One of the stream is of less size than the minimum - we give up */
				stglogit(func, "### Warning - stream on tape pool %s have size to be migrated %s < %s\n",
						 tppool_vs_stcp[i].stcp[0].u1.h.tppool,
						 u64tostr(tppool_vs_stcp[i].size, tmpbuf1, 0),
						 u64tostr(minsize, tmpbuf2, 0));
			}
		}
	}

	/* No need to waste memory inherited by parent anymore */
	if (stcs != NULL) free(stcs); stcs = NULL;
	if (stps != NULL) free(stps); stps = NULL;
  
	/* At first found we do not want to remember about previous tape pool because, per definition, */
	/* there is exactly one stream per tape pool */
	remember_tppool[0] = '\0';
	{
		/* We count the number of stageout pools */
		char savdefpoolname_out[10*(CA_MAXPOOLNAMELEN + 1)];
		char *p;
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
		char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

		strcpy(savdefpoolname_out, defpoolname_out);
		npoolname_out = 0;
		if ((p = strtok(defpoolname_out, ":")) != NULL) {
			while (1) {
				npoolname_out++;
				if ((p = strtok(NULL, ":")) == NULL) break;
			}
		}
		strcpy(defpoolname_out, savdefpoolname_out);
	}

	if (npoolname_out <= 0) {
		stglogit(func, "### Warning - npoolname_out <= 0 - Forced to be 1\n");
		npoolname_out = 1;
	}

	/* For all tape pools we determine up to how many streams it can be expanded */
	original_nb_of_stream = ntppool_vs_stcp;
	stglogit(func, "... Original number of streams : %d\n", original_nb_of_stream);
	for (j = 0; j < original_nb_of_stream; j++) {
		int max_nfree_stream;
		int sav_ntppool_vs_stcp;
		int nstcp;
		int found_nideal_minsize;
		int created_new_streams;
#ifdef STAGER_DEBUG
		char tmpbuf1[21];
		char tmpbuf2[21];
#endif

		/* This stream has already been subject to expansion */
		if (tppool_vs_stcp[j].nb_substreams > 0) continue;

		/* We loop on all the fileclasses to determine which ones are concerned by this stream and up to */
		/* how many streams it allows it to be expanded */
		ideal_minsize = 0;
		nideal_minsize = 0;
		nstcp = 0;
		max_nfree_stream = 0;
		for (i = 0; i < pool_p->migr->nfileclass; i++) {
			int nfree_stream;
			int has_fileclass_i;
      
			if (pool_p->migr->fileclass[i]->orig_streams <= 0) continue; /* This fileclass cannot be concerned */

			/* We start with a number of virtual stream equal to the real number of streams */
			pool_p->migr->fileclass[i]->nfree_stream = 0;

			/* We adapt the number of streams available v.s. number of stageout pools and original number of streams for this fileclass */

			/* First the following determines the total number of streams available for this fileclass */
			nfree_stream = pool_p->migr->fileclass[i]->Cnsfileclass.maxdrives / pool_p->migr->fileclass[i]->orig_streams;
			if ((nfree_stream * pool_p->migr->fileclass[i]->orig_streams) != pool_p->migr->fileclass[i]->Cnsfileclass.maxdrives) {
				/* We loose a bit precision when dividing, so the possible ++ below */
				nfree_stream++;
			}

			/* We then take into account the fact that there can be multiple stageout pools */
			if (npoolname_out > 1) {
				/* npoolname > 1 : we loose a bit precision when dividing, so the possible ++ below */
				if ((nfree_stream /= npoolname_out) <= 0) {
					nfree_stream = 1;
				}
			}

#ifdef STAGER_DEBUG
			stglogit(func, "... Stream No %d : Free streams is %d\n", j, nfree_stream);
#endif

			if (nfree_stream <= 1) continue;       /* Cannot create any stream ? */

			/* We check if stream No j references the class No i */
			has_fileclass_i = 0;
			for (k = 0; k < tppool_vs_stcp[j].nstcp; k++) {
				if ((strcmp(tppool_vs_stcp[j].stcp[k].u1.h.server,pool_p->migr->fileclass[i]->server) == 0) &&
					(tppool_vs_stcp[j].stcp[k].u1.h.fileclass == pool_p->migr->fileclass[i]->Cnsfileclass.classid)) {
					has_fileclass_i = 1;
					break;
				}
			}

#ifdef STAGER_DEBUG
			stglogit(func, "... Stream No %d : Do not reference Fileclass No %d\n", j, i);
#endif

			if (has_fileclass_i == 0) continue;        /* No reference to fileclass No i in this stream */

#ifdef STAGER_DEBUG
			stglogit(func, "... Stream No %d : Grand total size of stream is %s > minsize=%s ?\n", j, u64tostru(tppool_vs_stcp[j].size, tmpbuf1, 0), u64tostru(minsize, tmpbuf2, 0));
#endif

			/* We keep in mind the grand total size of this stream and the number of entries in it */
			if (tppool_vs_stcp[j].size > minsize) {
				ideal_minsize += tppool_vs_stcp[j].size;
			} else {
				ideal_minsize += minsize;
			}
			nstcp += tppool_vs_stcp[j].nstcp;

			if (nfree_stream > max_nfree_stream) max_nfree_stream = nfree_stream;

			pool_p->migr->fileclass[i]->nfree_stream = nfree_stream;

		}

#ifdef STAGER_DEBUG
		stglogit(func, "... Stream No %d : ideal_minsize is %s\n", j, u64tostru(ideal_minsize, tmpbuf, 0));
#endif

		if (ideal_minsize <= 0) continue;        /* No expansion for stream No j */

		/* We determine how many streams can be done by estimating the ideal_minsize */
		found_nideal_minsize = 0;
		for (nideal_minsize = 1; nideal_minsize <= max_nfree_stream; nideal_minsize++) {
			if ((ideal_minsize / nideal_minsize) < minsize) {
				found_nideal_minsize = --nideal_minsize;
				break;
			}
		}

		if (nideal_minsize == (max_nfree_stream + 1)) {
			/* We've done the whole for() loop upper */
			--nideal_minsize;
		}

#ifdef STAGER_DEBUG
		stglogit(func, "... Stream No %d : nideal_minsize is %d\n", j, nideal_minsize);
#endif

		if (nideal_minsize <= 1) continue; /* No point to create another stream */

		if ((ideal_minsize /= nideal_minsize) < minsize) {
			ideal_minsize = minsize;
		}

		stglogit(func, "ideal minsize per stream for tape pool %s is %s, split could go up to %d streams\n",
				 tppool_vs_stcp[j].tppool,
				 u64tostr((u_signed64) minsize, tmpbuf, 0),
				 nideal_minsize
			);

		/* We know that we can create up to nideal_minsize streams        */
		/* We create room for them, assuming that memory is not a pb here */
		/* Please note that, by construction here, we have already */
		/* ntppool_vs_stcp streams, the nideal_minsize ones that we are */
		/* going to create are going to the end */
		created_new_streams = 1;
		sav_ntppool_vs_stcp = ntppool_vs_stcp;
		{
			struct files_per_stream *dummy;
        
			if ((dummy = (struct files_per_stream *) realloc(tppool_vs_stcp,(ntppool_vs_stcp + nideal_minsize) * sizeof(struct files_per_stream))) == NULL) {
				stglogit(func, "### realloc error (%s)\n",strerror(errno));
				/* But this is NOT fatal in the sense that we already have working streams - we just log this */
				/* and we do as if we found no need to create another stream... */
				created_new_streams = 0;
			} else {
				tppool_vs_stcp = dummy;
				for (k = 0; k < nideal_minsize; k++) {
					tppool_vs_stcp[sav_ntppool_vs_stcp + k].stcp = NULL;
					tppool_vs_stcp[sav_ntppool_vs_stcp + k].stpp = NULL;
					tppool_vs_stcp[sav_ntppool_vs_stcp + k].tppool[0] = '\0';
					tppool_vs_stcp[sav_ntppool_vs_stcp + k].nstcp = 0;
					tppool_vs_stcp[sav_ntppool_vs_stcp + k].size = 0;
					tppool_vs_stcp[sav_ntppool_vs_stcp + k].euid = tppool_vs_stcp[j].euid;
					tppool_vs_stcp[sav_ntppool_vs_stcp + k].egid = tppool_vs_stcp[j].egid;
					/* We know try to create enough room for our new stream */
					if (((stcp = tppool_vs_stcp[sav_ntppool_vs_stcp + k].stcp = (struct stgcat_entry *) calloc(nstcp,sizeof(struct stgcat_entry))) == NULL) ||
						((stpp = tppool_vs_stcp[sav_ntppool_vs_stcp + k].stpp = (struct stgpath_entry *) calloc(nstcp,sizeof(struct stgpath_entry))) == NULL)) {
						stglogit(func, "### calloc error (%s)\n",strerror(errno));
						/* But this is NOT fatal in the sense that we already have working streams - we just log this */
						if (stcp != NULL) free(stcp);
						if (stpp != NULL) free(stpp);
						/* We left here nideal_minsize streams that will not be used - tant pis */
						created_new_streams = 0;
						break;
					} else {
						ntppool_vs_stcp++;
					}
				}
			}
		}

		if (created_new_streams == 0) continue;       /* Could not create the streams ! */

		/* We fill the streams with respect to fileclass */
		/* We have a sort of conceptual problem here when multiple fileclasses are present in stream No j */
		/* but when they said that they do not have equal number of free streams */

		/* We reset internal indexes */
		for (i = 0; i < pool_p->migr->nfileclass; i++) {
			/* Will range from 0 to (pool_p->migr->fileclass[i]->nfree_stream - 1) */
			pool_p->migr->fileclass[i]->ifree_stream = 0;
			if (pool_p->migr->fileclass[i]->nfree_stream > nideal_minsize) {
				pool_p->migr->fileclass[i]->nfree_stream = nideal_minsize;
			}
		}
		/* l variable will loop in range [sav_ntppool_vs_stcp,ntppool_vs_stcp] */
		for (k = 0; k < tppool_vs_stcp[j].nstcp; k++) {
			struct stgcat_entry *stcp;
			struct stgpath_entry *stpp;
			int ifileclass;

			if ((ifileclass = upd_fileclass(pool_p,&(tppool_vs_stcp[j].stcp[k]),0,1,1)) < 0) {
				stglogit(func, "### cannot determine fileclass of %s\n", tppool_vs_stcp[j].stcp[k].u1.h.xfile);
				/* Should never happen - by convention we put this stcp in first stream */
				l = 0;
			} else {
				if ((l = pool_p->migr->fileclass[ifileclass]->ifree_stream++) > (pool_p->migr->fileclass[ifileclass]->nfree_stream - 1)) {
					l = 0;
					pool_p->migr->fileclass[ifileclass]->ifree_stream = 1; /* So that next round l will be set to 1 */
				}
			}

			if (tppool_vs_stcp[sav_ntppool_vs_stcp + l].size > ideal_minsize) {
				int sav_l = l;
				/* We look if there is another stream that have enough room for us */

				while (1) {
					if ((ifileclass = upd_fileclass(pool_p,&(tppool_vs_stcp[j].stcp[k]),0,1,1)) < 0) {
						stglogit(func, "### cannot determine fileclass of %s\n", tppool_vs_stcp[j].stcp[k].u1.h.xfile);
						/* Should never happen - by convention we put this stcp in first stream */
						l = 0;
					} else {
						if ((l = pool_p->migr->fileclass[ifileclass]->ifree_stream++) > (pool_p->migr->fileclass[ifileclass]->nfree_stream - 1)) {
							l = 0;
							pool_p->migr->fileclass[ifileclass]->ifree_stream = 1; /* So that next round l will be set to 1 */
						}
					}
					if (l == sav_l) break;       /* We made a turnaround */
					if (tppool_vs_stcp[sav_ntppool_vs_stcp + l].size > ideal_minsize) continue; /* Not a good candidate */
				}
			}

			/* We copy entry No k of stream No j into entry No tppool_vs_stcp[sav_ntppool_vs_stcp + l].nstcp */
			/* of pool No l */
			stcp = tppool_vs_stcp[sav_ntppool_vs_stcp + l].stcp;
			stcp += tppool_vs_stcp[sav_ntppool_vs_stcp + l].nstcp;
			stpp = tppool_vs_stcp[sav_ntppool_vs_stcp + l].stpp;
			stpp += tppool_vs_stcp[sav_ntppool_vs_stcp + l].nstcp;
			*stcp = tppool_vs_stcp[j].stcp[k];
			strcpy(stpp->upath,tppool_vs_stcp[j].stpp[k].upath);
			tppool_vs_stcp[sav_ntppool_vs_stcp + l].size += tppool_vs_stcp[j].stcp[k].actual_size;
			tppool_vs_stcp[sav_ntppool_vs_stcp + l].nstcp++;
		}

		/* By definition stream No j have been completely emptied */
		tppool_vs_stcp[j].size = 0;
		tppool_vs_stcp[j].nstcp = 0;

		/* We flag it so */
		tppool_vs_stcp[j].nb_substreams = nideal_minsize;

		/* And we flag also the new streams */
		for (l = sav_ntppool_vs_stcp; l < (sav_ntppool_vs_stcp + nideal_minsize); l++) {
			tppool_vs_stcp[l].nb_substreams = nideal_minsize;
		}
	}


	/* Streams are created - By construction if a stream have nb_substreams > 0 but size == 0, this means */
	/* that it has been expanded */

	{
		int iprint = 0;

		for (j = 0; j < ntppool_vs_stcp; j++) {
			if ((tppool_vs_stcp[j].nb_substreams > 0) && (tppool_vs_stcp[j].size <= 0)) continue;
			stglogit(func, STG135,
					 ++iprint,
					 tppool_vs_stcp[j].nstcp,
					 u64tostr(tppool_vs_stcp[j].size, tmpbuf, 0),
					 tppool_vs_stcp[j].stcp[0].u1.h.tppool);
		}
	}

	/* We count how meany streams we will have to fork */
	nstreams_to_fork = nstreams_forked = 0;
	for (j = 0; j < ntppool_vs_stcp; j++) {
		if ((tppool_vs_stcp[j].nb_substreams > 0) && (tppool_vs_stcp[j].size <= 0)) continue;
		nstreams_to_fork++;
	}

	/* We fork and execute the stagewrt request(s) */
	for (j = 0; j < ntppool_vs_stcp; j++) {
		if ((tppool_vs_stcp[j].nb_substreams > 0) && (tppool_vs_stcp[j].size <= 0)) continue;
	  migpoolfiles_retry_fork:
		if ((fork_pid = fork()) < 0) {
			stglogit(func, "### Cannot fork (%s)\n",strerror(errno));
			if (errno == EAGAIN) {
				stglogit (func, "### errno == EAGAIN : keep on retrying in 60 seconds\n");
				stage_sleep(60);
				goto migpoolfiles_retry_fork;
			}
		} else if (fork_pid == 0) {
			/* We are in the child */
			int rc = 0;
			char *myenv = "STAGE_STGMAGIC=0x13140704"; /* STGMAGIC4 explicit value - Note: size was reset to 0 */
			char tmpbufpid[1024];
			
			/* We try to append pid onto migpoolfiles_migrname */
			sprintf(tmpbufpid,"[%d/%d]",nstreams_forked+1, nstreams_to_fork);
			if ((strlen(migpoolfiles_migrname) + strlen(tmpbufpid)) <= (CA_MAXMIGRNAMELEN+1024)) {
				strcat(migpoolfiles_migrname,tmpbufpid);
			}
			rfio_mstat_reset();  /* Reset permanent RFIO stat connections */
			rfio_munlink_reset(); /* Reset permanent RFIO unlink connections */
			if (putenv(myenv) != 0) {
				stglogit(func, "Cannot putenv(\"%s\"), %s\n", myenv, strerror(errno));
			} else {
				stglogit(func, "Setted environment variable %s\n", myenv);
			}

#ifdef STAGER_DEBUG
			stglogit(func, "### stage_wrt_hsm request : Please gdb /usr/local/bin/stgdaemon %d, then break %d\n",getpid(),__LINE__+2);
			sleep(9);
			sleep(1);
#endif

			free(scs);                                      /* Neither for the sorted entries */
			/* We remove the non-needed ipath members */
			{
				int j2;

				for (j2 = 0; j2 < tppool_vs_stcp[j].nstcp; j2++) {
					tppool_vs_stcp[j].stcp[j2].ipath[0] = '\0';
				}
			}
			setegid(start_passwd.pw_gid);                   /* Move to admin (who knows) */
			seteuid(start_passwd.pw_uid);
			setegid(tppool_vs_stcp[j].egid);                /* Move to requestor (from fileclasses) */
			seteuid(tppool_vs_stcp[j].euid);
			{
				int nb_done_request = 0;
				int nb_per_request = tppool_vs_stcp[j].nstcp;
				u_signed64 estimated_total_reqsize = 
					(u_signed64) tppool_vs_stcp[j].nstcp * (sizeof(struct stgcat_entry) + sizeof(struct stgpath_entry)) +
					/*  And we leave 1000 bytes for other stuff, like header, etc... */
					/* (we overestimate delibirately the header size in the protocol) */
					(u_signed64) 1000;
        
				/* We look if you request will exceed MAX_NETDATA_SIZE ? */
				if (estimated_total_reqsize > (u_signed64) MAX_NETDATA_SIZE) {
					char tmpbuf1[21];
					char tmpbuf2[21];

					int nb_consecutive_stream = ((int) (estimated_total_reqsize / MAX_NETDATA_SIZE)) + 1;
					if (nb_consecutive_stream > nb_per_request) nb_consecutive_stream = nb_per_request;
					setegid(start_passwd.pw_gid);                   /* Move to admin (who knows) */
					seteuid(start_passwd.pw_uid);
					stglogit(func, "### stage_wrt_hsm request socket buffer size (approx %s) predicted to be too big, or at the limit (which is %s)\n",u64tostr((u_signed64) estimated_total_reqsize, tmpbuf1, 0),u64tostr((u_signed64) MAX_NETDATA_SIZE, tmpbuf2, 0));
					stglogit(func, "### stage_wrt_hsm splitted into %d consecutive requests\n", nb_consecutive_stream);
					setegid(tppool_vs_stcp[j].egid);                /* Move to requestor (from fileclasses) */
					seteuid(tppool_vs_stcp[j].euid);
					nb_per_request /= nb_consecutive_stream;
				}
#if ! defined(_WIN32)
				_stage_signal (SIGHUP, stage_stgcleanup);
				_stage_signal (SIGQUIT, stage_stgcleanup);
#endif
				_stage_signal (SIGINT, stage_stgcleanup);
				_stage_signal (SIGTERM, stage_stgcleanup);
				/* We loop on all the consecutive streams */
				while (nb_done_request < tppool_vs_stcp[j].nstcp) {
					int nb_in_this_request;
					int istart_for_this_request;
				  stage_wrt_hsm_retry:

					istart_for_this_request = nb_done_request;
					if ((nb_done_request + nb_per_request) < tppool_vs_stcp[j].nstcp) {
						/* We are doing a partial stream */
						/* from index (nb_done_request+1) to (nb_done_request+nb_per_request) */
						nb_in_this_request = nb_per_request;
					} else {
						nb_in_this_request = tppool_vs_stcp[j].nstcp - nb_done_request;
					}
					if ((rc = stage_wrt_hsm((u_signed64) STAGE_NOHSMCREAT|STAGE_REQID|STAGE_HSM_ENOENT_OK|STAGE_NOLINKCHECK|STAGE_VOLATILE_TPPOOL|STAGE_MIGLOG, /* Flags */
											0,                            /* open flags - disabled */
											localhost,                    /* Hostname */
											NULL,                         /* Pooluser */
											nb_in_this_request,           /* nstcp_input */
											tppool_vs_stcp[j].stcp + istart_for_this_request, /* stcp_input */
											0,                            /* nstcp_output - none wanted */
											NULL,                         /* stcp_output - none wanted */
											nb_in_this_request,           /* nstpp_input */
											tppool_vs_stcp[j].stpp + istart_for_this_request  /* stpp_input */
						)) != 0) {
						setegid(start_passwd.pw_gid);                   /* Move to admin (who knows) */
						seteuid(start_passwd.pw_uid);
						stglogit(func, "### stage_wrt_hsm request error No %d (%s)\n", serrno, sstrerror(serrno));
						setegid(tppool_vs_stcp[j].egid);                /* Move to requestor (from fileclasses) */
						seteuid(tppool_vs_stcp[j].euid);
						if ((serrno == SECOMERR) || (serrno == SECONNDROP)) {
							/* There was a communication error */
							setegid(start_passwd.pw_gid);                   /* Move to admin (who knows) */
							seteuid(start_passwd.pw_uid);
							stglogit(func, "### retrying in 1 second\n");
							setegid(tppool_vs_stcp[j].egid);                /* Move to requestor (from fileclasses) */
							seteuid(tppool_vs_stcp[j].euid);
							stage_sleep(1);
							goto stage_wrt_hsm_retry;
						} else if ((serrno == SEINTERNAL) ||
								   (serrno == ESTKILLED)  ||
								   (serrno == ESTCLEARED) ||
								   (serrno == SESYSERR)   ||
								   (serrno == EACCES)     ||
								   (serrno == EPERM)      ||
								   (serrno == EAGAIN)     ||
								   (serrno == EBUSY)      ||
								   ISTAPESERRNO(serrno)   ||
								   ISVDQMSERRNO(serrno)
							) {
							break;
						} else {
							nb_done_request += nb_in_this_request;
						}
					} else {
						nb_done_request += nb_in_this_request;
					}
				}
			}
			for (i = 0; i < ntppool_vs_stcp; i++) {
				if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
				if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
			}
			free(tppool_vs_stcp);
			setegid(start_passwd.pw_gid);                   /* Go back to admin */
			seteuid(start_passwd.pw_uid);
#ifdef STAGER_DEBUG
			stglogit(func, "### stage_wrt_hsm request exiting with status %d\n", rc);
			if (rc != 0) {
				stglogit(func, "### ... serrno=%d (%s), errno=%d (%s)\n", serrno, sstrerror(serrno), errno, strerror(errno));
			}
#else
			if (rc != 0) {
				stglogit(func, "### stage_wrt_hsm request exiting with status %d\n", rc);
				stglogit(func, "### ... serrno=%d (%s), errno=%d (%s)\n", serrno, sstrerror(serrno), errno, strerror(errno));
			}
#endif
			exit((rc != 0) ? SYERR : 0); /* Exit code of a process, not a return code */
		} else {
			/* We are in the parent */
			nstreams_forked++;
		}
	}

	free(scs);
	for (i = 0; i < ntppool_vs_stcp; i++) {
		if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
		if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
	}
	free(tppool_vs_stcp);

	/* No need to waste memory inherited by parent anymore */

	/* Wait for all child to exit */
	while (1) {
		if ((child_pid = waitpid(-1, &term_status, WNOHANG)) < 0) {
			break;
		}
		if (child_pid > 0) {
			if (WIFEXITED(term_status)) {
				stglogit("migpoolfiles","Migration child pid=%d exited, status %d\n",
						 child_pid, WEXITSTATUS(term_status));
			} else if (WIFSIGNALED(term_status)) {
				stglogit("migpoolfiles","Migration child pid=%d exited due to uncaught signal %d\n",
						 child_pid, WTERMSIG(term_status));
			} else {
				stglogit("migpoolfiles","Migration child pid=%d was stopped\n",
						 child_pid);
			}
		}
		sleep(1);
	}

	/* Return v.s. if we have forked all the streams we wanted */
	return((nstreams_to_fork == nstreams_forked) ? 0 : SESYSERR);
}

void migpoolfiles_log_callback(level,message)
	int level;
	char *message;
{
	uid_t save_euid;
	gid_t save_egid;
	extern struct passwd start_passwd;         /* Generic uid/gid at startup (admin) */

	save_euid = geteuid();
	save_egid = getegid();

	setegid(start_passwd.pw_gid);              /* Move to admin (who knows) */
	seteuid(start_passwd.pw_uid);
	stgmiglogit(migpoolfiles_migrname,"%s",message);
	setegid(save_egid);
	seteuid(save_euid);
	return;
}

void stageclr_log_callback(level,message)
	int level;
	char *message;
{
	stglogit("stageclr","%s",message); /* stageclr's are always done under original's uid [root] */
	return;
}

int isuserlevel(path)
	char *path;
{
	char *p[4];
	int c, rc;

	/* A user-level files begins like this: /xxx/yyy.zz/user/ */
	/*                                      1   2      3    4 */
	/* So we search up to 4 '/' and requires that string between */
	/* 3rd and 4th ones is "user" */

	/* Search No 1 */
	if ((p[0] = strchr(path,'/')) == NULL)
		return(0);
	/* Search No 2 */
	if ((p[1] = strchr(++p[0],'/')) == NULL)
		return(0);
	/* Search No 3 */
	if ((p[2] = strchr(++p[1],'/')) == NULL)
		return(0);
	/* Search No 4 */
	if ((p[3] = strchr(++p[2],'/')) == NULL)
		return(0);

	c = p[3][0];
	p[3][0] = '\0';

	rc = (strcmp(p[2],"user") == 0 ? 1 : 0);

	p[3][0] = c;
	return(rc);
}

#ifndef _WIN32
void poolmgr_wait4child(signo)
	int signo;
{
}
#endif

/* This routine is creating if necessary a new fileclass element in the fileclasses array */
/* forced_Cns_statx can have the following values: */
/*  0 : do not call Cns_statx if fileclass is already known in the structure */
/*      skip eventual call to Cns_queryclass for */
/*      STAGEOUT or STAGEOUT|CAN_BE_MIGR|PUT_FAILED files */
/*  1 : call Cns_statx in any case except for STAGEOUT or STAGEOUT|CAN_BE_MIGR|PUT_FAILED files */
/*      skip eventual call to Cns_queryclass for */
/*      STAGEOUT or STAGEOUT|CAN_BE_MIGR|PUT_FAILED files */
/*  2 : call Cns_statx even if it is a pure STAGEOUT or STAGEOUT|CAN_BE_MIGR|PUT_FAILED file */
/*      in this case, if there is failure, a pure STAGEOUT record will be moved immediately to a */
/*      STAGEOUT|CAN_BE_MIGR|PUT_FAILED entry */
/*      eventual call to Cns_queryclass is not skipped */
/*  3 : do never call Cns_statx() or Cns_queryclass() : just check knowledge of fileclasses */
/*  4 : do never call Cns_statx(), can call Cns_queryclass */
/*  5 : Act completely regardless of stcp->status : if record is in the cache, return it, otherwise */
/*      try to fetch */
/* output can be: */
/*  0 : ok */
/* -1 : error (serrno is setted) */

int upd_fileclass(pool_p,stcp,forced_Cns_statx,only_memory,no_db_update)
	struct pool *pool_p;
	struct stgcat_entry *stcp;
	int forced_Cns_statx;
	int only_memory;
	int no_db_update;
{
	struct Cns_fileid Cnsfileid;
	struct Cns_filestat Cnsfilestat;
	struct Cns_fileclass Cnsfileclass;
	int ifileclass = -1;
	int ifileclass_vs_migrator = -1;
	int i;
	int skip_Cns_queryclass = 0;
	extern struct passwd start_passwd;         /* Generic uid/gid at startup (admin) */

	if ((stcp == NULL) || (stcp->t_or_d != 'h')) {
		serrno = SEINTERNAL;
		return(-1);
	}

	if (forced_Cns_statx != 5) {
		if ((stcp->status == STAGEOUT)   ||
			(stcp->status == (STAGEOUT|CAN_BE_MIGR|PUT_FAILED))) {
			switch (forced_Cns_statx) {
			case 0:
				/* Default: never call Cns_statx() in such a case */
				/* There is intentionnaly no break here */
			case 1:
				/* Always call Cns_statx() if necessary except in this case */
				if ((stcp->u1.h.server[0] == '\0') || (stcp->u1.h.fileclass <= 0)) {
					/* No valid definitions */
					serrno = ENOENT;
					return(-1);
				}
				forced_Cns_statx = 0;
				/* Reset the flag and continue normal processing: */
				/* Because we known that stcp->u1.h.fileclass > 0 AND */
				/* forced_Cns_statx == 0, the if() statement after will */
				/* never be executed */
				skip_Cns_queryclass = 1;
				/* We make sure that there is no call to name server in any case, even if */
				/* fileclass would not be in our cache */
				break;
			default:
				/* Otherwise accept forcing the call to Cns_statx() */
				break;
			}
		}
	}

	if ((forced_Cns_statx == 3) || (forced_Cns_statx == 4)) {
		if ((stcp->u1.h.server[0] == '\0') || (stcp->u1.h.fileclass <= 0)) {
			/* Nope */
			serrno = ENOENT;
			return(-1);
		}
		/* Fileclass > 0 in structure - make sure we do not call Cns_queryclass nor Cns_statx  */
		forced_Cns_statx = 0;
		if (forced_Cns_statx == 3) {
			skip_Cns_queryclass = 1;
		}
	}

	if (forced_Cns_statx == 5) {
		/* Use cache as max as possible */
		forced_Cns_statx = 0;
	}

	if ((stcp->u1.h.fileclass <= 0) || (forced_Cns_statx)) {
		int Cns_statx_rc;
		strcpy(Cnsfileid.server,stcp->u1.h.server);
		Cnsfileid.fileid = stcp->u1.h.fileid;
		setegid(stcp->gid);
		seteuid(stcp->uid);
		Cns_statx_rc = Cns_statx(stcp->u1.h.xfile, &Cnsfileid, &Cnsfilestat);
		setegid(start_passwd.pw_gid);
		seteuid(start_passwd.pw_uid);
		if (Cns_statx_rc != 0) {
			int save_serrno;
			int c;

			save_serrno = serrno;
			if (! only_memory) {
				if (! no_db_update) {
					if ((stcp->status == STAGEOUT) && (save_serrno == ENOENT) && (Cnsfileid.server[0] != '\0') && (Cnsfileid.fileid > 0)) {
						/* We decrement the r/w counter on this file system */
						/* In the case of startup, the rwcounters on filesystem were not yet */
						/* incremented for this stcp - so decrementing will cause an */
						/* error */
						sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_statx", sstrerror(save_serrno));
						if ((c = upd_stageout (STAGEUPDC, stcp->ipath, NULL, 0, stcp, 0, 1)) != 0) {
							serrno = (c == CLEARED) ? ESTCLEARED : c;
							if (serrno != ENOENT) return(-1);
							sendrep (&rpfd, MSG_ERR, "STG02 - %s is absent from name server but disk version is not empty: moving from STAGEOUT to STAGEOUT|CAN_BE_MIGR|PUT_FAILED\n", stcp->u1.h.xfile);
							/* File have been definitely deleted from the name server */
							stcp->status = STAGEOUT|CAN_BE_MIGR|PUT_FAILED;
#ifdef USECDB
							if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
							  stglogit ("upd_fileclass", STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
							}
#endif
							savereqs();
						}
					}
				}
			}
			serrno = save_serrno;
			return(-1);
		}
		if (stcp->u1.h.fileclass != Cnsfilestat.fileclass) {
			if ((stcp->u1.h.fileclass > 0) && (Cnsfilestat.fileclass > 0)) {
				/* Never executed when original stcp have no fileclass yet... */
				char tmpbuf[21];
				sendrep(&rpfd, MSG_ERR, STG173, stcp->u1.h.xfile, stcp->u1.h.fileclass, Cnsfilestat.fileclass, u64tostr((u_signed64) stcp->u1.h.fileid, tmpbuf, 0), stcp->u1.h.server);
			}
			stcp->u1.h.fileclass = Cnsfilestat.fileclass;
			if (! only_memory) {
				if (! no_db_update) {
#ifdef USECDB
					if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
						stglogit ("upd_fileclass", STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
					}
#endif
					savereqs();
				}
			}
		}
	}

	/* We query the class if necessary */
	for (i = 0; i < nbfileclasses; i++) {
		if (fileclasses[i].Cnsfileclass.classid == stcp->u1.h.fileclass &&
			strcmp(fileclasses[i].server, stcp->u1.h.server) == 0) {
			ifileclass = i;
			break;
		}
	}
	if (ifileclass < 0) {
		int sav_nbcopies;
		char *pi, *pj, *pk;
		int j, k, have_duplicate, nb_duplicate, new_nbtppools;

		if (skip_Cns_queryclass != 0) {
			serrno = ENOENT;
			return(-1);
		}

		if (Cns_queryclass(stcp->u1.h.server, stcp->u1.h.fileclass, NULL, &Cnsfileclass) != 0) {
			sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_queryclass", sstrerror(serrno));
			return(-1);
		}

		/* @@@@ EXCEPTIONNAL @@@@ */
		/* Cnsfileclass.migr_time_interval = 1; */
		/* Cnsfileclass.mintime_beforemigr = 0; */
		/* Cnsfileclass.maxdrives = 1; */
		/* Cnsfileclass.retenp_on_disk = 0; */
		/* @@@@ END OF EXCEPTIONNAL @@@@ */

		/* We check that this fileclass does not contain values that we cannot sustain */
		if ((sav_nbcopies = Cnsfileclass.nbcopies) < 0) {
			sendrep (&rpfd, MSG_ERR, STG126, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbcopies);
			serrno = EINVAL;
			return(-1);
		}
		/* We check that the number of tape pools is valid */
		if (Cnsfileclass.nbtppools < 0) {
			sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_queryclass", "returns invalid number of tape pool - invalid fileclass - Please contact your admin");
			serrno = EINVAL;
			return(-1);
		}

		/* We allow (nbcopies == 0 && nbtppool == 0) only */
		if ((sav_nbcopies == 0) || (Cnsfileclass.nbtppools == 0)) {
			if ((sav_nbcopies > 0) || (Cnsfileclass.nbtppools > 0)) {
				sendrep (&rpfd, MSG_ERR, STG138, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, sav_nbcopies, Cnsfileclass.nbtppools);
				serrno = EINVAL;
				return(-1);
			}
			/* Here we are sure that both nbcopies and nbtpools are equal to zero */
			/* There is no need to check anything - we immediately add this fileclass */
			/* into the main array */
			goto upd_fileclass_add;
		}

		if (Cnsfileclass.nbtppools > 0) {
			pi = Cnsfileclass.tppools;
			i = 0;
			while (*pi != '\0') {
				pi += (CA_MAXPOOLNAMELEN+1);
				if (++i >= Cnsfileclass.nbtppools) {
					/* We already have count as many tape pools as the fileclass specifies */
					break;
				}
			}
			if (i != Cnsfileclass.nbtppools) {
				sendrep (&rpfd, MSG_ERR, STG127, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbtppools, i);
				Cnsfileclass.nbtppools = i;
			}
			/* We reduce eventual list of duplicate tape pool in tppools element */
			nb_duplicate = 0;
			new_nbtppools = Cnsfileclass.nbtppools;
			while (1) {
				pi = Cnsfileclass.tppools;
				have_duplicate = 0;
				for (i = 0; i < new_nbtppools; i++) {
					pj = pi + (CA_MAXPOOLNAMELEN+1);
					for (j = i + 1; j < new_nbtppools; j++) {
						if (strcmp(pi,pj) == 0) {
							sendrep (&rpfd, MSG_ERR, STG128, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, pi);
							have_duplicate = j;
							nb_duplicate++;
							break;
						}
						pj += (CA_MAXPOOLNAMELEN+1);
					}
					if (have_duplicate > 0) {   /* Because it starts a j+1, i starting at zero */
						pk = pj;
						for (k = j + 1; j < new_nbtppools; j++) {
							strcpy(pk - (CA_MAXPOOLNAMELEN+1), pk); 
							pk += (CA_MAXPOOLNAMELEN+1);
						}
						new_nbtppools--;
						break;
					}
					pi += (CA_MAXPOOLNAMELEN+1);
				}
				if (have_duplicate == 0) break;
			}
			if (new_nbtppools != Cnsfileclass.nbtppools) {
				/* We found duplicate(s) - so we rescan number of tape pools */
				sendrep (&rpfd, MSG_ERR, STG129, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbtppools, new_nbtppools);
				Cnsfileclass.nbtppools = new_nbtppools;
			}
			/* We check the number of copies vs. number of tape pools */
			if (Cnsfileclass.nbcopies > Cnsfileclass.nbtppools) {
				sendrep (&rpfd, MSG_ERR, STG130, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbcopies, Cnsfileclass.nbtppools, Cnsfileclass.nbtppools);
				Cnsfileclass.nbcopies = new_nbtppools;
			}
		}
	  upd_fileclass_add:
		/* We add this fileclass to the list of known fileclasses */
		if (nbfileclasses <= 0) {
			if ((fileclasses = (struct fileclass *) malloc(sizeof(struct fileclass))) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "malloc", strerror(errno));
				serrno = SEINTERNAL;
				return(-1);
			}
			nbfileclasses = 0;
		} else {
			struct fileclass *dummy;
			struct pool *pool_n;

			if ((dummy = (struct fileclass *)
				 malloc((nbfileclasses + 1) * sizeof(struct fileclass))) == NULL) {
				sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "malloc", strerror(errno));
				serrno = SEINTERNAL;
				return(-1);
			}
			/* We copy old content */
			memcpy(dummy, fileclasses, nbfileclasses * sizeof(struct fileclass));
			/* The realloc has been successful but fileclasses may have been moved - so we update all */
			/* references to previous indexes within fileclasses to indexes within dummy */
			/* This is simpler for me rather than playing with deltas in the memory */
			for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
				if (pool_n->migr == NULL) continue;
				for (i = 0; i < pool_n->migr->nfileclass; i++) {
					for (k = 0; k < nbfileclasses; k++) {               /* Nota - nbfileclasses have not been yet changed */
						if (pool_n->migr->fileclass[i] == &(fileclasses[k])) {
							pool_n->migr->fileclass[i] = &(dummy[k]);
							break;
						}
					}
				}
			}
			/* Update finished */
			free(fileclasses);
			fileclasses = dummy;
		}
		/* We update last element */
		fileclasses[nbfileclasses].Cnsfileclass = Cnsfileclass;
		strcpy(fileclasses[nbfileclasses].server,stcp->u1.h.server);
		fileclasses[nbfileclasses].last_tppool_used[0] = '\0';
		if (! only_memory) {
			stglogit ("upd_fileclass", STG109,
					  fileclasses[nbfileclasses].Cnsfileclass.name,
					  fileclasses[nbfileclasses].server,
					  fileclasses[nbfileclasses].Cnsfileclass.classid,
					  nbfileclasses);
			stglogfileclass(&Cnsfileclass);
		}
		ifileclass = nbfileclasses++;
		/* File class at index (nbfileclasses - 1) has been created */
	}
  
	if (pool_p != NULL) {
		/* We check that this pool's migrator is aware about this fileclass */
		if (pool_p->migr != NULL) {
			for (i = 0; i < pool_p->migr->nfileclass; i++) {
				if (pool_p->migr->fileclass[i] == &(fileclasses[ifileclass])) {
					ifileclass_vs_migrator = i;
				}
			}
			if (ifileclass_vs_migrator < 0) {
				if (pool_p->migr->nfileclass <= 0) {
					if (((pool_p->migr->fileclass            = (struct fileclass **) malloc(sizeof(struct fileclass *))) == NULL) || 
						((pool_p->migr->fileclass_predicates = (struct predicates *) malloc(sizeof(struct predicates))) == NULL)) {
						sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "malloc", strerror(errno));
						serrno = SEINTERNAL;
						return(-1);
					}
					pool_p->migr->nfileclass = 0;
				} else {
					struct fileclass **dummy;
					struct predicates *dummy2;
          
					if ((dummy = (struct fileclass **) 
						 realloc(pool_p->migr->fileclass,(pool_p->migr->nfileclass + 1) * sizeof(struct fileclass *))) == NULL) {
						sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "realloc", strerror(errno));
						serrno = SEINTERNAL;
						return(-1);
					} else {
						if ((dummy2 = (struct predicates *)
							 realloc(pool_p->migr->fileclass_predicates,(pool_p->migr->nfileclass + 1) * sizeof(struct predicates))) == NULL) {
							free(dummy);
							sendrep (&rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "realloc", strerror(errno));
							serrno = SEINTERNAL;
							return(-1);
						}
					}
					pool_p->migr->fileclass = dummy;
					pool_p->migr->fileclass_predicates = dummy2;
				}
				pool_p->migr->fileclass[pool_p->migr->nfileclass] = &(fileclasses[ifileclass]);
				memset(&(pool_p->migr->fileclass_predicates[pool_p->migr->nfileclass]),0,sizeof(struct predicates));
				ifileclass_vs_migrator = pool_p->migr->nfileclass++;
			}
		}
		return(ifileclass_vs_migrator);
	} else {
		/* No pool_p specified : we return the global index */
		return(ifileclass);
	}
}

/* This routine updates all the known fileclasses */
int upd_fileclasses()
{
	struct Cns_fileclass Cnsfileclass;
	int i;

	for (i = 0; i < nbfileclasses; i++) {
		if (Cns_queryclass(fileclasses[i].server, fileclasses[i].Cnsfileclass.classid, NULL, &Cnsfileclass) != 0) {
			stglogit ("upd_fileclasses", STG57,
					  fileclasses[i].Cnsfileclass.name,
					  fileclasses[i].Cnsfileclass.classid,
					  fileclasses[i].server,
					  i,
					  "Cns_queryclass",
					  sstrerror(serrno));
			/* We do not want to stop the update of other fileclasses, if any */
			continue;
		}
		if (fileclasses[i].Cnsfileclass.tppools != NULL) {
			free(fileclasses[i].Cnsfileclass.tppools);
		}
		/* @@@@ EXCEPTIONNAL @@@@ */
		/* Cnsfileclass.migr_time_interval = 10; */
		/* Cnsfileclass.mintime_beforemigr = 0; */
		/* Cnsfileclass.maxdrives = 1; */
		/* Cnsfileclass.retenp_on_disk = 0; */
		/* @@@@ END OF EXCEPTIONNAL @@@@ */
		fileclasses[i].Cnsfileclass = Cnsfileclass;
		/* File class at index (nbfileclasses - 1) has been created */
		/* We check if the last tape pool used still belongs to this fileclass */
		/* If not, we reset last_tppool_used variable */
		upd_last_tppool_used(&(fileclasses[i]));
		stglogfileclass(&Cnsfileclass);
	}
	return(0);
}

void upd_last_tppool_used(fileclass)
	struct fileclass *fileclass;
{
	int j;
	int found = 0;
	char *p;

	if (fileclass->last_tppool_used[0] != '\0') {
		p = fileclass->Cnsfileclass.tppools;
		for (j = 0; j < fileclass->Cnsfileclass.nbtppools; j++) {
			if (strcmp(p,fileclass->last_tppool_used) == 0) {
				found = 1;
				break;
			}
			/* Tape pools are separated by '\0' */
			p += (CA_MAXPOOLNAMELEN+1);
		}
		if (found == 0) {
			/* Not found : we reset this knowledge... */
			stglogit("upd_last_tppool_used", STG111,
					 fileclass->last_tppool_used,
					 fileclass->Cnsfileclass.name,
					 fileclass->server,
					 fileclass->Cnsfileclass.classid);
			fileclass->last_tppool_used[0] = '\0';
		}
	}
}

char *next_tppool(fileclass)
	struct fileclass *fileclass;
{
	int i;
	int found_last_tppool = -1;
	char *p;

	/* The array tppool_used[0..(nbtppools_used - 1)] is a filter to tppools[0..(nbtppools - 1)] */

	p = fileclass->Cnsfileclass.tppools;
	if ((fileclass->last_tppool_used[0] == '\0') || (fileclass->Cnsfileclass.nbtppools == 1)) {
		/* No last_tppool_used yet - or the answer is always the same... */
		strcpy(fileclass->last_tppool_used,p);
		return(p);
	}
	for (i = 0; i < fileclass->Cnsfileclass.nbtppools; i++) {
		found_last_tppool = 0;
		if (strcmp(p,fileclass->last_tppool_used) == 0) {
			found_last_tppool = i;
			break;
		}
		p += (CA_MAXPOOLNAMELEN+1);
	}
	if (found_last_tppool == -1) {
		/* Not found : we reset this knowledge... */
		stglogit("next_tppool", STG113);
		p = fileclass->Cnsfileclass.tppools;
		strcpy(fileclass->last_tppool_used,p);
		return(p);
	}
	/* Found : either there is something after, either we have to turnaround */
	if (found_last_tppool == (fileclass->Cnsfileclass.nbtppools - 1)) {
		/* Was at the end of the list */
		p = fileclass->Cnsfileclass.tppools;
	} else {
		/* Got to next entry */
		p += (CA_MAXPOOLNAMELEN+1);
	}
	/* Update persistent knowledge for further selection */
	strcpy(fileclass->last_tppool_used,p);
	/* Please note that p is a pointer to a global array (declared static in this source) */
	return(p);
}

int euid_egid(euid,egid,tppool,migr,stcp,stcp_check,tppool_out,being_migr,only_memory)
	uid_t *euid;
	gid_t *egid;
	char *tppool;
	struct migrator *migr;   /* Migrator - case of all automatic migration calls */
	struct stgcat_entry *stcp;   /* Must be non-NULL if migr == NULL */
	struct stgcat_entry *stcp_check;   /* Used to check requestor compatible uid/gid */
	char **tppool_out;
	int being_migr;              /* We check CAN_BE_MIGR, BEING_MIGR otherwise - only if migr != NULL */
	int only_memory;
{
	int i, j;
	char *p;
	int found_tppool;
	int found_fileclass;
	uid_t last_fileclass_euid = 0;
	gid_t last_fileclass_egid = 0;
	extern struct passwd start_passwd;             /* Start uid/gid at startup (admin) */
	extern struct passwd stage_passwd;             /* Generic uid/gid stage:st */

	/* At first call - application have to set them to zero - default is then STAGERSUPERUSER uid/gid */
	if (*euid != (uid_t) 0) {
		last_fileclass_euid = *euid;  /* We simulate a virtual previous explicit filter */
	}
	if (*egid != (gid_t) 0) {
		last_fileclass_egid = *egid;  /* We simulate a virtual previous explicit filter */
	}

	/* If migrator is not specified, then stcp have to be speficied */
	if (migr == NULL) {
		if (stcp == NULL) {
			serrno = SEINTERNAL;
			return(-1);
		}
		/* We need to know what is the fileclass for this stcp */
		if ((i = upd_fileclass(NULL,stcp,0,only_memory,0)) < 0) {
			return(-1);
		}
		p = fileclasses[i].Cnsfileclass.tppools;
		if ((tppool != NULL) && (tppool[0] != '\0')) {
			/* We check that this tppool really belongs to this fileclass */
			found_tppool = 0;
			for (j = 0; j < fileclasses[i].Cnsfileclass.nbtppools; j++) {
				if (strcmp(tppool,p) == 0) {
					found_tppool = 1;
					break;
				}
				p += (CA_MAXPOOLNAMELEN+1);
			}
			if (found_tppool == 0) {
				sendrep(&rpfd, MSG_ERR, STG122, tppool);
				serrno = EINVAL;
				return(-1);
			}
			if (tppool_out != NULL) *tppool_out = p;
		} else {
			tppool = next_tppool(&(fileclasses[i]));
			if (tppool_out != NULL) *tppool_out = tppool;       /* Already allocated on the heap */
		}
		goto update_euid_egid;
	} else {
		/* If migrator is non-NULL, then pool have to be specified */
		if (tppool == NULL) {
			serrno = SEINTERNAL;
			return(-1);
		}
		if (tppool_out != NULL) *tppool_out = tppool;
	}

	for (i = 0; i < nbfileclasses; i++) {
		/* We check if this fileclass belongs to thoses that emerge for our migrator predicates */
		found_fileclass = 0;
		for (j = 0; j < migr->nfileclass; j++) {
			if (migr->fileclass[j] == &(fileclasses[i])) {
				found_fileclass = 1;
				break;
			}
		}
		if (found_fileclass == 0) continue;
		/*
		  if (being_migr != 0) {
		  if (migr->fileclass[j]->being_migr <= 0) continue;
		  } else {
		  if ((migr->fileclass_predicates[j].nbfiles_canbemig - migr->fileclass_predicates[j].nbfiles_beingmig) <= 0) continue;
		  }
		  if (found_fileclass == 0) {
		  sendrep(&rpfd, MSG_ERR, "STG02 - Cannot find fileclass that can trigger your migration\n");
		  return(-1);
		  }
		*/
		/* From now on, we are sure that this fileclass contains files that are migration candidates */
		p = fileclasses[i].Cnsfileclass.tppools;
		found_tppool = 0;
		for (j = 0; j < fileclasses[i].Cnsfileclass.nbtppools; j++) {
			if (strcmp(tppool,p) == 0) {
				found_tppool = 1;
				break;
			}
			p += (CA_MAXPOOLNAMELEN+1);
		}
		if (found_tppool == 0) continue;
		if (tppool_out != NULL) *tppool_out = p;
	  update_euid_egid:
		/* If uid in this fileclass is set and we did not yet set it ourself - we do so */
		if (fileclasses[i].Cnsfileclass.uid != (uid_t) 0) {
			/* If euid was not already set by a previous call or if euid was not already set by */
			/* a previous loop iteration */
			if (last_fileclass_euid == 0) {
				last_fileclass_euid = *euid = fileclasses[i].Cnsfileclass.uid;
			} else {
				/* If we already updated it - we check consistency */
				if (*euid != fileclasses[i].Cnsfileclass.uid) {
					sendrep(&rpfd, MSG_ERR, STG118, fileclasses[i].Cnsfileclass.name, tppool, "uid", *euid, fileclasses[i].Cnsfileclass.uid);
					serrno = EINVAL;
					return(-1);
				}
			}
		}
		/* If gid in this fileclass is set and we did not yet set it ourself - we do so */
		if (fileclasses[i].Cnsfileclass.gid != (gid_t) 0) {
			/* If egid was not already set by a previous call or if egid was not already set by */
			/* a previous loop iteration */
			if (last_fileclass_egid == 0) {
				last_fileclass_egid = *egid = fileclasses[i].Cnsfileclass.gid;
			} else {
				/* If we already updated it - we check consistency */
				if (*egid != fileclasses[i].Cnsfileclass.gid) {
					sendrep(&rpfd, MSG_ERR, STG118, fileclasses[i].Cnsfileclass.name, tppool, "gid", *egid, fileclasses[i].Cnsfileclass.gid);
					serrno = EINVAL;
					return(-1);
				}
			}
		}
		if (migr == NULL) {
			/* Case where we went there because of the goto upper */
			goto euid_egid_return;
		}
	}
  euid_egid_return:
	/* So here we have decided about euid and egid - If there is a stcp_check != NULL in the arguments, we also checks */
	/* validity of the requestor stcp_check */
	if (stcp_check != NULL) {
		/* We check the found group id in priority */
		if (last_fileclass_egid != 0) {
			/* There is an explicit filter on group id - current's stcp_check->gid have to match it */
			if ((*egid != stcp_check->gid) && (stcp_check->gid != start_passwd.pw_gid)) {
				sendrep(&rpfd, MSG_ERR, STG125, stcp_check->user, "gid", stcp_check->gid, "group", *egid, stcp_check->u1.h.xfile);
				serrno = EINVAL;
				return(-1);
			}
			/* We check compatibility of uid */
			if (last_fileclass_euid != 0) {
				/* There is an explicit filter on user id - current's stcp_check->uid have to match it */
				if ((*euid != stcp_check->uid) && (stcp_check->uid != start_passwd.pw_uid)) {
					sendrep(&rpfd, MSG_ERR, STG125, stcp_check->user, "uid", stcp_check->uid, "user", *euid, stcp_check->u1.h.xfile);
					serrno = EINVAL;
					return(-1);
				} else {
					/* Current's stcp_check's uid is matching ok */
					*euid = (stcp_check->uid != start_passwd.pw_uid) ? stcp_check->uid : (last_fileclass_euid != 0 ? last_fileclass_euid : stcp_check->uid);
					/* And it is also matching gid per def in this branch */
					*egid = (stcp_check->gid != start_passwd.pw_gid) ? stcp_check->gid : (last_fileclass_egid != 0 ? last_fileclass_egid : stcp_check->gid);
				}
			} else {
				/* Current's stcp_check's uid is matching since there is no filter on this */
				*euid = (stcp_check->uid != start_passwd.pw_uid) ? stcp_check->uid : (last_fileclass_euid != 0 ? last_fileclass_euid : stcp_check->uid);
				/* And it is also matching gid per def in this branch */
				*egid = (stcp_check->gid != start_passwd.pw_gid) ? stcp_check->gid : (last_fileclass_egid != 0 ? last_fileclass_egid : stcp_check->gid);
			}
		} else {
			/* There is no explicit filter on group id - Is there explicit filter on user id ? */
			if (last_fileclass_euid != 0) {
				/* There is an explicit filter on user id - current's stcp_check->uid have to match it */
				if ((*euid != stcp_check->uid) && (stcp_check->uid != start_passwd.pw_uid)) {
					sendrep(&rpfd, MSG_ERR, STG125, stcp_check->user, "uid", stcp_check->uid, "user", *euid, stcp_check->u1.h.xfile);
					serrno = EINVAL;
					return(-1);
				} else {
					/* Current's stcp_check's uid is matching ok */
					*euid = (stcp_check->uid != start_passwd.pw_uid) ? stcp_check->uid : (last_fileclass_euid != 0 ? last_fileclass_euid : stcp_check->uid);
					/* Current's stcp_check's gid is matching since there is no filter on this */
					*egid = (stcp_check->gid != start_passwd.pw_gid) ? stcp_check->gid : (last_fileclass_egid != 0 ? last_fileclass_egid : stcp_check->gid);
				}
			} else {
				/* There is no filter at all so the default is STAGERSUPERUSER account */
				/* Current's stcp_check's uid is matching since there is no filter on this */
				*euid = stage_passwd.pw_uid;
				/* Current's stcp_check's gid is matching since there is no filter on this */
				*egid = stage_passwd.pw_gid;
			}
		}
	}
	/* OK */
	return(0);
}

void stglogfileclass(Cnsfileclass)
	struct Cns_fileclass *Cnsfileclass;
{
	struct group *gr;
	int i;
	char *p;
	struct passwd *pw;
	gid_t sav_gid = -1;
	char sav_gidstr[7];
	uid_t sav_uid = -1;
	char sav_uidstr[CA_MAXUSRNAMELEN+1];
	char *func = "stglogfileclass";
	int verif_nbtppools = 0;

	if (Cnsfileclass->uid != sav_uid) {
		sav_uid = Cnsfileclass->uid;
		if (sav_uid == 0) {
			strcpy (sav_uidstr, "-");
		} else if ((pw = Cgetpwuid (sav_uid)) != NULL) {
			strncpy (sav_uidstr, pw->pw_name, (size_t) CA_MAXUSRNAMELEN);
			sav_uidstr[CA_MAXUSRNAMELEN] = '\0';
		} else {
			Csnprintf (sav_uidstr, (size_t) CA_MAXUSRNAMELEN, "%-8u", sav_uid);
			sav_uidstr[CA_MAXUSRNAMELEN] = '\0';
		}
	}
	if (Cnsfileclass->gid != sav_gid) {
		sav_gid = Cnsfileclass->gid;
		if (sav_gid == 0) {
			strcpy (sav_gidstr, "-");
		} else if ((gr = Cgetgrgid (sav_gid)) != NULL) {
			strncpy (sav_gidstr, gr->gr_name, (size_t) 6);
			sav_gidstr[6] = '\0';
		} else {
			Csnprintf (sav_gidstr, (size_t) 6, "%-6u", sav_gid);
			sav_gidstr[6] = '\0';
		}
	}
	p = Cnsfileclass->tppools;

	stglogit(func,"CLASS_ID       %d\n", Cnsfileclass->classid);
	stglogit(func,"CLASS_NAME     %s\n", Cnsfileclass->name);
	stglogit(func,"CLASS_UID      %d (%s)\n", (int) sav_uid, sav_uidstr);
	stglogit(func,"CLASS_GID      %d (%s)\n", (int) sav_gid, sav_gidstr);
	stglogit(func,"FLAGS          0x%x\n", Cnsfileclass->flags);
	stglogit(func,"MAXDRIVES      %d\n", Cnsfileclass->maxdrives);
	stglogit(func,"MIN FILESIZE   %d\n", Cnsfileclass->min_filesize);
	stglogit(func,"MAX FILESIZE   %d\n", Cnsfileclass->max_filesize);
	stglogit(func,"MAX SEGSIZE    %d\n", Cnsfileclass->max_segsize);
	stglogit(func,"MIGR INTERVAL  %d\n", Cnsfileclass->migr_time_interval);
	stglogit(func,"MIN TIME       %d\n", Cnsfileclass->mintime_beforemigr);
	stglogit(func,"NBCOPIES       %d\n", Cnsfileclass->nbcopies);
	switch (Cnsfileclass->retenp_on_disk) {
	case AS_LONG_AS_POSSIBLE:
		stglogit(func,"RETENP_ON_DISK %s\n", "AS_LONG_AS_POSSIBLE");
		break;
	case INFINITE_LIFETIME:
		stglogit(func,"RETENP_ON_DISK %s\n", "INFINITE_LIFETIME");
		break;
	default:
		stglogit(func,"RETENP_ON_DISK %d\n", Cnsfileclass->retenp_on_disk);
		break;
	}
	stglogit(func,"NBTPPOOLS      %d\n", Cnsfileclass->nbtppools);
	if (Cnsfileclass->nbtppools > 0) {
		if (*p != '\0') {
			verif_nbtppools++;
			stglogit(func,"TAPE POOL No %d %s\n", verif_nbtppools, p);
			for (i = 1; i < Cnsfileclass->nbtppools; i++) {
				p += (CA_MAXPOOLNAMELEN+1);
				if (*p != '\0') {
					verif_nbtppools++;
					stglogit(func,"TAPE POOL No %d %s\n", verif_nbtppools, p);
				} else {
					break;
				}
			}
		}
	} else {
		if (verif_nbtppools != Cnsfileclass->nbtppools) {
			stglogit(func,"### Warning : Found tape pools does not match fileclass - forced to %d\n", verif_nbtppools);
			Cnsfileclass->nbtppools = verif_nbtppools;
		}
	}
	if (Cnsfileclass->nbcopies > Cnsfileclass->nbtppools) {
		stglogit(func,"### Warning : Found number of copies is higher than number of tape pools - forced to %d\n", Cnsfileclass->nbtppools);
		Cnsfileclass->nbcopies = Cnsfileclass->nbtppools;
	}
}

void printfileclass(rpfd,fileclass)
	int *rpfd;
	struct fileclass *fileclass;
{
	struct group *gr;
	int i;
	char *p;
	struct passwd *pw;
	gid_t sav_gid = -1;
	char sav_gidstr[7];
	uid_t sav_uid = -1;
	char sav_uidstr[CA_MAXUSRNAMELEN+1];
	int verif_nbtppools = 0;

	if (*rpfd < 0) return;

	if (fileclass->Cnsfileclass.uid != sav_uid) {
		sav_uid = fileclass->Cnsfileclass.uid;
		if (sav_uid == 0) {
			strcpy (sav_uidstr, "-");
		} else if ((pw = Cgetpwuid (sav_uid)) != NULL) {
			strncpy (sav_uidstr, pw->pw_name, (size_t) CA_MAXUSRNAMELEN);
			sav_uidstr[CA_MAXUSRNAMELEN] = '\0';
		} else {
			Csnprintf (sav_uidstr, (size_t) CA_MAXUSRNAMELEN, "%-8u", sav_uid);
			sav_uidstr[CA_MAXUSRNAMELEN] = '\0';
		}
	}
	if (fileclass->Cnsfileclass.gid != sav_gid) {
		sav_gid = fileclass->Cnsfileclass.gid;
		if (sav_gid == 0) {
			strcpy (sav_gidstr, "-");
		} else if ((gr = Cgetgrgid (sav_gid)) != NULL) {
			strncpy (sav_gidstr, gr->gr_name, (size_t) 6);
			sav_gidstr[6] = '\0';
		} else {
			Csnprintf (sav_gidstr, (size_t) 6, "%-6u", sav_gid);
			sav_gidstr[6] = '\0';
		}
	}
	p = fileclass->Cnsfileclass.tppools;

	sendrep(rpfd,MSG_OUT,"FILECLASS %s@%s (classid=%d)\n", fileclass->Cnsfileclass.name, fileclass->server, fileclass->Cnsfileclass.classid);
	sendrep(rpfd,MSG_OUT,"\tCLASS_UID      %d (%s)\n", (int) sav_uid, sav_uidstr);
	sendrep(rpfd,MSG_OUT,"\tCLASS_GID      %d (%s)\n", (int) sav_gid, sav_gidstr);
	sendrep(rpfd,MSG_OUT,"\tFLAGS          0x%x\n", fileclass->Cnsfileclass.flags);
	sendrep(rpfd,MSG_OUT,"\tMAXDRIVES      %d\n", fileclass->Cnsfileclass.maxdrives);
	sendrep(rpfd,MSG_OUT,"\tMIN FILESIZE   %d\n", fileclass->Cnsfileclass.min_filesize);
	sendrep(rpfd,MSG_OUT,"\tMAX FILESIZE   %d\n", fileclass->Cnsfileclass.max_filesize);
	sendrep(rpfd,MSG_OUT,"\tMAX SEGSIZE    %d\n", fileclass->Cnsfileclass.max_segsize);
	sendrep(rpfd,MSG_OUT,"\tMIGR INTERVAL  %d\n", fileclass->Cnsfileclass.migr_time_interval);
	sendrep(rpfd,MSG_OUT,"\tMIN TIME       %d\n", fileclass->Cnsfileclass.mintime_beforemigr);
	sendrep(rpfd,MSG_OUT,"\tNBCOPIES       %d\n", fileclass->Cnsfileclass.nbcopies);
	switch (fileclass->Cnsfileclass.retenp_on_disk) {
	case AS_LONG_AS_POSSIBLE:
		sendrep(rpfd,MSG_OUT,"\tRETENP_ON_DISK %s\n", "AS_LONG_AS_POSSIBLE");
		break;
	case INFINITE_LIFETIME:
		sendrep(rpfd,MSG_OUT,"\tRETENP_ON_DISK %s\n", "INFINITE_LIFETIME");
		break;
	default:
		sendrep(rpfd,MSG_OUT,"\tRETENP_ON_DISK %d\n", fileclass->Cnsfileclass.retenp_on_disk);
		break;
	}
	sendrep(rpfd,MSG_OUT,"\tNBTPPOOLS      %d\n", fileclass->Cnsfileclass.nbtppools);
	if (*p != '\0') {
		verif_nbtppools++;
		sendrep(rpfd,MSG_OUT,"\tTAPE POOL No %d %s\n", verif_nbtppools, p);
		for (i = 1; i < fileclass->Cnsfileclass.nbtppools; i++) {
			p += (CA_MAXPOOLNAMELEN+1);
			if (*p != '\0') {
				verif_nbtppools++;
				sendrep(rpfd,MSG_OUT,"\tTAPE POOL No %d %s\n", verif_nbtppools, p);
			} else {
				break;
			}
		}
	}

	if (verif_nbtppools != fileclass->Cnsfileclass.nbtppools) {
		sendrep(rpfd,MSG_OUT,"### Warning : Found tape pools does not match fileclass - forced to %d\n", verif_nbtppools);
		fileclass->Cnsfileclass.nbtppools = verif_nbtppools;
	}
	if (fileclass->Cnsfileclass.nbcopies > fileclass->Cnsfileclass.nbtppools) {
		sendrep(rpfd,MSG_OUT,"### Warning : Found number of copies is higher than number of tape pools - forced to %d\n", fileclass->Cnsfileclass.nbtppools);
		fileclass->Cnsfileclass.nbcopies = fileclass->Cnsfileclass.nbtppools;
	}
}

int retenp_on_disk(ifileclass)
	int ifileclass;
{
	return(fileclasses[ifileclass].Cnsfileclass.retenp_on_disk);
}

int mintime_beforemigr(ifileclass)
	int ifileclass;
{
	return(fileclasses[ifileclass].Cnsfileclass.mintime_beforemigr);
}

int mintime_beforemigr_vs_pool(pool_p,ifileclass)
	struct pool *pool_p;
	int ifileclass;
{
	return(pool_p->migr->fileclass[ifileclass]->Cnsfileclass.mintime_beforemigr);
}

void check_delaymig() {
	time_t thistime;
	struct stgcat_entry *stcp;
    int i;
    struct migrator *migr_p;
    int have_delaymig = 0;

    /* We do NOT loop if there is nothing to move from delay_migr to can_be_migr */
    if (migrators != NULL) {
		for (i = 0, migr_p = migrators; i < nbmigrator; i++, migr_p++) {
			if ((migr_p->global_predicates.nbfiles_delaymig > 0) ||
				(migr_p->global_predicates.space_delaymig   > 0)) {
				have_delaymig = 1;
				break;
			}
		}
    }

    if (! have_delaymig) return;

	thistime = time(NULL);

	for (stcp = stcs; stcp < stce; stcp++) {
		int ifileclass;
		int thismintime_beforemigr;

		if (stcp->reqid == 0) break;
		if (stcp->t_or_d != 'h') continue;      /* Not a CASTOR file */

		if (stcp->filler[0] != 'd') continue; /* Here is a candidate with delayed migration */

		/* There is NO automatic deletion for any record that is waiting on something else */
		if (ISWAITING(stcp)) continue;

		/* Check fileclass */
		if ((ifileclass = upd_fileclass(NULL,stcp,0,0,0)) < 0) continue; /* Unknown fileclass */

		/* ============================================================================= */
		/* CHECK CASTOR's STAGEOUT|CAN_BE_MIGR LATENCY (mintime_beforemigr -> canbemigr) */
		/* ============================================================================= */
        
		if (stcp->status == (STAGEOUT|CAN_BE_MIGR)) {
			if ((thismintime_beforemigr = stcp->u1.h.mintime_beforemigr) < 0)
				thismintime_beforemigr = mintime_beforemigr(ifileclass);
			/* Please note that stcp->a_time + thismintime_beforemigr can go out of bounds */
			/* That's why I typecase everything to u_signed64 */
			if ((stcp->filler[0] != 'm') && ((u_signed64) ((u_signed64) stcp->a_time + (u_signed64) thismintime_beforemigr) <= (u_signed64) thistime)) {
				/* This entry had a mintime_beforemigr and we have reached its latency value */
				update_migpool(&stcp, 1, 3);   /* immediate flag is 3 */
			}
		}
	}
	return;
}

void check_expired() {
	time_t thistime;
	struct stgcat_entry *stcp;
	char *func = "check_expired";
	int fork_pid;
	int have_stagealloc_retenp;
	int have_stageout_retenp;
	
    /* We do NOT loop if there is nothing to check */
    if ((nbstageout <= 0) && (nbstagealloc <= 0)) return;

	have_stagealloc_retenp = have_at_least_one_stagealloc_retenp_raw();
	have_stageout_retenp = have_at_least_one_stageout_retenp_raw();

	if (nbstageout <= 0) {
		/* This mean that we are here only because of nbstagealloc > 0 */
		/* Is there any pool that gives a retention period > 0 for them ? */
		if (have_stagealloc_retenp < 0) return;
	}
	
	if (nbstagealloc <= 0) {
		/* This mean that we are here only because of nbstageout > 0 */
		/* Is there any pool that gives a retention period > 0 for them ? */
		if (have_stageout_retenp < 0) return;
	}
	
	thistime = time(NULL);

	for (stcp = stcs; stcp < stce; stcp++) {
		signed64 thisretenp;
		char tmpbuf[21];
		
		if (stcp->reqid == 0) break;
		if ((stcp->status != STAGEOUT) && (stcp->status != STAGEALLOC)) continue;
		if (stcp->filler[0] == 'Z') continue; /* Flag to prevent another stageclr */
		if (stcp->a_time > thistime) continue; /* We will overflow */
		if (stcp->status == STAGEOUT) {
			if (have_stageout_retenp < 0) continue; /* No retenp in any pool */
			if ((thisretenp = get_stageout_retenp_raw(stcp)) <= 0) continue;
		}
		if (stcp->status == STAGEALLOC) {
			if (have_stagealloc_retenp < 0) continue; /* No retenp in any pool */
			if ((thisretenp = get_stagealloc_retenp_raw(stcp)) <= 0) continue;
		}
		if ((thistime - stcp->a_time) <= thisretenp) continue; /* Not expired */

		/* Generate a stageclr on this internal path */
		if ((fork_pid = fork()) < 0) {
			stglogit(func, "### Cannot fork (%s)\n",strerror(errno));
			return;
		}
		if (fork_pid == 0) {
			/* We are in the child */
			struct stgcat_entry thisstcp = *stcp;

			stglogit (func, "execing stageclr, pid=%d\n", getpid());

			/* No need to waste memory inherited by parent anymore */
			if (stcs != NULL) free(stcs); stcs = NULL;
			if (stps != NULL) free(stps); stps = NULL;
			
			rfio_mstat_reset();  /* Reset permanent RFIO stat connections */
			rfio_munlink_reset(); /* Reset permanent RFIO unlink connections */

			if (stage_setlog((void (*) _PROTO((int, char *))) &stageclr_log_callback) != 0) {
				stglogit(func, "### stage_setlog error (%s)\n",sstrerror(serrno));
				exit(SYERR);
			}

			if (stage_clr_Path((u_signed64) STAGE_NOLINKCHECK, NULL, thisstcp.ipath) != 0) {
				exit(rc_castor2shift(serrno));
			}
			exit(0);
			/* Our exit status will be catched by main process */
		}
		stglogit(func, STG177,
				 stcp->t_or_d == 'h' ? stcp->u1.h.xfile :
				 (stcp->t_or_d == 'm' ? stcp->u1.m.xfile :
				  (stcp->t_or_d == 'a' ? stcp->u1.d.xfile :
				   (stcp->t_or_d == 'd' ? stcp->u1.d.xfile :
					stcp->ipath))), u64tostr((u_signed64) thisretenp, tmpbuf, 0));

		stcp->filler[0] = 'Z';
	}
	return;
}

int ispool_out(poolname)
	char *poolname;
{
	char *p;

	if (poolname[0] == '\0') {
		/* No point */
		return(-1);
	}

	/* defpoolname_out could countain more than one entry, all separated with a ':' */
	/* We search poolname */
	if ((p = strstr(defpoolname_out, poolname)) == NULL) {
		return(-1);
	}

	/* We accept it only if it is: */
	/* - at the start of defpoolname_out and ends with an ':' or an '\0' */
	/* - not at the start and is preceded with an ':' and ends with an ':' or an '\0' */
	if ((p == defpoolname_out) && ((p[strlen(p)] == ':') || (p[strlen(p)] == '\0'))) return(0);
	if ((p != defpoolname_out) && (*(p - 1) == ':') && ((p[strlen(p)] == ':') || (p[strlen(p)] == '\0'))) return(0);

	/* No... */
	return(-1);
}

/* We return with this routine the best stageout pool we could select v.s. migration activity or not */
/* and v.s. estimated contention */
/* mode is READ_MODE for reading, WRITE_MODE for writing */

void bestnextpool_out(nextout,mode)
	char *nextout;
	int mode;
{
	char firstpool_out[CA_MAXPOOLNAMELEN+1];
	char thispool_out[CA_MAXPOOLNAMELEN+1];
	char sav_thispool_out[CA_MAXPOOLNAMELEN+1];
	struct pool_element *this_element;
	struct pool_element_ext *best_elements = NULL;
	struct pool_element_ext *this_best_element;
	struct pool_element *found_element = NULL;
	int nbest_elements = 0;
	int i, j;
	struct pool *pool_p;
	struct pool_element *elemp;
	char *func = "bestnextpool_out";
#ifdef STAGER_DEBUG
	char tmpbuf[21];
	char tmpbuf2[21];
#endif
	char timestr[64] ;   /* Time in its ASCII format             */


	/* We loop until we find a poolout that have no migrator, the bigger space available, the less */
	/* contention possible */

	firstpool_out[0] = '\0';
	nextpool_out(firstpool_out);
#ifdef STAGER_DEBUG
	stglogit(func, "first poolout is %s\n", firstpool_out);
#endif
	strcpy(thispool_out, firstpool_out);

	while (1) {
		strcpy(sav_thispool_out,thispool_out);
		if ((this_element = betterfs_vs_pool(thispool_out,mode,0,NULL)) == NULL) {
			/* Oups... Tant-pis for that pool */
			goto nextpool;
		}

		if (nbest_elements == 0) {
			if ((this_best_element = best_elements = (struct pool_element_ext *) malloc(sizeof(struct pool_element_ext))) == NULL) {
				/* Oups... Tant-pis for that pool */
				stglogit(func, STG33, "malloc", strerror(errno));
				goto nextpool;
			}
		} else {
			struct pool_element_ext *dummy;

			if ((dummy = (struct pool_element_ext *) realloc(best_elements, (nbest_elements + 1) * sizeof(struct pool_element_ext))) == NULL) {
				/* Oups... Tant-pis for that pool */
				stglogit(func, STG33, "realloc", strerror(errno));
				goto nextpool;
			}
			best_elements = dummy;
			this_best_element = best_elements;
			this_best_element += nbest_elements;
		}
		this_best_element->free = this_element->free;
		this_best_element->capacity = this_element->capacity;
		if (((mode == WRITE_MODE) && (selectfs_rw_with_nb_stream != 0)) ||
			((mode == READ_MODE ) && (selectfs_ro_with_nb_stream != 0))) {
			this_best_element->nbreadaccess = this_element->nbreadaccess;
			this_best_element->nbwriteaccess = this_element->nbwriteaccess;
		} else {
			this_best_element->nbreadaccess = 0;
			this_best_element->nbwriteaccess = 0;
		}
		this_best_element->mode = mode;
		this_best_element->nbreadserver = 0;
		this_best_element->nbwriteserver = 0;
		this_best_element->poolmigrating = ispoolmigrating(currentpool_out);
		this_best_element->server[0] = '\0';
		strcpy(this_best_element->server,this_element->server);
		this_best_element->dirpath[0] = '\0';
		strcpy(this_best_element->dirpath,this_element->dirpath);
		/* To select the best pool we use the global pool last_allocation timestamp */
		this_best_element->last_allocation = pool_last_allocation(currentpool_out);
		/* We will use POOL's DEFSIZE to try to get rid of those the ones that are */
		/* definitely too low on space */
		getdefsize(currentpool_out,&(this_best_element->defsize));
#ifdef STAGER_DEBUG
		if (this_best_element->last_allocation > 0) {
			stage_util_time(this_best_element->last_allocation,timestr);
		} else {
			strcpy(timestr,"<none>");
		}
		stglogit(func, "Last allocation of pool %s is %s\n", currentpool_out, timestr);
#endif
		nbest_elements++;

	  nextpool:
		/* Go to the next pool unless round about or same pool returned */
		nextpool_out(thispool_out);
#ifdef STAGER_DEBUG
		stglogit(func, "next poolout is %s\n", thispool_out);
#endif
		if ((strcmp(thispool_out, firstpool_out) == 0) || (strcmp(thispool_out, sav_thispool_out) == 0)) {
			/* stglogit(func, "Turnaround reached at outpool %s\n", thispool_out); */
			break;
		}
	}

	if (nbest_elements <= 0) {
		/* Nothing !? - So return the first defpool out (there is always one) */
		firstpool_out[0] = '\0';
		nextpool_out(firstpool_out);
#ifdef STAGER_DEBUG
		stglogit(func, "Nothing... Return first poolout: %s\n", firstpool_out);
#endif
		strcpy(nextout, firstpool_out);
		if (best_elements != NULL) free(best_elements);
		return;
	}

	if (nbest_elements > 1) {
		/* We are doing again the qsort using the best fs v.s. all poolouts */

		/* We fill the global number of streams per server, used in the qsort to optimize server location */
		/* This time we overwrite last_allocation with the real last allocation per filesystem */
		for (i = 0, this_best_element = best_elements; i < nbest_elements; i++, this_best_element++) {
			time_t sav_last_allocation;
			if (((mode == WRITE_MODE) && (selectfs_rw_with_nb_stream != 0)) ||
				((mode == READ_MODE ) && (selectfs_ro_with_nb_stream != 0))) {
				get_global_stream_count(this_best_element->server, &(this_best_element->nbreadserver), &(this_best_element->nbwriteserver));
			}
			sav_last_allocation = this_best_element->last_allocation;
#ifdef STAGER_DEBUG
			if (this_best_element->last_allocation > 0) {
				stage_util_time(this_best_element->last_allocation,timestr);
			} else {
				strcpy(timestr,"<none>");
			}
			stglogit(func, "Last allocation of %s:%s's pool is %s\n", this_best_element->server, this_best_element->dirpath, timestr);
#endif
			this_best_element->last_allocation = elemp_last_allocation(this_best_element->server,this_best_element->dirpath);
#ifdef STAGER_DEBUG
			if (this_best_element->last_allocation > 0) {
				stage_util_time(this_best_element->last_allocation,timestr);
			} else {
				strcpy(timestr,"<none>");
			}
			stglogit(func, "Last allocation of %s:%s itself is %s\n", this_best_element->server, this_best_element->dirpath, timestr);
#endif
			if (this_best_element->last_allocation <= 0) {
				/* That file system was not yet allocated but perhaps its pool was (if yes, it was used in the sort upper) */
				this_best_element->last_allocation = sav_last_allocation;
			}
#ifdef STAGER_DEBUG
			if (this_best_element->last_allocation > 0) {
				stage_util_time(this_best_element->last_allocation,timestr);
			} else {
				strcpy(timestr,"<none>");
			}
			stglogit(func, "Last allocation for qsort of %s:%s setted to %s\n", this_best_element->server, this_best_element->dirpath, timestr);
#endif
		}
    
		/* Sort them */
		qsort((void *) best_elements, nbest_elements, sizeof(struct pool_element_ext), &pool_elements_cmp_simple);

#ifdef STAGER_DEBUG
		for (j = 0, this_best_element = best_elements; j < nbest_elements; j++, this_best_element++) {
			if (this_best_element->last_allocation > 0) {
				stage_util_time(this_best_element->last_allocation,timestr);
			} else {
				strcpy(timestr,"<none>");
			}
			stglogit(func, "rank %2d: %s %s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s (pool)last_allocation=%s\n",
					 j,
					 this_best_element->server,
					 this_best_element->dirpath,
					 this_best_element->nbreadaccess,
					 this_best_element->nbwriteaccess,
					 this_best_element->nbreadserver,
					 this_best_element->nbwriteserver,
					 this_best_element->poolmigrating,
					 u64tostru(this_best_element->free, tmpbuf, 0),
					 u64tostru(this_best_element->capacity, tmpbuf2, 0),
					 timestr
				);
		}
#endif
	} else {
#ifdef STAGER_DEBUG
		if (this_best_element->last_allocation > 0) {
			stage_util_time(this_best_element->last_allocation,timestr);
		} else {
			strcpy(timestr,"<none>");
		}
		stglogit(func, "only one element: %10s %30s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s (pool)last_allocation=%s\n",
				 this_best_element->server,
				 this_best_element->dirpath,
				 this_best_element->nbreadaccess,
				 this_best_element->nbwriteaccess,
				 this_best_element->nbreadserver,
				 this_best_element->nbwriteserver,
				 this_best_element->poolmigrating,
				 u64tostru(this_best_element->free, tmpbuf, 0),
				 u64tostru(this_best_element->capacity, tmpbuf2, 0),
				 timestr
			);
#endif
	}

	/* So we have found here the best next pool */
	/* We anticipate the coming call to selectfs() with the following trick: */
	/* - we know that selectfs() takes poolname as first argument and pool_p->next_pool_elem */
	/* as the fs to start the scanning with */
	/* We thus ly to pool_p->next_pool_elem, simply */

	/* Grab the result - this is the one on the top of the list */
	this_best_element = best_elements;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if ((strcmp(this_best_element->server ,elemp->server) == 0) &&
				(strcmp(this_best_element->dirpath,elemp->dirpath) == 0)) {
				found_element = elemp;
				/* Here is the trick */
				strcpy(nextout, pool_p->name);
				pool_p->next_pool_elem = j;
				break;
			}
		}
	}

	if (found_element == NULL) {
		/* Something fishy has happened ? */
		stglogit(func, "something fishy happened - using %s\n", thispool_out);
		strcpy(nextout, thispool_out);
	} else {
		if (this_best_element->last_allocation > 0) {
			stage_util_time(this_best_element->last_allocation,timestr);
		} else {
			strcpy(timestr,"<none>");
		}
		/*
		  stglogit(func, "selected element: %s %s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s last_allocation=%s\n",
		  this_best_element->server,
		  this_best_element->dirpath,
		  this_best_element->nbreadaccess,
		  this_best_element->nbwriteaccess,
		  this_best_element->nbreadserver,
		  this_best_element->nbwriteserver,
		  this_best_element->poolmigrating,
		  u64tostru(this_best_element->free, tmpbuf, 0),
		  u64tostru(this_best_element->capacity, tmpbuf2, 0),
		  timestr
		  );
		*/
	}

	if (best_elements != NULL) free(best_elements);
}

void nextpool_out(nextout)
	char *nextout;
{
	char *p;
	int first_time = 0;
#ifdef STAGER_DEBUG
	char *func = "nextpool_out";
#endif

#ifdef STAGER_DEBUG
	stglogit(func, "Called with argument %s\n", nextout);
	stglogit(func, "Current pool out is %s\n", currentpool_out);
#endif

	/* We check if current pool out belongs to the poolout list */
	if (ispool_out(currentpool_out) != 0) {
		/* Either very first call, either list of poolout has changed and current poolout from */
		/* last call does not anymore belong to this list */
		currentpool_out[0] = '\0';
#ifdef STAGER_DEBUG
		stglogit(func, "Changed current pool out to %s\n", currentpool_out);
#endif
	}

	if (currentpool_out[0] == '\0') {
		first_time = 1;
	  first_nextpool:
		/* We return the first one in the list up to first possible ':' delimiter */
		if ((p = strchr(defpoolname_out, ':')) != NULL) {
			*p = '\0';
			strcpy(nextout, defpoolname_out);
			*p = ':';
		} else {
			strcpy(nextout, defpoolname_out);
		}
		strcpy (currentpool_out, nextout);
#ifdef STAGER_DEBUG
		stglogit(func, "[1] Return pool %s\n", nextout);
#endif
		return;
	} else {
		char *p2;

		/* If current pool is not migrating, we still return it */
		/*
		if (ispoolmigrating(currentpool_out) == 0) {
			strcpy(nextout, currentpool_out);
#ifdef STAGER_DEBUG
			stglogit(func, "[2] Return pool %s\n", nextout);
#endif
			return;
		}
		*/
#ifdef STAGER_DEBUG
		stglogit(func, "[3] Searching pool after %s\n", currentpool_out);
#endif
		/* We return the next one in the list */
		if ((p = strstr(defpoolname_out, currentpool_out)) == NULL) {
			/* Current pool not in the list ? This should not happen because of ispool_out() call before */
			stglogit("nextpool_out", STG32, currentpool_out);
			goto first_nextpool;
		}
		/* We check if there is another one after */
		p += strlen(currentpool_out);
		if (p[0] == '\0') {
			/* We are the end - we go back to the first one */
			goto first_nextpool;
		}
		if ((p2 = strchr(++p, ':')) != NULL) {
			/* We are not at the end */
			*p2 = '\0';
			strcpy(nextout, p);
			strcpy (currentpool_out, nextout);
			*p2 = ':';
		} else {
			/* We are at the end... */
			strcpy(nextout, p);
			strcpy (currentpool_out, nextout);
		}
	}
#ifdef STAGER_DEBUG
	stglogit(func, "[2] Return pool %s\n", nextout);
#endif
	return;
}

/* We return with this routine the best stagein pool we could select v.s. migration activity or not */
/* and v.s. estimated contention */
/* It is called only when accessing a file in API mode with O_RDONLY and giving a metapool name */

void bestnextpool_from_metapool(metapool,bestpool,mode)
	char *metapool;
	char *bestpool;
	int mode;
{
	struct pool_element *this_element;
	struct pool_element_ext *best_elements = NULL;
	struct pool_element_ext *this_best_element;
	struct pool_element *found_element = NULL;
	int nbest_elements = 0;
	int i, j;
	struct pool *pool_p;
	struct pool_element *elemp;
	char *func = "bestnextpool_from_metapool";
#ifdef STAGER_DEBUG
	char tmpbuf[21];
	char tmpbuf2[21];
#endif
	char timestr[64] ;   /* Time in its ASCII format             */

	/* We fill best_elements[] with the list of filesystems in pools that have metapool in their definition */
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp(pool_p->metapool,metapool) == 0) {
			struct pool_element *elemp;
			for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
				if (nbest_elements == 0) {
					if ((this_best_element = best_elements = (struct pool_element_ext *) malloc(sizeof(struct pool_element_ext))) == NULL) {
						/* Oups... Tant-pis for that element */
						stglogit(func, STG33, "malloc", strerror(errno));
						continue;
					}
				} else {
					struct pool_element_ext *dummy;
					
					if ((dummy = (struct pool_element_ext *) realloc(best_elements, (nbest_elements + 1) * sizeof(struct pool_element_ext))) == NULL) {
						/* Oups... Tant-pis for that element */
						stglogit(func, STG33, "realloc", strerror(errno));
						continue;
					}
					best_elements = dummy;
					this_best_element = best_elements;
					this_best_element += nbest_elements;
				}
				this_best_element->free = elemp->free;
				this_best_element->capacity = elemp->capacity;
				if (selectfs_ro_with_nb_stream != 0) {
					this_best_element->nbreadaccess = elemp->nbreadaccess;
					this_best_element->nbwriteaccess = elemp->nbwriteaccess;
				} else {
					this_best_element->nbreadaccess = 0;
					this_best_element->nbwriteaccess = 0;
				}
				this_best_element->mode = mode;
				this_best_element->nbreadserver = 0;
				this_best_element->nbwriteserver = 0;
				this_best_element->poolmigrating = ispoolmigrating(pool_p->name);
				this_best_element->server[0] = '\0';
				strcpy(this_best_element->server,elemp->server);
				this_best_element->dirpath[0] = '\0';
				strcpy(this_best_element->dirpath,elemp->dirpath);
				/* To select the best pool we use the global pool last_allocation timestamp */
				this_best_element->last_allocation = pool_last_allocation(pool_p->name);
				/* We will use POOL's DEFSIZE to try to get rid of those the ones that are */
				/* definitely too low on space */
				getdefsize(currentpool_out,&(this_best_element->defsize));
				nbest_elements++;
			}
		}
	}

	if (nbest_elements <= 0) {
		/* Nothing !? - So return the first defpool in (there is always one) */
		strcpy(bestpool,defpoolname_in);
#ifdef STAGER_DEBUG
		stglogit(func, "Nothing... Return first defpoolname_in: %s\n", defpoolname_in);
#endif
		if (best_elements != NULL) free(best_elements);
		return;
	}

	if (nbest_elements > 1) {
		/* We are doing a qsort using the best fs v.s. all poolins */
		
		/* We fill the global number of streams per server, used in the qsort to optimize server location */
		for (i = 0, this_best_element = best_elements; i < nbest_elements; i++, this_best_element++) {
			if (selectfs_ro_with_nb_stream != 0) {
				get_global_stream_count(this_best_element->server, &(this_best_element->nbreadserver), &(this_best_element->nbwriteserver));
			}
		}
    
		/* Sort them */
		qsort((void *) best_elements, nbest_elements, sizeof(struct pool_element_ext), &pool_elements_cmp_simple);
		
#ifdef STAGER_DEBUG
		for (j = 0, this_best_element = best_elements; j < nbest_elements; j++, this_best_element++) {
			if (this_best_element->last_allocation > 0) {
				stage_util_time(this_best_element->last_allocation,timestr);
			} else {
				strcpy(timestr,"<none>");
			}
			stglogit(func, "rank %2d: %s %s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s last_allocation=%s\n",
					 j,
					 this_best_element->server,
					 this_best_element->dirpath,
					 this_best_element->nbreadaccess,
					 this_best_element->nbwriteaccess,
					 this_best_element->nbreadserver,
					 this_best_element->nbwriteserver,
					 this_best_element->poolmigrating,
					 u64tostru(this_best_element->free, tmpbuf, 0),
					 u64tostru(this_best_element->capacity, tmpbuf2, 0),
					 timestr
				);
		}
#endif
	} else {
#ifdef STAGER_DEBUG
		if (this_best_element->last_allocation > 0) {
			stage_util_time(this_best_element->last_allocation,timestr);
		} else {
			strcpy(timestr,"<none>");
		}
		stglogit(func, "only one element: %10s %30s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s last_allocation=%s\n",
				 this_best_element->server,
				 this_best_element->dirpath,
				 this_best_element->nbreadaccess,
				 this_best_element->nbwriteaccess,
				 this_best_element->nbreadserver,
				 this_best_element->nbwriteserver,
				 this_best_element->poolmigrating,
				 u64tostru(this_best_element->free, tmpbuf, 0),
				 u64tostru(this_best_element->capacity, tmpbuf2, 0),
				 timestr
			);
#endif
	}

	/* So we have found here the best fs */
	/* We anticipate the coming call to selectfs() with the following trick: */
	/* - we know that selectfs() takes poolname as first argument and pool_p->next_pool_elem */
	/* as the fs to start the scanning with */
	/* We thus ly to pool_p->next_pool_elem, simply */

	/* Grab the result - this is the one on the top of the list */
	this_best_element = best_elements;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if ((strcmp(this_best_element->server ,elemp->server) == 0) &&
				(strcmp(this_best_element->dirpath,elemp->dirpath) == 0)) {
				found_element = elemp;
				/* Here is the trick */
				strcpy(bestpool, pool_p->name);
				pool_p->next_pool_elem = j;
				break;
			}
		}
	}

	if (found_element == NULL) {
		/* Something fishy has happened ? */
		stglogit(func, "something fishy happened - using %s\n", metapool);
	} else {
		if (this_best_element->last_allocation > 0) {
			stage_util_time(this_best_element->last_allocation,timestr);
		} else {
			strcpy(timestr,"<none>");
		}
#ifdef STAGER_DEBUG
		  stglogit(func, "selected element: %s %s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s last_allocation=%s\n",
		  this_best_element->server,
		  this_best_element->dirpath,
		  this_best_element->nbreadaccess,
		  this_best_element->nbwriteaccess,
		  this_best_element->nbreadserver,
		  this_best_element->nbwriteserver,
		  this_best_element->poolmigrating,
		  u64tostru(this_best_element->free, tmpbuf, 0),
		  u64tostru(this_best_element->capacity, tmpbuf2, 0),
		  timestr
		  );
#endif
	}

	if (best_elements != NULL) free(best_elements);
}

int ispoolmigrating(poolname)
	char *poolname;
{
	int i;
	int found;
	struct pool *pool_p;

	found = 0;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) {
			found = 1;
			break;
		}
  
	if ((found != 0) && (pool_p->migr != NULL)) {
		return(pool_p->migr->mig_pid != 0 ? 1 : 0);       /* Will be != 0 if there is a migrator running */
	} else {
		return(0);
	}
}

time_t pool_last_allocation(poolname)
	char *poolname;
{
	int i;
	struct pool *pool_p;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (strcmp (poolname, pool_p->name) == 0) {
			return(pool_p->last_allocation);
		}
	}
  
	return((time_t) 0);
}

time_t server_last_allocation(server)
	char *server;
{
	int i, j;
	struct pool *pool_p;
	struct pool_element *elemp;
	time_t result = 0;
	
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if (strcmp(elemp->server, server) == 0 &&
				elemp->last_allocation > result) {
				result = elemp->last_allocation;
			}
		}
	}
  
	return(result);
}

time_t elemp_last_allocation(server,dirpath)
	char *server;
	char *dirpath;
{
	int i, j;
	struct pool *pool_p;
	struct pool_element *elemp;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if (strcmp(elemp->server, server) == 0 &&
				strcmp(elemp->dirpath,dirpath)) {
				return(elemp->last_allocation);
			}
		}
	}
  
	return((time_t) 0);
}

/* mode is READ_MODE for reading, WRITE_MODE for writing, if req_size is specified (like in a selectfs() retry) */
/* it used as a threshold filter */

struct pool_element *betterfs_vs_pool(poolname,mode,reqsize,index)
	char *poolname;
	int mode;
	u_signed64 reqsize;
	int *index;
{
	int i, j, jfound;
	int found;
	struct pool *pool_p;
	struct pool_element *rc = NULL;
	struct pool_element_ext *elements;
	char *func = "betterfs_vs_pool";
#ifdef STAGER_DEBUG
	char tmpbuf[21];
	char tmpbuf2[21];
	char tmpbufreqsize[21];
#endif
#ifdef STAGER_DEBUG
	char timestr[64] ;   /* Time in its ASCII format             */
#endif

	found = 0;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) {
			found = 1;
			break;
		}

	if (found == 0) {
		/* Hmmm... Pool not found! */
		stglogit(func, STG32, poolname);
		return(NULL);
	}

	/* We prepare the coming qsort */
	if ((elements = (struct pool_element_ext *) malloc(pool_p->nbelem * sizeof(struct pool_element_ext))) == NULL) {
		stglogit(func, STG02, poolname, "malloc", strerror(errno));
		return(NULL);
	}

	for (i = 0; i < pool_p->nbelem; i++) {
		elements[i].free = pool_p->elemp[i].free;
		if (((mode == WRITE_MODE) && (selectfs_rw_with_nb_stream != 0)) ||
			((mode == READ_MODE ) && (selectfs_ro_with_nb_stream != 0))) {
			elements[i].nbreadaccess = pool_p->elemp[i].nbreadaccess;
			elements[i].nbwriteaccess = pool_p->elemp[i].nbwriteaccess;
		} else {
			elements[i].nbreadaccess = 0;
			elements[i].nbwriteaccess = 0;
		}
		elements[i].mode = mode;
		elements[i].nbreadserver = 0;
		elements[i].nbwriteserver = 0;
		elements[i].poolmigrating = ispoolmigrating(poolname); /* Not usefull in this sort, but pretty for output */
		strcpy(elements[i].server,pool_p->elemp[i].server);
		strcpy(elements[i].dirpath,pool_p->elemp[i].dirpath);
		elements[i].last_allocation = pool_p->elemp[i].last_allocation;
		elements[i].server_last_allocation = server_last_allocation(pool_p->elemp[i].server);
	}

	/* We fill the global number of streams per server, used in the qsort to optimize server location */
	for (i = 0; i < pool_p->nbelem; i++) {
		if (((mode == WRITE_MODE) && (selectfs_rw_with_nb_stream != 0)) ||
			((mode == READ_MODE ) && (selectfs_ro_with_nb_stream != 0))) {
			get_global_stream_count(elements[i].server, &(elements[i].nbreadserver), &(elements[i].nbwriteserver));
		}
	}
    
	/* Sort them */
	qsort((void *) elements, pool_p->nbelem, sizeof(struct pool_element_ext), &pool_elements_cmp);

	jfound = -1;
	for (j = 0; j < pool_p->nbelem; j++) {
		if (elements[j].free <= 0) continue;
		if (reqsize <= 0) {
#ifdef STAGER_DEBUG
			if (elements[j].last_allocation > 0) {
				stage_util_time(elements[j].last_allocation,timestr);
			} else {
				strcpy(timestr,"<none>");
			}
			stglogit(func, "rank %2d: %s %s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s last_allocation=%s\n",
					 j,
					 elements[j].server,
					 elements[j].dirpath,
					 elements[j].nbreadaccess,
					 elements[j].nbwriteaccess,
					 elements[j].nbreadserver,
					 elements[j].nbwriteserver,
					 elements[j].poolmigrating,
					 u64tostru(elements[j].free, tmpbuf, 0),
					 u64tostru(elements[j].capacity, tmpbuf2, 0),
					 timestr
				);
#endif
			if (jfound < 0) jfound = j;
		} else {
			if (elements[j].free < reqsize) {
#ifdef STAGER_DEBUG
				if (elements[j].last_allocation > 0) {
					stage_util_time(elements[j].last_allocation,timestr);
				} else {
					strcpy(timestr,"<none>");
				}
				stglogit(func, "[reqsize=%s => rejected] rank %2d: %s %s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s last_allocation=%s\n",
						 u64tostru(reqsize, tmpbufreqsize, 0),
						 j,
						 elements[j].server,
						 elements[j].dirpath,
						 elements[j].nbreadaccess,
						 elements[j].nbwriteaccess,
						 elements[j].nbreadserver,
						 elements[j].nbwriteserver,
						 elements[j].poolmigrating,
						 u64tostru(elements[j].free, tmpbuf, 0),
						 u64tostru(elements[j].capacity, tmpbuf2, 0),
						 timestr
					);
#endif
			} else {
#ifdef STAGER_DEBUG
				if (elements[j].last_allocation > 0) {
					stage_util_time(elements[j].last_allocation,timestr);
				} else {
					strcpy(timestr,"<none>");
				}
				stglogit(func, "[reqsize=%s => accepted] rank %2d: %s %s read=%d write=%d readserver=%d writeserver=%d poolmigrating=%d free=%s capacity=%s last_allocation=%s\n",
						 u64tostru(reqsize, tmpbufreqsize, 0),
						 j,
						 elements[j].server,
						 elements[j].dirpath,
						 elements[j].nbreadaccess,
						 elements[j].nbwriteaccess,
						 elements[j].nbreadserver,
						 elements[j].nbwriteserver,
						 elements[j].poolmigrating,
						 u64tostru(elements[j].free, tmpbuf, 0),
						 u64tostru(elements[j].capacity, tmpbuf2, 0),
						 timestr
					);
#endif
				if (jfound < 0) jfound = j;
			}
		}
	}

	if (jfound >= 0) {
		/* Grab the result */
		for (i = 0; i < pool_p->nbelem; i++) {
			if ((strcmp(elements[jfound].server ,pool_p->elemp[i].server) == 0) &&
				(strcmp(elements[jfound].dirpath,pool_p->elemp[i].dirpath) == 0)) {
				rc = &(pool_p->elemp[i]);
				if (index != NULL) {
					*index = i;
				}
				break;
			}
		}
	}

	/* Free space */
	free(elements);

	/* And return the element address */
	return(rc);
}

int pool_elements_cmp(p1,p2)
	CONST void *p1;
	CONST void *p2;
{
	struct pool_element_ext *fp1 = (struct pool_element_ext *) p1;
	struct pool_element_ext *fp2 = (struct pool_element_ext *) p2;
#ifdef STAGER_DEBUG
	char *func = "pool_elements_cmp";
#endif

	/* Versus mode, we first compare nbreadaccess and nbwriteaccess */
	/* We sort with reverse order v.s. size */
	/* mode is READ_MODE for reading, WRITE_MODE for writing */
	/* Note that by construction all the elements shares the same mode in the qsort() */

	/* If one has a associated migration (where it is accessed or not) and not the other, no doubt */
	if (fp1->poolmigrating != fp2->poolmigrating) {
		if (fp2->poolmigrating != 0) {
			return(-1);
		} else {
			return(1);
		}
	}

	/* Note: fp1->mode is always equal to fp2->mode per construction */

	if ((strcmp(fp1->server,fp2->server) == 0) ||
		((fp1->mode == WRITE_MODE) && (selectfs_rw_with_nb_stream != 0) && (fp1->nbreadserver + fp1->nbwriteserver) == (fp2->nbreadserver + fp2->nbwriteserver)) ||
		((fp1->mode == READ_MODE)  && (selectfs_ro_with_nb_stream != 0) && (fp1->nbreadserver + fp1->nbwriteserver) == (fp2->nbreadserver + fp2->nbwriteserver))
		) {

		if (fp1->mode == WRITE_MODE) {

			/* Write mode */

			/* If selectfs_rw_with_nb_stream is disabled we go here only if machine is the same */
			/* And per def fp1->nbreadaccess = fp2->nbreadaccess = fp1->nbwriteaccess = fp2->nbwriteaccess = 0 */
			/* So will will always do comparison v.s. last_alloc and then v.s. free space */
			
			if (fp1->nbreadaccess == fp2->nbreadaccess) {

				if (fp1->nbwriteaccess == fp2->nbwriteaccess) {

				  pool_elements_cmp_vs_alloc:
					
					/* We compare last known allocation timestamp */
					if (fp1->last_allocation < fp2->last_allocation) {
						/* filesystem (or its pool) fp1 had a successful space allocation that is older */
						/* than filesystem (or its pool) fp2 */
						return(-1);
					} else if (fp1->last_allocation > fp2->last_allocation) {
						return(1);
					} else {
						/* Low probability ... */
#ifdef STAGER_DEBUG
						{
							char tmpbuf1[21];
							char tmpbuf2[21];
							stglogit(func,"cmp free space of (%s:%s v.s. %s:%s) : (%s v.s. %s)\n",
									 fp1->server,
									 fp1->dirpath,
									 fp2->server,
									 fp2->dirpath,
									 u64tostr(fp1->free, tmpbuf1, 0),
									 u64tostr(fp2->free, tmpbuf2, 0)
								);
						}
#endif
						/* We compare v.s. free space */
						if (fp1->free > fp2->free) {
							return(-1);
						} else if (fp1->free < fp2->free) {
							return(1);
						} else {
							return(0);
						}
					}

				} else {

					if (fp1->nbwriteaccess < fp2->nbwriteaccess) {
						return(-1);
					} else {
						return(1);
					}

				}

			} else {

				if ((fp1->nbwriteaccess + fp1->nbreadaccess) == (fp2->nbwriteaccess + fp2->nbreadaccess)) {

					goto pool_elements_cmp_vs_alloc;

				} else if ((fp1->nbwriteaccess + fp1->nbreadaccess) < (fp2->nbwriteaccess + fp2->nbreadaccess)) {

					return(-1);

				} else {

					return(1);
				}

			}

		} else {

			/* Read mode */

			/* If selectfs_ro_with_nb_stream is disabled we go here only if machine is the same */
			/* And per def fp1->nbreadaccess = fp2->nbreadaccess = fp1->nbwriteaccess = fp2->nbwriteaccess = 0 */
			/* Se will will always do comparison v.s. last_alloc and then v.s. free space */
			
			if (fp1->nbwriteaccess == fp2->nbwriteaccess) {

				if (fp1->nbreadaccess == fp2->nbreadaccess) {

					goto pool_elements_cmp_vs_alloc;

				} else {

					if (fp1->nbreadaccess < fp2->nbreadaccess) {
						return(-1);
					} else {
						return(1);
					}

				}

			} else {

				if ((fp1->nbreadaccess + fp1->nbwriteaccess) == (fp2->nbreadaccess + fp2->nbwriteaccess)) {

					goto pool_elements_cmp_vs_alloc;

				} else if ((fp1->nbreadaccess + fp1->nbwriteaccess) < (fp2->nbreadaccess + fp2->nbwriteaccess)) {

					return(-1);

				} else {

					return(1);
				}

			}

		}

	} else {

		/* Not the same host */

		if ((fp1->nbreadserver + fp1->nbwriteserver) == (fp2->nbreadserver + fp2->nbwriteserver)) {
      
			/* We compare last known server allocation timestamp */
			if (fp1->server_last_allocation < fp2->server_last_allocation) {
				/* filesystem (or its pool) fp1 had a successful space allocation that is older */
				/* than filesystem (or its pool) fp2 */
				return(-1);
			} else if (fp1->server_last_allocation > fp2->server_last_allocation) {
				return(1);
			} else {
				/* Low probability ... */
#ifdef STAGER_DEBUG
				{
					char tmpbuf1[21];
					char tmpbuf2[21];
					stglogit(func,"cmp free space of (%s:%s v.s. %s:%s) : (%s v.s. %s)\n",
							 fp1->server,
							 fp1->dirpath,
							 fp2->server,
							 fp2->dirpath,
							 u64tostr(fp1->free, tmpbuf1, 0),
							 u64tostr(fp2->free, tmpbuf2, 0)
						);
				}
#endif
				/* We compare v.s. free space */
				if (fp1->free > fp2->free) {
					return(-1);
				} else if (fp1->free < fp2->free) {
					return(1);
				} else {
					return(0);
				}
			}
      
		} else if ((fp1->nbreadserver + fp1->nbwriteserver) < (fp2->nbreadserver + fp2->nbwriteserver)) {
      
			return(-1);
      
		} else {
      
			return(1);
		}

	}

}

/* Version of pool_elements_cmp based only on last allocation to promote load-balancing between pools */
int pool_elements_cmp_simple(p1,p2)
	CONST void *p1;
	CONST void *p2;
{
	struct pool_element_ext *fp1 = (struct pool_element_ext *) p1;
	struct pool_element_ext *fp2 = (struct pool_element_ext *) p2;

	/* Versus mode, we first compare nbreadaccess and nbwriteaccess */
	/* We sort with reverse order v.s. size */
	/* mode is READ_MODE for reading, WRITE_MODE for writing */
	/* Note that by construction all the elements shares the same mode in the qsort() */

	if ((fp1->free > fp1->defsize) && (fp2->free > fp2->defsize)) {
		/* Both have more space than their defsize */
		/* We compare last known allocation timestamp */
	  pool_elements_cmp_simple_vs_alloc:
		if (fp1->last_allocation < fp2->last_allocation) {
			/* filesystem (or its pool) fp1 had a successful space allocation that is older */
			/* than filesystem (or its pool) fp2 */
			return(-1);
		} else if (fp1->last_allocation > fp2->last_allocation) {
			return(1);
		} else {
			return(0);
		}
	} else {
		/* One at least have a problem of space: we select the one that have the more space */
		if (fp1->free > fp2->free) {
			return(-1);
		} else if (fp1->free < fp2->free) {
			return(1);
		} else {
			/* Low probability ... */
			/* We go back comparing last known allocation timestamp... */
			goto pool_elements_cmp_simple_vs_alloc;
		}
	}
}

void get_global_stream_count(server,nbreadserver,nbwriteserver)
	char *server;
	int *nbreadserver;
	int *nbwriteserver;
{
	struct pool_element *elemp;
	struct pool *pool_p;
	int i, j;

	*nbreadserver = 0;
	*nbwriteserver = 0;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			if (strcmp(elemp->server, server) != 0) continue;
			*nbreadserver += elemp->nbreadaccess;
			*nbwriteserver += elemp->nbwriteaccess;
		}
	}
}

#ifndef _WIN32
Sigfunc *_stage_signal(signo, func)
	int signo;
	Sigfunc *func;
{
	struct sigaction	act, oact;
	int n = 0;

	act.sa_handler = func;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	if (signo == SIGALRM) {
#ifdef	SA_INTERRUPT
		act.sa_flags |= SA_INTERRUPT;	/* SunOS 4.x */
#endif
	} else {
#ifdef	SA_RESTART
		act.sa_flags |= SA_RESTART;		/* SVR4, 44BSD */
#endif
	}
	n = sigaction(signo, &act, &oact);
	if (n < 0) {
		return(SIG_ERR);
	}
	return(oact.sa_handler);
}
#endif /* #ifndef _WIN32 */

void stage_stgcleanup(sig)
	int sig;
{
	stage_kill(sig);
	exit(USERR);
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
			return((signed64) pool_p->stageout_retenp);
	return ((signed64) -1);
}

signed64 get_stagealloc_retenp(stcp)
	struct stgcat_entry *stcp;
{
	int i;
	struct pool *pool_p;

	if ((stcp->t_or_d == 'h') && (stcp->u1.h.retenp_on_disk >= 0))
		return((signed64) stcp->u1.h.retenp_on_disk);
	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (stcp->poolname, pool_p->name) == 0) 
			return((signed64) pool_p->stagealloc_retenp);
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
			return((signed64) pool_p->put_failed_retenp);
	return ((signed64) -1);
}

signed64 get_stageout_retenp_raw(stcp)
	struct stgcat_entry *stcp;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (stcp->poolname, pool_p->name) == 0) 
			return((signed64) pool_p->stageout_retenp);
	return ((signed64) -1);
}

signed64 get_stagealloc_retenp_raw(stcp)
	struct stgcat_entry *stcp;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (stcp->poolname, pool_p->name) == 0) 
			return((signed64) pool_p->stagealloc_retenp);
	return ((signed64) -1);
}

signed64 get_put_failed_retenp_raw(stcp)
	struct stgcat_entry *stcp;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (stcp->poolname, pool_p->name) == 0) 
			return((signed64) pool_p->put_failed_retenp);
	return ((signed64) -1);
}

int have_at_least_one_stageout_retenp_raw()
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (pool_p->stageout_retenp > 0) return(0);
	return (-1);
}

int have_at_least_one_stagealloc_retenp_raw()
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (pool_p->stagealloc_retenp > 0) return(0);
	return (-1);
}

int have_at_least_one_put_failed_retenp_raw()
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (-1);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (pool_p->put_failed_retenp > 0) return(0);
	return (-1);
}
