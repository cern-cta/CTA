/*
 * $Id: poolmgr.c,v 1.50 2000/11/17 08:25:20 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: poolmgr.c,v $ $Revision: 1.50 $ $Date: 2000/11/17 08:25:20 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
#endif /* not lint */

#include <stdio.h>
#include <stdlib.h>
#include <pwd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#ifndef _WIN32
#include <unistd.h>
#include <signal.h>
#endif
#include "osdep.h"
#ifndef _WIN32
#include <netinet/in.h>
#endif
#include <errno.h>
#include "rfio_api.h"
#include "stage.h"
#include "stage_api.h"
#include "u64subr.h"
#include "serrno.h"
#include "marshall.h"
#include "osdep.h"
#undef  unmarshall_STRING
#define unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }
#if defined(_WIN32)
static char strftime_format[] = "%b %d %H:%M:%S";
#else /* _WIN32 */
static char strftime_format[] = "%b %e %H:%M:%S";
#endif /* _WIN32 */

#if (defined(IRIX5) || defined(IRIX6) || defined(IRIX64))
/* Surpringly, on Silicon Graphics, strdup declaration depends on non-obvious macros */
extern char *strdup _PROTO((CONST char *));
#endif

extern char *getconfent();
extern char defpoolname[];
extern int rpfd;
extern int req2argv _PROTO((char *, char ***));
extern struct stgcat_entry *stce;	/* end of stage catalog */
extern struct stgcat_entry *stcs;	/* start of stage catalog */
extern int maxfds;

#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep (int, int, ...);
#endif
#if !defined(linux)
extern char *sys_errlist[];
#endif
static struct migpolicy *migpolicies;
static struct migrator *migrators;
static int nbmigpolicy;
static int nbmigrator;
static int nbpool;
static char *nfsroot;
static char **poolc;
static struct pool *pools;
#ifndef _WIN32
struct sigaction sa_poolmgr;
#endif

void print_pool_utilization _PROTO((int, char *, char *));
char *selecttapepool _PROTO((char *));
void procmigpoolreq _PROTO((char *, char *));
int update_migpool _PROTO((struct stgcat_entry *, int));
void checkfile2mig _PROTO(());
int migrate_files _PROTO((struct migrator *));
int migpoolfiles _PROTO((struct migrator *));
int iscleanovl _PROTO((int, int));
int ismigovl _PROTO((int, int));
int isvalidpool _PROTO((char *));
void migpoolfiles_log_callback _PROTO((int, char *));
int isuserlevel _PROTO((char *));
void poolmgr_wait4child _PROTO(());
int selectfs _PROTO((char *, int *, char *));
int updfreespace _PROTO((char *, char *, signed64));

getpoolconf(defpoolname)
		 char *defpoolname;
{
	char buf[128];
	int defsize;
	char *dp;
	struct pool_element *elemp;
	int errflg = 0;
	FILE *fopen(), *s;
	char func[16];
	int i, j;
	struct migpolicy *migp_p;
	struct migrator *migr_p;
	int nbpool_ent;
	int nbtape_pools;
	char *p;
	char path[MAXPATH];
	struct pool *pool_p;
	char *q;
	struct rfstatfs st;
	char *tape_pool_str;
	char tmpbuf[21];

	strcpy (func, "getpoolconf");
	if ((s = fopen (STGCONFIG, "r")) == NULL) {
		stglogit (func, STG23, STGCONFIG);
		return (CONFERR);
	}
	
	/* 1st pass: count number of pools and migration policies */

	migpolicies = NULL;
	nbmigpolicy = 0;
	migrators = NULL;
	nbmigrator = 0;
	nbpool = 0;
	*defpoolname = '\0';
	defsize = 0;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		if ((p = strtok (buf, " \t\n")) == NULL) continue; 
		if (strcmp (p, "POOL") == 0) nbpool++;
		else if (strcmp (p, "MIGPOLICY") == 0) nbmigpolicy++;
		else if (strcmp (p, "MIGRATOR") == 0) nbmigrator++;
	}
	if (nbpool == 0) {
		stglogit (func, STG29);
		fclose (s);
		return (0);
	}
	pools = (struct pool *) calloc (nbpool, sizeof(struct pool));
	if (nbmigpolicy)
		migpolicies = (struct migpolicy *)
			calloc (nbmigpolicy, sizeof(struct migpolicy));
	if (nbmigrator)
		migrators = (struct migrator *)
			calloc (nbmigrator, sizeof(struct migrator));

	/* 2nd pass: count number of members in each pool and
	   store migration policies parameters */

	rewind (s);
	migp_p = migpolicies - 1;
	migr_p = migrators - 1;
	nbpool_ent = -1;
	pool_p = pools - 1;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		if (buf[0] == '#') continue;	/* comment line */
		if ((p = strtok (buf, " \t\n")) == NULL) continue;
		if (strcmp (p, "POOL") == 0) {
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = 0;
			pool_p++;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				stglogit (func, STG25, "pool");	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > CA_MAXPOOLNAMELEN) {
				stglogit (func, STG27, "pool", p);
				errflg++;
				goto reply;
			}
			strcpy (pool_p->name, p);
			while ((p = strtok (NULL, " \t\n")) != NULL) {
				if (strcmp (p, "DEFSIZE") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26, "pool", pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->defsize = strtol (p, &dp, 10);
					if (*dp != '\0' || pool_p->defsize <= 0) {
						stglogit (func, STG26, "pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "MINFREE") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26, "pool", pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->minfree = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26, "pool", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp(p, "GC") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL ||
							strchr (p, ':') == NULL ) {
						stglogit (func, STG26, "pool", pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->gc, p);
				} else if (strcmp(p, "NO_FILE_CREATION") == 0) {
					pool_p->no_file_creation = 1;
				} else if (strcmp (p, "MIGRATOR") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG25, "migrator");	/* name missing */
						errflg++;
						goto reply;
					}
					if ((int) strlen (p) > CA_MAXMIGRNAMELEN) {
						stglogit (func, STG27, "migrator", pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->migr_name, p);
				} else {
					stglogit (func, STG26, "pool", pool_p->name);
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
				stglogit (func, STG30);	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > CA_MAXPOOLNAMELEN) {
				stglogit (func, STG27, "pool", p);
				errflg++;
				goto reply;
			}
			strcpy (defpoolname, p);
		} else if (strcmp (p, "DEFSIZE") == 0) {
			if (poolalloc (pool_p, nbpool_ent) < 0) {
				errflg++;
				goto reply;
			}
			nbpool_ent = -1;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				stglogit (func, STG28);
				errflg++;
				goto reply;
			}
			defsize = strtol (p, &dp, 10);
			if (*dp != '\0' || defsize <= 0) {
				stglogit (func, STG28);
				errflg++;
				goto reply;
			}
		} else if (strcmp (p, "MIGPOLICY") == 0) {
			migp_p++;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				stglogit (func, STG25, "migration policy");	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > CA_MAXMIGPNAMELEN) {
				stglogit (func, STG27, "migration policy", p);
				errflg++;
				goto reply;
			}
			strcpy (migp_p->name, p);
			while ((p = strtok (NULL, " \t\n")) != NULL) {
				if (strcmp (p, "DATATHRESHOLD") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
					migp_p->data_mig_threshold = strutou64 (p);
				} else if (strcmp (p, "PERCFREE") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
					migp_p->freespace_threshold = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "TIMEINTERVAL") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
					migp_p->time_interval = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "MAXDRIVES") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
					migp_p->maxdrives = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "SEGALLOWED") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
					migp_p->seg_allowed = (strtol (p, &dp, 10) != 0);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "TAPE_POOL") == 0) {
					if ((tape_pool_str = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
					nbtape_pools = 0;
					p = tape_pool_str;
					while (1) {
						if (q = strchr (p, ':'))
							*q = '\0';
						if (*p == '\0' ||
						    strlen (p) > CA_MAXPOOLNAMELEN) {
							stglogit (func, STG26,
							    "migration policy", migp_p->name);
							errflg++;
							goto reply;
						}
						nbtape_pools++;
						if (q == NULL) break;
						*q = ':';
						p = q + 1;
					}
					migp_p->tape_pool = (char **)
						calloc (nbtape_pools + 1, sizeof(char *));
					migp_p->tape_pool[0] = strdup (tape_pool_str);
					p = migp_p->tape_pool[0];
					nbtape_pools = 1;
					while (p = strchr (p, ':')) {
						*p = '\0';
						migp_p->tape_pool[nbtape_pools++] = ++p;
					}
				} else {
					stglogit (func, STG26,
					    "migration policy", pool_p->name);
					errflg++;
					goto reply;
				}
			}
		} else if (strcmp (p, "MIGRATOR") == 0) {
			migr_p++;
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				stglogit (func, STG25, "migrator name");	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) > CA_MAXMIGRNAMELEN) {
				stglogit (func, STG27, "migrator name", p);
				errflg++;
				goto reply;
			}
			strcpy (migr_p->name, p);
			while ((p = strtok (NULL, " \t\n")) != NULL) {
				if (strcmp (p, "MIGPOLICY") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migrator's migration policy", migp_p->name);
						errflg++;
						goto reply;
					}
					if ((int) strlen (p) > CA_MAXMIGRNAMELEN) {
						stglogit (func, STG27, "migrator's migration policy", p);
						errflg++;
						goto reply;
					}
					strcpy (migr_p->migp_name, p);
				} else {
					stglogit (func, STG26,
					    "migrator's migration policy", pool_p->name);
					errflg++;
					goto reply;
				}
			}
		} else {
			if (nbpool_ent < 0) {
				stglogit (func, STG26, "pool", "");
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
	if (*defpoolname == '\0') {
		if (nbpool == 1) {
			strcpy (defpoolname, pools->name);
		} else {
			stglogit (func, STG30);
			errflg++;
			goto reply;
		}
	} else {
		if (! isvalidpool (defpoolname)) {
			stglogit (func, STG32, defpoolname);
			errflg++;
			goto reply;
		}
	}

	/* associate migrators with migration policies */

	for (i = 0, migr_p = migrators; i < nbmigrator; i++, migr_p++) {
		if (! *migr_p->migp_name) continue;
		for (j = 0, migp_p = migpolicies; j < nbmigpolicy; j++, migp_p++)
			if (strcmp (migr_p->migp_name, migp_p->name) == 0) break;
		if (nbmigpolicy == 0 || j >= nbmigpolicy) {
			stglogit (func, STG56, migr_p->migp_name);
			errflg++;
			goto reply;
		}
		migr_p->migp = migp_p;
	}

	/* associate pools with migrators */

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (! *pool_p->migr_name) continue;
		for (j = 0, migr_p = migrators; j < nbmigrator; j++, migr_p++)
			if (strcmp (pool_p->migr_name, migr_p->name) == 0) break;
		if (nbmigrator == 0 || j >= nbmigrator) {
			stglogit (func, STG55, pool_p->migr_name);
			errflg++;
			goto reply;
		}
		pool_p->migr = migr_p;
		if (pool_p->migr->nbpool == 0) {
			if ((pool_p->migr->poolp = (struct pool **) malloc(sizeof(char *))) == NULL) {
				stglogit (func, "### malloc error (%s)\n", strerror(errno));
				errflg++;
				goto reply;
			}
		} else {
			struct pool **dummy;
			if ((dummy = (struct pool **) realloc(pool_p->migr->poolp,(pool_p->migr->nbpool + 1) * sizeof(char *))) == NULL) {
				stglogit (func, "### realloc error (%s)\n", strerror(errno));
				errflg++;
				goto reply;
			}
			pool_p->migr->poolp = dummy;
		}
		pool_p->migr->poolp[pool_p->migr->nbpool++] = pool_p;
	}

	/* 3rd pass: store pool elements */

	rewind (s);
	pool_p = pools - 1;
	migr_p = migrators - 1;
	migp_p = migpolicies - 1;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		if (buf[0] == '#') continue;    /* comment line */
		if ((p = strtok (buf, " \t\n")) == NULL) continue;
		if (strcmp (p, "POOL") == 0) {
			pool_p++;
			elemp = pool_p->elemp;
			if (pool_p->defsize == 0) pool_p->defsize = defsize;
			if (pool_p->defsize <= 0) {
				stglogit(func,
						"### CONFIGURATION ERROR : POOL %s with DEFSIZE <= 0\n",
						pool_p->name);
				errflg++;
				goto reply;
			}
			stglogit (func,"POOL %s DEFSIZE %d MINFREE %d\n",
				pool_p->name, pool_p->defsize, pool_p->minfree);
			stglogit (func,".... GC %s\n",
				*(pool_p->gc) != '\0' ? pool_p->gc : "<none>");
			stglogit (func,".... NO_FILE_CREATION %d\n",
				pool_p->no_file_creation);
			stglogit (func,".... MIGRATOR %s\n",
				*(pool_p->migr_name) != '\0' ? pool_p->migr_name : "<none>");
		} else if (strcmp (p, "DEFPOOL") == 0) continue;
		else if (strcmp (p, "DEFSIZE") == 0) continue;
		else if (strcmp (p, "MIGPOLICY") == 0) {
			int thisindex = 0;
			migp_p++;
			stglogit (func,"MIGPOLICY %s\n",
				*(migp_p->name) != '\0' ? migp_p->name : "<none>");
			if (migp_p->name == NULL) {
				stglogit (func,"### INTERNAL ERROR : MIGPOLICY with no name\n");
				errflg++;
				goto reply;
			}
			stglogit (func,".... DATATHRESHOLD %s%s\n",
				u64tostru(migp_p->data_mig_threshold, tmpbuf, 0),
				migp_p->data_mig_threshold == 0 ? " (DISABLED)" : "");
			stglogit (func,".... PERCFREE      %d%s\n",
				migp_p->freespace_threshold,
				migp_p->freespace_threshold == 0 ? " (DISABLED)" : "");
			stglogit (func,".... MAXDRIVES     %d%s\n",
				migp_p->maxdrives,
				migp_p->maxdrives == 0 ? " (will default to 1)" : "");
			stglogit (func,".... SEGALLOWED    %s\n",migp_p->seg_allowed ? "YES" : "NO");
			stglogit (func,".... TIMEINTERVAL  %s\n",u64tostr((u_signed64) migp_p->time_interval, tmpbuf, 0));
			while (migp_p->tape_pool[thisindex] != NULL) {
				stglogit (func,".... TAPE_POOL No %d : %s\n",thisindex,migp_p->tape_pool[thisindex]);
				thisindex++;
			}
		} else if (strcmp (p, "MIGRATOR") == 0) {
			int thisindex = 0;
			migr_p++;
			stglogit (func,"MIGRATOR %s\n",
				*(migr_p->name) != '\0' ? migr_p->name : "<none>");
			if (migr_p->name == NULL) {
				stglogit (func,"### INTERNAL ERROR : MIGRATOR with no name\n");
				errflg++;
				goto reply;
			}
			if (migr_p->migp_name == NULL) {
				stglogit (func,"### INTERNAL ERROR : MIGRATOR %s with no MIGPOLICY name\n",migr_p->name);
				errflg++;
				goto reply;
			}
			if (migr_p->migp == NULL) {
				stglogit (func,"### INTERNAL ERROR : MIGRATOR %s with MIGPOLICY %s but migpolicy pointer NULL !\n",migr_p->name, migr_p->migp_name);
				errflg++;
				goto reply;
			}
			stglogit (func,".... MIGPOLICY %s\n",migr_p->migp_name);
			for (thisindex = 0; thisindex < migr_p->nbpool; thisindex++) {
				stglogit (func,".... DISK_POOL No %d : %s\n",thisindex,migr_p->poolp[thisindex]);
			}
		} else {
			if ((int) strlen (p) > CA_MAXHOSTNAMELEN) {
				stglogit (func, STG26, pool_p->name);
				errflg++;
				goto reply;
			}
			strcpy (elemp->server, p);
			if ((p = strtok (NULL, " \t\n")) == NULL) {
				stglogit (func, STG26, pool_p->name);
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) >= MAXPATH) {
				stglogit (func, STG26, pool_p->name);
				errflg++;
				goto reply;
			}
			strcpy (elemp->dirpath, p);
			if ((nfsroot = getconfent ("RFIO", "NFS_ROOT", 0)) != NULL &&
			    strncmp (elemp->dirpath, nfsroot, strlen (nfsroot)) == 0 &&
			    *(elemp->dirpath + strlen(nfsroot)) == '/')	/* /shift syntax */
				strcpy (path, elemp->dirpath);
			else
				sprintf (path, "%s:%s", elemp->server, elemp->dirpath);
			if (rfio_statfs (path, &st) < 0) {
				stglogit (func, STG02, path, "rfio_statfs",
					rfio_serror());
			} else {
				char tmpbuf1[21];
				char tmpbuf2[21];
				elemp->capacity = (u_signed64) ((u_signed64) st.totblks * (u_signed64) st.bsize);
				elemp->free = (u_signed64) ((u_signed64) st.freeblks * (u_signed64) st.bsize);
				pool_p->capacity += elemp->capacity;
				pool_p->free += elemp->free;
				stglogit (func,
					"... %s capacity=%s, free=%s\n",
					path, u64tostru(elemp->capacity, tmpbuf1, 0), u64tostru(elemp->free, tmpbuf2, 0));
			}
			elemp++;
		}
	}
reply:
	fclose (s);
	if (errflg) {
		for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
			for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++)
				if (elemp) free (elemp);
		}
		free (pools);
		for (i = 0, migp_p = migpolicies; i < nbmigpolicy; i++, migp_p++) {
			if (migp_p->tape_pool != NULL) {
				if (migp_p->tape_pool[0]) free (migp_p->tape_pool[0]);
				free (migp_p->tape_pool);
			}
		}
		if (migpolicies) free (migpolicies);
		if (migrators) free (migrators);
		return (CONFERR);
	} else {
		poolc = (char **) calloc (nbpool, sizeof(char *));
		return (0);
	}
}

checklastaccess(poolname, atime)
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

checkpoolcleaned(pool_list)
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

cleanpool(poolname)
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
	if (pool_p->ovl_pid) return (0);
	pool_p->ovl_pid = fork ();
	pid = pool_p->ovl_pid;
	if (pid < 0) {
		stglogit (func, STG02, "", "fork", sys_errlist[errno]);
		return (SYERR);
	} else if (pid == 0) {  /* we are in the child */
		gethostname (hostname, CA_MAXHOSTNAMELEN + 1);
		sprintf (progfullpath, "%s/cleaner", BIN);
		stglogit (func, "execing cleaner, pid=%d\n", getpid());
		execl (progfullpath, "cleaner", pool_p->gc, poolname, hostname, 0);
		stglogit (func, STG02, "cleaner", "execl", sys_errlist[errno]);
		exit (SYERR);
	} else
		pool_p->cleanreqtime = time (0);
	return (0);
}

enoughfreespace(poolname, minf)
		 char *poolname;
		 int minf;
{
	int i;
	int minfree;
	struct pool *pool_p;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (i == nbpool) return (0);	/* old entry; pool does not exist */
	if (minf)
		minfree = minf;
	else
		minfree = pool_p->minfree;
	return((pool_p->free * 100) > (pool_p->capacity * minfree));
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

	if ((p = strchr (path, ':')) != NULL) {
		strncpy (server, path, p - path);
		server[p - path] = '\0';
	}
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++)
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
	return (NULL);
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
	pool_p->cleanreqtime = 0;
	if (status == 0)
		pool_p->cleanstatus = 1;
	return (1);
}

int ismigovl(pid, status)
		 int pid;
		 int status;
{
	int found;
	int i;
	struct pool *pool_p;

	if (nbpool == 0) return (0);
	found = 0;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (pool_p->migr == NULL) continue;
		if (pool_p->migr->mig_pid == pid) {
			found = 1;
			break;
		}
	}
	if (! found) return (0);
	pool_p->migr->mig_pid = 0;
	pool_p->migr->migreqtime = 0;
	return (1);
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

poolalloc(pool_p, nbpool_ent)
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

void print_pool_utilization(rpfd, poolname, defpoolname)
		 int rpfd;
		 char *poolname, *defpoolname;
{
	struct pool_element *elemp;
	int i, j;
	int migr_newline = 1;
	int migp_newline = 1;
	struct pool *pool_p;
	struct migrator *migr_p;
	struct migrator *pool_p_migr = NULL;
	struct migpolicy *migp_p;
	struct migpolicy *pool_p_migr_migp;
	char tmpbuf[21];
	char timestr[64] ;   /* Time in its ASCII format             */
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
	struct tm tmstruc;
#endif /* _REENTRANT || _THREAD_SAFE */
	struct tm *tp;
	char tmpbuf1[21];
	char tmpbuf2[21];
	char tmpbuf3[21];
	char tmpbuf4[21];
	u_signed64 before_fraction, after_fraction;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (*poolname && strcmp (poolname, pool_p->name)) continue;
		if (*poolname) pool_p_migr = pool_p->migr;
		sendrep (rpfd, MSG_OUT, "POOL %s DEFSIZE %d MINFREE %d GC %s%s%s\n",
			pool_p->name,
			pool_p->defsize,
			pool_p->minfree,
			pool_p->gc,
			pool_p->migr_name[0] != '\0' ? " MIGRATOR " : "",
			pool_p->migr_name[0] != '\0' ? pool_p->migr_name : ""
			);
		if (pool_p->cleanreqtime > 0) {
#if ((defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32))
			localtime_r(&(pool_p->cleanreqtime),&tmstruc);
			tp = &tmstruc;
#else
			tp = localtime(&(pool_p->cleanreqtime));
#endif /* _REENTRANT || _THREAD_SAFE */
			strftime(timestr,64,strftime_format,tp);
			sendrep (rpfd, MSG_OUT, "\tLAST GARBAGE COLLECTION STARTED %s%s\n", timestr, pool_p->ovl_pid > 0 ? " STILL ACTIVE" : "");
		} else {
			sendrep (rpfd, MSG_OUT, "\tLAST GARBAGE COLLECTION STARTED <none>\n");
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
			sendrep (rpfd, MSG_OUT, "  %s %s CAPACITY %s FREE %s (%s.%s%%)\n",
				elemp->server,
				elemp->dirpath,
				u64tostru(elemp->capacity, tmpbuf1, 0),
				u64tostru(elemp->free, tmpbuf2, 0),
				u64tostr(before_fraction, tmpbuf3, 0),
				u64tostr(after_fraction, tmpbuf4, 0));
		}
	}
	if (*poolname == '\0') sendrep (rpfd, MSG_OUT, "DEFPOOL %s\n", defpoolname);
	if (migrators != NULL) {
		for (i = 0, migr_p = migrators; i < nbmigrator; i++, migr_p++) {
			if (*poolname && pool_p_migr != migr_p) continue;
			if (*poolname) pool_p_migr_migp = migr_p->migp;
			if (migr_newline) {
				sendrep (rpfd, MSG_OUT, "\n");
				migr_newline = 0;
			}
			sendrep (rpfd, MSG_OUT, "MIGRATOR %s MIGPOLICY %s\n",migr_p->name,migr_p->migp->name);
			sendrep (rpfd, MSG_OUT, "\tNBFILES_CAN_BE_MIGR    %d\n", migr_p->nbfiles_canbemig);
			sendrep (rpfd, MSG_OUT, "\tSPACE_CAN_BE_MIGR      %s\n", u64tostru(migr_p->space_canbemig, tmpbuf, 0));
			sendrep (rpfd, MSG_OUT, "\tNBFILES_BEING_MIGR     %d\n", migr_p->nbfiles_beingmig);
			sendrep (rpfd, MSG_OUT, "\tSPACE_BEING_MIGR       %s\n", u64tostru(migr_p->space_beingmig, tmpbuf, 0));
			if (migr_p->migreqtime > 0) {
#if ((defined(_REENTRANT) || defined(_THREAD_SAFE)) && !defined(_WIN32))
				localtime_r(&(migr_p->migreqtime),&tmstruc);
				tp = &tmstruc;
#else
				tp = localtime(&(migr_p->migreqtime));
#endif /* _REENTRANT || _THREAD_SAFE */
				strftime(timestr,64,strftime_format,tp);
				sendrep (rpfd, MSG_OUT, "\tLAST MIGRATION STARTED %s%s\n", timestr, migr_p->mig_pid > 0 ? " STILL ACTIVE" : "");
			} else {
				sendrep (rpfd, MSG_OUT, "\tLAST MIGRATION STARTED <none>\n");
			}
		}
	}
	if (migpolicies != NULL) {
		for (i = 0, migp_p = migpolicies; i < nbmigpolicy; i++, migp_p++) {
			int thisindex = 0;

			if (*poolname && pool_p_migr_migp != migp_p) continue;
			if (migp_newline) {
				sendrep (rpfd, MSG_OUT, "\n");
				migp_newline = 0;
			}
			sendrep (rpfd, MSG_OUT, "MIGPOLICY %s\n",migp_p->name);
			if (migp_p->data_mig_threshold > 0) {
				sendrep (rpfd, MSG_OUT, "\tDATATHRESHOLD %s\n", u64tostru(migp_p->data_mig_threshold, tmpbuf, 0));
			}
			if (migp_p->freespace_threshold > 0) {
				sendrep (rpfd, MSG_OUT, "\tPERCFREE      %d%%\n", migp_p->freespace_threshold);
			}
			if (migp_p->maxdrives > 0) {
				sendrep (rpfd, MSG_OUT, "\tMAXDRIVES     %d\n", migp_p->maxdrives);
			}
			if (migp_p->seg_allowed != 0) {
				sendrep (rpfd, MSG_OUT, "\tSEGALLOWED    %s\n", migp_p->seg_allowed ? "YES" : "NO");
			}
			if (migp_p->time_interval > 0) {
				sendrep (rpfd, MSG_OUT, "\tTIMEINTERVAL  %d SECONDS\n", migp_p->time_interval);
			}
			while (migp_p->tape_pool[thisindex] != NULL) {
				sendrep (rpfd, MSG_OUT, "\tTAPE_POOL No %2d : %s\n",thisindex,migp_p->tape_pool[thisindex]);
				thisindex++;
			}
		}
	}
}

int
selectfs(poolname, size, path)
		 char *poolname;
		 int *size;
		 char *path;
{
	struct pool_element *elemp;
	int found = 0;
	int i;
	struct pool *pool_p;
	u_signed64 reqsize;
	char tmpbuf0[21];
	char tmpbuf1[21];
	char tmpbuf2[21];

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (*size == 0) *size = pool_p->defsize;
	reqsize = (u_signed64) ((u_signed64) *size * (u_signed64) ONE_MB);	/* size in bytes */
	i = pool_p->next_pool_elem;
	do {
		elemp = pool_p->elemp + i;
		if (elemp->free >= reqsize) {
			found = 1;
			break;
		}
		i++;
		if (i >= pool_p->nbelem) i = 0;
	} while (i != pool_p->next_pool_elem);
	if (!found)
		return (-1);
	pool_p->next_pool_elem = i + 1;
	if (pool_p->next_pool_elem >= pool_p->nbelem) pool_p->next_pool_elem = 0;
	pool_p->free -= reqsize;
	elemp->free -= reqsize;
	if (pool_p->free > pool_p->capacity) {
		/* This is impossible. In theory it can happen only if reqsize > previous pool_p->free and */
		/* if pool_p->capacity < max_u_signed64 */
		stglogit ("selectfs", "### Warning, pool_p->free > pool_p->capacity. pool_p->free set to 0\n");
		pool_p->free = 0;
	}
	if (elemp->free > elemp->capacity) {
		/* This is impossible. In theory it can happen only if reqsize > previous elemp->free and */
		/* if elemp->capacity < max_u_signed64 */
		stglogit ("selectfs", "### Warning, elemp->free > elemp->capacity. elemp->free set to 0\n");
		elemp->free = 0;
	}
	if ((nfsroot = getconfent ("RFIO", "NFS_ROOT", 0)) != NULL &&
			strncmp (elemp->dirpath, nfsroot, strlen (nfsroot)) == 0 &&
			*(elemp->dirpath + strlen(nfsroot)) == '/')	/* /shift syntax */
		strcpy (path, elemp->dirpath);
	else
		sprintf (path, "%s:%s", elemp->server, elemp->dirpath);
	stglogit ("selectfs", "%s reqsize=%s, elemp->free=%s, pool_p->free=%s\n",
						path, u64tostr(reqsize, tmpbuf0, 0), u64tostr(elemp->free, tmpbuf1, 0), u64tostr(pool_p->free, tmpbuf2, 0));
	return (1);
}

int
updfreespace(poolname, ipath, incr)
		 char *poolname;
		 char *ipath;
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

	if (*poolname == '\0')
		return (0);
	if ((p = strchr (ipath, ':')) != NULL) {
		strncpy (server, ipath, p - ipath);
		*(server + (p - ipath)) = '\0';
	}
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (i == nbpool) return (0);	/* old entry; pool does not exist */
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
		elemp->free += incr;
		pool_p->free += incr;
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
	}
	return (0);
}

updpoolconf(defpoolname)
		 char *defpoolname;
{
	int c, i, j;
	struct pool_element *elemp;
	struct migpolicy *migp_p;
	struct migrator *migr_p;
	struct pool *pool_n, *pool_p;
	char sav_defpoolname[CA_MAXPOOLNAMELEN + 1];
	struct migpolicy *sav_migpol;
	struct migrator *sav_migrator;
	int sav_nbmigpol;
	int sav_nbmigrator;
	int sav_nbpool;
	char **sav_poolc;
	struct pool *sav_pools;

	/* save the current configuration */
	strcpy (sav_defpoolname, defpoolname);
	sav_migpol = migpolicies;
	sav_migrator = migrators;
	sav_nbmigpol = nbmigpolicy;
	sav_nbmigrator = nbmigrator;
	sav_nbpool = nbpool;
	sav_poolc = poolc;
	sav_pools = pools;

	if (c = getpoolconf (defpoolname)) {	/* new configuration is incorrect */
		/* restore the previous configuration */
		strcpy (defpoolname, sav_defpoolname);
		migpolicies = sav_migpol;
		migrators = sav_migrator;
		nbmigpolicy = sav_nbmigpol;
		nbmigrator = sav_nbmigrator;
		nbpool = sav_nbpool;
		pools = sav_pools;
	} else {			/* free the old configuration */
		/* but keep pids of cleaner/migrator as well as started time if any */
		free (sav_poolc);
		for (i = 0, pool_p = sav_pools; i < sav_nbpool; i++, pool_p++) {
			if (pool_p->ovl_pid || (pool_p->migr != NULL && pool_p->migr->mig_pid)) {
				for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
					if (strcmp (pool_n->name, pool_p->name) == 0) {
						pool_n->ovl_pid = pool_p->ovl_pid;
						pool_n->cleanreqtime = pool_p->cleanreqtime;
						if (pool_n->migr != NULL && pool_p->migr != NULL) {
							pool_n->migr->mig_pid = pool_p->migr->mig_pid;
							pool_n->migr->migreqtime = pool_p->migr->migreqtime;
						}
						break;
					}
				}
			}
			free (pool_p->elemp);
		}
		free (sav_pools);
		for (i = 0, migp_p = sav_migpol; i < sav_nbmigpol; i++, migp_p++) {
			if (migp_p->tape_pool != NULL) {
				if (migp_p->tape_pool[0]) free (migp_p->tape_pool[0]);
				if (migp_p->tape_pool) free (migp_p->tape_pool);
			}
		}
		if (sav_migpol) free (sav_migpol);
		if (sav_migrator) free (sav_migrator);
	}
	return (c);
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

char *selecttapepool(poolname)
		 char *poolname;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0 || poolname == NULL) return (NULL);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) {
			if (pool_p->migr != NULL) {
				if (pool_p->migr->migp != NULL) {
					if (pool_p->migr->migp->tape_pool != NULL) {
		   				if (pool_p->migr->migp->tape_pool[pool_p->migr->migp->tape_pool_index] == NULL)
							pool_p->migr->migp->tape_pool_index = 0;
						return(pool_p->migr->migp->tape_pool[pool_p->migr->migp->tape_pool_index++]);
					}
				}
			}
		}
	return (NULL);
}

void procmigpoolreq(req_data, clienthost)
		 char *req_data;
		 char *clienthost;
{
	char **argv;
	int c, i, ipool;
	uid_t uid;
	gid_t gid;
	int nargs;
	char *rbp;
	char *user;
	char *poolname;
	char *tapepoolname;
	struct pool *pool_p;

	rbp = req_data;
	unmarshall_STRING (rbp, user);	/* login name */
	unmarshall_WORD (rbp, uid);
	unmarshall_WORD (rbp, gid);
	nargs = req2argv (rbp, &argv);

	stglogit ("progmigpoolreq", STG92, "stage_migpool", user, uid, gid, clienthost);

	poolname = argv[1];
    if (strcmp(poolname,"-") == 0) {
      poolname = defpoolname;
    }

    /* We check that this poolname exist */
	ipool = -1;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
      if (strcmp(pool_p->name,poolname) == 0) {
		ipool = i;
        break;
      }
	}
	if (ipool < 0) {
		sendrep (rpfd, MSG_ERR, STG32, argv[1]); /* Not in the list of pools */
		sendrep (rpfd, STAGERC, STAGEMIGPOOL, USERR);
	} else {
		/* We check that this pool have a migration policy */
		if (pool_p->migr == NULL) {
			sendrep (rpfd, MSG_ERR, "STG55 - pool %s have no associated migrator\n", argv[1]);
			sendrep (rpfd, STAGERC, STAGEMIGPOOL, USERR);
		} else {
			if (pool_p->migr->migp == NULL) {
				sendrep (rpfd, MSG_ERR, "STG55 - pool %s have no associated migration policy with migrator %s\n", argv[1],*(pool_p->migr->name) != '\0' ? pool_p->migr->name : "<none>");
				sendrep (rpfd, STAGERC, STAGEMIGPOOL, USERR);
			} else {
				/* We select tape pool */
				if ((tapepoolname = selecttapepool(poolname)) == NULL) {
					sendrep (rpfd, MSG_ERR, "STG55 - cannot select a tape pool for disk pool %s\n", argv[1]);
					sendrep (rpfd, STAGERC, STAGEMIGPOOL, USERR);
				} else {
					sendrep (rpfd, MSG_OUT, "%s", tapepoolname);
					sendrep (rpfd, STAGERC, STAGEMIGPOOL, 0);
				}
			}
		}
	}
	free (argv);
}

/* Flag means :                                       */
/*  1             file put in stack can_be_migr       */
/* -1             file removed from stack can_be_migr */
/* Returns: 0 (OK) or -1 (NOT OK) */
int update_migpool(stcp,flag)
	struct stgcat_entry *stcp;
    int flag;
{
	int i, ipool;
	struct pool *pool_p;

	if (flag != 1 && flag != -1) {
		sendrep(rpfd, MSG_ERR, "STG02 - update_migpool : Internal error : flag != 1 && flag != -1\n");
		serrno = EINVAL;
		return(-1);
	}

	/* We check that this poolname exist */
	ipool = -1;
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
      if (strcmp(pool_p->name,stcp->poolname) == 0) {
		ipool = i;
        break;
      }
	}
	if (ipool < 0) {
		sendrep (rpfd, MSG_ERR, STG32, stcp->poolname); /* Not in the list of pools */
		serrno = EINVAL;
		return(-1);
	}
	/* We check that this pool have a migrator */
	if (pool_p->migr == NULL) {
		return(0);
	}

	switch (flag) {
	case -1:
		if ((stcp->status & CAN_BE_MIGR) != CAN_BE_MIGR) {
			/* This is a not a return from automatic migration */
			return(0);
		}
		/* This is a return from automatic migration */
		if (pool_p->migr->nbfiles_canbemig-- < 0) {
			sendrep(rpfd, MSG_ERR, "STG02 - update_migpool : Internal error for pool %s, nbfiles_canbemig < 0 after automatic migration OK (resetted to 0)\n",
					stcp->poolname);
			pool_p->migr->nbfiles_canbemig = 0;
		}
		if (pool_p->migr->space_canbemig < stcp->actual_size) {
			sendrep(rpfd, MSG_ERR, "STG02 - update_migpool : Internal error for pool %s, space_canbemig < stcp->actual_size after automatic migration OK (resetted to 0)\n",
				stcp->poolname);
			pool_p->migr->space_canbemig = 0;
		} else {
			pool_p->migr->space_canbemig -= stcp->actual_size;
		}
		if ((stcp->status == (STAGEPUT|CAN_BE_MIGR)) || ((stcp->status & BEING_MIGR) == BEING_MIGR)) {
			if ((stcp->status & BEING_MIGR) == BEING_MIGR) stcp->status &= ~BEING_MIGR;
			if (pool_p->migr->nbfiles_beingmig-- < 0) {
				sendrep(rpfd, MSG_ERR, "STG02 - update_migpool : Internal error for pool %s, nbfiles_beingmig < 0 after automatic migration OK (resetted to 0)\n",
						stcp->poolname);
				pool_p->migr->nbfiles_beingmig = 0;
			}
			if (pool_p->migr->space_beingmig < stcp->actual_size) {
				sendrep(rpfd, MSG_ERR, "STG02 - update_migpool : Internal error for pool %s, space_beingmig < stcp->actual_size after automatic migration OK (resetted to 0)\n",
					stcp->poolname);
				pool_p->migr->space_beingmig = 0;
			} else {
				pool_p->migr->space_beingmig -= stcp->actual_size;
			}
		}
		break;
	case 1:
		/* This is to add an entry for the next automatic migration */
		pool_p->migr->nbfiles_canbemig++;
		pool_p->migr->space_canbemig += stcp->actual_size;
		stcp->status |= CAN_BE_MIGR;
		break;
	default:
		sendrep(rpfd, MSG_ERR, "STG02 - update_migpool : Internal error : flag != 1 && flag != -1\n");
		serrno = EINVAL;
		return(-1);
	}
	return(0);
}

/* If poolname == NULL this the following routine will check ALL the pools */

void checkfile2mig()
{
	int i;
	struct pool *pool_p;

	/* The migration will be started if one of the 4 following criteria is met:
	  - space occupied by files in state CAN_BE_MIGR execeeds a given value
	  - amount of free space in the disk pool is below a given threshold
	  - time elapsed since last migration is bigger than a given value.

	  The number of parallel migration streams is set for each pool
	 */

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (pool_p->migr == NULL)	/* migration not wanted in this pool */
			continue;
		if (pool_p->migr->mig_pid != 0)	/* migration already running */
			continue;
		if (pool_p->migr->migp == NULL)	/* Internal error */
			continue;
		if (pool_p->migr->nbfiles_canbemig <= 0)	/* No point anyway */
			continue;
		if ((pool_p->migr->migp->data_mig_threshold &&
			pool_p->migr->space_canbemig > pool_p->migr->migp->data_mig_threshold) ||
			(pool_p->free * 100) < (pool_p->capacity * pool_p->migr->migp->freespace_threshold) ||
			((time(0) - pool_p->migr->migreqtime) > pool_p->migr->migp->time_interval))
				migrate_files (pool_p->migr);
	}
}

int migrate_files(migr_p)
		 struct migrator *migr_p;
{
	char func[16];
	char *p;
	struct sorted_ent *prev, *scc, *sci, *scf, *scs;
	struct stgcat_entry *stcp;
    int pid;

	strcpy (func, "migrate_files");
	migr_p->mig_pid = fork ();
	pid = migr_p->mig_pid;
	if (pid < 0) {
		stglogit (func, STG02, "", "fork", sys_errlist[errno]);
        migr_p->mig_pid = 0;
		return (SYERR);
	} else if (pid == 0) {  /* we are in the child */
      exit(migpoolfiles(migr_p));
	} else {  /* we are in the parent */
      int okpoolname;
      int ipoolname;
      char tmpbuf[21];

      /* We remember all the entries that will be treated in this migration */
      for (stcp = stcs; stcp < stce; stcp++) {
        if (stcp->reqid == 0) break;
        if ((stcp->status & CAN_BE_MIGR) != CAN_BE_MIGR) continue;
        if ((stcp->status & PUT_FAILED) == PUT_FAILED) continue;
        okpoolname = 0;
		/* Does it belong to a pool managed by this migrator ? */
		for (ipoolname = 0; ipoolname < migr_p->nbpool; ipoolname++) {
          if (strcmp (migr_p->poolp[ipoolname]->name, stcp->poolname)) continue;
          okpoolname = 1;
		}
		if (okpoolname == 0) continue;
        stcp->status |= BEING_MIGR;
		migr_p->nbfiles_beingmig++;
		migr_p->space_beingmig += stcp->actual_size;
      }
      stglogit (func, "execing migrator %s for %d HSM files (total of %s), pid=%d\n",
                migr_p->name,
                migr_p->nbfiles_beingmig,
                u64tostru(migr_p->space_beingmig, tmpbuf, 0),
                pid);
      migr_p->migreqtime = time (0);
	}
    return (0);
}

int migpoolfiles(migr_p)
		 struct migrator *migr_p;
{
	/* We use the weight algorithm defined by Fabrizio Cane for DPM */

	int c;
	char *p;
	struct sorted_ent *prev, *scc, *sci, *scf, *scs;
	struct stgcat_entry *stcp;
	int found_nbfiles = 0;
    stage_hsm_t *files = NULL;
	int nfiles_per_request;
	int nfiles = 0;
	char func[16];
    int iclass;
    int itype;
	extern struct passwd *stpasswd;             /* Generic uid/gid stage:st */
    int *processes = NULL;
    int pid;
    int npidwait;
    int okpoolname;
    int ipoolname;
    int term_status;
    pid_t child_pid;

	strcpy (func, "migpoolfiles");

#ifdef STAGER_DEBUG
    stglogit(func, "### Please gdb /usr/local/bin/stgdaemon %d, then break %d\n",getpid(),__LINE__+2);
    sleep(9);
    sleep(1);
#endif

    if (stage_setlog((void (*) _PROTO((int, char *))) &migpoolfiles_log_callback) != 0) {
      stglogit(func, "### stage_setlog error (%s)\n",sstrerror(serrno));
      return(SYERR);
    }

    /* We do not want any connection but the one to the stgdaemon */
	for (c = 0; c < maxfds; c++)
		close (c);

#ifndef _WIN32
    signal(SIGCHLD, poolmgr_wait4child);
#endif

	/* build a sorted list of stage catalog entries for the specified pool */
	scf = NULL;
	if ((scs = (struct sorted_ent *) calloc (migr_p->nbfiles_canbemig, sizeof(struct sorted_ent))) == NULL) {
      stglogit(func, "### calloc error (%s)\n",strerror(errno));
      return(SYERR);
    }
	sci = scs;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
 		if ((stcp->status & CAN_BE_MIGR) != CAN_BE_MIGR) continue;
 		if ((stcp->status & PUT_FAILED) == PUT_FAILED) continue;
		okpoolname = 0;
		/* Does it belong to a pool managed by this migrator ? */
		for (ipoolname = 0; ipoolname < migr_p->nbpool; ipoolname++) {
			if (strcmp (migr_p->poolp[ipoolname]->name, stcp->poolname)) continue;
			okpoolname = 1;
		}
		if (okpoolname == 0) continue;
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
		if (++found_nbfiles >= migr_p->nbfiles_canbemig) {
          /* Oupss... We reach the max of entries CAN_BE_MIGR that are known to pool ? */
          break;
        }
		sci++;
	}

	/* build the stageput request(s)            */
    /* parameters are: migr_p->migp->maxdrives  */
    /*                 migr_p->nbfiles_canbemig */

    if (migr_p->migp->maxdrives > 0) {
      if ((nfiles_per_request = (found_nbfiles / migr_p->migp->maxdrives)) <= 0) {
        /* Strange configuration but that's it : there is more maxdrives than files to migrate */
        nfiles_per_request = 1;
      } else {
        if (nfiles_per_request * migr_p->migp->maxdrives < found_nbfiles) {
          /* Will occur if the result is fractionnal */
          nfiles_per_request++;
        }
      }
    } else {
      nfiles_per_request = found_nbfiles;
    }

    /* We limit the number of files per request to 50 so that we should not overload the buffer */
    /*
    if (nfiles_per_request > 50) {
      nfiles_per_request = 50;
    }
    */

    if ((files = (stage_hsm_t *) calloc(nfiles_per_request,sizeof(stage_hsm_t))) == NULL) {
      stglogit(func, "### calloc error (%s)\n", strerror(errno));
      free (scs);
      return(SYERR);
    }

    for (iclass = 0; iclass < 3; iclass++) {
      /* We do three classes : not an hsm OR HPSS or CASTOR */
      for (itype = 0; itype < (iclass == 0 ? 1 : 2); itype++) {
        /* We do two types : user or experiment level */
        nfiles = 0;
        for (scc = scf; scc; scc = scc->next) {
          int nextloop;
          int fork_pid;

          nextloop = 0;
          switch (iclass) {
          case 0:
            /* Not an HSM */
            nextloop = (scc->stcp->t_or_d == 'm' || scc->stcp->t_or_d == 'h');
            break;
          case 1:
            /* HPSS */
            nextloop = (scc->stcp->t_or_d != 'm');
            break;
          case 2:
            /* CASTOR */
            nextloop = (scc->stcp->t_or_d != 'h');
            break;
          }
          if (nextloop) continue;

          if (iclass > 0) {
            /* This is an HSM class */
            nextloop = 0;
            switch (itype) {
            case 0:
              /* User level */
              nextloop = (! isuserlevel(scc->stcp->t_or_d == 'm' ? scc->stcp->u1.m.xfile : scc->stcp->u1.h.xfile));
              break;
            case 1:
              /* Exp. level */
              nextloop = isuserlevel(scc->stcp->t_or_d == 'm' ? scc->stcp->u1.m.xfile : scc->stcp->u1.h.xfile);
              break;
            }
            if (nextloop) continue;
          }

          files[nfiles].xfile = (scc->stcp->t_or_d == 'm' ? scc->stcp->u1.m.xfile : scc->stcp->u1.h.xfile);
          if (nfiles > 0) {
            files[nfiles - 1].next = &(files[nfiles]);
          }
          nfiles++;
          if (nfiles >= nfiles_per_request || scc->next == NULL) {
            /* We reached the maximum number of files per request, or the end */
            if ((fork_pid= fork()) < 0) {
              stglogit(func, "### Cannot fork (%s)\n",strerror(errno));
            } else if (fork_pid == 0) {
              int rc;
              
              setegid(0);
              seteuid(0);
              setegid(itype != 0 ? scc->stcp->gid : stpasswd->pw_gid);
              seteuid(itype != 0 ? scc->stcp->uid : stpasswd->pw_uid);
              rc = stage_put_hsm(NULL, 1, files);
              free(files);
              free (scs);
              exit(rc != 0 ? SYERR : 0);
            } else {
              /* Next round */
              memset((void *) files, 0, nfiles_per_request * sizeof(stage_hsm_t));
              nfiles = 0;
            }
          }
        }
      }
    }

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
    /* stglogit("migpoolfiles","waitpid : %s\n",strerror(errno)); */

    free(files);
	free (scs);
	return (0);
}

void migpoolfiles_log_callback(level,message)
     int level;
     char *message;
{
  /*
  char func[16];

  if (level == MSG_OUT) {
    strcpy (func, "migpoolfiles");
    stglogit(func,"%s",message);
  }
  */
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

