/*
 * $Id: poolmgr.c,v 1.13 2000/05/12 12:28:20 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: poolmgr.c,v $ $Revision: 1.13 $ $Date: 2000/05/12 12:28:20 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
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
#define RFIO_KERNEL 1
#include "rfio.h"
#include "stage.h"
#include "stage_api.h"
#include "u64subr.h"
#include "serrno.h"
#include "marshall.h"
#include "osdep.h"
#undef  unmarshall_STRING
#define unmarshall_STRING(ptr,str)  { str = ptr ; INC_PTR(ptr,strlen(str)+1) ; }

extern char *getconfent();
extern char *rfio_serror();
extern char defpoolname[];
extern int rpfd;
extern int req2argv _PROTO((char *, char ***));
struct stgcat_entry *stce;	/* end of stage catalog */
struct stgcat_entry *stcs;	/* start of stage catalog */
int stg_s;

#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep (int, int, ...);
#endif
#if !defined(linux)
extern char *sys_errlist[];
#endif
static struct migpolicy *migpolicies;
static int nbmigpolicy;
static int nbpool;
static char *nfsroot;
static char **poolc;
static struct pool *pools;
#ifndef _WIN32
struct sigaction sa;
#endif

void cvtdbl2str _PROTO((double, char *));
void print_pool_utilization _PROTO((int, char *, char *));
char *selecttapepool _PROTO((char *));
void procmigpoolreq _PROTO((char *, char *));
void update_migpool _PROTO((struct stgcat_entry *, int));
void checkfile2mig _PROTO(());
int migrate_files _PROTO((struct pool *));
int migpoolfiles _PROTO((struct pool *));
int iscleanovl _PROTO((int, int));
int ismigovl _PROTO((int, int));
int isvalidpool _PROTO((char *));
void migpoolfiles_log_callback _PROTO((int, char *));
int isuserlevel _PROTO((char *));
void poolmgr_wait4child _PROTO(());

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
	struct migpolicy *mig_p;
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
	nbpool = 0;
	*defpoolname = '\0';
	defsize = 0;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		if ((p = strtok (buf, " \t\n")) == NULL) continue; 
		if (strcmp (p, "POOL") == 0) nbpool++;
		else if (strcmp (p, "MIGPOLICY") == 0) nbmigpolicy++;
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

	/* 2nd pass: count number of members in each pool and
	   store migration policies */

	rewind (s);
	mig_p = migpolicies - 1;
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
					if (*dp != '\0') {
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
				} else if (strcmp (p, "MIGPOLICY") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG25, "pool");	/* name missing */
						errflg++;
						goto reply;
					}
					if ((int) strlen (p) > CA_MAXMIGPNAMELEN) {
						stglogit (func, STG27, "pool", pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->migp_name, p);
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
			if (*dp != '\0') {
				stglogit (func, STG28);
				errflg++;
				goto reply;
			}
		} else if (strcmp (p, "MIGPOLICY") == 0) {
			mig_p++;
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
			strcpy (mig_p->name, p);
			while ((p = strtok (NULL, " \t\n")) != NULL) {
				if (strcmp (p, "DATATHRESHOLD") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
						errflg++;
						goto reply;
					}
					mig_p->data_mig_threshold = strutou64 (p);
				} else if (strcmp (p, "PERCFREE") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
						errflg++;
						goto reply;
					}
					mig_p->freespace_threshold = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "TIMEINTERVAL") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
						errflg++;
						goto reply;
					}
					mig_p->time_interval = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "MAXDRIVES") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
						errflg++;
						goto reply;
					}
					mig_p->maxdrives = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "SEGALLOWED") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
						errflg++;
						goto reply;
					}
					mig_p->seg_allowed = (strtol (p, &dp, 10) != 0);
					if (*dp != '\0') {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "TAPE_POOL") == 0) {
					if ((tape_pool_str = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26,
						    "migration policy", mig_p->name);
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
							    "migration policy", mig_p->name);
							errflg++;
							goto reply;
						}
						nbtape_pools++;
						if (q == NULL) break;
						*q = '/';
						p = q + 1;
					}
					mig_p->tape_pool = (char **)
						calloc (nbtape_pools + 1, sizeof(char *));
					mig_p->tape_pool[0] = strdup (tape_pool_str);
					p = mig_p->tape_pool[0];
					nbtape_pools = 1;
					while (p = strchr (p, ':')) {
						*p = '\0';
						mig_p->tape_pool[nbtape_pools++] = ++p;
					}
				} else {
					stglogit (func, STG26,
					    "migration policy", pool_p->name);
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

	/* associate pools with policies */

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (! *pool_p->migp_name) continue;
		for (j = 0, mig_p = migpolicies; j < nbmigpolicy; j++)
			if (strcmp (pool_p->migp_name, mig_p->name) == 0) break;
		if (nbmigpolicy == 0 || j >= nbmigpolicy) {
			stglogit (func, STG55, pool_p->migp_name);
			errflg++;
			goto reply;
		}
		pool_p->migp = mig_p;
	}

	/* 3rd pass: store pool elements */

	rewind (s);
	pool_p = pools - 1;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		if (buf[0] == '#') continue;    /* comment line */
		if ((p = strtok (buf, " \t\n")) == NULL) continue;
		if (strcmp (p, "POOL") == 0) {
			pool_p++;
			elemp = pool_p->elemp;
			if (pool_p->defsize == 0) pool_p->defsize = defsize;
			stglogit (func,"POOL %s DEFSIZE %d MINFREE %d\n",
				pool_p->name, pool_p->defsize, pool_p->minfree);
			stglogit (func,".... GC %s\n",
				*(pool_p->gc) != '\0' ? pool_p->gc : "<none>");
			stglogit (func,".... NO_FILE_CREATION %d\n",
				pool_p->no_file_creation);
			stglogit (func,".... MIGPOLICY %s\n",
				*(pool_p->migp_name) != '\0' ? pool_p->migp_name : "<none>");
			if (*(pool_p->migp_name) != '\0') {
              int thisindex = 0;
				if (pool_p->migp == NULL) {
					stglogit (func,"### INTERNAL ERROR : MIGPOLICY %s defined but pool migration structure pointer == NULL\n",pool_p->migp_name);
                    errflg++;
                    goto reply;
                }
				if (pool_p->migp->tape_pool == NULL) {
					stglogit (func,"### INTERNAL ERROR : MIGPOLICY %s defined without TAPE_POOL\n",pool_p->migp_name);
                    errflg++;
                    goto reply;
                }
                stglogit (func,".... .... DATATHRESHOLD %s%s\n",
                          u64tostr(pool_p->migp->data_mig_threshold, tmpbuf, 0),
                          pool_p->migp->data_mig_threshold == 0 ? " (DISABLED)" : "");
                stglogit (func,".... .... PERCFREE      %d%s\n",
                          pool_p->migp->freespace_threshold,
                          pool_p->migp->freespace_threshold == 0 ? " (DISABLED)" : "");
                stglogit (func,".... .... MAXDRIVES     %d%s\n",
                          pool_p->migp->maxdrives,
                          pool_p->migp->maxdrives == 0 ? " (will default to 1)" : "");
                stglogit (func,".... .... SEGALLOWED    %s\n",pool_p->migp->seg_allowed ? "YES" : "NO");
                stglogit (func,".... .... TIMEINTERVAL  %s\n",u64tostr((u_signed64) pool_p->migp->time_interval, tmpbuf, 0));
                while (pool_p->migp->tape_pool[thisindex] != NULL) {
                  stglogit (func,".... .... TAPE_POOL No %d : %s\n",thisindex,pool_p->migp->tape_pool[thisindex]);
                  thisindex++;
                }
			}
		} else if (strcmp (p, "DEFPOOL") == 0) continue;
		else if (strcmp (p, "DEFSIZE") == 0) continue;
		else if (strcmp (p, "MIGPOLICY") == 0) continue;
		else {
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
				elemp->capacity = st.totblks;
				elemp->free = st.freeblks;
				elemp->bsize = st.bsize;
				pool_p->capacity += (st.totblks * (st.bsize / 512));
				pool_p->free += (st.freeblks * (st.bsize / 512));
				stglogit (func,
					"%s capacity=%ld, free=%ld, bsize=%d\n",
					path, elemp->capacity, elemp->free, elemp->bsize);
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
		for (i = 0, mig_p = migpolicies; i < nbmigpolicy; i++, mig_p++) {
			if (mig_p->tape_pool != NULL) {
				if (mig_p->tape_pool[0]) free (mig_p->tape_pool[0]);
				free (mig_p->tape_pool);
			}
		}
		if (migpolicies) free (migpolicies);
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
	return ((((double) pool_p->free * 100.) / (double) pool_p->capacity) > minfree);
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
		if (pool_p->migp == NULL) continue;
		if (pool_p->mig_pid == pid) {
			found = 1;
			break;
		}
	}
	if (! found) return (0);
	pool_p->mig_pid = 0;
	pool_p->migreqtime = 0;
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

void cvtdbl2str(dnum, buf)
		 double dnum;
		 char *buf;
{
	double absdnum;
	float fnum;
	int inum;
	char unit;

	absdnum = fabs (dnum);
	if (absdnum > (1024. * 1024. * 1024.)) {
		fnum = dnum / (1024. * 1024. * 1024.);
		unit = 'G';
	} else if (absdnum > (1024. * 1024.)) {
		fnum = dnum / (1024. * 1024.);
		unit = 'M';
	} else if (absdnum > (1024.)) {
		fnum = dnum / (1024.);
		unit = 'k';
	} else {
		inum = (int)dnum;
		unit = ' ';
	}
	if (unit != ' ')
		sprintf (buf, "%6.2f%c", fnum, unit);
	else
		sprintf (buf, "%6d", inum);
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
	char capacity_s[9];
	struct pool_element *elemp;
	char free_s[9];
	int i, j;
	struct pool *pool_p;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++) {
		if (*poolname && strcmp (poolname, pool_p->name)) continue;
		sendrep (rpfd, MSG_OUT, "POOL %s DEFSIZE %d MINFREE %d GC %s\n",
			pool_p->name, pool_p->defsize, pool_p->minfree, pool_p->gc);
			cvtdbl2str ((double) pool_p->capacity * (double) 512, capacity_s);
			cvtdbl2str ((double) pool_p->free * (double) 512, free_s);
		sendrep (rpfd, MSG_OUT,
			"                              CAPACITY %s FREE %s (%5.1f%%)\n",
			capacity_s, free_s, pool_p->capacity ?
			(((double) pool_p->free * 100.) / (double) pool_p->capacity) : 0);
		for (j = 0, elemp = pool_p->elemp; j < pool_p->nbelem; j++, elemp++) {
			cvtdbl2str ((double) elemp->capacity * (double) elemp->bsize, capacity_s);
			cvtdbl2str ((double) elemp->free * (double) elemp->bsize, free_s);
			sendrep (rpfd, MSG_OUT, "  %s %s CAPACITY %s FREE %s (%5.1f%%)\n",
				elemp->server, elemp->dirpath,
				capacity_s, free_s, elemp->capacity ?
				(((double) elemp->free * 100.) / (double) elemp->capacity) : 0);
		}
	}
	if (*poolname == '\0') sendrep (rpfd, MSG_OUT, "DEFPOOL %s\n", defpoolname);
}

selectfs(poolname, size, path)
		 char *poolname;
		 int *size;
		 char *path;
{
	struct pool_element *elemp;
	int found = 0;
	int i;
	struct pool *pool_p;
	long reqsize;

	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) break;
	if (*size == 0) *size = pool_p->defsize;
	reqsize = *size * 1024 * 1024;	/* size in bytes */
	i = pool_p->next_pool_elem;
	do {
		elemp = pool_p->elemp + i;
		if (elemp->bsize && (elemp->free >= reqsize / elemp->bsize)) {
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
	pool_p->free -= reqsize / 512;
	elemp->free -= reqsize / elemp->bsize;
	if ((nfsroot = getconfent ("RFIO", "NFS_ROOT", 0)) != NULL &&
			strncmp (elemp->dirpath, nfsroot, strlen (nfsroot)) == 0 &&
			*(elemp->dirpath + strlen(nfsroot)) == '/')	/* /shift syntax */
		strcpy (path, elemp->dirpath);
	else
		sprintf (path, "%s:%s", elemp->server, elemp->dirpath);
	stglogit ("selectfs", "%s reqsize=%ld, elemp->free=%ld, pool_p->free=%ld\n",
						path, reqsize, elemp->free, pool_p->free);
	return (1);
}

updfreespace(poolname, ipath, incr)
		 char *poolname;
		 char *ipath;
		 int incr;
{
	struct pool_element *elemp;
	int i, j;
	char *p;
	char path[MAXPATH];
	struct pool *pool_p;
	char server[CA_MAXHOSTNAMELEN + 1];

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
	if (j < pool_p->nbelem && elemp->bsize != 0) {
		elemp->free += incr / elemp->bsize;
		pool_p->free += incr / 512;
		stglogit ("updfreespace", "%s incr=%d, elemp->free=%ld, pool_p->free=%ld\n",
					path, incr, elemp->free, pool_p->free);
	}
	return (0);
}

updpoolconf(defpoolname)
		 char *defpoolname;
{
	int c, i, j;
	struct pool_element *elemp;
	struct migpolicy *mig_p;
	struct pool *pool_n, *pool_p;
	char sav_defpoolname[CA_MAXPOOLNAMELEN + 1];
	struct migpolicy *sav_migpol;
	int sav_nbmigpol;
	int sav_nbpool;
	char **sav_poolc;
	struct pool *sav_pools;

	/* save the current configuration */
	strcpy (sav_defpoolname, defpoolname);
	sav_migpol = migpolicies;
	sav_nbmigpol = nbmigpolicy;
	sav_nbpool = nbpool;
	sav_poolc = poolc;
	sav_pools = pools;

	if (c = getpoolconf (defpoolname)) {	/* new configuration is incorrect */
		/* restore the previous configuration */
		strcpy (defpoolname, sav_defpoolname);
		migpolicies = sav_migpol;
		nbmigpolicy = sav_nbmigpol;
		nbpool = sav_nbpool;
		pools = sav_pools;
	} else {			/* free the old configuration */
		/* but keep pids of cleaner/migrator if any */
		free (sav_poolc);
		for (i = 0, pool_p = sav_pools; i < sav_nbpool; i++, pool_p++) {
			if (pool_p->ovl_pid || pool_p->mig_pid) {
				for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
					if (strcmp (pool_n->name, pool_p->name) == 0) {
						pool_n->ovl_pid = pool_p->ovl_pid;
						pool_n->mig_pid = pool_p->mig_pid;
						break;
					}
				}
			}
			free (pool_p->elemp);
		}
		free (sav_pools);
		for (i = 0, mig_p = sav_migpol; i < sav_nbmigpol; i++, mig_p++) {
			if (mig_p->tape_pool != NULL) {
				if (mig_p->tape_pool[0]) free (mig_p->tape_pool[0]);
				if (mig_p->tape_pool) free (mig_p->tape_pool);
			}
		}
		if (sav_migpol) free (sav_migpol);
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
			if (pool_p->migp->tape_pool != NULL) {
   				if (pool_p->migp->tape_pool[pool_p->migp->tape_pool_index] == NULL)
					pool_p->migp->tape_pool_index = 0;
				return(pool_p->migp->tape_pool[pool_p->migp->tape_pool_index++]);
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
		if (pool_p->migp == NULL) {
			sendrep (rpfd, MSG_ERR, "STG55 - pool %s have no migration policy\n", argv[1]);
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
	free (argv);
}

void update_migpool(stcp,flag)
	struct stgcat_entry *stcp;
    int flag;
{
	int i, ipool;
	struct pool *pool_p;

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
	} else {
		/* We check that this pool have a migration policy */
		if (pool_p->migp != NULL) {
			if (pool_p->nbfiles_canbemig < 0) {
				sendrep (rpfd, MSG_ERR, "Internal error for pool %s, nbfiles_canbemig < 0 (resetted to 0)\n",
						stcp->poolname);
				/* This should never happen but who knows */
				pool_p->nbfiles_canbemig = 0;
			}
			if (flag < 0) {
				/* This is from a return of automatic migration */
				if (pool_p->nbfiles_canbemig-- < 0) {
					sendrep (rpfd, MSG_ERR, "Internal error for pool %s, nbfiles_canbemig < 0 after migration OK (resetted to 0)\n",
							stcp->poolname);
					pool_p->nbfiles_canbemig = 0;
				}
				if (pool_p->space_canbemig < stcp->size) {
					sendrep (rpfd, MSG_ERR, "Internal error for pool %s, space_canbemig < stcp->size after migration OK (resetted to 0)\n",
							stcp->poolname);
					pool_p->space_canbemig = 0;
				} else {
					pool_p->space_canbemig -= stcp->size;
				}
			} else if (flag > 0) {
				/* This is to add an entry for a next automatic migration */
				pool_p->nbfiles_canbemig++;
				pool_p->space_canbemig += stcp->size;
			}
		}
	}
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
		if (! pool_p->migp)	/* migration not wanted in this pool */
			continue;
		if (pool_p->mig_pid)	/* migration already running */
			continue;
		if ((pool_p->migp->data_mig_threshold &&
			pool_p->space_canbemig > pool_p->migp->data_mig_threshold) ||
			(((double) pool_p->free * 100.) / (double) pool_p->capacity <
			pool_p->migp->freespace_threshold) ||
			((time(0) - pool_p->migreqtime) > pool_p->migp->time_interval &&
			pool_p->nbfiles_canbemig > 0))
				migrate_files (pool_p);
	}
}

int migrate_files(pool_p)
		 struct pool *pool_p;
{
	char func[16];
	char *p;
	struct sorted_ent *prev, *scc, *sci, *scf, *scs;
	struct stgcat_entry *stcp;
    int pid;

	strcpy (func, "migrate_files");
	pool_p->mig_pid = fork ();
	pid = pool_p->mig_pid;
	if (pid < 0) {
		stglogit (func, STG02, "", "fork", sys_errlist[errno]);
        pool_p->mig_pid = 0;
		return (SYERR);
	} else if (pid == 0) {  /* we are in the child */
      exit(migpoolfiles(pool_p));
	} else {  /* we are in the parent */
      stglogit (func, "execing migration process on pool %s, pid=%d\n",pool_p->name,pid);
      pool_p->migreqtime = time (0);
	}
    return (0);
}

int migpoolfiles(pool_p)
		 struct pool *pool_p;
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
	struct passwd *stpasswd;             /* Generic uid/gid stage:st */
    int *processes = NULL;
    int nprocesses = 0;
    int iprocess;
    int pid;
    int npidwait;

	strcpy (func, "migpoolfiles");

#ifdef STAGER_DEBUG
    stglogit(func, "### Please gdb /usr/local/bin/stgdaemon %d, then break %d\n",getpid(),__LINE__+2);
    sleep(9);
    sleep(1);
#endif

	/* We get information on generic stage:st uid/gid */
	if ((stpasswd = getpwnam("stage")) == NULL) {
		stglogit(func, "### Cannot getpwnam(\"%s\") (%s)\n","stage",strerror(errno));
		return(SYERR);
	}

    if (stage_setlog((void (*) _PROTO((int, char *))) &migpoolfiles_log_callback) != 0) {
      stglogit(func, "### stage_setlog error (%s)\n",sstrerror(serrno));
      return(SYERR);
    }

    /* We do not want any connection but the one to the stgdaemon */
    close(stg_s);
    close(rpfd);
#ifndef _WIN32
	sa.sa_handler = poolmgr_wait4child;
	sa.sa_flags = SA_RESTART;
	sigaction (SIGCHLD, &sa, NULL);
	sigaction (SIGALRM, &sa, NULL); /* because of sleep */
#endif

	/* build a sorted list of stage catalog entries for the specified pool */
	scf = NULL;
	if ((scs = (struct sorted_ent *) calloc (pool_p->nbfiles_canbemig, sizeof(struct sorted_ent))) == NULL) {
      stglogit(func, "### calloc error (%s)\n",strerror(errno));
      return(SYERR);
    }
	sci = scs;
	for (stcp = stcs; stcp < stce; stcp++) {
		if (stcp->reqid == 0) break;
		if ((stcp->status & CAN_BE_MIGR) != CAN_BE_MIGR) continue;
		if (strcmp (pool_p->name, stcp->poolname)) continue;
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
		sci->stcp = stcp;
		if (++found_nbfiles >= pool_p->nbfiles_canbemig) {
          /* Oupss... We reach the max of entries CAN_BE_MIGR that are known to pool ? */
          break;
        }
		sci++;
	}

	/* build the stageput request(s)            */
    /* parameters are: pool_p->migp->maxdrives  */
    /*                 pool_p->nbfiles_canbemig */

    if (pool_p->migp->maxdrives > 0) {
      if ((nfiles_per_request = (found_nbfiles / pool_p->migp->maxdrives)) <= 0) {
        /* Strange configuration but that's it : there is more maxdrives than files to migrate */
        nfiles_per_request = 1;
      } else {
        if (nfiles_per_request * pool_p->migp->maxdrives < found_nbfiles) {
          /* Will occur if the result is fractionnal */
          nfiles_per_request++;
        }
      }
    } else {
      nfiles_per_request = found_nbfiles;
    }

    /* We limit the number of files per request to 50 so that we should not overload the buffer */
    if (nfiles_per_request > 50) {
      nfiles_per_request = 50;
    }

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
            nextloop = (ISHPSS(scc->stcp->u1.m.xfile) || ISCASTOR(scc->stcp->u1.m.xfile));
            break;
          case 1:
            /* HPSS */
            nextloop = (! ISHPSS(scc->stcp->u1.m.xfile));
            break;
          case 2:
            /* CASTOR */
            nextloop = (! ISCASTOR(scc->stcp->u1.m.xfile));
            break;
          }
          if (nextloop) continue;

          if (iclass > 0) {
            /* This is an HSM class */
            nextloop = 0;
            switch (itype) {
            case 0:
              /* User level */
              nextloop = (! isuserlevel(scc->stcp->u1.m.xfile));
              break;
            case 1:
              /* Exp. level */
              nextloop = isuserlevel(scc->stcp->u1.m.xfile);
              break;
            }
            if (nextloop) continue;
          }

          files[nfiles].xfile = scc->stcp->u1.m.xfile;
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
              /* We remember the pid of this process - it will be a zombie until we wait for it */
              if (nprocesses == 0) {
                if ((processes = (int *) malloc(sizeof(int))) != NULL) {
                  processes[nprocesses++] = fork_pid;
                }
              } else {
                int *dummy;

                if ((dummy = (int *) realloc(processes,sizeof(int))) != NULL) {
                  processes = dummy;
                  processes[nprocesses++] = fork_pid;
                }
              }
              /* Next round */
              nfiles = 0;
            }
          }
        }
      }
    }

    npidwait = nprocesses;
    /* We wait for all child(s) to terminate */
    while (npidwait > 0) {
      if ((pid = waitpid (-1, NULL, WNOHANG)) > 0) {
        /* Got one child terminated */
        for (iprocess = 0; iprocess < nprocesses; iprocess++) {
          if (processes[iprocess] == pid) {
            processes[iprocess] = -1;
            npidwait--;
            break;
          }
        }
      }
      sleep(1);
    }

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

#if ! defined(_WIN32)
void poolmgr_wait4child()
{
}
#endif

