/*
 * $Id: poolmgr.c,v 1.10 2000/03/23 01:41:07 jdurand Exp $
 */

/*
 * Copyright (C) 1993-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: poolmgr.c,v $ $Revision: 1.10 $ $Date: 2000/03/23 01:41:07 $ CERN IT-PDP/DM Jean-Philippe Baud";
#endif /* not lint */

#include <stdio.h>
#include <sys/types.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#define RFIO_KERNEL 1
#include "rfio.h"
#include "stage.h"
#include "osdep.h"

extern char *getconfent();
extern char *rfio_serror();
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep (int, int, ...);
#endif
#if !defined(linux)
extern char *sys_errlist[];
#endif
static char *nfsroot;
static int nbpool;
static char **poolc;
static struct pool *pools;

void cvtdbl2str _PROTO((double, char *));
void print_pool_utilization _PROTO((int, char *, char *));
char *poolname2tapepool _PROTO((char *));

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
	int nbpool_ent;
	char *p;
	char path[MAXPATH];
	struct pool *pool_p;
	struct rfstatfs st;

	strcpy (func, "getpoolconf");
	if ((s = fopen (STGCONFIG, "r")) == NULL) {
		stglogit (func, STG23, STGCONFIG);
		return (CONFERR);
	}
	
	/* 1st pass: count number of pools */

	nbpool = 0;
	*defpoolname = '\0';
	defsize = 0;
	while (fgets (buf, sizeof(buf), s) != NULL) {
		if ((p = strtok (buf, " \t\n")) == NULL) continue; 
		if (strcmp (p, "POOL") == 0) nbpool++;
	}
	if (nbpool == 0) {
		stglogit (func, STG29);
		fclose (s);
		return (0);
	}
	pools = (struct pool *) calloc (nbpool, sizeof(struct pool));

	/* 2nd pass: count number of members in each pool */

	rewind (s);
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
				stglogit (func, STG25);	/* name missing */
				errflg++;
				goto reply;
			}
			if ((int) strlen (p) >= (CA_MAXPOOLNAMELEN + 1)) {
				stglogit (func, STG27, p);
				errflg++;
				goto reply;
			}
			strcpy (pool_p->name, p);
			while ((p = strtok (NULL, " \t\n")) != NULL) {
				if (strcmp (p, "DEFSIZE") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->defsize = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp (p, "MINFREE") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL) {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->minfree = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp(p, "GC") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL ||
							strchr (p, ':') == NULL ) {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->gc, p);
				} else if (strcmp(p, "NO_FILE_CREATION") == 0) {
					pool_p->no_file_creation = 1;
				} else if (strcmp(p, "TAPE_POOL") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL ||
							strchr (p, ':') == NULL ) {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->tape_pool, p);
				} else if (strcmp(p, "FREE4MIG") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL ||
							strchr (p, ':') == NULL ) {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
					pool_p->free4mig = strtol (p, &dp, 10);
					if (*dp != '\0') {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
				} else if (strcmp(p, "MIG") == 0) {
					if ((p = strtok (NULL, " \t\n")) == NULL ||
							strchr (p, ':') == NULL ) {
						stglogit (func, STG26, pool_p->name);
						errflg++;
						goto reply;
					}
					strcpy (pool_p->mig, p);
				} else {
					stglogit (func, STG26, pool_p->name);
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
			if ((int) strlen (p) >= (CA_MAXPOOLNAMELEN + 1)) {
				stglogit (func, STG27, p);
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
		} else {
			if (nbpool_ent < 0) {
				stglogit (func, STG26, "");
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
			stglogit (func,".... TAPE_POOL %s FREE4MIG %d MIG %s\n",
								*(pool_p->tape_pool) != '\0' ? pool_p->tape_pool : "<none>" ,
								pool_p->free4mig,
								*(pool_p->mig) != '\0' ? pool_p->mig : "<none>");
		} else if (strcmp (p, "DEFPOOL") == 0) continue;
		else if (strcmp (p, "DEFSIZE") == 0) continue;
		else {
			if ((int) strlen (p) >= (CA_MAXHOSTNAMELEN + 1)) {
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

iscleanovl(pid, status)
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

isvalidpool(poolname)
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
	if (*poolname == '\0') sendrep (rpfd, MSG_OUT, "DEFPOOL %s\n",
																	defpoolname);
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
	struct pool *pool_n, *pool_p;
	char sav_defpoolname[CA_MAXPOOLNAMELEN + 1];
	int sav_nbpool;
	char **sav_poolc;
	struct pool *sav_pools;

	/* save the current configuration */
	strcpy (sav_defpoolname, defpoolname);
	sav_nbpool = nbpool;
	sav_poolc = poolc;
	sav_pools = pools;

	if (c = getpoolconf (defpoolname)) {	/* new configuration is incorrect */
		/* restore the previous configuration */
		strcpy (defpoolname, sav_defpoolname);
		nbpool = sav_nbpool;
		pools = sav_pools;
	} else {			/* free the old configuration */
		/* but keep pid of cleaner if any */
		free (sav_poolc);
		for (i = 0, pool_p = sav_pools; i < sav_nbpool; i++, pool_p++) {
			if (pool_p->ovl_pid) {
				for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
					if (strcmp (pool_n->name, pool_p->name) == 0) {
						pool_n->ovl_pid = pool_p->ovl_pid;
						break;
					}
				}
			}
			free (pool_p->elemp);
		}
		free (sav_pools);
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

char *poolname2tapepool(poolname)
		 char *poolname;
{
	int i;
	struct pool *pool_p;

	if (nbpool == 0 || poolname == NULL) return (NULL);
	for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
		if (strcmp (poolname, pool_p->name) == 0) 
			return(pool_p->tape_pool);
	return (NULL);
}
