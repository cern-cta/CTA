/*
 * $Id: poolmgr.c,v 1.88 2001/02/13 07:55:28 jdurand Exp $
 */

/*
 * Copyright (C) 1993-2000 by CERN/IT/PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: poolmgr.c,v $ $Revision: 1.88 $ $Date: 2001/02/13 07:55:28 $ CERN IT-PDP/DM Jean-Philippe Baud Jean-Damien Durand";
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
#ifdef USECDB
#include "stgdb_Cdb_ifce.h"
#endif
#include "Cns_api.h"
#include "Castor_limits.h"
#include "Cpwd.h"
#include "Cgrp.h"

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
extern struct stgpath_entry *stps;	/* start of stage path catalog */
extern int maxfds;
extern int reqid;
extern int stglogit _PROTO(());
#if (defined(IRIX64) || defined(IRIX5) || defined(IRIX6))
extern int sendrep _PROTO((int, int, ...));
#else
extern int sendrep _PROTO(());
#endif
extern struct stgcat_entry *newreq _PROTO(());
extern int delfile _PROTO((struct stgcat_entry *, int, int, int, char *, uid_t, gid_t, int));
extern int nextreqid _PROTO(());

#if !defined(linux)
extern char *sys_errlist[];
#endif
static struct migrator *migrators;
struct fileclass *fileclasses;
static int nbmigrator;
static int nbpool;
static char *nfsroot;
static char **poolc;
static struct pool *pools;
#ifndef _WIN32
struct sigaction sa_poolmgr;
#endif
static int nbfileclasses;
#ifdef USECDB
extern struct stgdb_fd dbfd;
#endif
extern int savereqs _PROTO(());
extern char localhost[CA_MAXHOSTNAMELEN+1];

struct files_per_stream {
  struct stgcat_entry *stcp;
  struct stgpath_entry *stpp;
  int nstcp;
  u_signed64 size;
  uid_t euid;
  gid_t egid;
};
void print_pool_utilization _PROTO((int, char *, char *, char *, char *, int, int));
int update_migpool _PROTO((struct stgcat_entry *, int, int));
int insert_in_migpool _PROTO((struct stgcat_entry *));
void checkfile2mig _PROTO(());
int migrate_files _PROTO((struct pool *));
int migpoolfiles _PROTO((struct pool *));
int iscleanovl _PROTO((int, int));
void killcleanovl _PROTO((int));
int ismigovl _PROTO((int, int));
void killmigovl _PROTO((int));
int isvalidpool _PROTO((char *));
void migpoolfiles_log_callback _PROTO((int, char *));
int isuserlevel _PROTO((char *));
void poolmgr_wait4child _PROTO(());
int selectfs _PROTO((char *, int *, char *));
void getdefsize _PROTO((char *, int *));
int updfreespace _PROTO((char *, char *, signed64));
void redomigpool _PROTO(());
int updpoolconf _PROTO((char *, char *, char *));
int getpoolconf _PROTO((char *, char *, char *));
int checkpoolcleaned _PROTO((char ***));
void checkpoolspace _PROTO(());
int cleanpool _PROTO((char *));
int get_create_file_option _PROTO((char *));
int poolalloc _PROTO((struct pool *, int));
int checklastaccess _PROTO((char *, time_t));
int enoughfreespace _PROTO((char *, int));
int upd_fileclass _PROTO((struct pool *, struct stgcat_entry *));
int upd_fileclasses _PROTO(());
void upd_last_tppool_used _PROTO((struct fileclass *));
char *next_tppool _PROTO((struct fileclass *));
int euid_egid _PROTO((uid_t *, gid_t *, char *, struct migrator *, struct stgcat_entry *, struct stgcat_entry *, char **, int));
extern int verif_euid_egid _PROTO((uid_t, gid_t, char *, char *));
void stglogfileclass _PROTO((struct Cns_fileclass *));
void printfileclass _PROTO((int, struct fileclass *));
int retenp_on_disk _PROTO((int));
void check_retenp_on_disk _PROTO(());
int tppool_vs_stcp_cmp_vs_size _PROTO((CONST void *, CONST void *));

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
  int defsize;
  char *dp;
  struct pool_element *elemp;
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

  strcpy (func, "getpoolconf");
  if ((s = fopen (STGCONFIG, "r")) == NULL) {
    stglogit (func, STG23, STGCONFIG);
    return (CONFERR);
  }
	
  /* 1st pass: count number of pools and migrators  */

  migrators = NULL;
  nbmigrator_real = 0;
  nbmigrator = 0;
  nbpool = 0;
  *defpoolname = '\0';
  *defpoolname_in = '\0';
  *defpoolname_out = '\0';
  defsize = 0;
  while (fgets (buf, sizeof(buf), s) != NULL) {
    int gotit;
    if (buf[0] == '#') continue;	/* comment line */
    if ((p = strtok (buf, " \t\n")) == NULL) continue; 
    if (strcmp (p, "POOL") == 0) {
      nbpool++;
    }
    gotit = 0;
    while ((p = strtok (NULL, " \t\n")) != NULL) {
      if (strcmp (p, "MIGRATOR") == 0) {
        if (gotit != 0) {
          stglogit (func, STG29);
          fclose (s);
          return (0);
        }
        nbmigrator++;
        gotit = 1;
      }
    }
  }
  if (nbpool == 0) {
    stglogit (func, STG29);
    fclose (s);
    return (0);
  }
  pools = (struct pool *) calloc (nbpool, sizeof(struct pool));
  if (nbmigrator > 0)
    migrators = (struct migrator *)
      calloc (nbmigrator, sizeof(struct migrator));
  /* Although this is no pid for any migration yet, the element migreqtime */
  /* has to be initialized to current time... */
  thistime = time(NULL);
  for (i = 0; i < nbmigrator; i++) {
    migrators[i].migreqtime_last_end = thistime;
  }

  /* 2nd pass: count number of members in each pool and
     store migration parameters */

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
        } else if ((strcmp (p, "MINFREE") == 0) || (strcmp (p, "GC_STOP_THRESH") == 0)) {
          if ((p = strtok (NULL, " \t\n")) == NULL) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
          pool_p->gc_stop_thresh = strtol (p, &dp, 10);
          if (*dp != '\0' || pool_p->gc_stop_thresh < 0) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
        } else if (strcmp (p, "GC_START_THRESH") == 0) {
          if ((p = strtok (NULL, " \t\n")) == NULL) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
          pool_p->gc_start_thresh = strtol (p, &dp, 10);
          if (*dp != '\0' || pool_p->gc_start_thresh < 0) {
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
        } else if (strcmp (p, "MIG_START_THRESH") == 0) {
          if ((p = strtok (NULL, " \t\n")) == NULL) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
          pool_p->mig_start_thresh = strtol (p, &dp, 10);
          if (*dp != '\0' || pool_p->mig_start_thresh < 0) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
        } else if (strcmp (p, "MIG_STOP_THRESH") == 0) {
          if ((p = strtok (NULL, " \t\n")) == NULL) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
          pool_p->mig_stop_thresh = strtol (p, &dp, 10);
          if (*dp != '\0' || pool_p->mig_stop_thresh < 0) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
        } else if (strcmp (p, "MIG_DATA_THRESH") == 0) {
          if ((p = strtok (NULL, " \t\n")) == NULL) {
            stglogit (func, STG26, "pool", pool_p->name);
            errflg++;
            goto reply;
          }
          pool_p->mig_data_thresh = strutou64 (p);
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
    } else if (strcmp (p, "DEFPOOL_IN") == 0) {
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
      strcpy (defpoolname_in, p);
    } else if (strcmp (p, "DEFPOOL_OUT") == 0) {
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
      strcpy (defpoolname_out, p);
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
  if (*defpoolname_in == '\0') {
      strcpy (defpoolname_in, defpoolname);
  } else {
    if (! isvalidpool (defpoolname_in)) {
      stglogit (func, STG32, defpoolname_in);
      errflg++;
      goto reply;
    }
  }
  if (*defpoolname_out == '\0') {
      strcpy (defpoolname_out, defpoolname);
  } else {
    if (! isvalidpool (defpoolname_out)) {
      stglogit (func, STG32, defpoolname_out);
      errflg++;
      goto reply;
    }
  }

  /* Reduce number of migrator in case of pools sharing the same one */
  if ((nbmigrator_real > 0) && (nbmigrator > 0) && (nbmigrator_real < nbmigrator)) {
    struct migrator *dummy;
    
    dummy = (struct migrator *) realloc(migrators, nbmigrator_real * sizeof(struct migrator));
    migrators = dummy;
    nbmigrator = nbmigrator_real;
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
  }

  /* 3rd pass: store pool elements */

  rewind (s);
  pool_p = pools - 1;
  migr_p = migrators - 1;
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
      stglogit (func,"POOL %s DEFSIZE %d GC_STOP_THRESH %d GC_START_THRESH %d\n",
				pool_p->name, pool_p->defsize, pool_p->gc_stop_thresh, pool_p->gc_start_thresh);
      stglogit (func,".... GC %s\n",
				*(pool_p->gc) != '\0' ? pool_p->gc : "<none>");
      stglogit (func,".... NO_FILE_CREATION %d\n",
				pool_p->no_file_creation);
      if (pool_p->migr_name[0] != '\0') {
        stglogit (func,".... MIGRATOR %s\n", pool_p->migr_name);
        stglogit (func,".... MIG_STOP_THRESH %d MIG_START_THRESH %d\n",
                  pool_p->mig_stop_thresh, pool_p->mig_start_thresh);
      }
    } else if (strcmp (p, "DEFPOOL") == 0) continue;
    else if (strcmp (p, "DEFSIZE") == 0) continue;
    else {
      if ((int) strlen (p) > CA_MAXHOSTNAMELEN) {
        stglogit (func, STG26, "pool element [host too long]", p);
        errflg++;
        goto reply;
      }
      strcpy (elemp->server, p);
      if ((p = strtok (NULL, " \t\n")) == NULL) {
        stglogit (func, STG26, "pool element [no path]", elemp->server);
        errflg++;
        goto reply;
      }
      if ((int) strlen (p) >= MAXPATH) {
        stglogit (func, STG26, "pool element [path too long]", p);
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
      if (pool_p->elemp != NULL) free (pool_p->elemp);
    }
    free (pools);
    if (migrators) free (migrators);
    return (CONFERR);
  } else {
    /* This pointer will maintain a list of pools on which a gc have finished since last call */
    poolc = (char **) calloc (nbpool, sizeof(char *));
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
    if (pool_p->ovl_pid != 0) continue;           /* Already has a cleaner running */
    if ((pool_p->free * 100) < (pool_p->capacity * pool_p->gc_start_thresh)) {
      /* Not enough minimum space */
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
    pool_p->cleanreqtime = time(NULL);
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
  pool_p->migr->migreqtime_last_end = time(NULL);
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

void print_pool_utilization(rpfd, poolname, defpoolname, defpoolname_in, defpoolname_out, migrator_flag, class_flag)
     int rpfd;
     char *poolname, *defpoolname, *defpoolname_in, *defpoolname_out;
     int migrator_flag;
     int class_flag;
{
  struct pool_element *elemp;
  int i, j;
  int migr_newline = 1;
  struct pool *pool_p;
  struct migrator *migr_p;
  struct migrator *pool_p_migr = NULL;
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
    sendrep (rpfd, MSG_OUT, "POOL %s%s DEFSIZE %d GC_START_THRESH %d GC_STOP_THRESH %d GC %s%s%s%s%s%s%s%s%s\n",
             pool_p->name,
             pool_p->no_file_creation ? " NO_FILE_CREATION" : "",
             pool_p->defsize,
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
             (pool_p->migr_name[0] != '\0') ? u64tostru(pool_p->mig_data_thresh, tmpbuf3, 0) : ""
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
      sendrep (rpfd, MSG_OUT, "\tNBFILES_BEING_MIGR     %d\n", migr_p->global_predicates.nbfiles_beingmig);
      sendrep (rpfd, MSG_OUT, "\tSPACE_BEING_MIGR       %s\n", u64tostru(migr_p->global_predicates.space_beingmig, tmpbuf, 0));
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
      for (j = 0; j < migr_p->nfileclass; j++) {
        sendrep (rpfd, MSG_OUT, "\tFILECLASS %s@%s (classid=%d)\n",
                 migr_p->fileclass[j]->Cnsfileclass.name,
                 migr_p->fileclass[j]->server,
                 migr_p->fileclass[j]->Cnsfileclass.classid);
        sendrep (rpfd, MSG_OUT, "\t\tNBFILES_CAN_BE_MIGR    %d\n", migr_p->fileclass_predicates[j].nbfiles_canbemig);
        sendrep (rpfd, MSG_OUT, "\t\tSPACE_CAN_BE_MIGR      %s\n", u64tostru(migr_p->fileclass_predicates[j].space_canbemig, tmpbuf, 0));
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
            path, u64tostr(reqsize, tmpbuf0, 0), u64tostr(elemp->free, tmpbuf1, 0), u64tostr(pool_p->free, tmpbuf2, 0));
  return (1);
}

void
getdefsize(poolname, size)
     char *poolname;
     int *size;
{
  int i;
  struct pool *pool_p;

  for (i = 0, pool_p = pools; i < nbpool; i++, pool_p++)
    if (strcmp (poolname, pool_p->name) == 0) break;
  *size = pool_p->defsize;
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

int updpoolconf(defpoolname,defpoolname_in,defpoolname_out)
     char *defpoolname;
     char *defpoolname_in;
     char *defpoolname_out;
{
  int c, i, j;
  struct pool *pool_n, *pool_p;
  char sav_defpoolname[CA_MAXPOOLNAMELEN + 1];
  char sav_defpoolname_in[CA_MAXPOOLNAMELEN + 1];
  char sav_defpoolname_out[CA_MAXPOOLNAMELEN + 1];
  struct migrator *sav_migrator;
  int sav_nbmigrator;
  int sav_nbpool;
  char **sav_poolc;
  struct pool *sav_pools;
  extern int migr_init;

  /* save the current configuration */
  strcpy (sav_defpoolname, defpoolname);
  strcpy (sav_defpoolname_in, defpoolname_in);
  strcpy (sav_defpoolname_out, defpoolname_out);
  sav_migrator = migrators;
  sav_nbmigrator = nbmigrator;
  sav_nbpool = nbpool;
  sav_poolc = poolc;
  sav_pools = pools;

  if ((c = getpoolconf (defpoolname,defpoolname_in,defpoolname_out))) {	/* new configuration is incorrect */
    /* restore the previous configuration */
    strcpy (defpoolname, sav_defpoolname);
    strcpy (defpoolname_in, sav_defpoolname_in);
    strcpy (defpoolname_out, sav_defpoolname_out);
    migrators = sav_migrator;
    nbmigrator = sav_nbmigrator;
    nbpool = sav_nbpool;
    pools = sav_pools;
  } else {			/* free the old configuration */
    /* but keep pids of cleaner/migrator as well as started time if any */
    free (sav_poolc);
    for (i = 0, pool_p = sav_pools; i < sav_nbpool; i++, pool_p++) {
      if ((pool_p->ovl_pid != 0) || (pool_p->migr != NULL && pool_p->migr->mig_pid)) {
        for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
          if (strcmp (pool_n->name, pool_p->name) == 0) {
            pool_n->ovl_pid = pool_p->ovl_pid;
            pool_n->cleanreqtime = pool_p->cleanreqtime;
            if (pool_n->migr != NULL && pool_p->migr != NULL) {
              pool_n->migr->mig_pid = pool_p->migr->mig_pid;
              pool_n->migr->migreqtime = pool_p->migr->migreqtime;
              pool_n->migr->migreqtime_last_end = pool_p->migr->migreqtime_last_end;
            }
            break;
          }
        }
      }
      free (pool_p->elemp);
    }
    free (sav_pools);
    if (sav_migrator) free (sav_migrator);
  }
  if (migr_init != 0) {
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
        for (k = 0; k < pool_n->migr->nfileclass; k++) {
          pool_n->migr->fileclass_predicates[k].nbfiles_canbemig = 0;
          pool_n->migr->fileclass_predicates[k].space_canbemig = 0;
          pool_n->migr->fileclass_predicates[k].nbfiles_beingmig = 0;
          pool_n->migr->fileclass_predicates[k].space_beingmig = 0;
        }
      }
    }
    redomigpool();
  }
  return (c);
}

void redomigpool()
{
  struct stgcat_entry *stcp;

  for (stcp = stcs; stcp < stce; stcp++) {
    if ((stcp->status & CAN_BE_MIGR) != CAN_BE_MIGR) continue;
    if ((stcp->status & PUT_FAILED) == PUT_FAILED) continue;
    insert_in_migpool(stcp);
  }
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

/* flag means :                                           */
/*  1             file put in stack can_be_migr           */
/* -1             file removed from stack can_be_migr     */
/* immediate parameter: used only if flag == 1;           */
/* If 0           set only canbemigr stack                */
/* If 1           set both canbemigr and beingmigr stacks */
/* If 2           set only being migr stack               */
/* Returns: 0 (OK) or -1 (NOT OK)                         */
int update_migpool(stcp,flag,immediate)
     struct stgcat_entry *stcp;
     int flag;
     int immediate;
{
  int i, ipool;
  struct pool *pool_p;
  int ifileclass;
  struct stgcat_entry savestcp = *stcp;
  char *func = "update_migpool";
  int savereqid = reqid;
  int rc = 0;

  if (((flag != 1) && (flag != -1)) || ((immediate != 0) && (immediate != 1) && (immediate != 2))) {
    sendrep(rpfd, MSG_ERR, STG105, func,  "flag should be 1 or -1, and immediate should be 1, 2 or 3");
    serrno = EINVAL;
    rc = -1;
    goto update_migpool_return;
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
    stglogit (func, STG32, stcp->poolname);
    serrno = EINVAL;
    rc = -1;
    goto update_migpool_return;
  }
  /* We check that this pool have a migrator */
  if (pool_p->migr == NULL) {
    return(0);
  }
  /* We check that this stcp have a known fileclass v.s. its pool migrator */
  if ((ifileclass = upd_fileclass(pool_p,stcp)) < 0) {
    rc = -1;
    goto update_migpool_return;
  }

  /* If this fileclass specifies nbcopies == 0 or nbtppools == 0 (in practice, both) */
  /* [it is protected to restrict to this - see routine upd_fileclass() in this source] */
  /* this stcp cannot be a candidate for migration - we put it is STAGED status */
  /* immediately */
  if ((pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbcopies == 0) || 
      (pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbtppools == 0)) {
    sendrep(rpfd, MSG_ERR, STG139,
            stcp->u1.h.xfile, 
            pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
            pool_p->migr->fileclass[ifileclass]->server,
            pool_p->migr->fileclass[ifileclass]->Cnsfileclass.classid,
            pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbcopies,
            pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbtppools);
    /* We mark it as staged */
    stcp->status |= STAGED;
    if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
      /* And not as a migration candidate */
      stcp->status &= ~CAN_BE_MIGR;
    }
    /* But we still returns ok... */
    return(0);
  }

  switch (flag) {
  case -1:
    if ((stcp->status & CAN_BE_MIGR) != CAN_BE_MIGR) {
      /* This is a not a return from automatic migration */
      return(0);
    }
    /* This is a return from automatic migration */
    /* We update global migrator variables */
    if (pool_p->migr->global_predicates.nbfiles_canbemig-- < 0) {
      sendrep(rpfd, MSG_ERR, STG106, func,
              stcp->poolname,
              "nbfiles_canbemig < 0 after automatic migration OK (resetted to 0)");
      pool_p->migr->global_predicates.nbfiles_canbemig = 0;
    }
    if (pool_p->migr->global_predicates.space_canbemig < stcp->actual_size) {
      sendrep(rpfd, MSG_ERR, STG106,
              func,
              stcp->poolname,
              "space_canbemig < stcp->actual_size after automatic migration OK (resetted to 0)");
      pool_p->migr->global_predicates.space_canbemig = 0;
    } else {
      pool_p->migr->global_predicates.space_canbemig -= stcp->actual_size;
    }
    if ((stcp->status == (STAGEPUT|CAN_BE_MIGR)) || ((stcp->status & BEING_MIGR) == BEING_MIGR)) {
      if (pool_p->migr->global_predicates.nbfiles_beingmig-- < 0) {
        sendrep(rpfd, MSG_ERR, STG106,
                func,
                stcp->poolname,
                "nbfiles_beingmig < 0 after automatic migration OK (resetted to 0)");
        pool_p->migr->global_predicates.nbfiles_beingmig = 0;
      }
      if (pool_p->migr->global_predicates.space_beingmig < stcp->actual_size) {
        sendrep(rpfd, MSG_ERR, STG106,
                func,
                stcp->poolname,
                "space_beingmig < stcp->actual_size after automatic migration OK (resetted to 0)");
        pool_p->migr->global_predicates.space_beingmig = 0;
      } else {
        pool_p->migr->global_predicates.space_beingmig -= stcp->actual_size;
      }
    }
    /* We update fileclass_vs_migrator variables */
    if (pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig-- < 0) {
      sendrep(rpfd, MSG_ERR, STG110,
              func,
              stcp->poolname,
              pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
              pool_p->migr->fileclass[ifileclass]->server,
              "nbfiles_canbemig < 0 after automatic migration OK (resetted to 0)");
      pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig = 0;
    }
    if (pool_p->migr->fileclass_predicates[ifileclass].space_canbemig < stcp->actual_size) {
      sendrep(rpfd, MSG_ERR, STG110,
              func,
              stcp->poolname,
              pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
              pool_p->migr->fileclass[ifileclass]->server,
              "space_canbemig < stcp->actual_size after automatic migration OK (resetted to 0)");
      pool_p->migr->fileclass_predicates[ifileclass].space_canbemig = 0;
    } else {
      pool_p->migr->fileclass_predicates[ifileclass].space_canbemig -= stcp->actual_size;
    }
    if ((stcp->status == (STAGEPUT|CAN_BE_MIGR)) || ((stcp->status & BEING_MIGR) == BEING_MIGR)) {
      if (pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig-- < 0) {
        sendrep(rpfd, MSG_ERR, STG110,
                func,
                stcp->poolname,
                pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
                pool_p->migr->fileclass[ifileclass]->server,
                "nbfiles_beingmig < 0 after automatic migration OK (resetted to 0)");
        pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig = 0;
      }
      if (pool_p->migr->fileclass_predicates[ifileclass].space_beingmig < stcp->actual_size) {
        sendrep(rpfd, MSG_ERR, STG110,
                func,
                stcp->poolname,
                pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
                pool_p->migr->fileclass[ifileclass]->server,
                "space_beingmig < stcp->actual_size after automatic migration OK (resetted to 0)");
        pool_p->migr->fileclass_predicates[ifileclass].space_beingmig = 0;
      } else {
        pool_p->migr->fileclass_predicates[ifileclass].space_beingmig -= stcp->actual_size;
      }
    }
    if ((stcp->status & BEING_MIGR) == BEING_MIGR) stcp->status &= ~BEING_MIGR;
    break;
  case 1:
    /* This is to add an entry for the next automatic migration */

    if (stcp->actual_size <= 0) {
      sendrep(rpfd, MSG_ERR, STG105, "update_migpool", "stcp->actual_size <= 0");
      /* No point */
      serrno = EINVAL;
      rc = -1;
      break;
    }

    /* If this entry has an empty member stcp->u1.h.tppool this means that this */
    /* is a new entry that we will have to migrate in conformity with its fileclass */
    /* Otherwise this means that this is an entry that is a survivor of a previous */
    /* migration that failed - this can happen in particular at a start of the stgdaemon */
    /* when it loads and check the full catalog before starting processing requests... */

    if (stcp->u1.h.tppool[0] == '\0') {
      for (i = 0; i < pool_p->migr->fileclass[ifileclass]->Cnsfileclass.nbcopies; i++) {
        /* We decide right now to which tape pool this stcp will go */
        /* We have the list of available tape pools in */
        if (i > 0) {
          /* We need to create a new entry in the catalog for this file */
          stcp = newreq();
          memcpy(stcp, &savestcp, sizeof(struct stgcat_entry));
          stcp->reqid = nextreqid();
          reqid = savereqid;
        }
        /* We flag this stcp as a candidate for migration */
        stcp->status |= CAN_BE_MIGR;
        strcpy(stcp->u1.h.tppool,next_tppool(pool_p->migr->fileclass[ifileclass]));
        stglogit(func, "STG98 - %s %s (copy No %d) with tape pool %s\n",
                 i > 0 ? "Extended" : "Modified",
                 stcp->u1.h.xfile,
                 i,
                 stcp->u1.h.tppool);
      }
    } else {
      stglogit(func, STG120,
               stcp->u1.h.xfile,
               stcp->u1.h.tppool,
               pool_p->migr->fileclass[ifileclass]->Cnsfileclass.name,
               pool_p->migr->fileclass[ifileclass]->server,
               pool_p->migr->fileclass[ifileclass]->Cnsfileclass.classid);
      /* We flag this stcp as a candidate for migration */
      stcp->status |= CAN_BE_MIGR;
    }
    switch (immediate) {
    case 0:
      /* We udpate global migrator variables */
      pool_p->migr->global_predicates.nbfiles_canbemig++;
      pool_p->migr->global_predicates.space_canbemig += stcp->actual_size;
      /* We udpate global fileclass variables */
      pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig++;
      pool_p->migr->fileclass_predicates[ifileclass].space_canbemig += stcp->actual_size;
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
      if (stcp->status != (STAGEPUT|CAN_BE_MIGR)) stcp->status |= BEING_MIGR;
      break;
    default:
      break;
    }
    break;
  default:
    sendrep(rpfd, MSG_ERR, STG105, "update_migpool", "flag != 1 && flag != -1");
    serrno = EINVAL;
    rc = -1;
    break;
  }
 update_migpool_return:
  if (rc != 0) {
    /* Oups... Error... We add PUT_FAILED to the the eventual CAN_BE_MIGR state */
    if ((stcp->status & CAN_BE_MIGR) == CAN_BE_MIGR) {
      stcp->status |= PUT_FAILED;
#ifdef USECDB
      if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
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
  pool_p->migr->global_predicates.nbfiles_canbemig++;
  pool_p->migr->global_predicates.space_canbemig += stcp->actual_size;
  if ((stcp->status == (STAGEPUT|CAN_BE_MIGR)) || ((stcp->status & BEING_MIGR) == BEING_MIGR)) {
    pool_p->migr->global_predicates.nbfiles_beingmig++;
    pool_p->migr->global_predicates.space_beingmig += stcp->actual_size;
  }
  if ((ifileclass = upd_fileclass(pool_p,stcp)) >= 0) {
    pool_p->migr->fileclass_predicates[ifileclass].nbfiles_canbemig++;
    pool_p->migr->fileclass_predicates[ifileclass].space_canbemig += stcp->actual_size;
    if ((stcp->status == (STAGEPUT|CAN_BE_MIGR)) || ((stcp->status & BEING_MIGR) == BEING_MIGR)) {
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
    if (((pool_p->mig_data_thresh > 0) && ((pool_p->migr->global_predicates.space_canbemig - pool_p->migr->global_predicates.space_beingmig) > pool_p->mig_data_thresh)) ||
        ((pool_p->mig_start_thresh > 0) && ((pool_p->free * 100) < (pool_p->capacity * pool_p->mig_start_thresh)))) {
      char tmpbuf1[21];
      char tmpbuf2[21];
      char tmpbuf3[21];
      char tmpbuf4[21];

      global_or = 1;
      stglogit("checkfile2mig", "STG98 - Migrator %s@%s - Global Predicate returns true:\n",
               pool_p->migr->name,
               pool_p->name);
      stglogit("checkfile2mig", "STG98 - 1) (%s) (space_canbemig=%s - space_beingmig=%s)=%s > data_mig_threshold=%s ?\n",
               pool_p->mig_data_thresh > 0 ? "ON" : "OFF",
               u64tostru(pool_p->migr->global_predicates.space_canbemig, tmpbuf1, 0),
               u64tostru(pool_p->migr->global_predicates.space_beingmig, tmpbuf2, 0),
               u64tostru(pool_p->migr->global_predicates.space_canbemig - pool_p->migr->global_predicates.space_beingmig, tmpbuf3, 0),
               u64tostru(pool_p->mig_data_thresh, tmpbuf4, 0)
               );
      stglogit("checkfile2mig", "STG98 - 2) (%s) free=%s < (capacity=%s * mig_start_thresh=%d%%)=%s ?\n",
               pool_p->mig_start_thresh > 0 ? "ON" : "OFF",
               u64tostru(pool_p->free, tmpbuf1, 0),
               u64tostru(pool_p->capacity, tmpbuf2, 0),
               pool_p->mig_start_thresh,
               u64tostru((pool_p->capacity * pool_p->mig_start_thresh) /
                         ((u_signed64) 100), tmpbuf3, 0)
               );
    }

    if (global_or == 0) {
      /* Global predicate is not enough - we go through each fileclass predicate */
      
      for (j = 0; j < pool_p->migr->nfileclass; j++) {
        if ((pool_p->migr->fileclass_predicates[j].nbfiles_canbemig - pool_p->migr->fileclass_predicates[j].nbfiles_beingmig) <= 0)	/* No point anyway */
          continue;
        if (((pool_p->mig_data_thresh > 0) && ((pool_p->migr->fileclass_predicates[j].space_canbemig - pool_p->migr->fileclass_predicates[j].space_beingmig) > pool_p->mig_data_thresh)) ||
            ((pool_p->migr->fileclass[j]->Cnsfileclass.migr_time_interval > 0) && ((time(NULL) - pool_p->migr->migreqtime_last_end) > pool_p->migr->fileclass[j]->Cnsfileclass.migr_time_interval))) {
          char tmpbuf1[21];
          char tmpbuf2[21];
          char tmpbuf3[21];
          char tmpbuf4[21];

          global_or = 1;
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
                   u64tostru(pool_p->migr->fileclass_predicates[j].space_canbemig - pool_p->migr->fileclass_predicates[j].space_beingmig, tmpbuf3, 0),
                   u64tostru(pool_p->mig_data_thresh, tmpbuf4, 0)
                   );
          stglogit("checkfile2mig", "STG98 - 2) (%s) last migrator ended %d seconds ago > %d seconds ?\n",
                   pool_p->migr->fileclass[j]->Cnsfileclass.migr_time_interval > 0 ? "ON" : "OFF",
                   (int) (time(NULL) - pool_p->migr->migreqtime_last_end),
                   pool_p->migr->fileclass[j]->Cnsfileclass.migr_time_interval);
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

  strcpy (func, "migrate_files");
  pool_p->migr->mig_pid = fork ();
  pid = pool_p->migr->mig_pid;
  if (pid < 0) {
    stglogit (func, STG02, "", "fork", sys_errlist[errno]);
    pool_p->migr->mig_pid = 0;
    return (SYERR);
  } else if (pid == 0) {  /* we are in the child */
    exit(migpoolfiles(pool_p));
  } else {  /* we are in the parent */
    struct pool *pool_n;
    int okpoolname;
    int ipoolname;
    int j;
    char tmpbuf[21];

    /* We remember all the entries that will be treated in this migration */
    for (stcp = stcs; stcp < stce; stcp++) {
      if (stcp->reqid == 0) break;
      if (! ISCASTORCANBEMIG(stcp)) continue;       /* Not a candidate for migration */
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
      if ((ifileclass = upd_fileclass(pool_p,stcp)) < 0) continue;
      /* We update the fileclass within this migrator watch variables */
      pool_p->migr->fileclass_predicates[ifileclass].nbfiles_beingmig++;
      pool_p->migr->fileclass_predicates[ifileclass].space_beingmig += stcp->actual_size;
      /* We flag this stcp a WAITING_MIGR (before being in STAGEOUT|CAN_BE_MIGR|BEING_MIGR) */
      stcp->status |= WAITING_MIGR;
      /* We update the migrator global watch variables */
      pool_p->migr->global_predicates.nbfiles_beingmig++;
      pool_p->migr->global_predicates.space_beingmig += stcp->actual_size;
    }
      
    stglogit (func, "execing migrator %s@%s for %d HSM files (total of %s), pid=%d\n",
              pool_p->migr->name,
              pool_p->name,
              pool_p->migr->global_predicates.nbfiles_beingmig,
              u64tostru(pool_p->migr->global_predicates.space_beingmig, tmpbuf, 0),
              pid);

    for (j = 0; j < pool_p->migr->nfileclass; j++) {
      if ((pool_p->migr->fileclass_predicates[j].nbfiles_canbemig - pool_p->migr->fileclass_predicates[j].nbfiles_beingmig) <= 0)	/* No point to log */
        continue;
      /* We log how many files per fileclass are concerned for this migration */
      stglogit (func, "detail> migrator %s@%s, Fileclass %s@%s (classid %d) : %d HSM files (total of %s)\n",
                pool_p->migr->name,
                pool_p->name,
                pool_p->migr->fileclass[j]->Cnsfileclass.name,
                pool_p->migr->fileclass[j]->server,
                pool_p->migr->fileclass[j]->Cnsfileclass.classid,
                pool_p->migr->fileclass_predicates[j].nbfiles_beingmig,
                u64tostru(pool_p->migr->fileclass_predicates[j].space_beingmig, tmpbuf, 0));
    }
    /* We keep track of last fork time with the associated pid */
    pool_p->migr->migreqtime = time(NULL);
    if ((pool_p->migr->global_predicates.nbfiles_beingmig == 0) ||
        (pool_p->migr->global_predicates.space_beingmig == 0)) {
      int j;
      struct pool *pool_n;

      stglogit (func, "### Executing recovery procedure - migrator should not have been executed\n");
      /* Update the migrators */
      for (j = 0, pool_n = pools; j < nbpool; j++, pool_n++) {
        if (pool_n->migr != NULL) {
          int k;
          pool_n->migr->global_predicates.nbfiles_canbemig = 0;
          pool_n->migr->global_predicates.space_canbemig = 0;
          pool_n->migr->global_predicates.nbfiles_beingmig = 0;
          pool_n->migr->global_predicates.space_beingmig = 0;
          for (k = 0; k < pool_n->migr->nfileclass; k++) {
            pool_n->migr->fileclass_predicates[k].nbfiles_canbemig = 0;
            pool_n->migr->fileclass_predicates[k].space_canbemig = 0;
            pool_n->migr->fileclass_predicates[k].nbfiles_beingmig = 0;
            pool_n->migr->fileclass_predicates[k].space_beingmig = 0;
          }
        }
      }
      redomigpool();
    }
  }
  return (0);
}

int migpoolfiles(pool_p)
     struct pool *pool_p;
{
  /* We use the weight algorithm defined by Fabrizio Cane for DPM */

  int c;
  struct sorted_ent *prev, *scc, *sci, *scf, *scs, *scc_found;
  struct stgcat_entry *stcp;
  struct stgpath_entry *stpp;
  int found_nbfiles = 0;
  char func[16];
  int okpoolname;
  int ipoolname;
  int term_status;
  pid_t child_pid;
  int i, j, k, l, m, nfiles_per_tppool;
  struct pool *pool_n;
  int found_not_scanned = -1;
  int fork_pid;
  extern struct passwd start_passwd;         /* Generic uid/gid stage:st */
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

  /* We get the minimum size to be transfered */
  minsize = defminsize;
  if ((minsize_per_stream = getconfent ("STG", "MINSIZE_PER_STREAM", 0)) != NULL) {
    minsize = defminsize;
    if ((minsize = strutou64(minsize_per_stream)) <= 0) {
      minsize = defminsize;
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
    return(SYERR);
  }

#ifndef __INSURE__
  /* We do not want any connection but the one to the stgdaemon */
  for (c = 0; c < maxfds; c++)
    close (c);
#endif

#ifndef _WIN32
  signal(SIGCHLD, poolmgr_wait4child);
#endif

  /* build a sorted list of stage catalog entries for the specified pool */
  scf = NULL;
  if ((scs = (struct sorted_ent *) calloc ((pool_p->migr->global_predicates.nbfiles_canbemig - pool_p->migr->global_predicates.nbfiles_beingmig), sizeof(struct sorted_ent))) == NULL) {
    stglogit(func, "### calloc error (%s)\n",strerror(errno));
    return(SYERR);
  }

  for (i = 0; i < pool_p->migr->nfileclass; i++) {
    pool_p->migr->fileclass[i]->streams = 0;
    pool_p->migr->fileclass[i]->being_migr = 0;
  }

  sci = scs;
  for (stcp = stcs; stcp < stce; stcp++) {
    if (stcp->reqid == 0) break;
    if (! ISCASTORCANBEMIG(stcp)) continue;       /* Not a being migrated candidate */
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
    pool_p->migr->fileclass[upd_fileclass(pool_p,stcp)]->being_migr++;
    if (++found_nbfiles > pool_p->migr->global_predicates.nbfiles_canbemig) {
      /* Oupss... We reach the max of entries to migrate that are known to the pool ? */
      stglogit(func, STG114, found_nbfiles, pool_p->migr->global_predicates.nbfiles_canbemig);
      break;
    }
    sci++;
  }

  if (found_nbfiles == 0) {
    stglogit(func, "### Internal error - Found 0 files to migrate !??\n");
    exit(0);
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
    i = -1;
    for (scc = scf; scc; scc = scc->next) {
      ++i;
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
    if (ntppool_vs_stcp == 0) {
      if ((tppool_vs_stcp = (struct files_per_stream *) malloc(sizeof(struct files_per_stream))) == NULL) {
        stglogit(func, "### malloc error (%s)\n",strerror(errno));
        free(scs);
        return(SYERR);
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
        return(SYERR);
      }
      tppool_vs_stcp = dummy;
    }
    ntppool_vs_stcp++;
    tppool_vs_stcp[ntppool_vs_stcp - 1].stcp = NULL;
    tppool_vs_stcp[ntppool_vs_stcp - 1].stpp = NULL;
    tppool_vs_stcp[ntppool_vs_stcp - 1].nstcp = 0;
    tppool_vs_stcp[ntppool_vs_stcp - 1].size = 0;       /* In BYTES - not MBytes */

    /* We grab the most restrictive (euid/egid) pair based on our known fileclasses */
    tppool_vs_stcp[ntppool_vs_stcp - 1].euid = 0;
    tppool_vs_stcp[ntppool_vs_stcp - 1].egid = 0;
    if (euid_egid(&(tppool_vs_stcp[ntppool_vs_stcp - 1].euid),
                  &(tppool_vs_stcp[ntppool_vs_stcp - 1].egid),
                  scc_found->stcp->u1.h.tppool,
                  pool_p->migr, scc_found->stcp, NULL, NULL, 1) != 0) {
      free(scs);
      for (i = 0; i < ntppool_vs_stcp; i++) {
        if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
        if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
      }
      free(tppool_vs_stcp);
      return(SYERR);
    }
    if (tppool_vs_stcp[ntppool_vs_stcp - 1].euid == 0) {
      tppool_vs_stcp[ntppool_vs_stcp - 1].euid = stage_passwd.pw_uid; /* Put default uid */
    }
    if (tppool_vs_stcp[ntppool_vs_stcp - 1].egid == 0) {
      tppool_vs_stcp[ntppool_vs_stcp - 1].egid = stage_passwd.pw_gid; /* Put default gid */
    }
    if (verif_euid_egid(tppool_vs_stcp[ntppool_vs_stcp - 1].euid,tppool_vs_stcp[ntppool_vs_stcp - 1].egid, NULL, NULL) != 0) {
      free(scs);
      for (i = 0; i < ntppool_vs_stcp; i++) {
        if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
        if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
      }
      free(tppool_vs_stcp);
      return(SYERR);
    }

    /* We search all other entries sharing the same tape pool */
    j = -1;
    nfiles_per_tppool = 0;
    /* We reset internal flag to not do double count */
    for (i = 0; i < pool_p->migr->nfileclass; i++) {
      pool_p->migr->fileclass[i]->flag = 0;
    }
    for (scc = scf; scc; scc = scc->next) {
      if (scc->scanned != 0) continue;
      if ((scc == scc_found) || (strcmp(scc->stcp->u1.h.tppool,scc_found->stcp->u1.h.tppool) == 0)) {
        int ifileclass;

        /* And we count how of them we got */
        ++nfiles_per_tppool;
        if ((ifileclass = upd_fileclass(pool_p,scc->stcp)) >= 0) {
          if (pool_p->migr->fileclass[ifileclass]->flag == 0) {
            /* This pool was not yet counted */
            pool_p->migr->fileclass[ifileclass]->streams++;
            pool_p->migr->fileclass[ifileclass]->flag = 1;
          }
        }
      }
    }
    /* We group them into a single entity for API compliance */
    if (((stcp = tppool_vs_stcp[ntppool_vs_stcp - 1].stcp = (struct stgcat_entry *) calloc(nfiles_per_tppool,sizeof(struct stgcat_entry))) == NULL) ||
        ((stpp = tppool_vs_stcp[ntppool_vs_stcp - 1].stpp = (struct stgpath_entry *) calloc(nfiles_per_tppool,sizeof(struct stgpath_entry))) == NULL)) {
      stglogit(func, "### calloc error (%s)\n",strerror(errno));
      free(scs);
      for (i = 0; i < ntppool_vs_stcp; i++) {
        if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
        if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
      }
      free(tppool_vs_stcp);
      return(SYERR);
    }
    tppool_vs_stcp[ntppool_vs_stcp - 1].nstcp = nfiles_per_tppool;
    /* We store them in this area */
    j = -1;
    for (scc = scf; scc; scc = scc->next) {
      if (scc->scanned != 0) continue;
      if ((scc == scc_found) || (strcmp(scc->stcp->u1.h.tppool,scc_found->stcp->u1.h.tppool) == 0)) {
        ++j;
        memcpy(stcp +j,scc->stcp,sizeof(struct stgcat_entry));
        /* We makes sure that the -K option is disabled... This means that when this copy will be migrated */
        /* The corresponding STAGEWRT entry in the catalog will disappear magically !... */
        (stcp+j)->keep = 0;
        strcpy((stpp+j)->upath, (stcp+j)->ipath);
        /* Please note that the (stcp+j)->ipath will be our user path (stpp) */
        scc->scanned = 1;
        tppool_vs_stcp[ntppool_vs_stcp - 1].size += scc->stcp->actual_size;
      }
    }
    /* Next round */
  }

  /* Here we have all the ntppool_vs_stcp streams to launch, each of them composed of */
  /* tppool_vs_stcp[0..ntppool_vs_stcp].nstcp entries */

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

  /* We want to know if we can expand some of the streams to another one, depending on fileclasses policies */
  for (i = 0; i < pool_p->migr->nfileclass; i++) {
    u_signed64 virtual_new_size;
    u_signed64 virtual_old_size;
    int can_create_new_stream;
    int nstcp_to_move;


  retry_for_this_fileclass:
    can_create_new_stream = -1;
    nstcp_to_move = 0;
    if (pool_p->migr->fileclass[i]->streams <= 0) continue;
    if (pool_p->migr->fileclass[i]->streams < pool_p->migr->fileclass[i]->Cnsfileclass.maxdrives) {
      /* We are using less streams than what this fileclass allows us to do */
      /* We check within this fileclass what is the stream that is migrating the most data and if it is */
      /* possible, then relevant, to grab some entries from any stcp from the same fileclass using this tape pool */

      /* In order to reduce as much as possible streams of different sizes, we always chose the */
      /* ones with the highest size first. */
      /* qsort((void *) tppool_vs_stcp, ntppool_vs_stcp, sizeof(struct files_per_stream), &tppool_vs_stcp_cmp_vs_size); */

      /* We calculate the ideal mean of all those streams */
      ideal_minsize = 0;
      nideal_minsize = 0;
      for (j = 0; j < ntppool_vs_stcp; j++) {
        if (tppool_vs_stcp[j].size > minsize) {
          ideal_minsize += (tppool_vs_stcp[j].size - minsize);
          nideal_minsize++;
        }
      }
      ideal_minsize /= ++nideal_minsize;

      for (j = 0; j < ntppool_vs_stcp; j++) {
        if (tppool_vs_stcp[j].size > ideal_minsize) {
          virtual_new_size = 0;
          virtual_old_size = tppool_vs_stcp[j].size;

          /* This stream has, from size point of view, largely enough, but is it possible to shift it by few entries ? */
          if (tppool_vs_stcp[j].nstcp <= 1) continue;
          /* There is for sure more than one entry in this stream - we can try the loop below, in reverse order */
          for (k = tppool_vs_stcp[j].nstcp - 1; k >= 0; k--) {
            /* If we are trying to expand more than once a given tape pool we check that this one matches */
            /* the old one */
            if ((remember_tppool[0] != '\0') && (strcmp(tppool_vs_stcp[j].stcp[k].u1.h.tppool,remember_tppool) != 0)) break;

            if (i != upd_fileclass(pool_p,&(tppool_vs_stcp[j].stcp[k]))) continue;
            /* This stcp shares the same fileclass as i's */
            if ((virtual_old_size -= tppool_vs_stcp[j].stcp[k].actual_size) <= 0) {
              /* We have scanned this whole stream ! */
              break;
            }
            virtual_new_size += tppool_vs_stcp[j].stcp[k].actual_size;
            nstcp_to_move++;
            if (virtual_new_size >= minsize) {
              /* There is right now, or already, material to have a new stream */
              /* We continue the loop up to when we have splitted the original stream almost equally */
              if ((virtual_old_size < ideal_minsize) || (virtual_old_size <= 0)) {
                /* We have scanned a bit too much - we cares only if we have emptied the old stream */
                if (virtual_old_size <= 0) {
                  virtual_old_size += tppool_vs_stcp[j].stcp[k].actual_size;
                  if ((virtual_new_size -= tppool_vs_stcp[j].stcp[k].actual_size) > 0) {
                    can_create_new_stream = k;
                    nstcp_to_move--;
                  }
                  break;
                }
              }
              /* Streams are meant to be approximatively of the same size */
              if (virtual_new_size > virtual_old_size) {
                virtual_old_size += tppool_vs_stcp[j].stcp[k].actual_size;
                if ((virtual_new_size -= tppool_vs_stcp[j].stcp[k].actual_size) > 0) {
                  can_create_new_stream = k;
                  nstcp_to_move--;
                }
                break;
              }
            }
          }
        }
      }
      if (can_create_new_stream >= 0) {
        struct files_per_stream *dummy;
        
        if ((dummy = (struct files_per_stream *) realloc(tppool_vs_stcp,(ntppool_vs_stcp + 1) * sizeof(struct files_per_stream))) == NULL) {
          stglogit(func, "### realloc error (%s)\n",strerror(errno));
          /* But this is NOT fatal in the sense that we already have working streams - we just log this */
          /* and we do as if we found no need to create another stream... */
          can_create_new_stream = -1;
          remember_tppool[0] = '\0';
        } else {
          tppool_vs_stcp = dummy;
          ntppool_vs_stcp++;
          tppool_vs_stcp[ntppool_vs_stcp - 1].stcp = NULL;
          tppool_vs_stcp[ntppool_vs_stcp - 1].stpp = NULL;
          tppool_vs_stcp[ntppool_vs_stcp - 1].nstcp = 0;
          tppool_vs_stcp[ntppool_vs_stcp - 1].size = 0;
          tppool_vs_stcp[ntppool_vs_stcp - 1].euid = 0;
          tppool_vs_stcp[ntppool_vs_stcp - 1].egid = 0;
          stcp = NULL;
          stpp = NULL;
          /* We know try to create enough room for our new stream */
          if (((stcp = tppool_vs_stcp[ntppool_vs_stcp - 1].stcp = (struct stgcat_entry *) calloc(nstcp_to_move,sizeof(struct stgcat_entry))) == NULL) ||
              ((stpp = tppool_vs_stcp[ntppool_vs_stcp - 1].stpp = (struct stgpath_entry *) calloc(nstcp_to_move,sizeof(struct stgpath_entry))) == NULL)) {
            stglogit(func, "### calloc error (%s)\n",strerror(errno));
            /* But this is NOT fatal in the sense that we already have working streams - we just log this */
            if (stcp != NULL) free(stcp);
            if (stpp != NULL) free(stpp);
            ntppool_vs_stcp--;
            /* We also do as if we found no need to create another stream... */
            can_create_new_stream = -1;
            remember_tppool[0] = '\0';
          } else {
            int nstcp_being_moved = 0;

            /* We move some entries to this new stream - We know that "can_create_new_stream" contains */
            /* the index where we stopped the scanning */
            /* We also make sure that we don't take into account the new created stream in the loop below */

            /* We also know in advance that this new streams will be of size virtual_new_size and */
            /* will contain nstcp_to_move entries */

            /* We copy in advance the uid/gid that we yet found for the previous tape pool that we are going */
            /* to duplicate */
            
            for (j = 0; j < (ntppool_vs_stcp - 1); j++) {
              if (tppool_vs_stcp[j].size > ideal_minsize) {
                int save_nstcp = tppool_vs_stcp[j].nstcp;

                if (tppool_vs_stcp[j].nstcp <= 1) continue;
                /* This stream have more than one entry - let's scan it, still in reverse order */
                /* (for consistency, we have to use the algorithm as done upper) */
                for (k = save_nstcp - 1; k >= can_create_new_stream; k--) {
                  /* If we are trying to expand more than once a given tape pool we check that this one matches */
                  /* the old one */
                  if ((remember_tppool[0] != '\0') && (strcmp(tppool_vs_stcp[j].stcp[k].u1.h.tppool,remember_tppool) != 0)) break;
                  /* For the moment, this algorithm can create new stream only for stcp's of the same fileclass */
                  if (i != upd_fileclass(pool_p,&(tppool_vs_stcp[j].stcp[k]))) continue;
                  /* We can move this stcp from tppool No j to tppool No (ntppool_vs_stcp - 1) */

                  /* We check the number of stcp that we move */
                  if (++nstcp_being_moved > nstcp_to_move) {
                    /* We have reached the quota of nstcp_to_move entries... */
                    break;
                  }

                  /* First we move the stcp */
                  *stcp++ = tppool_vs_stcp[j].stcp[k];
                  /* Then we move the stcpp */
                  strcpy(stpp->upath,tppool_vs_stcp[j].stcp[k].ipath);
                  stpp++;
                  /* We increment new stream number of entries */
                  tppool_vs_stcp[ntppool_vs_stcp - 1].nstcp++;
                  /* We increment new stream total size to migrate */
                  tppool_vs_stcp[ntppool_vs_stcp - 1].size += tppool_vs_stcp[j].stcp[k].actual_size;
                  /* We decrement old stream total size to migrate */
                  tppool_vs_stcp[j].size -= tppool_vs_stcp[j].stcp[k].actual_size;

                  if (tppool_vs_stcp[ntppool_vs_stcp - 1].euid == 0) {
                    tppool_vs_stcp[ntppool_vs_stcp - 1].euid = tppool_vs_stcp[j].euid;
                  }
                  if (tppool_vs_stcp[ntppool_vs_stcp - 1].egid == 0) {
                    tppool_vs_stcp[ntppool_vs_stcp - 1].egid = tppool_vs_stcp[j].egid;
                  }

                  /* We have moved entry No k from tape pool No j - we shift all remaining entries */
                  m = 0;
                  for (l = k + 1; l < tppool_vs_stcp[j].nstcp - 1; l++) {
                    tppool_vs_stcp[j].stcp[k+m] = tppool_vs_stcp[j].stcp[l];
                    strcpy(tppool_vs_stcp[j].stpp[k+m].upath, tppool_vs_stcp[j].stpp[l].upath);
                    m++;
                  }

                  /* We decrement old stream number of entries */
                  tppool_vs_stcp[j].nstcp--;
                }
              }
            }
            stglogit(func, STG135,
                     pool_p->migr->fileclass[i]->Cnsfileclass.name,
                     pool_p->migr->fileclass[i]->server,
                     pool_p->migr->fileclass[i]->Cnsfileclass.classid,
                     pool_p->migr->fileclass[i]->Cnsfileclass.maxdrives,
                     u64tostr(tppool_vs_stcp[ntppool_vs_stcp - 1].size, tmpbuf, 0),
                     tppool_vs_stcp[ntppool_vs_stcp - 1].stcp[0].u1.h.tppool);
            strcpy(remember_tppool,tppool_vs_stcp[ntppool_vs_stcp - 1].stcp[0].u1.h.tppool);
            pool_p->migr->fileclass[i]->streams++;
          }
        }
      }
    }
    if (can_create_new_stream >= 0) {
      /* We created a new stream because of this fileclass - can we create again another one ? */
      goto retry_for_this_fileclass;
    }
  }

  /* We fork and execute the stagewrt request(s) */
  for (j = 0; j < ntppool_vs_stcp; j++) {
    if ((fork_pid= fork()) < 0) {
      stglogit(func, "### Cannot fork (%s)\n",strerror(errno));
      free(scs);
      for (i = 0; i < ntppool_vs_stcp; i++) {
        if (tppool_vs_stcp[i].stcp != NULL) free(tppool_vs_stcp[i].stcp);
        if (tppool_vs_stcp[i].stpp != NULL) free(tppool_vs_stcp[i].stpp);
      }
      free(tppool_vs_stcp);
      return(SYERR);
    } else if (fork_pid == 0) {
      /* We are in the child */
      int rc;
      
#ifdef STAGER_DEBUG
      stglogit(func, "### stagewrt_hsm request : Please gdb /usr/local/bin/stgdaemon %d, then break %d\n",getpid(),__LINE__+2);
      sleep(9);
      sleep(1);
#endif

      free(scs);                                      /* Neither for the sorted entries */
      setegid(start_passwd.pw_gid);                   /* Move to admin (who knows) */
      seteuid(start_passwd.pw_uid);
      setegid(tppool_vs_stcp[j].egid);                /* Move to requestor (from fileclasses) */
      seteuid(tppool_vs_stcp[j].euid);
    stagewrt_hsm_retry:
      if ((rc = stagewrt_hsm((u_signed64) STAGE_SILENT,    /* Flags */
                             0,                            /* open flags - disabled */
                             (mode_t) 0,                   /* open mode - disabled */
                             localhost,                    /* Hostname */
                             NULL,                         /* Pooluser */
                             tppool_vs_stcp[j].nstcp,      /* nstcp_input */
                             tppool_vs_stcp[j].stcp,       /* records... */
                             0,                            /* nstcp_output */
                             NULL,                         /* stcp_output */
                             tppool_vs_stcp[j].nstcp,      /* nstpp_input */
                             tppool_vs_stcp[j].stpp        /* stpp_input */
                             )) != 0) {
        stglogit(func, "### stagewrt_hsm request error No %d (%s)\n", serrno, sstrerror(serrno));
        if ((serrno == SECOMERR) || (serrno == SECONNDROP)) {
          /* There was a communication error */
          goto stagewrt_hsm_retry;
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
      stglogit(func, "### stagewrt_hsm request exiting with status %d\n", rc);
      if (rc != 0) {
        stglogit(func, "### ... serrno=%d (%s), errno=%d (%s)\n", serrno, sstrerror(serrno), errno, strerror(errno));
      }
#endif
      exit((rc != 0) ? SYERR : 0);
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

  return(0);
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

/* This routine created if necessary a new fileclass element in the fileclasses array */
int upd_fileclass(pool_p,stcp)
     struct pool *pool_p;
     struct stgcat_entry *stcp;
{
  struct Cns_fileid Cnsfileid;
  struct Cns_filestat Cnsfilestat;
  struct Cns_fileclass Cnsfileclass;
  int ifileclass = -1;
  int ifileclass_vs_migrator = -1;
  int i;

  if ((stcp == NULL) || (stcp->t_or_d != 'h')) {
    serrno = SEINTERNAL;
    return(-1);
  }

  /* We grab fileclass if necessary */
  if (stcp->u1.h.fileclass <= 0) {
    strcpy(Cnsfileid.server,stcp->u1.h.server);
    Cnsfileid.fileid = stcp->u1.h.fileid;
    if (Cns_statx(stcp->u1.h.xfile, &Cnsfileid, &Cnsfilestat) != 0) {
      sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_statx", sstrerror(serrno));
      return(-1);
    }
    if (stcp->u1.h.fileclass != Cnsfilestat.fileclass) {
      stcp->u1.h.fileclass = Cnsfilestat.fileclass;
#ifdef USECDB
      if (stgdb_upd_stgcat(&dbfd,stcp) != 0) {
        stglogit ("upd_fileclass", STG100, "update", sstrerror(serrno), __FILE__, __LINE__);
      }
#endif
      savereqs();
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

    if (Cns_queryclass(stcp->u1.h.server, stcp->u1.h.fileclass, NULL, &Cnsfileclass) != 0) {
      sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_queryclass", sstrerror(serrno));
      return(-1);
    }

    /* We check that this fileclass does not contain values that we cannot sustain */
    if ((sav_nbcopies = Cnsfileclass.nbcopies) < 0) {
      sendrep (rpfd, MSG_ERR, STG126, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbcopies);
      return(-1);
    }
    /* We check that the number of tape pools is valid */
    if (Cnsfileclass.nbtppools < 0) {
      sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "Cns_queryclass", "returns invalid number of tape pool - invalid fileclass - Please contact your admin");
      return(-1);
    }

    /* We allow (nbcopies == 0 && nbtppool == 0) only */
    if ((sav_nbcopies == 0) || (Cnsfileclass.nbtppools == 0)) {
      if ((sav_nbcopies > 0) || (Cnsfileclass.nbtppools > 0)) {
        sendrep (rpfd, MSG_ERR, STG138, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, sav_nbcopies, Cnsfileclass.nbtppools);
        return(-1);
      }
      /* Here we are sure that both nbcopies and nbtpools are equal to zero */
      /* There is no need to check anything - we immediately add this fileclass */
      /* into the main array */
      goto upd_fileclass_add;
    }

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
      sendrep (rpfd, MSG_ERR, STG127, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbtppools, i);
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
            sendrep (rpfd, MSG_ERR, STG128, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, pi);
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
      sendrep (rpfd, MSG_ERR, STG129, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbtppools, new_nbtppools);
      Cnsfileclass.nbtppools = new_nbtppools;
    }
    /* We check the number of copies vs. number of tape pools */
    if (Cnsfileclass.nbcopies > Cnsfileclass.nbtppools) {
      sendrep (rpfd, MSG_ERR, STG130, Cnsfileclass.name, stcp->u1.h.server, Cnsfileclass.classid, Cnsfileclass.nbcopies, Cnsfileclass.nbtppools, Cnsfileclass.nbtppools);
      Cnsfileclass.nbcopies = new_nbtppools;
    }

  upd_fileclass_add:
    /* We add this fileclass to the list of known fileclasses */
    if (nbfileclasses <= 0) {
      if ((fileclasses = (struct fileclass *) malloc(sizeof(struct fileclass))) == NULL) {
        sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "malloc", strerror(errno));
        return(-1);
      }
      nbfileclasses = 0;
    } else {
      struct fileclass *dummy;
      struct pool *pool_n;

      if ((dummy = (struct fileclass *)
           malloc((nbfileclasses + 1) * sizeof(struct fileclass))) == NULL) {
        sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "malloc", strerror(errno));
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
    stglogit ("upd_fileclass", STG109,
              fileclasses[nbfileclasses].Cnsfileclass.name,
              fileclasses[nbfileclasses].server,
              fileclasses[nbfileclasses].Cnsfileclass.classid,
              nbfileclasses,
              fileclasses[nbfileclasses].Cnsfileclass.tppools);
    stglogfileclass(&Cnsfileclass);
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
            sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "malloc", strerror(errno));
            return(-1);
          }
          pool_p->migr->nfileclass = 0;
        } else {
          struct fileclass **dummy;
          struct predicates *dummy2;
          
          if ((dummy = (struct fileclass **) 
                realloc(pool_p->migr->fileclass,(pool_p->migr->nfileclass + 1) * sizeof(struct fileclass *))) == NULL) {
            sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "realloc", strerror(errno));
            return(-1);
          } else {
            if ((dummy2 = (struct predicates *)
                realloc(pool_p->migr->fileclass_predicates,(pool_p->migr->nfileclass + 1) * sizeof(struct predicates))) == NULL) {
              free(dummy);
              sendrep (rpfd, MSG_ERR, STG02, stcp->u1.h.xfile, "realloc", strerror(errno));
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
      /* return(-1); */
    }
    if (fileclasses[i].Cnsfileclass.tppools != NULL) {
      free(fileclasses[i].Cnsfileclass.tppools);
    }
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

int euid_egid(euid,egid,tppool,migr,stcp,stcp_check,tppool_out,being_migr)
     uid_t *euid;
     gid_t *egid;
     char *tppool;
     struct migrator *migr;   /* Migrator - case of all automatic migration calls */
     struct stgcat_entry *stcp;   /* Must be non-NULL if migr == NULL */
     struct stgcat_entry *stcp_check;   /* Used to check requestor compatible uid/gid */
     char **tppool_out;
     int being_migr;              /* We check CAN_BE_MIGR, BEING_MIGR otherwise - only if migr != NULL */
{
  int i, j;
  char *p;
  int found_tppool;
  int found_fileclass;
  uid_t last_fileclass_euid = 0;
  gid_t last_fileclass_egid = 0;

  /* At first call - application have to set them to zero - default is then "stage" uid/gid */
  if (*euid != (uid_t) 0) {
    last_fileclass_euid = *euid;  /* We simulate a virtual previous explicit filter */
  }
  if (*egid != (uid_t) 0) {
    last_fileclass_egid = *egid;  /* We simulate a virtual previous explicit filter */
  }

  /* If migrator is not specified, then stcp have to be speficied */
  if (migr == NULL) {
    if (stcp == NULL) {
      serrno = SEINTERNAL;
      return(-1);
    }
    /* We need to know what is the fileclass for this stcp */
    if ((i = upd_fileclass(NULL,stcp)) < 0) {
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
        sendrep(rpfd, MSG_ERR, STG122, tppool);
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
    if (being_migr != 0) {
      /* Right, this fileclass appears in the migrator knowledge - but does it really trigger it ? */
      if (migr->fileclass[j]->being_migr <= 0) continue;
    } else {
      /* Right, this fileclass appears in the migrator knowledge - but can it really trigger it ? */
      if ((migr->fileclass_predicates[j].nbfiles_canbemig - migr->fileclass_predicates[j].nbfiles_beingmig) <= 0) continue;
    }
    if (found_fileclass == 0) {
      sendrep(rpfd, MSG_ERR, "STG02 - Cannot find fileclass that can trigger your migration\n");
      return(-1);
    }
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
          sendrep(rpfd, MSG_ERR, STG118, last_fileclass_euid, fileclasses[i].Cnsfileclass.name, tppool, "uid", *euid, fileclasses[i].Cnsfileclass.uid);
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
          sendrep(rpfd, MSG_ERR, STG118, last_fileclass_egid, fileclasses[i].Cnsfileclass.name, tppool, "gid", *egid, fileclasses[i].Cnsfileclass.gid);
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
      if (*egid != stcp_check->gid) {
        sendrep(rpfd, MSG_ERR, STG125, stcp_check->user, "gid", stcp_check->gid, "group", *egid, stcp_check->u1.h.xfile);
        return(-1);
      }
      /* We check compatibility of uid */
      if (last_fileclass_euid != 0) {
        /* There is an explicit filter on user id - current's stcp_check->uid have to match it */
        if (*euid != stcp_check->uid) {
          sendrep(rpfd, MSG_ERR, STG125, stcp_check->user, "uid", stcp_check->uid, "user", *euid, stcp_check->u1.h.xfile);
          return(-1);
        }
      } else {
        /* Current's stcp_check's uid is matching since there is no filter on this */
        *euid = stcp_check->uid;
      }
    } else {
      /* There is no explicit filter on group id - Is there explicit filter on user id ? */
      if (last_fileclass_euid != 0) {
        /* There is an explicit filter on user id - current's stcp_check->uid have to match it */
        if (*euid != stcp_check->uid) {
          sendrep(rpfd, MSG_ERR, STG125, stcp_check->user, "uid", stcp_check->uid, "user", *euid, stcp_check->u1.h.xfile);
          return(-1);
        }
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
		if (sav_uid == 0)
          strcpy (sav_uidstr, "-");
		else if ((pw = Cgetpwuid (sav_uid)) != NULL)
          strcpy (sav_uidstr, pw->pw_name);
		else
          sprintf (sav_uidstr, "%-8u", sav_uid);
  }
  if (Cnsfileclass->gid != sav_gid) {
    sav_gid = Cnsfileclass->gid;
    if (sav_gid == 0)
      strcpy (sav_gidstr, "-");
    else if ((gr = Cgetgrgid (sav_gid)) != NULL)
      strcpy (sav_gidstr, gr->gr_name);
    else
      sprintf (sav_gidstr, "%-6u", sav_gid);
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
  stglogit(func,"RETENP_ON_DISK %d\n", Cnsfileclass->retenp_on_disk);
  stglogit(func,"NBTPPOOLS      %d\n", Cnsfileclass->nbtppools);
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

  if (verif_nbtppools != Cnsfileclass->nbtppools) {
    stglogit(func,"### Warning : Found tape pools does not match fileclass - forced to %d\n", verif_nbtppools);
    Cnsfileclass->nbtppools = verif_nbtppools;
  }
  if (Cnsfileclass->nbcopies > Cnsfileclass->nbtppools) {
    stglogit(func,"### Warning : Found number of copies is higher than number of tape pools - forced to %d\n", Cnsfileclass->nbtppools);
    Cnsfileclass->nbcopies = Cnsfileclass->nbtppools;
  }
}

void printfileclass(rpfd,fileclass)
     int rpfd;
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

  if (rpfd < 0) return;

  if (fileclass->Cnsfileclass.uid != sav_uid) {
    sav_uid = fileclass->Cnsfileclass.uid;
		if (sav_uid == 0)
          strcpy (sav_uidstr, "-");
		else if ((pw = Cgetpwuid (sav_uid)) != NULL)
          strcpy (sav_uidstr, pw->pw_name);
		else
          sprintf (sav_uidstr, "%-8u", sav_uid);
  }
  if (fileclass->Cnsfileclass.gid != sav_gid) {
    sav_gid = fileclass->Cnsfileclass.gid;
    if (sav_gid == 0)
      strcpy (sav_gidstr, "-");
    else if ((gr = Cgetgrgid (sav_gid)) != NULL)
      strcpy (sav_gidstr, gr->gr_name);
    else
      sprintf (sav_gidstr, "%-6u", sav_gid);
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
  sendrep(rpfd,MSG_OUT,"\tRETENP_ON_DISK %d\n", fileclass->Cnsfileclass.retenp_on_disk);
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

void check_retenp_on_disk() {
	time_t this_time = time(NULL);
	struct stgcat_entry *stcp;
	char *func = "check_retenp_on_disk";
	extern struct fileclass *fileclasses;
	extern struct passwd start_passwd;             /* Start uid/gid stage:st */

	for (stcp = stcs; stcp < stce; stcp++) {
		int ifileclass;

		if (stcp->t_or_d != 'h') continue;      /* Not a CASTOR file */
		if (stcp->keep) continue;               /* Has -K option */
		if (((stcp->status & STAGEOUT) != STAGEOUT) || ((stcp->status & STAGEWRT) != STAGEWRT)) continue; /* Not coming from a STAGEOUT or a STAGEWRT request */
		if ((stcp->status & STAGED) != STAGED) continue; /* Not STAGEd */
		if ((ifileclass = upd_fileclass(NULL,stcp)) < 0) continue; /* Unknown fileclass */
		if (((int) (this_time - stcp->a_time)) > retenp_on_disk(ifileclass)) { /* Lifetime exceeds ? */
			/* Candidate for garbage */
			stglogit (func, STG133, stcp->u1.h.xfile, fileclasses[ifileclass].Cnsfileclass.name, stcp->u1.h.server, fileclasses[ifileclass].Cnsfileclass.classid, fileclasses[ifileclass].Cnsfileclass.retenp_on_disk, (int) (this_time - stcp->a_time));
			if (delfile (stcp, 0, 1, 1, "check_retenp_on_disk", start_passwd.pw_uid, start_passwd.pw_gid, 0) < 0) {
				stglogit (STG02, stcp->ipath, "rfio_unlink", rfio_serror());
			}
		}
	}
}

int tppool_vs_stcp_cmp_vs_size(p1,p2)
		 CONST void *p1;
		 CONST void *p2;
{
  struct files_per_stream *fp1 = (struct files_per_stream *) p1;
  struct files_per_stream *fp2 = (struct files_per_stream *) p2;
  
  /* We sort with reverse order v.s. size */
  
  if (fp1->size < fp2->size) {
    return(1);
  } else if (fp1->size > fp2->size) {
    return(-1);
  } else {
    return(0);
  }
}
