/*
 * $Id: stgdb_Cdb_ifce.c,v 1.6 1999/12/14 14:51:53 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

#include <stdio.h>              /* Contains BUFSIZ */
#include <stdlib.h>             /* Contains qsort */
#include <serrno.h>

#include "osdep.h"
#include "stgdb_Cdb_ifce.h"
#include "Cstage_db.h"
#include "Cstage_ifce.h"

#ifdef __STDC__
#define CONST const
#else
#define CONST
#endif

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: stgdb_Cdb_ifce.c,v $ $Revision: 1.6 $ $Date: 1999/12/14 14:51:53 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

int stgdb_stcpcmp _PROTO((CONST void *, CONST void *));
int stgdb_stppcmp _PROTO((CONST void *, CONST void *));
extern stglogit();
extern char func[];

#ifdef STGDB_CONRETRY
#undef STGDB_CONRETRY
#endif
#define STGDB_CONRETRY 6

#ifdef STGDB_CONRETRYINT
#undef STGDB_CONRETRYINT
#endif
#define STGDB_CONRETRYINT 6

#ifdef RETURN
#undef RETURN
#endif

#define RETURN(a) {                                                                         \
  if (a != 0) {                                                                             \
    if (serrno == EDB_A_SESSION || serrno == SECOMERR || serrno == SECONNDROP) {            \
      int iretry;                                                                           \
      stglogit("stgdb_Cdb_ifce","### Warning : Connection with Cdb is lost. "               \
               "Retry to connect %d times (%d seconds between each retry).\n",              \
               STGDB_CONRETRY,STGDB_CONRETRYINT);                                           \
      for (iretry = 0; iretry < STGDB_CONRETRY; iretry++) {                                 \
        if (stgdb_login(dbfd) == 0) {                                                       \
          goto retry_ok;                                                                    \
        }                                                                                   \
        stglogit("stgdb_Cdb_ifce","### Warning : Cannot reconnect to Cdb at retry No %d\n", \
                 iretry);                                                                   \
      }                                                                                     \
      stglogit("stgdb_Cdb_ifce","### Error : Cannot reconnect to Cdb. Exit.\n");            \
      exit(1);                                                                              \
    retry_ok:                                                                               \
      if (stgdb_open(dbfd,"stage") != 0) {                                                  \
        stglogit("stgdb_Cdb_ifce","### Error : Cannot reopen \"stage\" database. Exit.\n"); \
        exit(1);                                                                            \
      }                                                                                     \
    }                                                                                       \
  }                                                                                         \
}

int DLL_DECL Stgdb_login(dbfd,file,line)
     stgdb_fd_t *dbfd;
     char *file;
     int line;
{
#ifdef USECDB
  /* stglogit(func, "In stgdb_login called at %s:%d\n",file,line); */

  if (Cdb_login(dbfd->username,dbfd->password,&(dbfd->Cdb_sess)) != 0) {
    return(-1);
  }
#endif

  return(0);
}

int DLL_DECL Stgdb_logout(dbfd,file,line)
     stgdb_fd_t *dbfd;
     char *file;
     int line;
{
#ifdef USECDB
  /* stglogit(func, "In stgdb_logout called at %s:%d\n",file,line); */
  return(Cdb_logout(&(dbfd->Cdb_sess)));
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_open(dbfd,dbname,file,line)
     stgdb_fd_t *dbfd;
     char *dbname;
     char *file;
     int line;
{
#ifdef USECDB
  /* stglogit(func, "In stgdb_open called at %s:%d\n",file,line); */

  if (Cdb_open(&(dbfd->Cdb_sess),dbname,&Cdb_stage_interface,&(dbfd->Cdb_db)) != 0) {
    return(-1);
  }
#endif

  return(0);
}

int DLL_DECL Stgdb_close(dbfd,file,line)
     stgdb_fd_t *dbfd;
     char *file;
     int line;
{
#ifdef USECDB
  /* stglogit(func, "In stgdb_close called at %s:%d\n",file,line); */

  return(Cdb_close(&(dbfd->Cdb_db)));
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_load(dbfd,stcsp,stcep,stgcat_bufsz,stpsp,stpep,stgpath_bufsz,file,line)
     stgdb_fd_t *dbfd;
     struct stgcat_entry **stcsp;
     struct stgcat_entry **stcep;
     size_t *stgcat_bufsz;
     struct stgpath_entry **stpsp;
     struct stgpath_entry **stpep;
     size_t *stgpath_bufsz;
     char *file;
     int line;
{
#ifdef USECDB
  struct stgcat_entry *stcp;
  struct stgpath_entry *stpp;
  struct stgcat_tape tape;
  struct stgcat_disk disk;
  struct stgcat_hsm hsm;
  struct stgcat_link link;
  struct stgcat_alloc alloc;

  /* stglogit(func, "In stgdb_load called at %s:%d\n",file,line); */

  /* We init the variable */
  *stcsp = NULL;
  *stpsp = NULL;

  /* We allocate blocks of stcp and stpp */
  *stgcat_bufsz = BUFSIZ;
  *stgpath_bufsz = BUFSIZ;

  if ((*stcsp = stcp = (struct stgcat_entry *) calloc(1, *stgcat_bufsz)) == NULL) {
    goto _stgdb_load_error;
  }
  *stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));

  if ((*stpsp = stpp = (struct stgpath_entry *) calloc(1, *stgpath_bufsz)) == NULL) {
    goto _stgdb_load_error;
  }
  *stpep = *stpsp + (*stgpath_bufsz/sizeof(struct stgpath_entry));

  /* We ask for a dump of tape table from Cdb */
  /* ---------------------------------------- */
  if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_tape","stgcat_tape_per_reqid") != 0) {
    goto _stgdb_load_error;
  }
  while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_tape",NULL,&tape) == 0) {
    struct stgcat_entry thisstcp;

    if (Cdb2stcp(&thisstcp,&tape,NULL,NULL,NULL) != 0) {
      continue;
    }
    if (stcp == *stcep) {
      struct stgcat_entry *newstcs;
      /* The calloced area is not enough */
      *stgcat_bufsz += BUFSIZ;
      if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
        Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_tape","stgcat_tape_per_reqid");
        goto _stgdb_load_error;
      }
      memset(*stcep, 0,
             ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry)));
      *stcsp = newstcs;
      *stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));
    }
    *stcp++ = thisstcp;
  }
  Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_tape","stgcat_tape_per_reqid");

  /* We ask for a dump of disk table from Cdb */
  /* ---------------------------------------- */
  if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_disk","stgcat_disk_per_reqid") != 0) {
    goto _stgdb_load_error;
  }
  while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_disk",NULL,&disk) == 0) {
    struct stgcat_entry thisstcp;
    if (Cdb2stcp(&thisstcp,NULL,&disk,NULL,NULL) != 0) {
      continue;
    }
    if (stcp == *stcep) {
      struct stgcat_entry *newstcs;
      /* The calloced area is not enough */
      *stgcat_bufsz += BUFSIZ;
      if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
        Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_disk","stgcat_disk_per_reqid");
        goto _stgdb_load_error;
      }
      memset(*stcep, 0,
             ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry)));
      *stcsp = newstcs;
      *stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));
    }
    *stcp++ = thisstcp;
  }
  Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_disk","stgcat_disk_per_reqid");

  /* We ask for a dump of hsm table from Cdb */
  /* --------------------------------------- */
  if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_hsm","stgcat_hsm_per_reqid") != 0) {
    goto _stgdb_load_error;
  }
  while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_hsm",NULL,&hsm) == 0) {
    struct stgcat_entry thisstcp;
    if (Cdb2stcp(&thisstcp,NULL,NULL,&hsm,NULL) != 0) {
      continue;
    }
    if (stcp == *stcep) {
      struct stgcat_entry *newstcs;
      /* The calloced area is not enough */
      *stgcat_bufsz += BUFSIZ;
      if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
        Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hsm","stgcat_hsm_per_reqid");
        goto _stgdb_load_error;
      }
      memset(*stcep, 0,
             ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry)));
      *stcsp = newstcs;
      *stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));
    }
    *stcp++ = thisstcp;
  }
  Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_hsm","stgcat_hsm_per_reqid");

  /* We ask for a dump of alloc table from Cdb */
  /* --------------------------------------- */
  if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_alloc","stgcat_alloc_per_reqid") != 0) {
    goto _stgdb_load_error;
  }
  while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_alloc",NULL,&alloc) == 0) {
    struct stgcat_entry thisstcp;
    if (Cdb2stcp(&thisstcp,NULL,NULL,NULL,&alloc) != 0) {
      continue;
    }
    if (stcp == *stcep) {
      struct stgcat_entry *newstcs;
      /* The calloced area is not enough */
      *stgcat_bufsz += BUFSIZ;
      if ((newstcs = (struct stgcat_entry *) realloc(*stcsp,*stgcat_bufsz)) == NULL) {
        Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_alloc","stgcat_alloc_per_reqid");
        goto _stgdb_load_error;
      }
      memset(*stcep, 0,
             ((*stgcat_bufsz - BUFSIZ)/sizeof(struct stgcat_entry)));
      *stcsp = newstcs;
      *stcep = *stcsp + (*stgcat_bufsz/sizeof(struct stgcat_entry));
    }
    *stcp++ = thisstcp;
  }
  Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_alloc","stgcat_alloc_per_reqid");

  /* We ask for a dump of link table from Cdb */
  /* --------------------------------------- */
  if (Cdb_dump_start(&(dbfd->Cdb_db),"stgcat_link","stgcat_link_per_reqid") != 0) {
    goto _stgdb_load_error;
  }
  while (Cdb_dump(&(dbfd->Cdb_db),"stgcat_link",NULL,&link) == 0) {
    struct stgpath_entry thisstpp;
    if (Cdb2stpp(&thisstpp,&link) != 0) {
      continue;
    }
    if (stpp == *stpep) {
      struct stgpath_entry *newstps;
      /* The calloced area is not enough */
      *stgpath_bufsz += BUFSIZ;
      if ((newstps = (struct stgpath_entry *) realloc(*stpsp,*stgpath_bufsz)) == NULL) {
        Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_link","stgcat_link_per_reqid");
        goto _stgdb_load_error;
      }
      memset(*stpep, 0,
             ((*stgpath_bufsz - BUFSIZ)/sizeof(struct stgpath_entry)));
      *stpsp = newstps;
      *stpep = *stpsp + (*stgpath_bufsz/sizeof(struct stgpath_entry));
    }
    *stpp++ = thisstpp;
  }
  Cdb_dump_end(&(dbfd->Cdb_db),"stgcat_link","stgcat_link_per_reqid");

  /*
  {
    struct stgcat_entry *dummy;

    for (dummy = *stcsp; dummy < stcp; dummy++) {
      stglogit("stgdb_load","Got (unsorted) stcp entry reqid %d\n",dummy->reqid);
    }
  }
  stglogit("sgdb_load","[1] stcp=0x%lx, *stcsp=0x%lx, calling qsort\n",(unsigned long) stcp, (unsigned long) *stcsp);
  */

  /* We sort the stgcat entries per c_time, and per reqid as secondary key */
  if (stcp > *stcsp) {
    qsort(*stcsp, (stcp - *stcsp), sizeof(struct stgcat_entry), &stgdb_stcpcmp);
  }

  /*
  {
    struct stgpath_entry *dummy;

    for (dummy = *stpsp; dummy < stpp; dummy++) {
      stglogit("stgdb_load","Got (unsorted) stpp entry reqid %d\n",dummy->reqid);
    }
  }
  stglogit("sgdb_load","[1] stpp=0x%lx, *stpsp=0x%lx, calling qsort\n",(unsigned long) stpp, (unsigned long) *stpsp);
  */

  /* We sort the stgpath entries per reqid */
  if (stpp > *stpsp) {
    qsort(*stpsp, (stpp - *stpsp), sizeof(struct stgpath_entry), &stgdb_stppcmp);
  }

  return(0);

 _stgdb_load_error:
  if (*stcsp != NULL) {
    free(*stcsp);
  }
  if (*stpsp != NULL) {
    free(*stpsp);
  }
  return(-1);
#else
  return(0);
#endif
}

int stgdb_stcpcmp(p1,p2)
     CONST void *p1;
     CONST void *p2;
{
#ifdef USECDB
  struct stgcat_entry *stcp1 = (struct stgcat_entry *) p1;
  struct stgcat_entry *stcp2 = (struct stgcat_entry *) p2;

  if (stcp1->c_time < stcp2->c_time) {
    return(-1);
  } else if (stcp1->c_time > stcp2->c_time) {
    return(1);
  } else {
    if (stcp1->reqid < stcp2->reqid) {
      return(-1);
    } else if (stcp1->reqid > stcp2->reqid) {
      return(1);
    } else {
      stglogit("stgdb_stcpcmp",
               "### Warning[%s:%d] : two elements in stgcat have same c_time (%d) and reqid (%d)\n",
               __FILE__,__LINE__,(int) stcp1->c_time, (int) stcp1->reqid);
      return(0);
    }
  }
#else
  return(0);
#endif
}

int stgdb_stppcmp(p1,p2)
     CONST void *p1;
     CONST void *p2;
{
#ifdef USECDB
  struct stgpath_entry *stpp1 = (struct stgpath_entry *) p1;
  struct stgpath_entry *stpp2 = (struct stgpath_entry *) p2;

  if (stpp1->reqid < stpp2->reqid) {
    return(-1);
  } else if (stpp1->reqid > stpp2->reqid) {
    return(1);
  } else {
    stglogit("stgdb_stppcmp",
             "### Warning[%s:%d] : two elements in stgpath have same reqid (%d)\n",
             __FILE__,__LINE__,(int) stpp1->reqid);
    return(0);
  }
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_upd_stgcat(dbfd,stcp,file,line)
     stgdb_fd_t *dbfd;
     struct stgcat_entry *stcp;
     char *file;
     int line;
{
#ifdef USECDB
  int find_status = -1;
  int update_status;
  Cdb_off_t Cdb_offset;
  struct stgcat_tape tape;
  struct stgcat_disk disk;
  struct stgcat_hsm hsm;
  struct stgcat_alloc alloc;

  /* stglogit(func, "In stgdb_upd_stgcat called at %s:%d\n",file,line); */

  if (stcp2Cdb(stcp,&tape,&disk,&hsm,&alloc) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning[%s:%d] : stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
             __FILE__,__LINE__,stcp->reqid,file,line);
    return(-1);
  }

  switch (stcp->t_or_d) {
  case 't':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_tape","stgcat_tape_per_reqid","w",
                              (void *) &tape,&Cdb_offset);
    break;
  case 'd':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_disk","stgcat_disk_per_reqid","w",
                              (void *) &disk,&Cdb_offset);
    break;
  case 'm':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_hsm","stgcat_hsm_per_reqid","w",
                              (void *) &hsm,&Cdb_offset);
    break;
  case 'a':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_alloc","stgcat_alloc_per_reqid","w",
                              (void *) &alloc,&Cdb_offset);
    break;
  default:
    stglogit("stgdb_upd_stgcat",
             "### Warning[%s:%d] : unknown t_or_d element ('%c') for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(char) stcp->t_or_d, (int) stcp->reqid,file,line);
    return(-1);
  }

  if (find_status != 0) {
    stglogit("stgdb_upd_stgcat",
             "### Warning[%s:%d] : unknown record to update for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stcp->reqid,file,line);
    return(-1);
  }

  switch (stcp->t_or_d) {
  case 't':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_tape",
                               (void *) &tape,&Cdb_offset,&Cdb_offset);
    break;
  case 'd':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_disk",
                               (void *) &disk,&Cdb_offset,&Cdb_offset);
    break;
  case 'm':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_hsm",
                               (void *) &hsm,&Cdb_offset,&Cdb_offset);
    break;
  case 'a':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_alloc",
                               (void *) &alloc,&Cdb_offset,&Cdb_offset);
    break;
  }

  if (update_status != 0) {
    stglogit("stgdb_upd_stgcat",
             "### Warning[%s:%d] : Cdb_update error for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stcp->reqid,file,line);
  }

  switch (stcp->t_or_d) {
  case 't':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_tape",&Cdb_offset) != 0) {
      stglogit("stgdb_upd_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  case 'd':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_disk",&Cdb_offset) != 0) {
      stglogit("stgdb_upd_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  case 'm':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_hsm",&Cdb_offset) != 0) {
      stglogit("stgdb_upd_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  case 'a':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_alloc",&Cdb_offset) != 0) {
      stglogit("stgdb_upd_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  }

  return(update_status);
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_upd_stgpath(dbfd,stpp,file,line)
     stgdb_fd_t *dbfd;
     struct stgpath_entry *stpp;
     char *file;
     int line;
{
#ifdef USECDB
  Cdb_off_t Cdb_offset;
  int update_status;
  struct stgcat_link link;

  /* stglogit(func, "In stgdb_upd_stgpath called at %s:%d\n",file,line); */

  if (stpp2Cdb(stpp,&link) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning[%s:%d] : stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
             __FILE__,__LINE__,stpp->reqid,file,line);
  }

  if (Cdb_keyfind(&(dbfd->Cdb_db), "stgcat_link","stgcat_link_per_reqid","w",
                  (void *) &link,&Cdb_offset) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning[%s:%d] : unknown record to update for reqid %d (%s) called at %s:%d\n",
             __FILE__,__LINE__,(int) stpp->reqid,sstrerror(serrno),file,line);
    return(-1);
  }

  if ((update_status = Cdb_update(&(dbfd->Cdb_db), "stgcat_link",
                                  (void *) &link,&Cdb_offset,&Cdb_offset)) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning[%s:%d] : Cdb_update error for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stpp->reqid,file,line);
  }

  if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_link",&Cdb_offset) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stpp->reqid,file,line);
  }

  return(update_status);
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_del_stgcat(dbfd,stcp,file,line)
     stgdb_fd_t *dbfd;
     struct stgcat_entry *stcp;
     char *file;
     int line;
{
#ifdef USECDB
  int find_status;
  int delete_status;
  Cdb_off_t Cdb_offset;
  struct stgcat_tape tape;
  struct stgcat_disk disk;
  struct stgcat_hsm hsm;
  struct stgcat_alloc alloc;


  /* stglogit(func, "In stgdb_del_stgcat called at %s:%d\n",file,line); */

  if (stcp2Cdb(stcp,&tape,&disk,&hsm,&alloc) != 0) {
    stglogit("stgdb_del_stgcat",
             "### Warning[%s:%d] : stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
             __FILE__,__LINE__,stcp->reqid,file,line);
    return(-1);
  }

  switch (stcp->t_or_d) {
  case 't':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_tape","stgcat_tape_per_reqid","w",
                              (void *) &tape,&Cdb_offset);
    break;
  case 'd':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_disk","stgcat_disk_per_reqid","w",
                              (void *) &disk,&Cdb_offset);
    break;
  case 'm':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_hsm","stgcat_hsm_per_reqid","w",
                              (void *) &hsm,&Cdb_offset);
    break;
  case 'a':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_alloc","stgcat_alloc_per_reqid","w",
                              (void *) &alloc,&Cdb_offset);
    break;
  default:
    stglogit("stgdb_del_stgcat",
             "### Warning[%s:%d] : unknown t_or_d element ('%c') for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(char) stcp->t_or_d, (int) stcp->reqid,file,line);
    return(-1);
  }

  if (find_status != 0) {
    stglogit("stgdb_del_stgcat",
             "### Warning[%s:%d] : unknown record to delete for reqid %d (%s) called at %s:%d\n",
             __FILE__,__LINE__,(int) stcp->reqid,sstrerror(serrno),file,line);
    return(-1);
  }

  switch (stcp->t_or_d) {
  case 't':
    delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_tape",&Cdb_offset);
    break;
  case 'd':
    delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_disk",&Cdb_offset);
    break;
  case 'm':
    delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_hsm",&Cdb_offset);
    break;
  case 'a':
    delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_alloc",&Cdb_offset);
    break;
  }

  if (delete_status != 0) {
    stglogit("stgdb_del_stgcat",
             "### Warning[%s:%d] : Cdb_delete error for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stcp->reqid,file,line);
  }

  switch (stcp->t_or_d) {
  case 't':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_tape",&Cdb_offset) != 0) {
      stglogit("stgdb_del_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  case 'd':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_disk",&Cdb_offset) != 0) {
      stglogit("stgdb_del_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  case 'm':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_hsm",&Cdb_offset) != 0) {
      stglogit("stgdb_del_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  case 'a':
    if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_alloc",&Cdb_offset) != 0) {
      stglogit("stgdb_del_stgcat",
               "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
               __FILE__,__LINE__,(int) stcp->reqid,file,line);
    }
    break;
  }

  return(delete_status);
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_del_stgpath(dbfd,stpp,file,line)
     stgdb_fd_t *dbfd;
     struct stgpath_entry *stpp;
     char *file;
     int line;
{
#ifdef USECDB
  Cdb_off_t Cdb_offset;
  int delete_status;
  struct stgcat_link link;

  /* stglogit(func, "In stgdb_del_stgpath called at %s:%d\n",file,line); */

  if (stpp2Cdb(stpp,&link) != 0) {
    stglogit("stgdb_del_stgpath",
             "### Warning[%s:%d] : stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
             __FILE__,__LINE__,stpp->reqid,file,line);
  }

  if (Cdb_keyfind(&(dbfd->Cdb_db), "stgcat_link","stgcat_link_per_reqid","w",
                  (void *) &link,&Cdb_offset) != 0) {
    stglogit("stgdb_del_stgpath",
             "### Warning[%s:%d] : unknown record to update for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stpp->reqid,file,line);
    return(-1);
  }

  if ((delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_link",&Cdb_offset)) != 0) {
    stglogit("stgdb_del_stgpath",
             "### Warning[%s:%d] : Cdb_update error for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stpp->reqid,file,line);
  }

  if (Cdb_unlock(&(dbfd->Cdb_db),"stgcat_link",&Cdb_offset) != 0) {
    stglogit("stgdb_del_stgpath",
             "### Warning[%s:%d] : Cdb_unlock error for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stpp->reqid,file,line);
  }

  return(delete_status);
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_ins_stgcat(dbfd,stcp,file,line)
     stgdb_fd_t *dbfd;
     struct stgcat_entry *stcp;
     char *file;
     int line;
{
#ifdef USECDB
  int insert_status;
  struct stgcat_tape tape;
  struct stgcat_disk disk;
  struct stgcat_hsm hsm;
  struct stgcat_alloc alloc;

  /* stglogit(func, "In stgdb_ins_stgcat called at %s:%d\n",file,line); */

  if (stcp2Cdb(stcp,&tape,&disk,&hsm,&alloc) != 0) {
    stglogit("stgdb_ins_stgcat",
             "### Warning[%s:%d] : stcp2Cdb (eg. stgcat -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
             __FILE__,__LINE__,stcp->reqid,file,line);
    return(-1);
  }

  switch (stcp->t_or_d) {
  case 't':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_tape",NULL,(void *) &tape,NULL);
    break;
  case 'd':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_disk",NULL,(void *) &disk,NULL);
    break;
  case 'm':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_hsm",NULL,(void *) &hsm,NULL);
    break;
  case 'a':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_alloc",NULL,(void *) &alloc,NULL);
    break;
  default:
    stglogit("stgdb_ins_stgcat",
             "### Warning[%s:%d] : unknown t_or_d element ('%c') for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(char) stcp->t_or_d, (int) stcp->reqid,file,line);
    return(-1);
  }

  if (insert_status != 0) {
    stglogit("stgdb_ins_stgcat",
             "### Warning[%s:%d] : cannot insert record for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stcp->reqid,file,line);
    return(-1);
  }

  return(insert_status);
#else
  return(0);
#endif
}

int DLL_DECL Stgdb_ins_stgpath(dbfd,stpp,file,line)
     stgdb_fd_t *dbfd;
     struct stgpath_entry *stpp;
     char *file;
     int line;
{
#ifdef USECDB
  int insert_status;
  struct stgcat_link link;

  /* stglogit(func, "In stgdb_ins_stgpath called at %s:%d\n",file,line); */

  if (stpp2Cdb(stpp,&link) != 0) {
    stglogit("stgdb_ins_stgpath",
             "### Warning[%s:%d] : stpp2Cdb (eg. stgpath -> Cdb on-the-fly) conversion error for reqid = %d called at %s:%d\n",
             __FILE__,__LINE__,stpp->reqid,file,line);
  }
  
  insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_link",NULL,(void *) &link,NULL);
  
  if (insert_status != 0) {
    stglogit("stgdb_ins_stgpath",
             "### Warning[%s:%d] : cannot insert record for reqid %d called at %s:%d\n",
             __FILE__,__LINE__,(int) stpp->reqid,file,line);
    return(-1);
  } else {
    if (serrno == EDB_D_UNIQUE) {
      return(stgdb_upd_stgpath(dbfd,stpp));
    } else {
      return(-1);
    }
  }
#else
  return(0);
#endif
}

