/*
 * $Id: stgdb_Cdb_ifce.c,v 1.2 1999/12/08 16:02:56 jdurand Exp $
 */

#include <stdio.h>              /* Contains BUFSIZ */
#include <stdlib.h>             /* Contains qsort */

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
static char sccsid[] = "@(#)$RCSfile: stgdb_Cdb_ifce.c,v $ $Revision: 1.2 $ $Date: 1999/12/08 16:02:56 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */

int stgdb_stcpcmp _PROTO((CONST void *, CONST void *));
int stgdb_stppcmp _PROTO((CONST void *, CONST void *));

int stgdb_login(username,password,dbfd)
     char *username;
     char *password;
     stgdb_fd_t *dbfd;
{
  if (Cdb_login(username,password,&(dbfd->Cdb_sess)) != 0) {
    return(-1);
  }
  return(0);
}

int stgdb_logout(dbfd)
     stgdb_fd_t *dbfd;
{
  return(Cdb_logout(&(dbfd->Cdb_sess)));
}

int stgdb_open(dbfd,dbname)
     stgdb_fd_t *dbfd;
     char *dbname;
{
  if (Cdb_open(&(dbfd->Cdb_sess),dbname,&Cdb_stage_interface,&(dbfd->Cdb_db)) != 0) {
    return(-1);
  }
  return(0);
}

int stgdb_close(dbfd)
     stgdb_fd_t *dbfd;
{
  return(Cdb_close(&(dbfd->Cdb_db)));
}

int stgdb_load(dbfd,stcsp,stcep,stgcat_bufsz,stpsp,stpep,stgpath_bufsz)
     stgdb_fd_t *dbfd;
     struct stgcat_entry **stcsp;
     struct stgcat_entry **stcep;
     size_t *stgcat_bufsz;
     struct stgpath_entry **stpsp;
     struct stgpath_entry **stpep;
     size_t *stgpath_bufsz;
{
  struct stgcat_entry *stcp;
  struct stgpath_entry *stpp;
  struct stgcat_tape tape;
  struct stgcat_disk disk;
  struct stgcat_hsm hsm;
  struct stgcat_link link;
  struct stgcat_alloc alloc;

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

  /* We sort the stgcat entries per c_time, and per reqid as secondary key */
  if (stcp > *stcsp) {
    qsort(*stcsp, (stcp - *stcsp), sizeof(struct stgcat_entry), &stgdb_stcpcmp);
  }

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
}

int stgdb_stcpcmp(p1,p2)
     CONST void *p1;
     CONST void *p2;
{
  struct stgcat_entry *stcp1 = (struct stgcat_entry *) p1;
  struct stgcat_entry *stcp2 = (struct stgcat_entry *) p2;
  extern stglogit();

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
               "### Warning : two elements in stgcat have same c_time (%d) and reqid (%d)\n",
               (int) stcp1->c_time, (int) stcp1->reqid);
      return(0);
    }
  }
}

int stgdb_stppcmp(p1,p2)
     CONST void *p1;
     CONST void *p2;
{
  struct stgpath_entry *stpp1 = (struct stgpath_entry *) p1;
  struct stgpath_entry *stpp2 = (struct stgpath_entry *) p2;
  extern stglogit();

  if (stpp1->reqid < stpp2->reqid) {
    return(-1);
  } else if (stpp1->reqid > stpp2->reqid) {
    return(1);
  } else {
    stglogit("stgdb_stppcmp",
             "### Warning : two elements in stgpath have same reqid (%d)\n",(int) stpp1->reqid);
    return(0);
  }
}

int stgdb_upd_stgcat(dbfd,stcp)
     stgdb_fd_t *dbfd;
     struct stgcat_entry *stcp;
{
  int find_status;
  int update_status;
  Cdb_off_t Cdb_offset;

  switch (stcp->t_or_d) {
  case 't':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_tape","stcat_tape_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  case 'd':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_disk","stcat_disk_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  case 'm':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_hsm","stcat_hsm_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  case 'a':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_alloc","stcat_alloc_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  default:
    stglogit("stgdb_upd_stgcat",
             "### Warning : unknown t_or_d element ('%c') for reqid %d\n",
             (char) stcp->t_or_d, (int) stcp->reqid);
    return(-1);
  }

  if (find_status != 0) {
    stglogit("stgdb_upd_stgcat",
             "### Warning : unknown record to update for reqid %d\n",
             (int) stcp->reqid);
    return(-1);
  }

  switch (stcp->t_or_d) {
  case 't':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_tape",(void *) stcp,&Cdb_offset,&Cdb_offset);
    break;
  case 'd':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_disk",(void *) stcp,&Cdb_offset,&Cdb_offset);
    break;
  case 'm':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_hsm",(void *) stcp,&Cdb_offset,&Cdb_offset);
    break;
  case 'a':
    update_status = Cdb_update(&(dbfd->Cdb_db),"stgcat_alloc",(void *) stcp,&Cdb_offset,&Cdb_offset);
    break;
  }

  if (update_status != 0) {
    stglogit("stgdb_upd_stgcat",
             "### Warning : Cdb_update error for reqid %d\n",
             (int) stcp->reqid);
  }

  if (Cdb_unlock(&(dbfd->Cdb_db),"stage",&Cdb_offset) != 0) {
    stglogit("stgdb_upd_stgcat",
             "### Warning : Cdb_unlock error for reqid %d\n",
             (int) stcp->reqid);
  }

  return(update_status);
}

int stgdb_upd_stgpath(dbfd,stpp)
     stgdb_fd_t *dbfd;
     struct stgpath_entry *stpp;
{
  Cdb_off_t Cdb_offset;
  int update_status;

  if (Cdb_keyfind(&(dbfd->Cdb_db),
                  "stgcat_link","stgcat_link_per_reqid","w",
                  (void *) stpp,&Cdb_offset) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning : unknown record to update for reqid %d\n",
             (int) stpp->reqid);
    return(-1);
  }

  if ((update_status = Cdb_update(&(dbfd->Cdb_db),
                                  "stgcat_link",(void *) stpp,&Cdb_offset,&Cdb_offset)) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning : Cdb_update error for reqid %d\n",
             (int) stpp->reqid);
  }

  if (Cdb_unlock(&(dbfd->Cdb_db),"stage",&Cdb_offset) != 0) {
    stglogit("stgdb_upd_stgpath",
             "### Warning : Cdb_unlock error for reqid %d\n",
             (int) stpp->reqid);
  }

  return(update_status);
}

int stgdb_del_stgcat(dbfd,stcp)
     stgdb_fd_t *dbfd;
     struct stgcat_entry *stcp;
{
  int find_status;
  int delete_status;
  Cdb_off_t Cdb_offset;

  switch (stcp->t_or_d) {
  case 't':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_tape","stcat_tape_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  case 'd':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_disk","stcat_disk_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  case 'm':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_hsm","stcat_hsm_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  case 'a':
    find_status = Cdb_keyfind(&(dbfd->Cdb_db),
                              "stgcat_alloc","stcat_alloc_per_reqid","w",
                              (void *) stcp,&Cdb_offset);
    break;
  default:
    stglogit("stgdb_del_stgcat",
             "### Warning : unknown t_or_d element ('%c') for reqid %d\n",
             (char) stcp->t_or_d, (int) stcp->reqid);
    return(-1);
  }

  if (find_status != 0) {
    stglogit("stgdb_del_stgcat",
             "### Warning : unknown record to delete for reqid %d\n",
             (int) stcp->reqid);
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
             "### Warning : Cdb_delete error for reqid %d\n",
             (int) stcp->reqid);
  }

  if (Cdb_unlock(&(dbfd->Cdb_db),"stage",&Cdb_offset) != 0) {
    stglogit("stgdb_del_stgcat",
             "### Warning : Cdb_unlock error for reqid %d\n",
             (int) stcp->reqid);
  }

  return(delete_status);
}

int stgdb_del_stgpath(dbfd,stpp)
     stgdb_fd_t *dbfd;
     struct stgpath_entry *stpp;
{
  Cdb_off_t Cdb_offset;
  int delete_status;

  if (Cdb_keyfind(&(dbfd->Cdb_db),
                  "stgcat_link","stgcat_link_per_reqid","w",
                  (void *) stpp,&Cdb_offset) != 0) {
    stglogit("stgdb_del_stgpath",
             "### Warning : unknown record to update for reqid %d\n",
             (int) stpp->reqid);
    return(-1);
  }

  if ((delete_status = Cdb_delete(&(dbfd->Cdb_db),"stgcat_link",&Cdb_offset)) != 0) {
    stglogit("stgdb_del_stgpath",
             "### Warning : Cdb_update error for reqid %d\n",
             (int) stpp->reqid);
  }

  if (Cdb_unlock(&(dbfd->Cdb_db),"stage",&Cdb_offset) != 0) {
    stglogit("stgdb_del_stgpath",
             "### Warning : Cdb_unlock error for reqid %d\n",
             (int) stpp->reqid);
  }

  return(delete_status);
}

int stgdb_ins_stgcat(dbfd,stcp)
     stgdb_fd_t *dbfd;
     struct stgcat_entry *stcp;
{
  int insert_status;

  switch (stcp->t_or_d) {
  case 't':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_tape",NULL,(void *) stcp,NULL);
    break;
  case 'd':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_disk",NULL,(void *) stcp,NULL);
    break;
  case 'm':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_hsm",NULL,(void *) stcp,NULL);
    break;
  case 'a':
    insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_alloc",NULL,(void *) stcp,NULL);
    break;
  default:
    stglogit("stgdb_ins_stgcat",
             "### Warning : unknown t_or_d element ('%c') for reqid %d\n",
             (char) stcp->t_or_d, (int) stcp->reqid);
    return(-1);
  }

  if (insert_status != 0) {
    stglogit("stgdb_ins_stgcat",
             "### Warning : cannot insert record for reqid %d\n",
             (int) stcp->reqid);
    return(-1);
  }

  return(insert_status);
}

int stgdb_ins_stgpath(dbfd,stpp)
     stgdb_fd_t *dbfd;
     struct stgpath_entry *stpp;
{
  int insert_status;

  insert_status = Cdb_insert(&(dbfd->Cdb_db),"stgcat_link",NULL,(void *) stpp,NULL);

  if (insert_status != 0) {
    stglogit("stgdb_ins_stgpath",
             "### Warning : cannot insert record for reqid %d\n",
             (int) stpp->reqid);
    return(-1);
  }

  return(insert_status);
}

