/*
 * $Id: Cstage_ifce.c,v 1.5 2000/01/03 17:40:33 jdurand Exp $
 */

/*
 * Copyright (C) 1990-1999 by CERN/IT/PDP/DM
 * All rights reserved
 */

/*
 * Cdb/STAGER mapping functions
 */

#include <sys/types.h>
#include <string.h>

#include "Cdb_api.h"
#include "Cstage_db.h"
#include "stage.h"

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cstage_ifce.c,v $ $Revision: 1.5 $ $Date: 2000/01/03 17:40:33 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */


int DLL_DECL stcp2Cdb(stcp,tape,disk,hsm,alloc)
     struct stgcat_entry *stcp;
     struct stgcat_tape  *tape;
     struct stgcat_disk *disk;
     struct stgcat_hsm *hsm;
     struct stgcat_alloc *alloc;
{
  if (stcp == NULL ||
      tape == NULL || disk  == NULL ||
      hsm == NULL  || alloc == NULL) {
    return(-1);
  }

  memset(tape,0,sizeof(struct stgcat_tape));
  memset(disk,0,sizeof(struct stgcat_disk));
  memset(hsm,0,sizeof(struct stgcat_hsm));
  memset(alloc,0,sizeof(struct stgcat_alloc));

  /* We fill the "common" part */
  switch (stcp->t_or_d) {
  case 't':
    tape->reqid       =   stcp->reqid;
    tape->nread       =   stcp->nread;
    tape->size        =   stcp->size;
    tape->nbaccesses  =   stcp->nbaccesses;
    tape->status      =   stcp->status;
    tape->uid         =   stcp->uid;
    tape->gid         =   stcp->gid;
    tape->mask        =   stcp->mask;
    tape->actual_size =   stcp->actual_size;
    tape->c_time      =   stcp->c_time;
    tape->a_time      =   stcp->a_time;
    strcpy(tape->poolname,stcp->poolname);
    strcpy(tape->ipath,   stcp->ipath);
    strcpy(tape->group,   stcp->group);
    strcpy(tape->user,    stcp->user);
    tape->keep        =   stcp->keep;
    break;
  case 'd':
    disk->reqid       =   stcp->reqid;
    disk->nread       =   stcp->nread;
    disk->size        =   stcp->size;
    disk->nbaccesses  =   stcp->nbaccesses;
    disk->status      =   stcp->status;
    disk->uid         =   stcp->uid;
    disk->gid         =   stcp->gid;
    disk->mask        =   stcp->mask;
    disk->actual_size =   stcp->actual_size;
    disk->c_time      =   stcp->c_time;
    disk->a_time      =   stcp->a_time;
    disk->lrecl       =   stcp->lrecl;
    disk->blksize     =   stcp->blksize;
    strcpy(disk->recfm,   stcp->recfm);
    strcpy(disk->poolname,stcp->poolname);
    strcpy(disk->ipath,   stcp->ipath);
    strcpy(disk->group,   stcp->group);
    strcpy(disk->user,    stcp->user);
    disk->keep        =   stcp->keep;
    break;
  case 'm':
    hsm->reqid       =   stcp->reqid;
    hsm->nread       =   stcp->nread;
    hsm->size        =   stcp->size;
    hsm->nbaccesses  =   stcp->nbaccesses;
    hsm->status      =   stcp->status;
    hsm->uid         =   stcp->uid;
    hsm->gid         =   stcp->gid;
    hsm->mask        =   stcp->mask;
    hsm->actual_size =   stcp->actual_size;
    hsm->c_time      =   stcp->c_time;
    hsm->a_time      =   stcp->a_time;
    strcpy(hsm->poolname,stcp->poolname);
    strcpy(hsm->ipath,   stcp->ipath);
    strcpy(hsm->group,   stcp->group);
    strcpy(hsm->user,    stcp->user);
    hsm->keep        =   stcp->keep;
    break;
  case 'a':
    alloc->reqid       =   stcp->reqid;
    alloc->nread       =   stcp->nread;
    alloc->size        =   stcp->size;
    alloc->nbaccesses  =   stcp->nbaccesses;
    alloc->status      =   stcp->status;
    alloc->uid         =   stcp->uid;
    alloc->gid         =   stcp->gid;
    alloc->mask        =   stcp->mask;
    alloc->actual_size =   stcp->actual_size;
    alloc->c_time      =   stcp->c_time;
    alloc->a_time      =   stcp->a_time;
    strcpy(alloc->poolname,stcp->poolname);
    strcpy(alloc->ipath,   stcp->ipath);
    strcpy(alloc->group,   stcp->group);
    strcpy(alloc->user,    stcp->user);
    alloc->keep        =   stcp->keep;
    break;
  default:
    return(-1);
  }

  /* We fill the non-common part of the tables */
  switch (stcp->t_or_d) {
    case 't':
      {
        int i;
        
        tape->blksize     =   stcp->blksize;
        tape->retentd     =   stcp->u1.t.retentd;
        tape->lrecl       =   stcp->lrecl;
        strcpy(tape->lbl,     stcp->u1.t.lbl);
        strcpy(tape->recfm,   stcp->recfm);
        strcpy(tape->den,     stcp->u1.t.den);
        strcpy(tape->dgn,     stcp->u1.t.dgn);
        strcpy(tape->fid,     stcp->u1.t.fid);
        strcpy(tape->fseq,    stcp->u1.t.fseq);
        strcpy(tape->tapesrvr,stcp->u1.t.tapesrvr);
        for (i = 0; i < MAXVSN; i++) {
          strcpy(tape->vid[i],stcp->u1.t.vid[i]);
          strcpy(tape->vsn[i],stcp->u1.t.vsn[i]);
        }
        tape->filstat    =    stcp->u1.t.filstat;
        tape->charconv   =    stcp->charconv;
        tape->E_Tflags   =    stcp->u1.t.E_Tflags;
      }
      break;
  case 'd':
    strcpy(disk->xfile,    stcp->u1.d.xfile);
    strcpy(disk->Xparm,    stcp->u1.d.Xparm);
    break;
  case 'a':
    strcpy(alloc->xfile,    stcp->u1.d.xfile);
    strcpy(alloc->Xparm,    stcp->u1.d.Xparm);
    break;
  case 'm':
    strcpy(hsm->xfile,     stcp->u1.m.xfile);
    break;
  }

  /* OKAY */
  return(0);
}

int DLL_DECL stpp2Cdb(stpp,link)
     struct stgpath_entry *stpp;
     struct stgcat_link *link;
     
{
  if (stpp == NULL || link == NULL) {
    return(-1);
  }

  memset(link,0,sizeof(struct stgcat_link));
  link->reqid       =   stpp->reqid;
  strcpy(link->upath,   stpp->upath);

  /* OKAY */
  return(0);
}

int DLL_DECL Cdb2stcp(stcp,tape,disk,hsm,alloc)
     struct stgcat_entry *stcp;
     struct stgcat_tape  *tape;
     struct stgcat_disk *disk;
     struct stgcat_hsm *hsm;
     struct stgcat_alloc *alloc;
{
  if (stcp == NULL || (tape == NULL && disk == NULL && hsm == NULL && alloc == NULL)) {
    return(-1);
  }

  if ((tape  != NULL && (disk != NULL ||  hsm  != NULL || alloc != NULL)) ||
      (disk  != NULL && (tape != NULL ||  hsm  != NULL || alloc != NULL)) ||
      (hsm   != NULL && (tape != NULL ||  disk != NULL || alloc != NULL)) ||
      (alloc != NULL && (tape != NULL ||  disk != NULL || hsm   != NULL))) {
    return(-1);
  }

  memset(stcp,0,sizeof(struct stgcat_entry));

  /* We fill the common path of stcp */
  if (tape != NULL) {
    stcp->t_or_d = 't';
  } else if (disk != NULL) {
    stcp->t_or_d = 'd';
  } else if (hsm != NULL) {
    stcp->t_or_d = 'm';
  } else {
    stcp->t_or_d = 'a';
  }
  if (tape != NULL) {
    stcp->reqid       =   tape->reqid;
    stcp->nread       =   tape->nread;
    stcp->size        =   tape->size;
    stcp->nbaccesses  =   tape->nbaccesses;
    stcp->status      =   tape->status;
    stcp->uid         =   tape->uid;
    stcp->gid         =   tape->gid;
    stcp->mask        =   tape->mask;
    stcp->actual_size =   tape->actual_size;
    stcp->c_time      =   tape->c_time;
    stcp->a_time      =   tape->a_time;
    strcpy(stcp->poolname,tape->poolname);
    strcpy(stcp->ipath,   tape->ipath);
    strcpy(stcp->group,   tape->group);
    strcpy(stcp->user,    tape->user);
    stcp->keep        =   tape->keep;
  } else if (disk != NULL) {
    stcp->reqid       =   disk->reqid;
    stcp->nread       =   disk->nread;
    stcp->size        =   disk->size;
    stcp->nbaccesses  =   disk->nbaccesses;
    stcp->status      =   disk->status;
    stcp->uid         =   disk->uid;
    stcp->gid         =   disk->gid;
    stcp->mask        =   disk->mask;
    stcp->actual_size =   disk->actual_size;
    stcp->c_time      =   disk->c_time;
    stcp->a_time      =   disk->a_time;
    stcp->blksize     =   disk->blksize;
    stcp->lrecl       =   disk->lrecl;
    strcpy(stcp->recfm,   disk->recfm);
    strcpy(stcp->poolname,disk->poolname);
    strcpy(stcp->ipath,   disk->ipath);
    strcpy(stcp->group,   disk->group);
    strcpy(stcp->user,    disk->user);
    stcp->keep        =   disk->keep;
  } else if (hsm != NULL) {
    stcp->reqid       =   hsm->reqid;
    stcp->nread       =   hsm->nread;
    stcp->size        =   hsm->size;
    stcp->nbaccesses  =   hsm->nbaccesses;
    stcp->status      =   hsm->status;
    stcp->uid         =   hsm->uid;
    stcp->gid         =   hsm->gid;
    stcp->mask        =   hsm->mask;
    stcp->actual_size =   hsm->actual_size;
    stcp->c_time      =   hsm->c_time;
    stcp->a_time      =   hsm->a_time;
    strcpy(stcp->poolname,hsm->poolname);
    strcpy(stcp->ipath,   hsm->ipath);
    strcpy(stcp->group,   hsm->group);
    strcpy(stcp->user,    hsm->user);
    stcp->keep        =   hsm->keep;
  } else {
    stcp->reqid       =   alloc->reqid;
    stcp->nread       =   alloc->nread;
    stcp->size        =   alloc->size;
    stcp->nbaccesses  =   alloc->nbaccesses;
    stcp->status      =   alloc->status;
    stcp->uid         =   alloc->uid;
    stcp->gid         =   alloc->gid;
    stcp->mask        =   alloc->mask;
    stcp->actual_size =   alloc->actual_size;
    stcp->c_time      =   alloc->c_time;
    stcp->a_time      =   alloc->a_time;
    strcpy(stcp->poolname,alloc->poolname);
    strcpy(stcp->ipath,   alloc->ipath);
    strcpy(stcp->group,   alloc->group);
    strcpy(stcp->user,    alloc->user);
    stcp->keep        =   alloc->keep;
  }

  /* We fill the non-common part of stcp */
  switch (stcp->t_or_d) {
  case 't':
    {
      int i;
        
      stcp->blksize      =   tape->blksize;
      stcp->u1.t.retentd =   tape->retentd;
      stcp->lrecl        =   tape->lrecl;
      strcpy(stcp->u1.t.lbl, tape->lbl);
      strcpy(stcp->recfm,    tape->recfm);
      strcpy(stcp->u1.t.den, tape->den);
      strcpy(stcp->u1.t.dgn, tape->dgn);
      strcpy(stcp->u1.t.fid, tape->fid);
      strcpy(stcp->u1.t.fseq,tape->fseq);
      strcpy(stcp->u1.t.tapesrvr,tape->tapesrvr);
      for (i = 0; i < MAXVSN; i++) {
        strcpy(stcp->u1.t.vid[i],tape->vid[i]);
        strcpy(stcp->u1.t.vsn[i],tape->vsn[i]);
      }
      stcp->u1.t.filstat  = tape->filstat;
      stcp->charconv      = tape->charconv;
      stcp->u1.t.E_Tflags = tape->E_Tflags;
    }
    break;
  case 'd':
    strcpy(stcp->u1.d.xfile,disk->xfile);
    strcpy(stcp->u1.d.Xparm,disk->Xparm);
    break;
  case 'm':
    strcpy(stcp->u1.m.xfile,hsm->xfile);
    break;
  case 'a':
    strcpy(stcp->u1.d.xfile,alloc->xfile);
    strcpy(stcp->u1.d.Xparm,alloc->Xparm);
    break;
  default:
    return(-1);
  }

  /* OKAY */
  return(0);
}

int DLL_DECL Cdb2stpp(stpp,link)
     struct stgpath_entry *stpp;
     struct stgcat_link *link;
     
{
  if (stpp == NULL || link == NULL) {
    return(-1);
  }

  memset(stpp,0,sizeof(struct stgpath_entry));
  stpp->reqid       =   link->reqid;
  strcpy(stpp->upath,   link->upath);

  /* OKAY */
  return(0);
}


