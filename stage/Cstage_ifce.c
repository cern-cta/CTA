/*
 * $Id: Cstage_ifce.c,v 1.1 1999/11/29 15:27:43 jdurand Exp $
 */

/*
 * Cdb/STAGER mapping functions
 */

#include <sys/types.h>
#include <string.h>

#include "Cdb_api.h"
#include "Cstage_db.h"
#include "stage.h"

int DLL_DECL stcp2Cdb(stcp,common,tape,disk,hsm)
     struct stgcat_entry *stcp;
     struct Cstgcat_common *common;
     struct Cstgcat_tape  *tape;
     struct Cstgcat_disk *disk;
     struct Cstgcat_hsm *hsm;
     
{
  if (stcp == NULL || common == NULL ||
      tape == NULL || disk == NULL   ||
      hsm == NULL) {
    return(-1);
  }

  common->reqid       =   stcp->reqid;
  common->nread       =   stcp->nread;
  common->size        =   stcp->size;
  common->nbaccesses  =   stcp->nbaccesses;
  common->status      =   stcp->status;
  common->uid         =   stcp->uid;
  common->gid         =   stcp->gid;
  common->mask        =   stcp->mask;
  common->actual_size =   stcp->actual_size;
  common->c_time      =   stcp->c_time;
  common->a_time      =   stcp->a_time;
  memcpy(common->filler,  stcp->filler,2);
  strcpy(common->poolname,stcp->poolname);
  strcpy(common->ipath,   stcp->ipath);
  strcpy(common->group,   stcp->group);
  strcpy(common->user,    stcp->user);
  common->t_or_d      =   stcp->t_or_d;
  common->keep        =   stcp->keep;

  switch (stcp->t_or_d) {
    case 't':
      {
        int i;
        
        tape->reqid       =   stcp->reqid;
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
    disk->reqid        =   stcp->reqid;
    strcpy(disk->xfile,    stcp->u1.d.xfile);
    strcpy(disk->Xparm,    stcp->u1.d.Xparm);
    break;
  case 'm':
    hsm->reqid         =   stcp->reqid;
    strcpy(hsm->xfile,     stcp->u1.m.xfile);
    break;
  default:
    return(-1);
  }
  /* OKAY */
  return(0);
}

int DLL_DECL stpp2Cdb(stpp,link)
     struct stgpath_entry *stpp;
     struct Cstgcat_link *link;
     
{
  if (stpp == NULL || link == NULL) {
    return(-1);
  }

  link->reqid       =   stpp->reqid;
  strcpy(link->upath,   stpp->upath);

  /* OKAY */
  return(0);
}

