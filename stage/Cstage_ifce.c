/*
 * $Id: Cstage_ifce.c,v 1.11 2001/11/30 11:24:11 jdurand Exp $
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
#include "Cstage_ifce.h"

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: Cstage_ifce.c,v $ $Revision: 1.11 $ $Date: 2001/11/30 11:24:11 $ CERN IT-PDP/DM Jean-Damien Durand";
#endif /* not lint */


int DLL_DECL stcp2Cdb(stcp,tape,disk,hsm,castor,alloc)
		 struct stgcat_entry *stcp;
		 struct stgcat_tape  *tape;
		 struct stgcat_disk *disk;
		 struct stgcat_hpss *hsm;
		 struct stgcat_hsm *castor;
		 struct stgcat_alloc *alloc;
{
	int i;

	if (stcp == NULL ||
		tape == NULL || disk  == NULL  ||
		hsm == NULL  || castor == NULL || alloc == NULL) {
		return(-1);
	}

	switch (stcp->t_or_d) {
	case 't':
		memset(tape,0,sizeof(struct stgcat_tape));
		tape->reqid       =   stcp->reqid;
		tape->nread       =   stcp->nread;
		tape->size        =   stcp->size;
		tape->nbaccesses  =   stcp->nbaccesses;
		tape->status      =   stcp->status;
		tape->blksize     =   stcp->blksize;
		tape->retentd     =   stcp->u1.t.retentd;
		tape->lrecl       =   stcp->lrecl;
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
		break;
	case 'd':
		memset(disk,0,sizeof(struct stgcat_disk));
		disk->reqid       =   stcp->reqid;
		disk->nread       =   stcp->nread;
		disk->size        =   stcp->size;
		disk->nbaccesses  =   stcp->nbaccesses;
		disk->status      =   stcp->status;
		disk->blksize     =   stcp->blksize;
		disk->lrecl       =   stcp->lrecl;
		disk->uid         =   stcp->uid;
		disk->gid         =   stcp->gid;
		disk->mask        =   stcp->mask;
		disk->actual_size =   stcp->actual_size;
		disk->c_time      =   stcp->c_time;
		disk->a_time      =   stcp->a_time;
		strcpy(disk->recfm,   stcp->recfm);
		strcpy(disk->poolname,stcp->poolname);
		strcpy(disk->ipath,   stcp->ipath);
		strcpy(disk->group,   stcp->group);
		strcpy(disk->user,    stcp->user);
		disk->keep        =   stcp->keep;
		strcpy(disk->xfile,    stcp->u1.d.xfile);
		strcpy(disk->Xparm,    stcp->u1.d.Xparm);
		break;
	case 'm':
		memset(hsm,0,sizeof(struct stgcat_hpss));
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
		strcpy(hsm->xfile,     stcp->u1.m.xfile);
		break;
	case 'h':
		memset(castor,0,sizeof(struct stgcat_hsm));
		castor->reqid       =   stcp->reqid;
		castor->nread       =   stcp->nread;
		castor->size        =   stcp->size;
		castor->nbaccesses  =   stcp->nbaccesses;
		castor->status      =   stcp->status;
		castor->uid         =   stcp->uid;
		castor->gid         =   stcp->gid;
		castor->mask        =   stcp->mask;
		castor->actual_size =   stcp->actual_size;
		castor->c_time      =   stcp->c_time;
		castor->a_time      =   stcp->a_time;
		strcpy(castor->poolname,stcp->poolname);
		strcpy(castor->ipath,   stcp->ipath);
		strcpy(castor->group,   stcp->group);
		strcpy(castor->user,    stcp->user);
		castor->keep        =   stcp->keep;
		strcpy(castor->xfile,     stcp->u1.h.xfile);
		strcpy(castor->server,    stcp->u1.h.server);
		castor->fileid      =   stcp->u1.h.fileid;
		castor->fileclass   =   stcp->u1.h.fileclass;
		strcpy(castor->tppool,  stcp->u1.h.tppool);
		castor->retenp_on_disk =  stcp->u1.h.retenp_on_disk;
		castor->mintime_beforemigr =  stcp->u1.h.mintime_beforemigr;
		break;
	case 'a':
		memset(alloc,0,sizeof(struct stgcat_alloc));
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
		strcpy(alloc->xfile,    stcp->u1.d.xfile);
		strcpy(alloc->Xparm,    stcp->u1.d.Xparm);
		break;
	default:
		return(-1);
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

int DLL_DECL Cdb2stcp(stcp,tape,disk,hsm,castor,alloc)
		 struct stgcat_entry *stcp;
		 struct stgcat_tape  *tape;
		 struct stgcat_disk *disk;
		 struct stgcat_hpss *hsm;
		 struct stgcat_hsm *castor;
		 struct stgcat_alloc *alloc;
{
	int i;

	if (stcp == NULL || (tape == NULL && disk == NULL && hsm == NULL && castor == NULL && alloc == NULL)) {
		return(-1);
	}

	if ((tape   != NULL && (disk != NULL ||  hsm  != NULL || castor != NULL || alloc != NULL)) ||
		(disk   != NULL && (tape != NULL ||  hsm  != NULL || castor != NULL || alloc != NULL)) ||
		(hsm    != NULL && (tape != NULL ||  disk != NULL || castor != NULL || alloc != NULL)) ||
		(castor != NULL && (tape != NULL ||  disk != NULL || hsm    != NULL || alloc != NULL)) ||
		(alloc  != NULL && (tape != NULL ||  disk != NULL || castor != NULL || hsm   != NULL))) {
		return(-1);
	}

	memset(stcp,0,sizeof(struct stgcat_entry));

	if (tape != NULL) {
		stcp->blksize     =         tape->blksize;
		/* No filler member in tape database */
		stcp->charconv    =         tape->charconv;
		stcp->keep        =         tape->keep;
		stcp->lrecl       =         tape->lrecl;
		stcp->nread       =         tape->nread;
		strcpy(stcp->poolname,      tape->poolname);
		strcpy(stcp->recfm,         tape->recfm);
		stcp->size        =         tape->size;
		strcpy(stcp->ipath,         tape->ipath);
		stcp->t_or_d      =         't';
		strcpy(stcp->group,         tape->group);
		strcpy(stcp->user,          tape->user);
		stcp->uid         =         tape->uid;
		stcp->gid         =         tape->gid;
		stcp->mask        =         tape->mask;
		stcp->reqid       =         tape->reqid;
		stcp->status      =         tape->status;
		stcp->actual_size =         tape->actual_size;
		stcp->c_time      =         tape->c_time;
		stcp->a_time      =         tape->a_time;
		stcp->nbaccesses  =         tape->nbaccesses;
		strcpy(stcp->u1.t.den,      tape->den);
		strcpy(stcp->u1.t.dgn,      tape->dgn);
		strcpy(stcp->u1.t.fid,      tape->fid);
		stcp->u1.t.filstat  =       tape->filstat;
		strcpy(stcp->u1.t.fseq,     tape->fseq);
		strcpy(stcp->u1.t.lbl,      tape->lbl);
		stcp->u1.t.retentd =        tape->retentd;
		strcpy(stcp->u1.t.tapesrvr, tape->tapesrvr);
		stcp->u1.t.E_Tflags =       tape->E_Tflags;
		for (i = 0; i < MAXVSN; i++) {
			strcpy(stcp->u1.t.vid[i], tape->vid[i]);
			strcpy(stcp->u1.t.vsn[i], tape->vsn[i]);
		}
	} else if (disk != NULL) {
		stcp->blksize     =      disk->blksize;
		/* No filler member in disk database */
		/* No charconv member in disk database */
		stcp->keep        =      disk->keep;
		stcp->lrecl       =      disk->lrecl;
		stcp->nread       =      disk->nread;
		strcpy(stcp->poolname,   disk->poolname);
		strcpy(stcp->recfm,      disk->recfm);
		stcp->size        =      disk->size;
		strcpy(stcp->ipath,      disk->ipath);
		stcp->t_or_d      =      'd';
		strcpy(stcp->group,      disk->group);
		strcpy(stcp->user,       disk->user);
		stcp->uid         =      disk->uid;
		stcp->gid         =      disk->gid;
		stcp->mask        =      disk->mask;
		stcp->reqid       =      disk->reqid;
		stcp->status      =      disk->status;
		stcp->actual_size =      disk->actual_size;
		stcp->c_time      =      disk->c_time;
		stcp->a_time      =      disk->a_time;
		stcp->nbaccesses  =      disk->nbaccesses;
		strcpy(stcp->u1.d.xfile, disk->xfile);
		strcpy(stcp->u1.d.Xparm, disk->Xparm);
	} else if (hsm != NULL) {
		/* No blksize member in hsm database */
		/* No filler member in hsm database */
		/* No charconv member in hsm database */
		stcp->keep        =      hsm->keep;
		/* No lrecl member in hsm database */
		stcp->nread       =      hsm->nread;
		strcpy(stcp->poolname,   hsm->poolname);
		/* No recfm member in hsm database */
		stcp->size        =      hsm->size;
		strcpy(stcp->ipath,      hsm->ipath);
		stcp->t_or_d      =      'm';
		strcpy(stcp->group,      hsm->group);
		strcpy(stcp->user,       hsm->user);
		stcp->uid         =      hsm->uid;
		stcp->gid         =      hsm->gid;
		stcp->mask        =      hsm->mask;
		stcp->reqid       =      hsm->reqid;
		stcp->status      =      hsm->status;
		stcp->actual_size =      hsm->actual_size;
		stcp->c_time      =      hsm->c_time;
		stcp->a_time      =      hsm->a_time;
		stcp->nbaccesses  =      hsm->nbaccesses;
		strcpy(stcp->u1.m.xfile, hsm->xfile);
	} else if (castor != NULL) {
		/* No blksize member in castor database */
		/* No filler member in castor database */
		/* No charconv member in castor database */
		stcp->keep        =      castor->keep;
		/* No lrecl member in castor database */
		stcp->nread       =      castor->nread;
		strcpy(stcp->poolname,   castor->poolname);
		/* No recfm member in castor database */
		stcp->size        =      castor->size;
		strcpy(stcp->ipath,      castor->ipath);
		stcp->t_or_d      =      'h';
		strcpy(stcp->group,      castor->group);
		strcpy(stcp->user,       castor->user);
		stcp->uid         =      castor->uid;
		stcp->gid         =      castor->gid;
		stcp->mask        =      castor->mask;
		stcp->reqid       =      castor->reqid;
		stcp->status      =      castor->status;
		stcp->actual_size =      castor->actual_size;
		stcp->c_time      =      castor->c_time;
		stcp->a_time      =      castor->a_time;
		stcp->nbaccesses  =      castor->nbaccesses;
		strcpy(stcp->u1.h.xfile, castor->xfile);
		strcpy(stcp->u1.h.server, castor->server);
		stcp->u1.h.fileid =      castor->fileid;
		stcp->u1.h.fileclass =   castor->fileclass;
		strcpy(stcp->u1.h.tppool, castor->tppool);
		stcp->u1.h.retenp_on_disk = castor->retenp_on_disk;
		stcp->u1.h.mintime_beforemigr = castor->mintime_beforemigr;
	} else if (alloc != NULL) {
		/* No blksize member in alloc database */
		/* No filler member in alloc database */
		/* No charconv member in alloc database */
		stcp->keep        =      alloc->keep;
		/* No lrecl member in alloc database */
		stcp->nread       =      alloc->nread;
		strcpy(stcp->poolname,   alloc->poolname);
		/* No recfm member in alloc database */
		stcp->size        =      alloc->size;
		strcpy(stcp->ipath,      alloc->ipath);
		stcp->t_or_d      =      'a';
		strcpy(stcp->group,      alloc->group);
		strcpy(stcp->user,       alloc->user);
		stcp->uid         =      alloc->uid;
		stcp->gid         =      alloc->gid;
		stcp->mask        =      alloc->mask;
		stcp->reqid       =      alloc->reqid;
		stcp->status      =      alloc->status;
		stcp->actual_size =      alloc->actual_size;
		stcp->c_time      =      alloc->c_time;
		stcp->a_time      =      alloc->a_time;
		stcp->nbaccesses  =      alloc->nbaccesses;
		strcpy(stcp->u1.d.xfile, alloc->xfile);
		strcpy(stcp->u1.d.Xparm, alloc->Xparm);
	} else {
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


