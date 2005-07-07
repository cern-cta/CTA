/*
 * Copyright (C) 1999-2001 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_QueueOp.c,v $ $Revision: 1.59 $ $Date: 2005/07/07 13:00:21 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_QueueOp.c - Queue volume and drive requests (server only).
 */

#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <stdio.h>
#include <osdep.h>
#include <net.h>
#include <log.h>
#include <serrno.h>
#include <Castor_limits.h>
#include <Cthread_api.h>
#include <Csnprintf.h>
#include <Ctape_api.h>
#include <vdqm_constants.h>
#include <vdqm.h>

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */

extern char *getconfent _PROTO((char *, char *, int));

#define INVALID_DGN_CONTEXT (dgn_context == NULL)

dgn_element_t *dgn_queues = NULL;
int nb_dgn = 0;
void *queue_lock;
static char drives_filename[CA_MAXPATHLEN+1] = "";


static int vdqm_maxtimediff = VDQM_MAXTIMEDIFF;
static int vdqm_retrydrvtime = VDQM_RETRYDRVTIM;

static int Rollback = 0;
static void *Rollback_lock = NULL;

static int InitQueueLock() {
    int save_serrno, rc;

    rc = Cthread_mutex_lock(&nb_dgn);
    save_serrno = serrno;
    if ( rc == -1 ) {
        log(LOG_ERR,"InitQueueLock() Cthread_mutex_lock(): %s\n",
            sstrerror(save_serrno));
        serrno = save_serrno;
        return(-1);
    }
    rc = Cthread_mutex_unlock(&nb_dgn);
    save_serrno = serrno;
    if ( rc == -1 ) {
        log(LOG_ERR,"InitQueueLock() Cthread_mutex_unlock(): %s\n",
            sstrerror(save_serrno));
        serrno = save_serrno;
        return(-1);
    }
    queue_lock = Cthread_mutex_lock_addr(&nb_dgn);
    save_serrno = serrno;
    if ( queue_lock == NULL ) {
        log(LOG_ERR,"InitQueueLock() Cthread_mutex_lock_addr(): %s\n",
            sstrerror(save_serrno));
        serrno = save_serrno;
        return(-1);
    }
    return(0);
}

/*
 * Lock access to queues. Note that these don't lock the queues
 * themselves - threads which have already got a handle to a
 * queue will still operate as usual until it releases the queue.
 */
static int LockQueues() {
    return(Cthread_mutex_lock_ext(queue_lock));
}

static int UnlockQueues() {
    return(Cthread_mutex_unlock_ext(queue_lock));
}
/*
 * Init. locks on an individual DGN queue
 */
static int InitDgnQueue(dgn_element_t *dgn_context) {
    int rc;
    if ( dgn_context == NULL ) return(-1);
    rc = Cthread_mutex_lock((void *)dgn_context);
    if ( rc == -1 ) return(-1);
    rc = Cthread_mutex_unlock((void *)dgn_context);
    if ( rc == -1 ) return(-1);
    dgn_context->lock = Cthread_mutex_lock_addr((void *)dgn_context);
    if ( dgn_context->lock == NULL ) return(-1);
    return(0);
}
/*
 * Externalized interface (for vdqm_Replica.c)
 */
int vdqm_InitDgnQueue(dgn_element_t *dgn_context) {
    return(InitDgnQueue(dgn_context));
}

/*
 * Lock individual queues
 */
static int LockDgnQueue(dgn_element_t *dgn_context) {
    if ( dgn_context == NULL ) return(-1);
    return(Cthread_mutex_lock_ext(dgn_context->lock));
}
static int UnlockDgnQueue(dgn_element_t *dgn_context) {
    if ( dgn_context == NULL ) return(-1);
    return(Cthread_mutex_unlock_ext(dgn_context->lock));
}

/*
 * Lock all queues. Externalized functions called by vdqm_ProcReq()
 */
int vdqm_LockAllQueues() {
    int rc = 0;
    dgn_element_t *tmp = NULL;

    rc = LockQueues();
    if ( rc == -1 ) return(-1);
    CLIST_ITERATE_BEGIN(dgn_queues,tmp) {
        rc = UnlockQueues();
        if ( rc == -1 ) break;
        rc = LockDgnQueue(tmp);
        if ( rc == -1 ) break;
        rc = LockQueues();
        if ( rc == -1 ) break;
    } CLIST_ITERATE_END(dgn_queues,tmp);
    if ( rc == 0 ) rc = UnlockQueues();
    return(rc);
}

int vdqm_UnlockAllQueues() {
    int rc = 0;
    dgn_element_t *tmp = NULL;

    rc = LockQueues();
    if ( rc == -1 ) return(-1);
    CLIST_ITERATE_BEGIN(dgn_queues,tmp) {
        rc = UnlockQueues();
        if ( rc == -1 ) break;
        rc = UnlockDgnQueue(tmp);
        if ( rc == -1 ) break;
        rc = LockQueues();
        if ( rc == -1 ) break;
    } CLIST_ITERATE_END(dgn_queues,tmp);
    if ( rc == -1 ) (void)UnlockQueues();
    else rc = UnlockQueues();
    return(rc);
}

static int CheckDgn(char *dgn) {
    dgn_element_t *tmp = NULL;
    int found,rc;

    if ( dgn == NULL || *dgn == '\0' ) {
        vdqm_SetError(EVQDGNINVL);
        return(-1);
    }

    log(LOG_DEBUG,"CheckDgn(%s) lock queue access\n",dgn);
    if ( (rc = LockQueues()) == -1 ) {
        vdqm_SetError(EVQSYERR);
        return(rc);
    }
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_queues,tmp) {
        if ( !strcmp(tmp->dgn,dgn) && tmp->drv_queue != NULL ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_queues,tmp);
    log(LOG_DEBUG,"CheckDgn(%s) unlock queues access\n",dgn);
    (void)UnlockQueues();

    return(found);
}

static int SetDgnContext(dgn_element_t **dgn_context,const char *dgn) {
    dgn_element_t *tmp = NULL;
    int found,rc;
    
    if ( dgn == NULL || *dgn == '\0' || dgn_context == NULL) {
        vdqm_SetError(EVQDGNINVL);
        return(-1);
    }
    
    log(LOG_DEBUG,"SetDgnContext(%s) lock queue access\n",dgn);
    if ( (rc = LockQueues()) == -1 ) {
        vdqm_SetError(EVQSYERR);
        return(rc);
    }
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_queues,tmp) {
        if ( !strcmp(tmp->dgn,dgn) ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_queues,tmp);
    if ( !found ) {
        tmp = (dgn_element_t *)malloc(sizeof(dgn_element_t));
        if ( tmp == NULL ) {
            log(LOG_INFO,"SetDgnContext(%s) unlock queue access\n",dgn);
            (void)UnlockQueues();
            vdqm_SetError(EVQSYERR);
            return(-1);
        }
        nb_dgn++;
        strcpy(tmp->dgn,dgn);
        tmp->dgnindx = nb_dgn;
        tmp->drv_queue = NULL;
        tmp->vol_queue = NULL;
        CLIST_INSERT(dgn_queues,tmp);
        rc = InitDgnQueue(tmp);
        if ( rc == -1 ) {
            log(LOG_ERR,"SetDgnContext(%s) InitDgnQueue(): %s\n",
                dgn,sstrerror(serrno));
            (void)UnlockQueues();
            vdqm_SetError(EVQSYERR);
            return(-1);
        }
    }

    rc = 0;
    *dgn_context = tmp;
    log(LOG_DEBUG,"SetDgnContext(%s) unlock queue access\n",dgn);
    (void)UnlockQueues();
    if ( *dgn_context == NULL ) {
        log(LOG_ERR,"SetDgnContext(%s) internal error: cannot assign DGN context\n");
        vdqm_SetError(SEINTERNAL);
        rc = -1;
    } else {
        log(LOG_DEBUG,"SetDgnContext(%s) lock mutex 0x%x\n",dgn,(long)*dgn_context);
        rc = LockDgnQueue(*dgn_context);
    }
    return(rc);
}

static int FreeDgnContext(dgn_element_t **dgn_context) {

    int rc;

    if ( dgn_queues == NULL || dgn_context == NULL ) return(-1);
    log(LOG_DEBUG,"FreeDgnContext() freeing dgn_context at 0x%x\n",
        (long)*dgn_context);
    rc = vdqm_UpdateReplica(*dgn_context);
    if ( rc == -1 ) {
        log(LOG_ERR,"FreeDgnContext() vdqm_UpdateReplica() returned error\n");
    }
    UnlockDgnQueue(*dgn_context);
    *dgn_context = NULL;
    return(0);
}

static int NextDgnContext(dgn_element_t **dgn_context) {
    char tmpdgn[CA_MAXDGNLEN+1];
    
    if ( dgn_queues == NULL || dgn_context == NULL ) {
        vdqm_SetError(SEINTERNAL);
        return(-1);
    }
    *tmpdgn = '\0';
    log(LOG_DEBUG,"NextDgnContext() lock queues access\n");
    if ( LockQueues() == -1 ) {
        log(LOG_ERR,"NextDgnContext() error locking queue access\n");
        return(-1);
    }
    if ( *dgn_context == NULL ) {
        if ( dgn_queues == NULL ) return(-1);
        strcpy(tmpdgn,dgn_queues->dgn);
    } else {
        if ( (*dgn_context)->next != dgn_queues )
            strcpy(tmpdgn,(*dgn_context)->next->dgn);
    }
    log(LOG_DEBUG,"NextDgnContext() unlock queue access\n");
    (void)UnlockQueues();
    if ( *dgn_context != NULL ) FreeDgnContext(dgn_context);

    if ( *tmpdgn != '\0' ) return(SetDgnContext(dgn_context,tmpdgn));
    else {
        vdqm_SetError(EVQEOQREACHED);
        return(-1);
    }
}

static int AnyVolRecForDgn(const dgn_element_t *dgn_context) {
    vdqm_volrec_t *vol;
    int found;
    
    if ( INVALID_DGN_CONTEXT ) return(-1);
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
        if ( vol->vol.DrvReqID == 0 ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
    return(found);
}

/*
 * Generation of unique IDs for volume and drive requests.
 */
static int NewReqID(int *id, int *reqID, void **reqID_lock) {
    int rc;
    
	if ( reqID == NULL || reqID_lock == NULL ) {
		serrno = EINVAL;
		return(-1);
	}
	if ( id != NULL && *id == -1 ) {
		if ( Cthread_mutex_lock(reqID) == -1 ) {
			log(LOG_ERR,"NewReqID() Cthread_mutex_lock(): %s\n",
				sstrerror(serrno));
			return(-1);
		}
		if ( Cthread_mutex_unlock(reqID) == -1 ) {
			log(LOG_ERR,"NewReqID() Cthread_mutex_unlock(): %s\n",
				sstrerror(serrno));
			return(-1);
		}
		(*reqID_lock) = Cthread_mutex_lock_addr(reqID);
		if ( (*reqID_lock) == NULL ) {
			log(LOG_ERR,"NewReqID() Cthread_mutex_lock_addr(): %s\n",
				sstrerror(serrno));
			return(-1);
		}
		return(0);
	}
    if ( Cthread_mutex_lock_ext(*reqID_lock) == -1 ) return(-1);
    if ( id == NULL ) rc = ++(*reqID);
    else if ( (*id > 0) && (*id < *reqID) ) rc = *id;
    else rc = *reqID = *id;
    if ( Cthread_mutex_unlock_ext(*reqID_lock) == -1 ) return(-1);
    return(rc);
}

static int NewVolReqID(int *id) {
	static int reqID = 0;
	static void *reqID_lock = NULL;
	return(NewReqID(id,&reqID,&reqID_lock));
}

static int NewDrvReqID(int *id) {
	static int reqID = 0;
	static void *reqID_lock = NULL;
	return(NewReqID(id,&reqID,&reqID_lock));
}

static int InitReqIDs() {
	int id = -1;
	if ( NewVolReqID(&id) == -1 ) return(-1);
	if ( NewDrvReqID(&id) == -1 ) return(-1);
	return(0);
}

/*
 * Externalised interfaces (used in vdqm_Replica.c)
 */
int vdqm_NewVolReqID(int *id) {
    return(NewVolReqID(id));
}

int vdqm_NewDrvReqID(int *id) {
    return(NewDrvReqID(id));
}

static int AnyDrvFreeForDgn(const dgn_element_t *dgn_context) {
    vdqm_drvrec_t *drv;
    int found;
    
    if ( INVALID_DGN_CONTEXT ) return(-1);
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if (!(drv->drv.status & VDQM_UNIT_UNKNOWN) && 
             (drv->drv.status & VDQM_UNIT_FREE) &&
             (drv->drv.status & VDQM_UNIT_UP) ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    
    return(found);
}

/*
 * VolInUse(): Check whether another request is currently
 * using the specified volume. Note that volume can still
 * be mounted on a drive if not in use by another request.
 * The volume is also considered to be in use if it is mounted
 * on a drive which has UNKNOWN or ERROR status.
 */
static int VolInUse(const dgn_element_t *dgn_context, 
                    const char *volid) {
    vdqm_drvrec_t *drv;
    int found;
    
    if ( INVALID_DGN_CONTEXT || volid == NULL ) return(0);
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if ( (drv->vol != NULL &&
              *drv->vol->vol.volid != '\0' && 
              !strcmp(drv->vol->vol.volid,volid)) ||
             (!strcmp(drv->drv.volid,volid) && 
              (drv->drv.status & (VDQM_UNIT_UNKNOWN | VDQM_UNIT_ERROR |
                                  VDQM_FORCE_UNMOUNT))) ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    return(found);
}

/* 
 * VolMounted(): Check whether the input volume is mounted
 * on a drive or not.
 */
static int VolMounted(const dgn_element_t *dgn_context,
                      const char *volid) {
    vdqm_drvrec_t *drv;
    int found;
    
    if ( INVALID_DGN_CONTEXT || volid == NULL ) return(0);
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if ( *drv->drv.volid != '\0' && 
            !strcmp(drv->drv.volid,volid) ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    return(found);
}


static int AnyFreeDrvOnSrv(const dgn_element_t *dgn_context,
                           vdqm_volrec_t *volrec,
                           vdqm_drvrec_t **freedrv) {
    vdqm_drvrec_t *drv, *top;
    char *server, *drive, *volid;

    if ( INVALID_DGN_CONTEXT || volrec == NULL || 
        *volrec->vol.server == '\0' ) return(-1);
    if ( freedrv != NULL ) *freedrv = NULL;
    server = volrec->vol.server;
    drive = volrec->vol.drive;
    volid = volrec->vol.volid;
    top = NULL;
    /*
     * First check whether client matches any dedicated drive on this
     * server
     */
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if (!(drv->drv.status & VDQM_UNIT_UNKNOWN) &&
             (drv->drv.status & VDQM_UNIT_UP) &&
            ((drv->drv.status & VDQM_UNIT_FREE) ||
             (drv->drv.status & VDQM_UNIT_RELEASE) &&
             !strcmp(drv->drv.volid,volid)) ) {
            if ( !strcmp(drv->drv.server,server) && 
                 (*drive == '\0' || !strcmp(drv->drv.drive,drive)) &&
                 ((drv->drv.status & VDQM_UNIT_RELEASE) || 
                  !VolMounted(dgn_context,volid)) && 
                 *drv->drv.dedicate != '\0' && 
                 vdqm_DrvMatch(volrec,drv) == 1 ) {
                if ( (top == NULL) || 
                     (drv->drv.recvtime<top->drv.recvtime)) top = drv; 
            }
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);

    if ( top == NULL ) {
        CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
            if (!(drv->drv.status & VDQM_UNIT_UNKNOWN) &&
                 (drv->drv.status & VDQM_UNIT_UP) &&
                ((drv->drv.status & VDQM_UNIT_FREE) ||
                 (drv->drv.status & VDQM_UNIT_RELEASE) &&
                 !strcmp(drv->drv.volid,volid)) ) {
                if ( !strcmp(drv->drv.server,server) &&
                     (*drive == '\0' || !strcmp(drv->drv.drive,drive)) &&
                     ((drv->drv.status & VDQM_UNIT_RELEASE) ||
                      !VolMounted(dgn_context,volid)) &&
                      *drv->drv.dedicate == '\0' ) {
                    if ( (top == NULL) ||
                         (drv->drv.recvtime<top->drv.recvtime)) top = drv;
                }
            }
        } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    }
    if ( top != NULL ) {
        strcpy(drive,top->drv.drive);
        *freedrv = top;
        (*freedrv)->update = 1;
        return(1);
    }
    return(0);
}


static int CmpVolRec(const vdqm_volrec_t *vol1,
                     const vdqm_volrec_t *vol2) {

    if ( vol1 == NULL || vol2 == NULL ) return(-1);
    if ( vol1->vol.priority > vol2->vol.priority ) return(1);
    if ( (vol1->vol.priority == vol2->vol.priority) &&
         (vol1->vol.recvtime < vol2->vol.recvtime) ) return(1);
    return(0);
}


static int PopVolRecord(const dgn_element_t *dgn_context,
                        vdqm_volrec_t **volrec) {
    vdqm_volrec_t *start_vol, *vol, *top;
    
    if ( volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    top = NULL;
    start_vol = NULL;
    if ( *volrec != NULL ) start_vol = *volrec;
    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
        if ( (vol->vol.DrvReqID == 0 ) &&
             (start_vol == NULL || CmpVolRec(start_vol,vol) == 1 ) &&
             (top == NULL || CmpVolRec(vol,top) == 1) &&
            !VolInUse(dgn_context,vol->vol.volid) ) top = vol;
    } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
    if ( top != NULL ) top->update = 1;
    else vdqm_SetError(EVQNOVOL);
    *volrec = top;
    return(0);
}

static int AnyVolRecForMountedVol(const dgn_element_t *dgn_context,
                                  vdqm_drvrec_t *drv,
                                  vdqm_volrec_t **volrec) {
    vdqm_volrec_t *vol, *top;
    int i;
    
    if ( INVALID_DGN_CONTEXT || drv == NULL ) return(-1);
    if ( volrec != NULL ) *volrec = NULL;
    top = NULL;
    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
        if ( !strcmp(drv->drv.volid,vol->vol.volid) &&
             (*vol->vol.server == '\0' || 
              !strcmp(drv->drv.server,vol->vol.server)) &&
             (*vol->vol.drive == '\0' ||
              !strcmp(drv->drv.drive,vol->vol.drive)) ) {
            if ( (vol->vol.DrvReqID == 0) &&
                (top == NULL || CmpVolRec(vol,top) == 1) &&
                 (vdqm_DrvMatch(vol,drv) == 1) )
                top = vol;
        }
    } CLIST_ITERATE_END(dgn_context->vol_queue,vol);

    if ( top != NULL ) {
        /*
         * There is a request for this volume. Check if eligible.
         */
        vol = NULL;
        i = PopVolRecord(dgn_context,&vol);
        /*
         * This should never happen but....
         */
        if ( i == -1 || vol == NULL ) return(-1);
        
        /*
         * Reset the update since we are not sure this
         * record will be used.
         */
        vol->update = 0;
        /*
         * High priority requests always goes first. There is also a
         * possible (configurable) time difference to the oldest request
         * in order to assure that a single user won't flood the queue with
         * requests for same volume. At some point older requests must be
         * able to get through.
         */
        if ( top->vol.priority < vol->vol.priority ||
            top->vol.recvtime - vol->vol.recvtime > vdqm_maxtimediff ) top = NULL;
    }
    if ( volrec != NULL ) {
        if ( top != NULL ) top->update = 1;
        *volrec = top;
    }
    if ( top != NULL ) return(1);
    else return(0);
}

static int AnyVolRecForDrv(const dgn_element_t *dgn_context,
                           vdqm_drvrec_t *drv,
                           vdqm_volrec_t **volrec) {
    vdqm_volrec_t *vol, *top;
    int i;
    
    if ( INVALID_DGN_CONTEXT || drv == NULL ) return(-1);
    if ( volrec != NULL ) *volrec = NULL;
    top = NULL;

    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
        if ( (*vol->vol.server != '\0' &&
              !strcmp(drv->drv.server,vol->vol.server)) && 
             (*vol->vol.drive == '\0' || 
              !strcmp(drv->drv.drive,vol->vol.drive)) ) {
            if  ( (vol->vol.DrvReqID == 0) &&
                  (top == NULL || CmpVolRec(vol,top) == 1) &&
                  (!strcmp(drv->drv.volid,vol->vol.volid) ||
                   !VolInUse(dgn_context,vol->vol.volid)) &&
                  vdqm_DrvMatch(vol,drv) == 1 ) top = vol;
        }
    } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
    /*
     * If no request specificly asking for this server, we look for
     * a request accepting any server.
     */
    if ( top == NULL ) {
        CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
            if (*vol->vol.server == '\0') {
                if  ( (vol->vol.DrvReqID == 0) &&
                      (top == NULL || CmpVolRec(vol,top) == 1) &&
                      !VolInUse(dgn_context,vol->vol.volid) &&
                       vdqm_DrvMatch(vol,drv) == 1 ) top = vol;
            }
        } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
    }
    if ( top != NULL ) {
        vol = NULL;
        i = PopVolRecord(dgn_context,&vol);
        /*
         * This should never happen but....
         */
        if ( i == -1 || vol == NULL ) return(-1);

        /*
         * Reset the update since we are not sure this
         * record will be used.
         */
        vol->update = 0;
         /*
         * The popped volume record must either be explicitly 
         * for this server/drive or accept any server.
         */
        if ( *vol->vol.server == '\0' || 
             (!strcmp(vol->vol.server,drv->drv.server) &&
              (*vol->vol.drive == '\0' ||
              !strcmp(vol->vol.drive,drv->drv.drive))) ) {
            /*
             * High priority requests always goes first. There is also a
             * possible (configurable) time difference to the oldest request
             * in order to assure that a single user won't flood the queue with
             * requests for same volume. At some point older requests must be
             * able to get through.
             */
            if ( (top->vol.priority < vol->vol.priority) ||
                 ((top->vol.priority == vol->vol.priority) &&
                  (top->vol.recvtime - vol->vol.recvtime > vdqm_maxtimediff)) ) top = NULL;
        }
    }
    if ( volrec != NULL ) {
        if ( top != NULL ) top->update = 1;
        *volrec = top;
    }
    if ( top != NULL ) return(1);
    else return(0);
}


static int AddVolRecord(dgn_element_t *dgn_context,
                        vdqm_volrec_t *volrec) {
    vdqm_volrec_t *vol;
    
    if ( volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    /*
     * Make sure it is not already there
     */
    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
        if ( volrec == vol ) return(-1);
    } CLIST_ITERATE_END(dgn_context->vol_queue,vol); 
    if ( volrec->vol.VolReqID <= 0 ) volrec->vol.VolReqID = NewVolReqID(NULL);
    CLIST_INSERT(dgn_context->vol_queue,volrec);
    return(0);
}

static int NewVolRecord(vdqm_volrec_t **volrec) {
    
    if ( volrec == NULL ) return(-1);
    *volrec = (vdqm_volrec_t *)calloc(1,sizeof(vdqm_volrec_t));
    if ( *volrec == NULL ) {
        vdqm_SetError(EVQSYERR);
        return(-1);
    }
    (*volrec)->update = 1;
    return(0);
}

static int AddDrvRecord(dgn_element_t *dgn_context,
                        vdqm_drvrec_t *drvrec) {
    vdqm_drvrec_t *drv;
    
    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    /*
     * Make sure it is not already there
     */
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if ( drvrec == drv ) return(-1);
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);

    if ( drvrec->drv.DrvReqID <= 0 ) drvrec->drv.DrvReqID = NewDrvReqID(NULL);
    CLIST_INSERT(dgn_context->drv_queue,drvrec);
    return(0);
}

static int NewDrvRecord(vdqm_drvrec_t **drvrec) {
    
    if ( drvrec == NULL ) return(-1);
    *drvrec = (vdqm_drvrec_t *)calloc(1,sizeof(vdqm_drvrec_t));
    if ( *drvrec == NULL ) {
        vdqm_SetError(EVQSYERR);
        return(-1);
    }
    (*drvrec)->update = 1;
    return(0);
}


static int DelVolRecord(dgn_element_t *dgn_context,
                        vdqm_volrec_t *volrec) {
    int rc;

    if ( volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    CLIST_DELETE(dgn_context->vol_queue,volrec);
    rc = vdqm_DeleteFromReplica(&volrec->vol,NULL);
    if ( rc == -1 ) {
        log(LOG_ERR,"DelVolRecord() vdqm_DeleteFromReplica() returned error\n");
    }
    return(0);
}

static int DelDrvRecord(dgn_element_t *dgn_context,
                        vdqm_drvrec_t *drvrec) {
    int rc;

    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);

    CLIST_DELETE(dgn_context->drv_queue,drvrec);
    rc = vdqm_DeleteFromReplica(NULL,&drvrec->drv);
    if ( rc == -1 ) {
        log(LOG_ERR,"DelDrvRecord() vdqm_DeleteFromReplica() returned error\n");
        vdqm_SetError(EVQREPLICA);
    }
    return(0);
}

static int PopDrvRecord(const dgn_element_t *dgn_context,
                        vdqm_volrec_t *volrec,
                        vdqm_drvrec_t **drvrec) {
    vdqm_drvrec_t *drv, *top;
    
    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    top = NULL;
    if ( volrec != NULL ) {
        /*
         * If called with a volrec, we must first check if there
         * any dedicated drives for this client. If so, they should
         * be selected by preference.
         */
        CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
            log(LOG_DEBUG,"PopDrvRecord() check %s@%s\n",drv->drv.drive,
                drv->drv.server);
            if ( !(drv->drv.status & VDQM_UNIT_UNKNOWN) &&
                  (drv->drv.status & VDQM_UNIT_UP) &&
                  (drv->drv.status & VDQM_UNIT_FREE) && 
                 *drv->drv.dedicate != '\0' &&
                 vdqm_DrvMatch(volrec,drv) == 1 ) {
               if ((top == NULL) ||
                   (drv->drv.recvtime<top->drv.recvtime)) top = drv;
            }
            if ( top != drv) 
                log(LOG_DEBUG,"PopDrvRecord() %s@%s refused\n",drv->drv.drive,
                    drv->drv.server);
        } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    }

    if ( top == NULL ) {
        CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
            if ( !(drv->drv.status & VDQM_UNIT_UNKNOWN) &&
                  (drv->drv.status & VDQM_UNIT_UP) &&
                  (drv->drv.status & VDQM_UNIT_FREE) &&
                  *drv->drv.dedicate == '\0' ) {
                if ((top == NULL) ||
                    (drv->drv.recvtime<top->drv.recvtime)) top = drv;
            }
        } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    }
    if ( top != NULL ) top->update = 1;
    else vdqm_SetError(EVQNODRV);
    *drvrec = top;
    return(0);
}


static int VolRecQueuePos(const dgn_element_t *dgn_context,
                          vdqm_volrec_t *volrec) {
    vdqm_volrec_t *tmprec;
    int pos;
    
    if ( volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    /*
     * If request already running its queue position is 0
     */
    if ( volrec->drv != NULL ) return(0);

    pos = 0;
    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,tmprec) {
        if ( tmprec->vol.VolReqID != volrec->vol.VolReqID ) {
            if ( tmprec->vol.priority > volrec->vol.priority ) pos++;
            if ( tmprec->vol.recvtime < volrec->vol.recvtime ) pos++;
        }
    } CLIST_ITERATE_END(dgn_context->vol_queue,tmprec);
    return(pos);
}

static int GetVolRecord(const dgn_element_t *dgn_context,
                        vdqmVolReq_t *vol, vdqm_volrec_t **volrec) {
    vdqm_volrec_t *tmprec;
    int found;
    
    if ( vol == NULL || volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,tmprec) {
        if ( vol->VolReqID == tmprec->vol.VolReqID ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->vol_queue,tmprec);
    
    if ( found ) {
        tmprec->update = 1;
        *volrec = tmprec;
    } else {
        vdqm_SetError(EVQNOSVOL);
        *volrec = NULL;
    }
    
    return(0);
}

static int GetDrvRecord(const dgn_element_t *dgn_context,
                        vdqmDrvReq_t *drv, vdqm_drvrec_t **drvrec) {
    vdqm_drvrec_t *tmprec;
    int found;
    
    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    found = 0;
    /*
     * Cannot rely on DrvReqID here since the use of this function normally
     * is to search for a specific drive name
     */
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,tmprec) {
        if ( !strcmp(tmprec->drv.drive,drv->drive) &&
            !strcmp(tmprec->drv.server,drv->server) ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,tmprec);
    
    if ( found ) {
        tmprec->update = 1;
        *drvrec = tmprec;
    } else {
        *drvrec = NULL;
        vdqm_SetError(EVQNOSDRV);
    }
    
    return(0);
}

static int GetNextDrvOnSrv(const dgn_element_t *dgn_context,
                           char *server, vdqm_drvrec_t **drvrec) {
    vdqm_drvrec_t *drv;
    int found = 0;

    if ( INVALID_DGN_CONTEXT ) return(-1);
    if ( server == NULL || *server == '\0' ) return(-1);
    if ( drvrec != NULL ) *drvrec = NULL;

    log(LOG_DEBUG,"GetNextDrvOnSrv(%s,%s) called\n",dgn_context->dgn,server);
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if ( strcmp(drv->drv.server,server) == 0 ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    if ( found == 1 && drvrec != NULL ) *drvrec = drv;
    return(found);
} 

 

static int DelAllDrvOnSrv(dgn_element_t *dgn_context,
                          char *server) {
    vdqm_drvrec_t *drv = NULL;
    int rc;

    if ( INVALID_DGN_CONTEXT ) return(-1);
    if ( server == NULL || *server == '\0' ) return(-1);

    while ( GetNextDrvOnSrv(dgn_context,server,&drv) == 1 && drv != NULL ) {
        log(LOG_INFO,"DelAllDrvOnSrv(%s,%s) delete drive %s@%s\n",
            dgn_context->dgn,server,drv->drv.drive,drv->drv.server);
        /*
         * A drive found on this server. Remove it and free memory
         */
        rc = DelDrvRecord(dgn_context,drv);
        /*
         * Make sure to remove any old volume request for this drive
         */
        if ( drv->vol != NULL ) {
            rc = DelVolRecord(dgn_context,drv->vol);
            free(drv->vol);
        }
		vdqm_ResetDedicate(drv); /* If any */
        free(drv);
    }

    return(0);
}


static int SelectVolAndDrv(const dgn_element_t *dgn_context,
                           vdqm_volrec_t **vol,
                           vdqm_drvrec_t **drv) {

    vdqm_volrec_t *volrec, *last_volrec;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( vol != NULL ) *vol = (vdqm_volrec_t *)NULL;
    if ( drv != NULL ) *drv = (vdqm_drvrec_t *)NULL;

    if ( INVALID_DGN_CONTEXT ) return(-1);
    volrec = (vdqm_volrec_t *)NULL;
    drvrec = (vdqm_drvrec_t *)NULL;
    last_volrec = (vdqm_volrec_t *)NULL;

    /*
     * Loop until we find a vol-drive match or end of vol queue
     */
    for (;;) {
        volrec = last_volrec;
        rc = PopVolRecord(dgn_context,&volrec);
        last_volrec = volrec;
        if ( rc == -1 || volrec == NULL ) {
            log(LOG_ERR,"SelectVolAndDrv(): PopVolRecord() returned rc=%d, volrec=0x%x\n",
                rc,volrec);
           return(-1);
        }
        /*
         * Reset update until we are sure
         */
        volrec->update = 0;

        /*
         * If a specific server (and maybe also drive) has been
         * specified, we check for that first
         */
        log(LOG_DEBUG,"SelectVolAndDrv(): PopVolRecord() returned VolReqID: %d, VID: %s, server=%s\n",volrec->vol.VolReqID,volrec->vol.volid,volrec->vol.server);
        if ( *volrec->vol.server != '\0' ) {
            rc = AnyFreeDrvOnSrv(dgn_context,volrec,&drvrec);
            log(LOG_DEBUG,"SelectVolAndDrv()::DEBUG AnyFreeDrvOnSrv(%s) returned rc=%d\n",
                volrec->vol.server,rc);

            if ( drvrec == NULL ) {
                /*
                 * No free drive on that server. Reset the search
                 * volume record to allow for another volume record
                 * to be popped
                 */
                log(LOG_ERR,"SelectVolAndDrv(): AnyFreeDrvOnSrv() returned rc=%d\n",
                    rc);
                volrec = NULL;
            } else {
                /*
                 * Reset update until we are sure
                 */
                drvrec->update = 0;
                log(LOG_INFO,"SelectVolAndDrv(): AnyFreeDrvOnSrv() returned %s@%s, status=0x%x\n",drvrec->drv.drive,drvrec->drv.server,drvrec->drv.status);
                break;
            }
        }
        if ( drvrec == NULL ) {
            /*
             * Either the volrec asked for a specific server/drive
             * which wasn't free or it accepts any server
             */
            rc = PopDrvRecord(dgn_context,volrec,&drvrec);
            if ( rc == -1 || drvrec == NULL ) {
                if ( rc == 0 ) continue;
                else {
                    log(LOG_ERR,"SelectVolAndDrv(): PopDrvRecord() returned rc=%d, drvrec=0x%x\n",
                        rc,drvrec);
                    return(-1);
                }
            }
            /*
             * Reset update until we are sure
             */
            drvrec->update = 0;
            log(LOG_DEBUG,"SelectVolAndDrv()::DEBUG PopDrvRecord() returned rc=%d %s@%s\n",
                rc,drvrec->drv.drive,drvrec->drv.server);
            if ( volrec == NULL ) {
                /*
                 * Search for a volume record that either
                 * explicitly asked for this server/drive or can
                 * accept any server.
                 */
                rc = AnyVolRecForDrv(dgn_context,drvrec,&volrec);
                log(LOG_DEBUG,"SelectVolAndDrv()::DEBUG AnyVolRecForDrv() returned rc=%d\n",rc);
                if ( volrec == NULL ) {
                    if ( rc == 0 ) continue;
                    else {
                        log(LOG_ERR,"SelectVolAndDrv(): AnyVolRecForDrv() returned rc=%d\n",rc);
                        return(-1);
                    }
                }
                log(LOG_INFO,"SelectVolAndDrv(): AnyVolRecForDrv() returned VolReqID: %d, VID: %s, server: %s\n",volrec->vol.VolReqID,volrec->vol.volid,volrec->vol.server);
                /*
                 * Reset update until we are sure
                 */
                volrec->update = 0;
                break;
            } else break;
        }
    } /* for (;;) */
    if ( vol != NULL ) {
        volrec->update = 1;
        *vol = volrec;
    }
    if ( drv != NULL ) {
        drvrec->update = 1;
        *drv = drvrec;
    }
    return(0);
}

static void GetStatusString(int status, char *status_string) {
    int vdqm_status_values[] = VDQM_STATUS_VALUES;
    char vdqm_status_strings[][32] = VDQM_STATUS_STRINGS;
    int i;
    
    if ( status_string == NULL ) return;
    *status_string = '\0';
    i=0;
    while ( *vdqm_status_strings[i] != '\0' ) {
        if ( status & vdqm_status_values[i] ) {
            strcat(status_string,vdqm_status_strings[i]);
            strcat(status_string,"|");
        } 
        i++;
    }
    if ( *status_string != '\0' ) status_string[strlen(status_string)-1]='\0';
    return;
}


int vdqm_DelVolReq(vdqmVolReq_t *VolReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( VolReq == NULL ) return(-1);
    log(LOG_INFO,"vdqm_DelVolReq() request for volreq id=%d\n",VolReq->VolReqID);
    log(LOG_INFO,"vdqm_DelVolReq() set context to dgn=%s\n",VolReq->dgn);

    rc = SetDgnContext(&dgn_context,VolReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_DelVolReq() cannot set Dgn context for %s\n",
            VolReq->dgn);
        return(-1);
    }

    /*
     * Verify that the volume record exists
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( rc == -1 || volrec == NULL ) {
        log(LOG_ERR,"vdqm_DelVolReq() volume record not in queue.\n");
        FreeDgnContext(&dgn_context);
        return(-1);
    }
    /*
     * Can only remove request if not assigned to a drive. Otherwise
     * it should be cleanup by resetting the drive status to RELEASE + FREE.
     * Mark it as UNKNOWN until an unit status clarifies what has happened.
     */
    if ( (drvrec = volrec->drv) != NULL ) {
        drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
        volrec->vol.recvtime = time(NULL);
        drvrec->update = 1;
        volrec->update = 0;
        vdqm_SetError(EVQREQASS);
        rc = -1;
    } else {
        /*
         * Delete the volume record from the queue.
         */
        rc = DelVolRecord(dgn_context,volrec);
        /*
         * Free it
         */
        free(volrec);
    }
    FreeDgnContext(&dgn_context);
    return(rc);
}

int vdqm_DelDrvReq(vdqmDrvReq_t *DrvReq) {
    dgn_element_t *dgn_context;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( DrvReq == NULL ) return(-1);
    rc = 0;
    
    log(LOG_ERR,"vdqm_DelDrvReq() request for drive %s@%s\n",
        DrvReq->drive,DrvReq->server);

    /*
     * Reset Device Group Name context
     */
    log(LOG_INFO,"vdqm_DelDrvReq() set context to dgn=%s\n",DrvReq->dgn);
    rc = SetDgnContext(&dgn_context,DrvReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_DelDrvReq() cannot set Dgn context for %s\n",
            DrvReq->dgn);
        return(-1);
    }
        
    /* 
     * Check whether the drive record already exists
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context,DrvReq,&drvrec);
    if ( rc == -1 || drvrec == NULL ) {
        log(LOG_ERR,"vdqm_DelDrvReq() drive record not found for drive %s@%s\n",
            DrvReq->drive,DrvReq->server);
        FreeDgnContext(&dgn_context);
        return(-1);
    }
    /*
     * Don't allow to delete drives with running jobs.
     */
    if ( !(drvrec->drv.status & (VDQM_UNIT_UP | VDQM_UNIT_FREE)) &&
         !(drvrec->drv.status & VDQM_UNIT_DOWN) ) {
        drvrec->update = 0;
        log(LOG_ERR,"vdqm_DelDrvReq() cannot remove drive record with assigned job\n");
        vdqm_SetError(EVQREQASS);
        FreeDgnContext(&dgn_context);
        return(-1);
    }
    /*
     * OK, remove it and free memory
     */
    rc = DelDrvRecord(dgn_context,drvrec);
    /*
     * Make sure to remove any old volume request for this drive
     */
    if ( drvrec->vol != NULL ) {
        rc = DelVolRecord(dgn_context,drvrec->vol);
        free(drvrec->vol);
    }
    vdqm_ResetDedicate(drvrec); /* If any */
    free(drvrec);
    FreeDgnContext(&dgn_context);
    return(0);
}

/*
 * This routine is called from a separate thread to assure that
 * queues are checked if there has been a rollback. This is necessary
 * when for instance there are free drives and no more queued volumes
 * when the failing request was started. 
 */
int vdqm_OnRollback() {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc;

    log(LOG_INFO,"vdqm_OnRollback(): Locking Rollback mutex\n");
    rc = Cthread_mutex_lock_ext(Rollback_lock);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_OnRollback() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    
    for (;;) {
        log(LOG_INFO,"vdqm_OnRollback(): wait for rollback operation\n");
        rc = Cthread_cond_wait_ext(Rollback_lock);
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_OnRollback() Cthread_cond_wait_ext(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
        log(LOG_INFO,"vdqm_OnRollback(): woke up with Rollback=%d\n",Rollback);
        if ( Rollback > 0 ) {
            dgn_context = NULL;
            while (NextDgnContext(&dgn_context) != -1 && dgn_context != NULL ) {
                log(LOG_INFO,"vdqm_OnRollback() check DGN %s\n",dgn_context->dgn);
                rc = AnyDrvFreeForDgn(dgn_context);
                if ( rc < 0 ) {
                    log(LOG_ERR,"vdqm_OnRollback() AnyDrvFreeForDgn(%s) returned error\n",
                    dgn_context->dgn);
                    continue;
                }
                if ( rc == 1 ) {
                    for (;;) {
                        rc = SelectVolAndDrv(dgn_context,&volrec,&drvrec);
                        if ( rc == -1 || volrec == NULL || drvrec == NULL ) {
                            log(LOG_ERR,"vdqm_OnRollback() SelectVolAndDrv() returned rc=%d\n",rc);
                            break;
                        }
                        /*
                         * Free memory allocated for previous request 
                         * for this drive
                         */
                        if ( drvrec->vol != NULL ) free(drvrec->vol);
                        drvrec->vol = volrec;
                        volrec->drv = drvrec;
                        drvrec->drv.VolReqID = volrec->vol.VolReqID;
                        volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
                        drvrec->drv.jobID = 0;
                        /*
                         * Reset the drive status
                         */
                        drvrec->drv.status = drvrec->drv.status &
                         ~(VDQM_UNIT_RELEASE | VDQM_UNIT_BUSY | VDQM_VOL_MOUNT |
                           VDQM_VOL_UNMOUNT | VDQM_UNIT_ASSIGN);
                        drvrec->drv.recvtime = (int)time(NULL);
                        /*
                         * Start the job
                         */
                        rc = vdqm_StartJob(volrec);
                        if ( rc < 0 ) {
                            log(LOG_ERR,"vdqm_OnRollback(): vdqm_StartJob() returned error\n"
);
                            volrec->vol.DrvReqID = 0;
                            drvrec->drv.VolReqID = 0;
                            volrec->drv = NULL;
                            drvrec->vol = NULL;
                            break;
                        }
                    } /* for (;;) */
                } /*  if ( rc == 1 )  */
            } /* while (NextDgnContext() ...) */
       } /* if ( Rollback > 0 )  */
       Rollback = 0;
   } /* for (;;) */
}
/*
 * This routine is needed by vdqm_SendJobToRTCP() to
 * rollback a RTCOPY start request operation in case of
 * failure. Note that this routine should not be called
 * internally unless FreeDgnContext() has been called.
 */
int vdqm_QueueOpRollback(vdqmVolReq_t *VolReq,vdqmDrvReq_t *DrvReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc;

    log(LOG_INFO,"vdqm_QueueOpRollback() called\n");            

    if ( DrvReq == NULL ) return(-1);
    rc = SetDgnContext(&dgn_context,DrvReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_QueueOpRollback() cannot set Dgn context for %s\n",
            DrvReq->dgn);
        return(-1);
    }
    /* 
     * Get the volume record. If it doesn't exist anymore we
     * still need to find and correct the drive record.
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( volrec == NULL ) {
        log(LOG_ERR,"vdqm_QueueOpRollback() GetVolRecord() returned error\n");
    }
    /*
     * Get the drive record. If it doens't exist anymore we
     * still need to correct the volume record
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context,DrvReq,&drvrec);
    if ( volrec == NULL && drvrec != NULL ) volrec = drvrec->vol;
    
    /*
     * Now correct the records and make sure that volume request is requeued.
     */
    if ( volrec != NULL ) {
        volrec->vol.DrvReqID = 0;
        volrec->drv = NULL;
        volrec->update = 1;
        AddVolRecord(dgn_context,volrec);
    }
    if ( drvrec != NULL ) {
        /*
         * We need to flag the unit with unknown status until we hear
         * from it again.
         */
        drvrec->drv.VolReqID = 0;
        drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
        drvrec->vol = NULL;
    }
    FreeDgnContext(&dgn_context);
    rc = Cthread_mutex_lock_ext(Rollback_lock);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_QueueOpRollback() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    Rollback++;
    rc = Cthread_cond_broadcast_ext(Rollback_lock);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_QueueOpRollback() Cthread_cond_broadcast_ext(): %s\n",
            sstrerror(serrno));
        (void)Cthread_mutex_unlock_ext(Rollback_lock);
        return(-1);
    }
    rc = Cthread_mutex_unlock_ext(Rollback_lock);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_QueueOpRollback() Cthread_mutex_unlock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    return(0);
}

/*
 * Initialise the rollback lock
 */
static int InitRollback() {
	if ( Cthread_mutex_lock(&Rollback) == -1 ) {
		log(LOG_ERR,"InitRollback() Cthread_mutex_lock(): %s\n",
			sstrerror(serrno));
		return(-1);
	}
	if ( Cthread_mutex_unlock(&Rollback) == -1 ) {
		log(LOG_ERR,"InitRollback() Cthread_mutex_unlock(): %s\n",
			sstrerror(serrno));
		return(-1);
	}
	Rollback_lock = Cthread_mutex_lock_addr(&Rollback);
	if ( Rollback_lock == NULL ) {
		log(LOG_ERR,"InitRollback() Cthread_mutex_lock_addr(): %s\n",
			sstrerror(serrno));
		return(-1);
	}
	return(0);
}

int vdqm_InitQueueOp() {
	if ( InitQueueLock() == -1 ) return(-1);
	if ( InitReqIDs() == -1 ) return(-1);
	if ( InitRollback() == -1 ) return(-1);
	return(0);
}

int vdqm_NewVolReq(vdqmHdr_t *hdr, vdqmVolReq_t *VolReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec, *newvolrec;
    vdqm_drvrec_t *drvrec;
    char *p;
    int rc;
    
    if ( hdr == NULL || VolReq == NULL ) return(-1);
    
    log(LOG_INFO,"vdqm_NewVolReq() %s(%d,%d)@%s:%d requests %s, mode=%d in DGN %s (%s@%s)\n",
        VolReq->client_name,VolReq->clientUID,VolReq->clientGID,
        VolReq->client_host,VolReq->client_port,VolReq->volid,VolReq->mode,
        VolReq->dgn,(*VolReq->drive == '\0' ? "***" : VolReq->drive),
        (*VolReq->server == '\0' ? "***" : VolReq->server));
    /*
     * Check that the requested device exists.
     */
    rc = CheckDgn(VolReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_NewVolReq() CheckDgn(%s): %s\n",VolReq->dgn,
            sstrerror(serrno));
        return(-1);
    }
    if ( rc == 0 ) {
        log(LOG_ERR,"vdqm_NewVolReq() DGN %s does not exist\n",VolReq->dgn);
        vdqm_SetError(EVQDGNINVL);
        return(-1);
    }

    /*
     * Reset Device Group Name context
     */
    rc = SetDgnContext(&dgn_context,VolReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_NewVolReq() cannot set Dgn context for %s\n",
            VolReq->dgn);
        return(-1);
    }
    
    /*
     * Verify that the request doesn't (yet) exist
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( volrec != NULL ) {
        log(LOG_ERR,"vdqm_NewVolReq() input request already queued\n");
        FreeDgnContext(&dgn_context);
        vdqm_SetError(EVQALREADY);
        return(-1);
    }
    
    /*
     * Reserv new volume record
     */
    rc = NewVolRecord(&volrec);
    if ( rc < 0 || volrec == NULL ) {
        log(LOG_ERR,"vdqm_NewVolReq(): NewVolRecord() %s\n",sstrerror(errno));
        FreeDgnContext(&dgn_context);
        return(-1);
    }
    
    /*
     * Fill in the new information
     */
    volrec->magic = hdr->magic;
    volrec->vol = *VolReq;
    volrec->vol.recvtime = time(NULL);
    /*
     * We don't allow client to set priority
     */
    volrec->vol.priority = VDQM_PRIORITY_NORMAL;
    /*
     * Set priority for tpwrite
     */
    if ( (volrec->vol.mode == WRITE_ENABLE) &&
         ((p = getconfent("VDQM","WRITE_PRIORITY",0)) != NULL) ) {
      if ( strcmp(p,"YES") == 0 ) {
        volrec->vol.priority = VDQM_PRIORITY_MAX;
      }
    }
    log(LOG_INFO,"vdqm_NewVolReq(): request priority set to %d\n",
        volrec->vol.priority);
    
    /*
     * Remember the new volume record to assure replica is updated about it.
     */
     newvolrec = volrec;

    
    /*
     * Add the record to the volume queue
     */
    rc = AddVolRecord(dgn_context,volrec);
    VolReq->VolReqID = volrec->vol.VolReqID;
    if ( rc < 0 ) {
        log(LOG_ERR,"vdqm_NewVolReq(): AddVolRecord() returned error\n");
        FreeDgnContext(&dgn_context);
        return(-1);
    }
    log(LOG_INFO,"vdqm_NewVolReq() Assigned volume request ID=%d\n",
        VolReq->VolReqID);
   
    rc = AnyDrvFreeForDgn(dgn_context);
    if ( rc < 0 ) {
        log(LOG_ERR,"vdqm_NewVolReq(): AnyFreeDrvForDgn() returned error\n");
        FreeDgnContext(&dgn_context);
        return(0);
    }
    /*
     * If there was a free drive, start a new job
     */
    if ( rc == 1 ) {
        /* 
         * Loop until either the volume queue is empty or
         * there are no more suitable drives
         */
        for (;;) {
            rc = SelectVolAndDrv(dgn_context,&volrec,&drvrec);
            if ( rc == -1 || volrec == NULL || drvrec == NULL ) {
                log(LOG_ERR,"vdqm_NewVolReq(): SelectVolAndDrv() returned rc=%d\n",
                    rc);
                break;
            }
            /*
             * Free memory allocated for previous request for this drive
             */
            if ( drvrec->vol != NULL ) free(drvrec->vol);
            drvrec->vol = volrec;
            volrec->drv = drvrec;
            drvrec->drv.VolReqID = volrec->vol.VolReqID;
            volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
            drvrec->drv.jobID = 0;
            /*
             * Reset the drive status
             */
            drvrec->drv.status = drvrec->drv.status & 
                ~(VDQM_UNIT_RELEASE | VDQM_UNIT_BUSY | VDQM_VOL_MOUNT |
                VDQM_VOL_UNMOUNT | VDQM_UNIT_ASSIGN);
            drvrec->drv.recvtime = (int)time(NULL);
        
            /*
             * Start the job
             */
            rc = vdqm_StartJob(volrec);
            if ( rc < 0 ) {
                log(LOG_ERR,"vdqm_NewVolReq(): vdqm_StartJob() returned error\n");
                volrec->vol.DrvReqID = 0;
                drvrec->drv.VolReqID = 0;
                volrec->drv = NULL;
                drvrec->vol = NULL;
                break;
            }
       } /* end of for (;;) */
    }
    /*
     * Always update replica for this volume record (update status may have
     * been temporary reset by SelectVolAndDrv()).
     */
    newvolrec->update = 1;
    FreeDgnContext(&dgn_context);
    return(0);
}

int vdqm_NewDrvReq(vdqmHdr_t *hdr, vdqmDrvReq_t *DrvReq) {
    dgn_element_t *dgn_context = NULL;
    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc,unknown;
    char status_string[256];
    int new_drive_added = 0;
    
    if ( hdr == NULL || DrvReq == NULL ) return(-1);
    rc = 0;
    
    /*
     * Reset Device Group Name context
     */
    log(LOG_INFO,"vdqm_NewDrvReq() new %s request: %s@%s\n",
        DrvReq->dgn,DrvReq->drive,DrvReq->server);
    /*
     * If it is an tape daemon startup status we delete all drives 
     * on that tape server.
     */
    if ( DrvReq->status == VDQM_TPD_STARTED ) {
        if ( strcmp(DrvReq->reqhost,DrvReq->server) != 0 ) {
            log(LOG_ERR,"vdqm_NewDrvReq() unauthorized VDQM_TPD_STARTED for %s sent by %s\n",
                DrvReq->server,DrvReq->reqhost);
            vdqm_SetError(EPERM);
            return(-1);
        }
        while (NextDgnContext(&dgn_context) != -1 && dgn_context != NULL ) {
            log(LOG_INFO,"vdqm_NewDrvReq() dgn: %s, %s tape daemon startup\n",
                dgn_context->dgn,DrvReq->reqhost);
            rc = DelAllDrvOnSrv(dgn_context,DrvReq->reqhost);
        }
        return(0);
    }

    rc = SetDgnContext(&dgn_context,DrvReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_NewDrvReq() cannot set Dgn context for %s\n",
            DrvReq->dgn);
        return(-1);
    }
        
    /* 
     * Check whether the drive record already exists
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context,DrvReq,&drvrec);

    if ( DrvReq->status == VDQM_UNIT_QUERY ) {
        if ( rc < 0 || drvrec == NULL ) {
            log(LOG_ERR,"vdqm_NewDrvReq(): Query request for unknown drive %s@%s\n",DrvReq->drive,DrvReq->server);
            FreeDgnContext(&dgn_context);
            vdqm_SetError(EVQNOSDRV);
            return(-1);
        }
        *DrvReq = drvrec->drv;
        FreeDgnContext(&dgn_context);
        return(0);
    }

    if ( rc < 0 || drvrec == NULL ) {
        /*
         * Drive record did not exist, create it!
         */
        log(LOG_INFO,"vdqm_NewDrvReq() add new drive %s@%s\n",
            DrvReq->drive,DrvReq->server);
        rc = NewDrvRecord(&drvrec);
        if ( rc < 0 || drvrec == NULL ) {
            log(LOG_ERR,"vdqm_NewDrvReq(): NewDrvRecord() returned error\n");
            FreeDgnContext(&dgn_context);
            return(-1);
        }
        drvrec->drv = *DrvReq;
        /*
         * Make sure it is either up or down. If neither, we put it in
         * UNKNOWN status until further status information is received.
         */
        if ( (drvrec->drv.status & ( VDQM_UNIT_UP|VDQM_UNIT_DOWN)) == 0 )
            drvrec->drv.status |= VDQM_UNIT_UP|VDQM_UNIT_UNKNOWN;
        /*
         * Make sure it doesn't come up with some non-persistent status
         * becasue of a previous VDQM server crash.
         */
        drvrec->drv.status = drvrec->drv.status & ( ~VDQM_VOL_MOUNT &
            ~VDQM_VOL_UNMOUNT & ~VDQM_UNIT_MBCOUNT );
        /*
         * Add drive record to drive queue
         */
        rc = AddDrvRecord(dgn_context,drvrec);
        if ( rc < 0 ) {
            log(LOG_ERR,"vdqm_NewDrvReq(): AddDrvRecord() returned error\n");
            FreeDgnContext(&dgn_context);
            return(-1);
        }

        new_drive_added  = 1;
        
    }
    /*
     * Update dynamic drive info.
     */
    drvrec->magic = hdr->magic;
    drvrec->drv.recvtime = time(NULL);

    GetStatusString(drvrec->drv.status,status_string);
    log(LOG_INFO,"%s %s@%s (ID: %d, job %d): current status: %s\n",drvrec->drv.dgn,
        drvrec->drv.drive,drvrec->drv.server,drvrec->drv.VolReqID,
        drvrec->drv.jobID,status_string);
    GetStatusString(DrvReq->status,status_string);
    log(LOG_INFO,"%s %s@%s (ID: %d, job %d): requested status: %s\n",DrvReq->dgn,
        DrvReq->drive,DrvReq->server,DrvReq->VolReqID,DrvReq->jobID,
        status_string);
    /*
     * Reset UNKNOWN status if necessary. However we remember that status
     * in case there is a RELEASE since we then cannot allow the unit
     * to be assigned to another job until it is FREE (i.e. we must
     * wait until volume has been unmounted). 
     */
    unknown = drvrec->drv.status & VDQM_UNIT_UNKNOWN;
    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_UNKNOWN;
    /*
     * Verify that new unit status is consistent with the
     * current status of the drive.
     */
    if ( DrvReq->status & VDQM_UNIT_DOWN ) {
        /*
         * Unit configured down. No other status possible.
         * For security this status can only be set from
         * tape server.
         */
        if ( strcmp(DrvReq->reqhost,drvrec->drv.server) != 0 ) {
            log(LOG_ERR,"vdqm_NewDrvRequest(): unauthorized %s@%s DOWN from %s\n",
                DrvReq->drive,DrvReq->server,DrvReq->reqhost);
            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
            FreeDgnContext(&dgn_context);
            vdqm_SetError(EPERM);
            return(-1);
        }
        /*
         * Remove running request (if any). Client will normally retry
         */
        if ( drvrec->drv.status & VDQM_UNIT_BUSY ) {
            volrec = drvrec->vol;
            if ( volrec != NULL ) {
                log(LOG_INFO,"vdqm_NewDrvRequest(): Remove old volume record, id=%d\n",
                    volrec->vol.VolReqID);
                volrec->drv = NULL;
                DelVolRecord(dgn_context,volrec);
            }
            drvrec->vol = NULL;
            *drvrec->drv.volid = '\0';
            drvrec->drv.VolReqID = 0;
            drvrec->drv.jobID = 0;
        }
        drvrec->drv.status = VDQM_UNIT_DOWN;
		drvrec->update = 1;
    } else if ( DrvReq->status & VDQM_UNIT_WAITDOWN ) {
        /*
         * Intermediate state until tape daemon confirms that
         * the drive is down. If a volume request is assigned 
         * we cannot put it back in queue until drive is confirmed
         * down since the volume may still be stuck in the unit.
         * First check the drive isn't already down...
         */ 
        if ( !(drvrec->drv.status & VDQM_UNIT_DOWN) ) {
            log(LOG_INFO,"vdqm_NewDrvRequest(): WAIT DOWN request from %s\n",
                DrvReq->reqhost); 
            drvrec->drv.status = VDQM_UNIT_WAITDOWN;
			drvrec->update = 1;
        }
    } else if ( DrvReq->status & VDQM_UNIT_UP ) {
        /*
         * Unit configured up. Make sure that "down" status is reset.
         * We also mark drive free if input request doesn't specify
         * otherwise.
         * If the input request is a plain config up and the unit was not 
         * down there is a job assigned it probably  means that the tape 
         * server has been rebooted. It does then not make sense
         * to keep the job because client has lost connection long ago and
         * has normally issued a retry. We can therefore remove the 
         * job from queue.
         */
        if ( (DrvReq->status == VDQM_UNIT_UP ||
              DrvReq->status == (VDQM_UNIT_UP | VDQM_UNIT_FREE)) &&
            !(drvrec->drv.status & VDQM_UNIT_DOWN) ) {
            if ( drvrec->vol != NULL || *drvrec->drv.volid != '\0' ) {
                volrec = drvrec->vol;
                if ( volrec != NULL ) {
                    log(LOG_INFO,"vdqm_NewDrvRequest(): Remove old volume record, id=%d\n",
                        volrec->vol.VolReqID);
                    DelVolRecord(dgn_context,volrec);
                    drvrec->vol = NULL;
                }
                *drvrec->drv.volid = '\0';
                drvrec->drv.VolReqID = 0;
                drvrec->drv.jobID = 0;
            } 
            drvrec->drv.status = VDQM_UNIT_UP | VDQM_UNIT_FREE;
        }
        drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_DOWN;
        drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_WAITDOWN;

        if ( DrvReq->status == VDQM_UNIT_UP )
            drvrec->drv.status |= VDQM_UNIT_FREE;
        drvrec->drv.status |= DrvReq->status;
		drvrec->update = 1;
    } else {
        if ( drvrec->drv.status & VDQM_UNIT_DOWN ) {
            /*
             * Unit must be up before anything else is allowed
             */
            log(LOG_ERR,"vdqm_NewDrvReq(): unit is not UP\n");
            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
            FreeDgnContext(&dgn_context);
            vdqm_SetError(EVQUNNOTUP);
            return(-1);
        } 
        /*
         * If the unit is not DOWN, it must be UP! Make sure it's the case.
         * This is to facilitate the repair after a VDQM server crash.
         */
        if ( (drvrec->drv.status & VDQM_UNIT_UP) == 0 ) {
			drvrec->drv.status |= VDQM_UNIT_UP;
			drvrec->update = 1;
		}

        if ( DrvReq->status & VDQM_UNIT_BUSY ) {
            /*
             * Consistency check
             */
            if ( DrvReq->status & VDQM_UNIT_FREE ) {
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADSTAT);
                return(-1);
            }
            drvrec->drv.status = DrvReq->status;
            /*
             * Unit marked "busy". Reset free status.
             */
            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_FREE;
        } else if ( DrvReq->status & VDQM_UNIT_FREE ) {
            /*
             * Cannot free an assigned unit, it must be released first
             */
            if ( !(DrvReq->status & VDQM_UNIT_RELEASE) &&
                (drvrec->drv.status & VDQM_UNIT_ASSIGN) ) {
                log(LOG_ERR,"vdqm_NewDrvReq(): cannot free assigned unit %s@%s, jobID=%d\n",
                    drvrec->drv.drive,drvrec->drv.server,drvrec->drv.jobID);
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADSTAT);
                return(-1);
            }
            /*
             * Cannot free an unit with tape mounted
             */
            if ( !(DrvReq->status & VDQM_VOL_UNMOUNT) &&
                (*drvrec->drv.volid != '\0') ) {
                log(LOG_ERR,"vdqm_NewDrvReq(): cannot free unit with tape mounted, volid=%s\n",
                    drvrec->drv.volid);
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADSTAT);
                return(-1);
            }
        } else {
            /*
             * If unit is busy and being assigned the VolReqIDs must
             * be the same. If so, assign the jobID (normally the
             * process ID of the RTCOPY process on the tape server).
             */
            if ( (drvrec->drv.status & VDQM_UNIT_BUSY) &&
                 (DrvReq->status & VDQM_UNIT_ASSIGN) ) {                
                if (drvrec->drv.VolReqID != DrvReq->VolReqID){
                    log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent VolReqIDs (%d,%d) on ASSIGN\n",
                        DrvReq->VolReqID,drvrec->drv.VolReqID);
                    if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                    FreeDgnContext(&dgn_context);
                    vdqm_SetError(EVQBADID);
                    return(-1);
                } else {
                    drvrec->drv.jobID = DrvReq->jobID;
                    log(LOG_INFO,"vdqm_NewDrvReq() assign VolReqID %d to jobID %d\n",
                        DrvReq->VolReqID,DrvReq->jobID);
                }
            }
            /*
             * If unit is busy with a running job the job IDs must be same
             */
            if ( (drvrec->drv.status & VDQM_UNIT_BUSY) &&
                 !(drvrec->drv.status & VDQM_UNIT_RELEASE) &&
                (DrvReq->status & (VDQM_UNIT_ASSIGN | VDQM_UNIT_RELEASE |
                                   VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) &&
                (drvrec->drv.jobID != DrvReq->jobID) ) {
                log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent jobIDs (%d,%d)\n",
                    DrvReq->jobID,drvrec->drv.jobID);
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADID);
                return(-1);
            }
            
            /*
             * Prevent operations on a free unit. A job must have been
             * started and unit marked busy before it can be used.
             * 22/11/1999: we change this so that a free unit can be
             *             assigned. The reason is that a local job may
             *             running on the tape server (e.g. tplabel) may
             *             want to run without starting a rtcopy job.
             */
            if ( !(DrvReq->status & VDQM_UNIT_BUSY) &&
                (drvrec->drv.status & VDQM_UNIT_FREE) &&
                (DrvReq->status & (VDQM_UNIT_RELEASE |
                VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) ) {
                log(LOG_ERR,"vdqm_NewDrvReq(): status 0x%x requested on FREE drive\n",
                    DrvReq->status);
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADSTAT);
                return(-1);
            }
            if ( DrvReq->status & VDQM_UNIT_ASSIGN ) {
                /*
                 * Check whether unit was already assigned. If so, the
                 * volume request must be identical
                 */
                if ( (drvrec->drv.status & VDQM_UNIT_ASSIGN) &&
                    (drvrec->drv.jobID != DrvReq->jobID) ) {
                    log(LOG_ERR,"vdqm_NewDrvReq(): attempt to re-assign ID=%d to an unit assigned to ID=%d\n",
                        drvrec->drv.jobID,DrvReq->jobID);
                    if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                    FreeDgnContext(&dgn_context);
                    vdqm_SetError(EVQBADID);
                    return(-1);
                }
                /*
                 * If the unit was free, we set the new jobID. This is
                 * local assign bypassing the normal VDQM logic (see comment
                 * above). There is no VolReqID since VDQM is bypassed.
                 */
                if ( (drvrec->drv.status & VDQM_UNIT_FREE) ) {
                    /*
                     * We only allow this for local requests!
                     */
                    if ( strcmp(DrvReq->reqhost,drvrec->drv.server) != 0 ) {
                        log(LOG_ERR,"vdqm_NewDrvRequest(): unauthorized %s@%s local assign from %s\n",
                        DrvReq->drive,DrvReq->server,DrvReq->reqhost);
                        if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                        FreeDgnContext(&dgn_context);
                        vdqm_SetError(EPERM);
                        return(-1);
                    }
                    log(LOG_INFO,"vdqm_NewDrvRequest() local assign %s@%s to jobID %d\n",
                        DrvReq->drive,DrvReq->server,DrvReq->jobID);
                    drvrec->drv.jobID = DrvReq->jobID;
                }
            }
            /*
             * VDQM_VOL_MOUNT and VDQM_VOL_UNMOUNT are not persistent unit 
             * status values. Their purpose is twofold: 1) input - update 
             * the volid field in the drive record (both MOUNT and UNMOUNT)
             * and 2) output - tell client to unmount or keep volume mounted
             * in case of deferred unmount (UNMOUNT only). 
             *
             * VDQM_UNIT_MBCOUNT is not a persistent unit status value.
             * It request update of drive statistics.
             */
            if ( drvrec->drv.status & VDQM_UNIT_UP )
                drvrec->drv.status |= DrvReq->status & 
                                      (~VDQM_VOL_MOUNT & ~VDQM_VOL_UNMOUNT &
                                       ~VDQM_UNIT_MBCOUNT );
        }
		drvrec->update = 1;
    }
    
    volrec = NULL;
    if ( drvrec->drv.status & VDQM_UNIT_UP ) {
        if ( DrvReq->status & VDQM_UNIT_ASSIGN ) {
            /*
             * Unit assigned (reserved).
             */
            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_FREE;
            drvrec->drv.status |= VDQM_UNIT_BUSY;
        }
        if ( (DrvReq->status & VDQM_UNIT_MBCOUNT) ) {
            /*
             * Update TotalMB counter. Since this request is sent by
             * RTCOPY rather than the tape daemon we cannot yet reset
             * unknown status if it was previously set.
             */
            drvrec->drv.MBtransf = DrvReq->MBtransf;
            drvrec->drv.TotalMB += DrvReq->MBtransf;
            if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
        }
        if ( (DrvReq->status & VDQM_UNIT_ERROR) ) {
            /*
             * Update error counter.
             */
            drvrec->drv.errcount++;
        }
        if ( (DrvReq->status & VDQM_VOL_MOUNT) ) {
            /*
             * A mount volume request. The unit must first have been assigned.
             */
            if ( !(drvrec->drv.status & VDQM_UNIT_ASSIGN) ) {
                log(LOG_ERR,"vdqm_NewDrvReq(): mount request of %s for jobID %d on non-ASSIGNED unit\n",
                    DrvReq->volid,DrvReq->jobID);
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQNOTASS);
                return(-1);
            }
            if ( *DrvReq->volid == '\0' ) {
                log(LOG_ERR,"vdqm_NewDrvReq(): mount request with empty VOLID for jobID %d\n",
                    drvrec->drv.jobID);
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADVOLID);
                return(-1);
            }
            /*
             * Make sure that requested volume and assign volume record are the same
             */
            if ( drvrec->vol != NULL && strcmp(drvrec->vol->vol.volid,DrvReq->volid) ) {
                log(LOG_ERR,"vdqm_NewDrvReq(): inconsistent mount %s (should be %s) for jobID %d\n",
                    DrvReq->volid,drvrec->vol->vol.volid,DrvReq->jobID);
                if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADVOLID);
                return(-1);
            }
            /*
             * If there are no assigned volume request it means that this
             * is a local request. Make sure that server and reqhost are
             * the same and that the volume is free.
             */
            if ( drvrec->vol == NULL ) {
                if ( strcmp(drvrec->drv.server,DrvReq->reqhost) != 0 ) {
                    if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                    FreeDgnContext(&dgn_context);
                    vdqm_SetError(EPERM);
                    return(-1);
                 }
                 if ( VolInUse(dgn_context,DrvReq->volid) ||
                      VolMounted(dgn_context,DrvReq->volid) ) {
                     if ( unknown ) drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                     FreeDgnContext(&dgn_context);
                     vdqm_SetError(EBUSY);
                     return(-1);
                 }
                 drvrec->drv.mode = -1; /* Mode is unknown */
            } else drvrec->drv.mode = drvrec->vol->vol.mode;
            strcpy(drvrec->drv.volid,DrvReq->volid);
            drvrec->drv.status |= VDQM_UNIT_BUSY;
            /*
             * Update usage counter
             */
            drvrec->drv.usecount++;
        }
        if ( (DrvReq->status & VDQM_VOL_UNMOUNT) ) {
            *drvrec->drv.volid = '\0';
            drvrec->drv.mode = -1;
            /*
             * Volume has been unmounted. Reset release status (if set).
             */
            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
            /*
             * If it was an forced unmount due to an error we can reset
             * the ERROR status at this point.
             */
            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ERROR;
            /*
             * If the client forced an unmount with the release we can
             * reset the FORCE_UNMOUNT here.
             */
            drvrec->drv.status = drvrec->drv.status & ~VDQM_FORCE_UNMOUNT;
            /*
             * We should also reset UNMOUNT status in the request so that
             * we tell the drive to unmount twice
             */
            DrvReq->status = DrvReq->status & ~VDQM_VOL_UNMOUNT;
            /*
             * Set status to FREE if there is no job assigned to the unit
             */
            if ( drvrec->vol == NULL ) {
                drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_BUSY;
                drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ASSIGN;
                drvrec->drv.status = drvrec->drv.status | VDQM_UNIT_FREE;
            }
        }
        if ((DrvReq->status & VDQM_UNIT_RELEASE) &&
            !(DrvReq->status & VDQM_UNIT_FREE) ) { 
            /*
             * Reset assign status (drive is being released!).
             */
            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ASSIGN;
            /*
             * Reset request and job IDs
             */
            drvrec->drv.VolReqID = 0;
            drvrec->drv.jobID = 0;
            /*
             * Delete from queue and free memory allocated for 
             * previous volume request on this drive. 
             */
            if ( drvrec->vol != NULL ) {
                rc = DelVolRecord(dgn_context,drvrec->vol);
                free(drvrec->vol);
            }
            drvrec->vol = NULL;

            if ( *drvrec->drv.volid != '\0' ) {
                /*
                 * Consistency check: if input request contains a volid it
                 * should correspond to the one in the drive record. This
                 * is not fatal but should be logged.
                 */
                if ( *DrvReq->volid != '\0' && 
                     strcmp(DrvReq->volid,drvrec->drv.volid) ) {
                   log(LOG_ERR,"vdqm_NewDrvReq(): drive %s@%s inconsistent release on %s (should be %s), jobID=%d\n",
                       drvrec->drv.drive,drvrec->drv.server,
                       DrvReq->volid,drvrec->drv.volid,drvrec->drv.jobID);
                }
                /*
                 * Fill in current volid in case we need to request an unmount.
                 */
                if ( *DrvReq->volid == '\0' ) strcpy(DrvReq->volid,drvrec->drv.volid);
                /*
                 * If a volume is mounted but current job ended: check if there
                 * are any other valid request for the same volume. The volume
                 * can only be re-used on this unit if the modes (R/W) are the 
                 * same. If client indicated an error or forced unmount the
                 * will remain with that state until a VOL_UNMOUNT is received
                 * and both drive and volume cannot be reused until then.
                 * If the drive status was UNKNOWN at entry we must force an
                 * unmount in all cases. This normally happens when a RTCOPY
                 * client has aborted and sent VDQM_DEL_VOLREQ to delete his
                 * volume request before the RTCOPY server has released the
                 * job. 
                 */
                rc = 0;
                if (!unknown && !(drvrec->drv.status & (VDQM_UNIT_ERROR |
                                                        VDQM_FORCE_UNMOUNT)))
                    rc = AnyVolRecForMountedVol(dgn_context,drvrec,&volrec);
                if ( (drvrec->drv.status & VDQM_UNIT_ERROR) ||
                     unknown || rc == -1 || volrec == NULL || 
                     volrec->vol.mode != drvrec->drv.mode ) {
                    if ( drvrec->drv.status & VDQM_UNIT_ERROR ) 
                        log(LOG_ERR,"vdqm_NewDrvReq(): unit in error status. Force unmount!\n");
                    if ( drvrec->drv.status & VDQM_FORCE_UNMOUNT )
                        log(LOG_ERR,"vdqm_NewDrvReq(): client requests forced unmount\n");
                    if ( rc == -1 ) 
                        log(LOG_ERR,"vdqm_NewDrvReq(): AnyVolRecForVolid() returned error\n");
                    if ( unknown )
                        log(LOG_ERR,"vdqm_NewDrvReq(): drive in UNKNOWN status. Force unmount!\n");
                    /*
                     * No, there wasn't any other job for that volume. Tell the
                     * drive to unmount the volume. Put unit in unknown status
                     * until volume has been unmounted to prevent other
                     * request from being assigned.
                     */
                    DrvReq->status  = VDQM_VOL_UNMOUNT;
                    drvrec->drv.status |= VDQM_UNIT_UNKNOWN; 
                    volrec = NULL;
                } else {
                    volrec->drv = drvrec;
                    drvrec->vol = volrec;
                    volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
                    drvrec->drv.VolReqID = volrec->vol.VolReqID;
                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
                }
            } else {
                /*
                 * There is no volume on unit. Since a RELEASE has
                 * been requested the unit is FREE (no job and no volume
                 * assigned to it. Update status accordingly.
                 * If client specified FORCE_UNMOUNT it means that there is
                 * "something" (unknown to VDQM) mounted. We have to wait
                 * for the unmount before resetting the status.
                 */
                if ( !(DrvReq->status & VDQM_FORCE_UNMOUNT) &&
                     !(drvrec->drv.status & VDQM_FORCE_UNMOUNT) ) {
                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_BUSY;
                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
                    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_ASSIGN;
                    drvrec->drv.status = drvrec->drv.status | VDQM_UNIT_FREE;
                }
                /*
                 * Always tell the client to unmount just in case there was an
                 * error and VDQM wasn't notified.
                 */
                DrvReq->status  = VDQM_VOL_UNMOUNT;
            }
        } 
        /*
         * If unit is free, reset dynamic data in drive record
         */
        if ( (DrvReq->status & VDQM_UNIT_FREE) ) {
            /*
             * Reset status to reflect that drive is not assigned and nothing is mounted.
             */
            drvrec->drv.status = (drvrec->drv.status | VDQM_UNIT_FREE) &
                (~VDQM_UNIT_ASSIGN & ~VDQM_UNIT_RELEASE & ~VDQM_UNIT_BUSY);
            
            /*
             * Dequeue request and free memory allocated for previous 
             * volume request for this drive
             */
            if ( drvrec->vol != NULL ) {
                rc = DelVolRecord(dgn_context,drvrec->vol);
                if ( rc == -1 ) {
                    log(LOG_ERR,"vdqm_NewDrvReq(): DelVolRecord() returned error\n");
                } else {
                    free(drvrec->vol);
                }
                drvrec->vol = NULL;
            }
            /*
             * Reset request and job IDs
             */
            drvrec->drv.VolReqID = 0;
            drvrec->drv.jobID = 0;
        }
        /*
         * Loop until there is no more volume requests in queue or
         * no suitable free drive
         */
        for (;;) {
            /*
             * If volrec already assigned by AnyVolRecForMountedVol()
             * we skip to start request
             */
            if ( volrec == NULL ) {
                rc = AnyVolRecForDgn(dgn_context);
                if ( rc < 0 ) {

                    log(LOG_ERR,"vdqm_NewDrvReq(): AnyVolRecForDgn() returned error\n");
                    break;
                }
                if ( rc == 0 ) break;
                if ( rc == 1 ) {
                    rc = SelectVolAndDrv(dgn_context,&volrec,&drvrec);
                    if ( rc == -1 || volrec == NULL || drvrec == NULL ) {
                        log(LOG_ERR,"vdqm_NewDrvReq(): SelectVolAndDrv() returned rc=%d\n",
                            rc);
                        break;
                    } else {
                        drvrec->vol = volrec;
                        volrec->drv = drvrec;
                    }
                }
            }
            if ( volrec != NULL ) {
                drvrec->drv.VolReqID = volrec->vol.VolReqID;
                volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
                /*
                 * Start the job
                 */
                rc = vdqm_StartJob(volrec);
                if ( rc < 0 ) {
                    /*
                     * Job could not be started. Mark the drive
                     * status as unknown so that it will not be
                     * assigned to a new request immediately. The
                     * volume record is kept in queue. Note that
                     * if a volume is already mounted on unit it
                     * is inaccessible for other requests until 
                     * drive status is updated.
                     */
                    log(LOG_ERR,"vdqm_NewDrvReq(): vdqm_StartJob() returned error\n");
                    drvrec->drv.status |= VDQM_UNIT_UNKNOWN;
                    drvrec->drv.recvtime = (int)time(NULL);
                    drvrec->vol = NULL;
                    drvrec->drv.VolReqID = 0;
                    volrec->vol.DrvReqID = 0;
                    drvrec->drv.jobID = 0;
                    volrec->drv = NULL;
                    break;
                } 
            } else break;
            volrec = NULL;
        } /* End of for (;;) */
    } else {
        /*
         * If drive is down, report error for any requested update.
         */
        if ( DrvReq->status & (VDQM_UNIT_FREE | VDQM_UNIT_ASSIGN |
            VDQM_UNIT_BUSY | VDQM_UNIT_RELEASE | VDQM_VOL_MOUNT |
            VDQM_VOL_UNMOUNT ) ) {
            log(LOG_ERR,"vdqm_NewDrvReq(): drive is DOWN\n");
            FreeDgnContext(&dgn_context);
            vdqm_SetError(EVQUNNOTUP);
            return(-1);
        }
    }
    FreeDgnContext(&dgn_context);    
    
    /* At this point, backup the comple queue to a file */
    if (new_drive_added) {
         log(LOG_DEBUG,"vdqm_NewDrvReq(): Update drive config file\n");
         if (vdqm_save_queue() < 0) {
             log(LOG_ERR, "Could not save drive list\n");
         }         
    }

    return(0);
}

int vdqm_DedicateDrv(vdqmDrvReq_t *DrvReq) {
    dgn_element_t *dgn_context;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( DrvReq == NULL ) return(-1);
    rc = 0;

    log(LOG_INFO,"vdqm_DedicateDrv() from %s for dgn: %s, drive: %s@%s, dedicate=%s\n",
        DrvReq->reqhost,
        DrvReq->dgn,
        DrvReq->drive,
        DrvReq->server,
        DrvReq->dedicate);

    /*
     * Set DGN context
     */
    rc = SetDgnContext(&dgn_context,DrvReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_DedicateDrv() cannot set Dgn context for %s\n",
            DrvReq->dgn);
        return(-1);
    }

    /*
     * Check whether the drive record already exists
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context,DrvReq,&drvrec);

    if ( rc < 0 || drvrec == NULL ) {
        log(LOG_ERR,"vdqm_DedicateDrv(): unknown drive %s@%s\n",
            DrvReq->drive,DrvReq->server);
        FreeDgnContext(&dgn_context);
        vdqm_SetError(EVQNOSDRV);
        return(-1);
    }
    if ( *DrvReq->dedicate == '\0' ) {
        rc = vdqm_ResetDedicate(drvrec);
    } else {
        strcpy(drvrec->drv.dedicate,DrvReq->dedicate);
        rc = vdqm_SetDedicate(drvrec);
        if ( rc == -1 ) vdqm_SetError(EINVAL);
    }
    drvrec->update = 1;
    FreeDgnContext(&dgn_context);
    return(rc);
}

int vdqm_GetQueuePos(vdqmVolReq_t *VolReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    int rc;
    
    if ( VolReq == NULL ) return(-1);
    
    /*
     * Set Device Group Name context. First check that DGN exists.
     */
    rc = CheckDgn(VolReq->dgn);
    if ( rc == -1 ) return(-1);
    if ( rc == 0 ) {
        vdqm_SetError(EVQDGNINVL);
        return(-1);
    }
    rc = SetDgnContext(&dgn_context,VolReq->dgn);
    if ( rc == -1 ) return(-1);
    
    /*
     * Verify that the request exists
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( volrec == NULL ) {
        log(LOG_ERR,"vdqm_GetQueuePos() request VolReqID=%d, dgn=%s not found\n",
            VolReq->VolReqID,VolReq->dgn);
        FreeDgnContext(&dgn_context);
        vdqm_SetError(EVQNOSVOL);
        return(-1);
    }
    rc = VolRecQueuePos(dgn_context,volrec);
    /* 
     * No update
     */
    volrec->update = 0;
    FreeDgnContext(&dgn_context);
    return(rc);
}

int vdqm_GetVolQueue(char *dgn, vdqmVolReq_t *VolReq, 
                     void **dgn_place_holder,void **vol_place_holder) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    int rc;

    if ( dgn_place_holder == NULL || vol_place_holder == NULL ) return(-1);
    volrec = (vdqm_volrec_t *)*vol_place_holder;
    dgn_context = (dgn_element_t *)*dgn_place_holder;
    if ( VolReq == NULL ) {
        /*
         * Normally FreeDgnContext() was already called from
         * NextDgnContext() when the end of the DGN list is
         * reached. However, if the client interrupts the
         * request before it reached its end the context must
         * be freed. If the context has already been freed
         * there is no harm because dgn_context == NULL and
         * FreeDgnContext() is not called.
         */
        if ( dgn_context != NULL ) FreeDgnContext(&dgn_context);
        return(0);
    }
    
    if ( volrec == NULL ) {
        if ( dgn == NULL  || *dgn == '\0' ) {
            log(LOG_DEBUG,"vdqm_GetVolQueue(NULL)\n");
            rc = NextDgnContext(&dgn_context);
        } else {
            /* 
             * If caller has asked for a specific DGN and
             * we already have had its context, we are here
             * because the end of queue for that DGN has been
             * reached. Context was freed in previous loop so
             * it does not need to be done again.
             */
            rc = 0;
            if ( dgn_context != NULL ) dgn_context = NULL;
            else {
               log(LOG_DEBUG,"vdqm_GetVolQueue(%s)\n",dgn);
               rc = SetDgnContext(&dgn_context,dgn);
            }
        }
        *dgn_place_holder = dgn_context;
        if ( rc == -1 || dgn_context == NULL ) return(-1);
        volrec = dgn_context->vol_queue;
    }
    if ( volrec != NULL ) {
        *VolReq = volrec->vol;
        if ( volrec->next != dgn_context->vol_queue )
            *vol_place_holder = volrec->next;
        else *vol_place_holder = NULL;
    } else *vol_place_holder = NULL;
    if ( *vol_place_holder == NULL && dgn != NULL && *dgn != '\0' ) {
        /*
         * Caller asked for a specific device group and
         * we have reached the end of the queue.
         */
        FreeDgnContext(&dgn_context);
    }
    return(0);
}

int vdqm_GetDrvQueue(char *dgn, vdqmDrvReq_t *DrvReq, 
                     void **dgn_place_holder,void **drv_place_holder) {
    dgn_element_t *dgn_context;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( dgn_place_holder == NULL || drv_place_holder == NULL ) return(-1);
    drvrec = (vdqm_drvrec_t *)*drv_place_holder;
    dgn_context = (dgn_element_t *)*dgn_place_holder;
    if ( DrvReq == NULL ) {
        /*
         * Normally FreeDgnContext() was already called from
         * NextDgnContext() when the end of the DGN list is
         * reached. However, if the client interrupts the
         * request before it reached its end the context must
         * be freed. If the context has already been freed
         * there is no harm because dgn_context == NULL and
         * FreeDgnContext() is not called.
         */
        if ( dgn_context != NULL ) FreeDgnContext(&dgn_context);
        return(0);
    }
    
    if ( drvrec == NULL ) {
        if ( dgn == NULL  || *dgn == '\0' ) {
            log(LOG_DEBUG,"vdqm_GetDrvQueue(NULL)\n");
            rc = NextDgnContext(&dgn_context);
        } else {
            /* 
             * If caller has asked for a specific DGN and
             * we already have had its context, we are here
             * because the end of queue for that DGN has been
             * reached. Context was freed in previous loop so
             * it does not need to be done again.
             */
            rc = 0;
            if ( dgn_context != NULL ) dgn_context = NULL;
            else {
                log(LOG_DEBUG,"vdqm_GetDrvQueue(%s)\n",dgn);
                rc = SetDgnContext(&dgn_context,dgn);
            }
        }
        *dgn_place_holder = dgn_context;
        if ( rc == -1 || dgn_context == NULL ) return(-1);
        drvrec = dgn_context->drv_queue;
    }
    if ( drvrec != NULL ) {
        *DrvReq = drvrec->drv;
        if ( drvrec->next != dgn_context->drv_queue )
            *drv_place_holder = drvrec->next;
        else *drv_place_holder = NULL;
    } else *drv_place_holder = NULL;
    if ( *drv_place_holder == NULL && dgn != NULL && *dgn != '\0') {
        /*
         * Caller asked for a specific device group and
         * we have reached the end of the queue.
         */
        FreeDgnContext(&dgn_context);
    }
    return(0);
}


/**
 * Function that add drives to the VDQM internal structures.
 */
static int vdqm_add_drive(vdqmDrvReq_t *DrvReq) {

    dgn_element_t *dgn_context = NULL;
    vdqm_drvrec_t *drvrec;
    int rc;

    log(LOG_DEBUG, "Entering vdqm_add_drive\n");
    
    rc = SetDgnContext(&dgn_context,DrvReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_add_drive() cannot set Dgn context for %s\n", DrvReq->dgn);
        return(-1);
    }
    
    /* 
     * Check whether the drive record already exists
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context, DrvReq, &drvrec);

    if ( rc < 0 || drvrec == NULL ) {
        /*
         * Drive record did not exist, create it!
         */
        log(LOG_INFO,"vdqm_add_drive() add new drive %s@%s\n",
            DrvReq->drive,DrvReq->server);
        rc = NewDrvRecord(&drvrec);
        if ( rc < 0 || drvrec == NULL ) {
            log(LOG_ERR,"vdqm_NewDrvReq(): NewDrvRecord() returned error\n");
            FreeDgnContext(&dgn_context);
            return(-1);
        }
        drvrec->drv = *DrvReq;
        /*
         * Make sure it is either up or down. If neither, we put it in
         * UNKNOWN status until further status information is received.
         */
        if ( (drvrec->drv.status & ( VDQM_UNIT_UP|VDQM_UNIT_DOWN)) == 0 )
            drvrec->drv.status |= VDQM_UNIT_UP|VDQM_UNIT_UNKNOWN;
        /*
         * Make sure it doesn't come up with some non-persistent status
         * becasue of a previous VDQM server crash.
         */
        drvrec->drv.status = drvrec->drv.status & ( ~VDQM_VOL_MOUNT &
            ~VDQM_VOL_UNMOUNT & ~VDQM_UNIT_MBCOUNT );
        /*
         * Add drive record to drive queue
         */
        rc = AddDrvRecord(dgn_context,drvrec);
        if ( rc < 0 ) {
            log(LOG_ERR,"vdqm_NewDrvReq(): AddDrvRecord() returned error\n");
            FreeDgnContext(&dgn_context);
            return(-1);
        }
    }

    FreeDgnContext(&dgn_context);
    return 0;
    
}


/**
 * Function that writes a vdqmDrvReq_t to a file descriptor.
 */
static int vdqm_write_drv(int fd, vdqmDrvReq_t *drv) {

    char buf[CA_MAXLINELEN+1];
    int rc;
    
    if (drv == NULL) {
        log(LOG_ERR,"vdqm_write_drv: drv is NULL\n");
        return -1;
    }

    /* Writing the server information on one line */
    memset(buf, '\0', sizeof(buf));
    if (strlen(drv->dgn) == 0
        || strlen(drv->server) == 0
        || strlen(drv->drive) == 0) {
        log(LOG_ERR,"vdqm_write_drv: Tried to write empty record\n");
        /* This is not fatal, just do nothing in this case */
        return 0;
    }

    rc = Csnprintf(buf, CA_MAXLINELEN, "%s %s %s\n", drv->dgn, drv->server, drv->drive);   
    if (rc <= 0) {
        log(LOG_ERR,"vdqm_write_drv: Could not write to buffer\n");
        return -1;
    }
    
    rc = write(fd, buf, strlen(buf));
    if (rc <= 0) {
        log(LOG_ERR,"vdqm_write_drv: Could not write to file\n");
        return -1;
    }

    /* Writing the dedication on another line */
    memset(buf, '\0', sizeof(buf));
    rc = Csnprintf(buf, CA_MAXLINELEN, "%s\n", drv->dedicate);   
    if (rc <= 0) {
        log(LOG_ERR,"vdqm_write_drv: Could not write to buffer\n");
        return -1;
    }
    
    rc = write(fd, buf, strlen(buf));
    if (rc <= 0) {
        log(LOG_ERR,"vdqm_write_drv: Could not write to file\n");
        return -1;
    }
    
    return 0;
}

/**
 * Function that writes the vdqn DGN queues to a file.
 */
int vdqm_save_queue() {

    int fd;
    int rc = 0;
    dgn_element_t *queue = NULL;
    vdqm_drvrec_t *drvrec = NULL;

    if (strlen(drives_filename) == 0) {
         log(LOG_DEBUG,"vdqm_load_queue: drives filename not set\n");
         return 0;
    }
    
    log(LOG_DEBUG,"vdqm_save_queue: SAVING DRIVES TO FILE: %s\n", drives_filename);

    if(vdqm_LockAllQueues() != 0) {
        log(LOG_ERR,"vdqm_save_queue: Could not lock all queues\n");
        return -1;
    }
    
    fd = open(drives_filename, O_WRONLY | O_CREAT | O_TRUNC, 00666);
    if (fd < 0) {
        log(LOG_ERR,"vdqm_save_queue: Could not open: %s\n", drives_filename);
        goto end_loops;;
    }
    
    CLIST_ITERATE_BEGIN(dgn_queues,queue) {
        CLIST_ITERATE_BEGIN(queue->drv_queue, drvrec) {
            rc = vdqm_write_drv(fd, &(drvrec->drv));
            if (rc < 0) {
                goto end_loops;
            }
        } CLIST_ITERATE_END(queue->drv_queue, drvrec);
    } CLIST_ITERATE_END(dgn_queues,queue);

  end_loops:
    if (fd >= 0) {
        close(fd);
    }
    if(vdqm_UnlockAllQueues() != 0) {
        log(LOG_ERR,"vdqm_save_queue: Could not unlock all queues\n");
        return -1;
    }
    return(rc);
}

/**
 * Function that loads the vdqn DGN queues from a file.
 */
int vdqm_load_queue() {

    FILE *f;
    int end;
    char *rs;
    char buf[CA_MAXLINELEN+1];
    vdqmDrvReq_t drv;

    if (strlen(drives_filename) == 0) {
         log(LOG_DEBUG,"vdqm_load_queue: drives filename not set\n");
         return 0;
    }

    
    log(LOG_DEBUG,"vdqm_load_queue: LOADING DRIVES FROM FILE: %s\n", drives_filename);
    
    f = fopen(drives_filename, "r");
    if (f == NULL) {
        log(LOG_ERR,"vdqm_load_queue: Could not open: %s\n", drives_filename);
        return -1;
    }
    
    end = 0;
    while(!end) {
        char *rs, *ded;

        { /* Reading the line identifying the drive */
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
            char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */
            
            memset(&buf, '\0', sizeof(buf));
            rs = fgets(buf, CA_MAXLINELEN, f);
            if (rs == NULL)
                break;
            
            /* First reading the drive */ 
            memset(&drv, '\0', sizeof(vdqmDrvReq_t));
            
            /* Reading the DGN */
            rs = strtok(buf, " \t\n");
            if (rs == NULL)
                break;
            strncpy(drv.dgn, rs, CA_MAXDGNLEN);
            
            /* reading the server name */
            rs = strtok(NULL, " \t\n");
            if (rs == NULL)
                break;
            strncpy(drv.server, rs, CA_MAXHOSTNAMELEN);

            /* reading the drive name */
            rs = strtok(NULL, " \t\n");
            if (rs == NULL)
                break;
            strncpy(drv.drive, rs, CA_MAXUNMLEN);
            
            log(LOG_DEBUG, "vdqm_load_queue(): FOUND <%s> <%s> <%s>\n", drv.dgn, drv.server, drv.drive);
            vdqm_add_drive(&drv);
        }

        { /* Reading the dedication line */
            
#if defined(_REENTRANT) || defined(_THREAD_SAFE)
            char *last = NULL;
#endif /* _REENTRANT || _THREAD_SAFE */

            rs = fgets(buf, CA_MAXLINELEN, f);
            if (rs == NULL)
                break;

            /* Removing the trailing \n read from the file ! */
            
            ded = strtok(buf, "\n");
            if (ded != NULL) {
                if (strlen(rs) > 0) {
                    strncpy(drv.dedicate, rs, CA_MAXLINELEN);
                    log(LOG_DEBUG, "vdqm_load_queue(): DEDICATED TO <%s>\n", drv.dedicate);
                    vdqm_DedicateDrv(&drv);
                }
            }
        }
    }
    
    fclose(f);
    return 0;
}

/*
 * Inits the drive_filename variable used to save the drives list.
 */
void vdqm_init_drive_file(char *filename) {

    memset(drives_filename, '\0', sizeof(drives_filename)); 
    strncpy(drives_filename, filename, CA_MAXPATHLEN);
    
    log(LOG_INFO, "vdqm_init_drive_file(): Will save drives to file: %s\n", drives_filename);
}
