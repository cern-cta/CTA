/*
 * $Id: vdqm_QueueOp.c,v 1.3 1999/09/01 15:11:06 obarring Exp $
 * $Log: vdqm_QueueOp.c,v $
 * Revision 1.3  1999/09/01 15:11:06  obarring
 * Fix sccsid string
 *
 * Revision 1.2  1999/07/31 14:49:55  obarring
 * Set error codes for exceptions
 *
 * Revision 1.1  1999/07/27 09:21:23  obarring
 * First version
 *
 */

/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * vdqm_QueueOp.c - Queue volume and drive requests (server only).
 */

#ifndef lint
static char sccsid[] = "@(#)$Id: vdqm_QueueOp.c,v 1.3 1999/09/01 15:11:06 obarring Exp $";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <net.h>
#include <log.h>
#include <serrno.h>
#include <Castor_limits.h>
#include <Cthread_api.h>
#include <vdqm_constants.h>
#include <vdqm.h>

#if !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* linux */

#define INVALID_DGN_CONTEXT (dgn_context == NULL)
#define thID Cthread_self()

static dgn_element_t *dgn_queues = NULL;
static int nb_dgn = 0;

static int dgn_key;
static int vdqm_maxtimediff = VDQM_MAXTIMEDIFF;
static int vdqm_retrydrvtime = VDQM_RETRYDRVTIM;

static int SetDgnContext(dgn_element_t **dgn_context,const char *dgn) {
    dgn_element_t *tmp = NULL;
    int found,rc;
    
    if ( dgn == NULL || dgn_context == NULL) {
        vdqm_SetError(SEINTERNAL);
        return(-1);
    }
    
    log(LOG_DEBUG,"(ID %d)SetDgnContext(%s) lock mutex 0x%x\n",thID,dgn,(long)&nb_dgn);
    if ( (rc = Cthread_mutex_lock(&nb_dgn)) == -1 ) {
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
            log(LOG_INFO,"(ID %d)SetDgnContext(%s) unlock mutex 0x%x\n",thID,dgn,(long)&nb_dgn);
            Cthread_mutex_unlock(&nb_dgn);
            vdqm_SetError(EVQSYERR);
            return(-1);
        }
        nb_dgn++;
        strcpy(tmp->dgn,dgn);
        tmp->dgnindx = nb_dgn;
        tmp->drv_queue = NULL;
        tmp->vol_queue = NULL;
        CLIST_INSERT(dgn_queues,tmp);
    }

    rc = 0;
    *dgn_context = tmp;
    log(LOG_DEBUG,"(ID %d)SetDgnContext(%s) unlock mutex 0x%x\n",thID,dgn,(long)&nb_dgn);
    Cthread_mutex_unlock(&nb_dgn);
    if ( *dgn_context == NULL ) {
        log(LOG_ERR,"(ID %d)SetDgnContext(%s) internal error: cannot assign DGN context\n",thID);
        vdqm_SetError(SEINTERNAL);
        rc = -1;
    } else {
        log(LOG_DEBUG,"(ID %d)SetDgnContext(%s) lock mutex 0x%x\n",thID,dgn,(long)*dgn_context);
        rc = Cthread_mutex_lock(*dgn_context);
    }
    return(rc);
}

static int FreeDgnContext(dgn_element_t **dgn_context) {

    int rc;

    if ( dgn_queues == NULL || dgn_context == NULL ) return(-1);
    log(LOG_DEBUG,"(ID %d)FreeDgnContext() freeing dgn_context at 0x%x\n",thID,
        (long)*dgn_context);
    rc = vdqm_UpdateReplica(*dgn_context);
    if ( rc == -1 ) {
        log(LOG_ERR,"FreeDgnContext() vdqm_UpdateReplica() returned error\n");
    }
    Cthread_mutex_unlock(*dgn_context);
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
    log(LOG_DEBUG,"(ID %d)NextDgnContext() lock mutex 0x%x\n",thID,(long)&nb_dgn);
    Cthread_mutex_lock(&nb_dgn);
    if ( *dgn_context == NULL ) {
        if ( dgn_queues == NULL ) return(-1);
        strcpy(tmpdgn,dgn_queues->dgn);
    } else {
        if ( (*dgn_context)->next != dgn_queues )
            strcpy(tmpdgn,(*dgn_context)->next->dgn);
    }
    log(LOG_DEBUG,"(ID %d)NextDgnContext() unlock mutex 0x%x\n",thID,(long)&nb_dgn);
    Cthread_mutex_unlock(&nb_dgn);
    if ( *dgn_context != NULL ) FreeDgnContext(dgn_context);

    if ( *tmpdgn != '\0' ) return(SetDgnContext(dgn_context,tmpdgn));
    else return(-1);
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

static int NewVolReqID() {
    static int reqID = 0;
    int rc;
    
    Cthread_mutex_lock(&reqID);
    rc = ++reqID;
    Cthread_mutex_unlock(&reqID);
    return(rc);
}

static int NewDrvReqID() {
    static int reqID = 0;
    int rc;
    
    Cthread_mutex_lock(&reqID);
    rc = ++reqID;
    Cthread_mutex_unlock(&reqID);
    return(rc);
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
 */
static int VolInUse(const dgn_element_t *dgn_context, 
                    const char *volid) {
    vdqm_drvrec_t *drv;
    int found;
    
    if ( INVALID_DGN_CONTEXT || volid == NULL ) return(0);
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if ( drv->vol != NULL &&
            *drv->vol->vol.volid != '\0' && 
            !strcmp(drv->vol->vol.volid,volid) ) {
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
    vdqm_drvrec_t *drv;
    char *server, *drive, *volid;
    int found;

    if ( INVALID_DGN_CONTEXT || volrec == NULL || 
        *volrec->vol.server == '\0' ) return(-1);
    if ( freedrv != NULL ) *freedrv = NULL;
    server = volrec->vol.server;
    drive = volrec->vol.drive;
    volid = volrec->vol.volid;
    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if (!(drv->drv.status & VDQM_UNIT_UNKNOWN) &&
             (drv->drv.status & VDQM_UNIT_UP) &&
            ((drv->drv.status & VDQM_UNIT_FREE) ||
             (drv->drv.status & VDQM_UNIT_RELEASE) &&
             !strcmp(drv->drv.volid,volid)) ) {
            if ( !strcmp(drv->drv.server,server) && 
                 (*drive == '\0' || !strcmp(drv->drv.drive,drive)) &&
                 ((drv->drv.status & VDQM_UNIT_RELEASE) || 
                  !VolMounted(dgn_context,volid)) ) {
                strcpy(drive,drv->drv.drive);
                *freedrv = drv;
                found = 1;
                break;
            }
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    if ( found && *freedrv != NULL ) (*freedrv)->update = 1;
    return(found);
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
    vdqm_volrec_t *vol, *top;
    
    if ( volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    top = NULL;
    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
        if ( (vol->vol.DrvReqID == 0 ) &&
             (top == NULL || CmpVolRec(vol,top) == 1) &&
            !VolInUse(dgn_context,vol->vol.volid) ) top = vol;
    } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
    if ( top != NULL ) top->update = 1;
    else vdqm_SetError(EVQNOVOL);
    *volrec = top;
    return(0);
}

static int AnyVolRecForMountedVol(const dgn_element_t *dgn_context,
                                  const vdqm_drvrec_t *drv,
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
                (top == NULL || CmpVolRec(vol,top) == 1) )
                top = vol;
        }
    } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
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
                           const vdqm_drvrec_t *drv,
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
                   !VolMounted(dgn_context,vol->vol.volid)) ) top = vol;
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
                      !VolInUse(dgn_context,vol->vol.volid) ) top = vol;
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
    
    if ( volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    if ( volrec->vol.VolReqID <= 0 ) volrec->vol.VolReqID = NewVolReqID();
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
    
    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    if ( drvrec->drv.DrvReqID <= 0 ) drvrec->drv.DrvReqID = NewDrvReqID();
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
                        vdqm_drvrec_t **drvrec) {
    vdqm_drvrec_t *drv, *top;
    
    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    top = NULL;
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
        if ( !(drv->drv.status & VDQM_UNIT_UNKNOWN) &&
              (drv->drv.status & VDQM_UNIT_UP) &&
              (drv->drv.status & VDQM_UNIT_FREE) ) {
            if ((top == NULL) ||
                (drv->drv.recvtime<top->drv.recvtime)) top = drv;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
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
    
    if ( volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
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

static int FindDrvRecord(const dgn_element_t *dgn_context,
                         vdqmVolReq_t *vol, vdqm_drvrec_t **drvrec) {
    vdqm_drvrec_t *tmprec;
    int found;

    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);

    found = 0;
    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,tmprec) {
        if ( vol->VolReqID == tmprec->vol->vol.VolReqID ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,tmprec);
    
    if ( found ) *drvrec = tmprec;
    else {
        vdqm_SetError(EVQNOSVOL);
        *drvrec = NULL;
    }

    return(0);
}

static int GetDrvRecord(const dgn_element_t *dgn_context,
                        vdqmDrvReq_t *drv, vdqm_drvrec_t **drvrec) {
    vdqm_drvrec_t *tmprec;
    int found;
    
    if ( drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);
    
    found = 0;
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

static int SelectVolAndDrv(const dgn_element_t *dgn_context,
                           vdqm_volrec_t **vol,
                           vdqm_drvrec_t **drv) {

    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( vol != NULL ) *vol = (vdqm_volrec_t *)NULL;
    if ( drv != NULL ) *drv = (vdqm_drvrec_t *)NULL;

    if ( INVALID_DGN_CONTEXT ) return(-1);
    volrec = (vdqm_volrec_t *)NULL;
    drvrec = (vdqm_drvrec_t *)NULL;
    rc = PopVolRecord(dgn_context,&volrec);
    if ( rc == -1 || volrec == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_NewVolReq(): PopVolRecord() returned rc=%d, volrec=0x%x\n",thID,
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
    if ( *volrec->vol.server != '\0' ) {
        rc = AnyFreeDrvOnSrv(dgn_context,volrec,&drvrec);
        log(LOG_DEBUG,"(ID %d)SelectVolAndDrv()::DEBUG AnyFreeDrvOnSrv(%s) returned rc=%d\n",thID,
            volrec->vol.server,rc);
        /*
         * No free drive on that server. Reset the search 
         * volume record to allow for another volume record
         * to be popped
         */
        if ( drvrec == NULL ) {
            log(LOG_ERR,"(ID %d)SelectVolAndDrv(): AnyFreeDrvOnSrv() returned rc=%d\n",thID,
                rc);
            volrec = NULL;
        } else {
            /*
             * Reset update until we are sure
             */
            drvrec->update = 0;
        }
    }
    log(LOG_DEBUG,"(ID %d)SelectVolAndDrv()::DEBUG volrec=0x%x, drvrec=0x%x\n",thID,volrec,drvrec);
    if ( drvrec == NULL ) {
        /*
         * Either the volrec asked for a specific server/drive
         * which wasn't free or it accepts any server
         */
        rc = PopDrvRecord(dgn_context,&drvrec);
        if ( rc == -1 || drvrec == NULL ) {
            log(LOG_ERR,"(ID %d)SelectVolAndDrv(): PopDrvRecord() returned rc=%d, drvrec=0x%x\n",thID,
                rc,drvrec);
            return(-1);
        }
        /*
         * Reset update until we are sure
         */
        drvrec->update = 0;
        log(LOG_DEBUG,"(ID %d)SelectVolAndDrv()::DEBUG PopDrvRecord() returned rc=%d %s@%s\n",thID,rc,
            drvrec->drv.drive,drvrec->drv.server);
        if ( volrec == NULL ) {
            /*
             * Search for a volume record that either
             * explicitly asked for this server/drive or can
             * accept any server.
             */
            rc = AnyVolRecForDrv(dgn_context,drvrec,&volrec);
            log(LOG_DEBUG,"(ID %d)SelectVolAndDrv()::DEBUG AnyVolRecForDrv() returned rc=%d\n",thID,rc);
            if ( volrec == NULL ) {
                log(LOG_ERR,"(ID %d)SelectVolAndDrv(): AnyVolRecForDrv() returned rc=%d\n",
                    thID,rc);
                return(-1);
            }
            /*
             * Reset update until we are sure
             */
            volrec->update = 0;
        }
    }
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

int vdqm_DelVolReq(vdqmVolReq_t *VolReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( VolReq == NULL ) return(-1);
    log(LOG_INFO,"(ID %d)vdqm_DelVolReq() set context to dgn=%s\n",thID,VolReq->dgn);

    rc = SetDgnContext(&dgn_context,VolReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"(ID %d)vdqm_DelVolReq() cannot set Dgn context for %s\n",thID,
            VolReq->dgn);
        return(-1);
    }

    /*
     * Verify that the volume record exists
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( rc == -1 || volrec == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_DelVolReq() volume record not in queue. Checking Drives.\n",thID);
        rc = FindDrvRecord(dgn_context,VolReq,&drvrec);
        if ( drvrec == NULL ) {
            log(LOG_ERR,"(ID %d)vdqm_DelVolRecord() volume record not found\n",thID);
            FreeDgnContext(&dgn_context);
            return(-1);
        }
        /*
         * We don't reset the drive status here. It is expected
         * that the client also interrupts the tape job so that
         * the tape daemon properly cleanup and tell VDQM when the
         * drive is ready again.
         */
        volrec = drvrec->vol;
        drvrec->vol = NULL;
        drvrec->drv.VolReqID = 0;
    } else {
        /*
         * Delete the volume record from the queue.
         */
        rc = DelVolRecord(dgn_context,volrec);
    }
    /*
     * Free it
     */
    free(volrec);
    FreeDgnContext(&dgn_context);
    return(0);
}

int vdqm_DelDrvReq(vdqmDrvReq_t *DrvReq) {
    dgn_element_t *dgn_context;
    vdqm_drvrec_t *drvrec;
    int rc;

    if ( DrvReq == NULL ) return(-1);
    rc = 0;
    
    /*
     * Reset Device Group Name context
     */
    log(LOG_INFO,"(ID %d)vdqm_DelDrvReq() set context to dgn=%s\n",thID,DrvReq->dgn);
    rc = SetDgnContext(&dgn_context,DrvReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"(ID %d)vdqm_DelDrvReq() cannot set Dgn context for %s\n",thID,
            DrvReq->dgn);
        return(-1);
    }
        
    /* 
     * Check whether the drive record already exists
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context,DrvReq,&drvrec);
    if ( rc == -1 || drvrec == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_DelDrvReq() drive record not found for drive %s@%s\n",thID,
            DrvReq->drive,DrvReq->server);
        FreeDgnContext(&dgn_context);
        return(-1);
    }
    /*
     * Don't allow to delete drives with running jobs.
     */
    if ( !(drvrec->drv.status & (VDQM_UNIT_UP | VDQM_UNIT_FREE)) &&
         !(drvrec->drv.status & VDQM_UNIT_DOWN) ) {
        log(LOG_ERR,"(ID %d)vdqm_DelDrvReq() cannot remove drive record with assigned job\n",thID);
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
    free(drvrec);
    FreeDgnContext(&dgn_context);
    return(0);
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

    log(LOG_INFO,"(ID %d)vdqm_QueueOpRollback() called\n",thID);            

    if ( VolReq == NULL || DrvReq == NULL ) return(-1);
    rc = SetDgnContext(&dgn_context,VolReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"(ID %d)vdqm_QueueOpRollback() cannot set Dgn context for %s\n",thID,
            VolReq->dgn);
        return(-1);
    }
    /* 
     * Get the volume record. If it doesn't exist anymore we
     * still need to find and correct the drive record.
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( volrec == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_QueueOpRollback() input request already queued\n",thID);
    }
    /*
     * Get the drive record. If it doens't exist anymore we
     * still need to correct the volume record
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context,DrvReq,&drvrec);
    
    /*
     * Now correct the records
     */
    if ( volrec != NULL ) {
        volrec->vol.DrvReqID = 0;
        volrec->drv = NULL;
    }
    if ( drvrec != NULL ) {
        drvrec->drv.VolReqID = 0;
        drvrec->vol = NULL;
    }
    FreeDgnContext(&dgn_context);
    return(0);
}


int vdqm_NewVolReq(vdqmHdr_t *hdr, vdqmVolReq_t *VolReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc;
    
    if ( hdr == NULL || VolReq == NULL ) return(-1);
    
    /*
     * Reset Device Group Name context
     */
    log(LOG_INFO,"(ID %d)vdqm_NewVolReq() set context to dgn=%s\n",thID,VolReq->dgn);
    rc = SetDgnContext(&dgn_context,VolReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"(ID %d)vdqm_NewVolReq() cannot set Dgn context for %s\n",thID,
            VolReq->dgn);
        return(-1);
    }
    
    /*
     * Verify that the request doesn't (yet) exist
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( volrec != NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_NewVolReq() input request already queued\n",thID);
        FreeDgnContext(&dgn_context);
        vdqm_SetError(EVQALREADY);
        return(-1);
    }
    
    /*
     * Reserv new volume record
     */
    rc = NewVolRecord(&volrec);
    if ( rc < 0 || volrec == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_NewVolReq(): NewVolRecord() %s\n",thID,ERRTXT);
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
     * Add the record to the volume queue
     */
    rc = AddVolRecord(dgn_context,volrec);
    VolReq->VolReqID = volrec->vol.VolReqID;
    if ( rc < 0 ) {
        log(LOG_ERR,"(ID %d)vdqm_NewVolReq(): AddVolRecord() returned error\n",thID);
        FreeDgnContext(&dgn_context);
        return(-1);
    }
   
    rc = AnyDrvFreeForDgn(dgn_context);
    if ( rc < 0 ) {
        log(LOG_ERR,"(ID %d)vdqm_NewVolReq(): AnyFreeDrvForDgn() returned error\n",thID);
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
                log(LOG_ERR,"(ID %d)vdqm_NewVolReq(): SelectVolAndDrv() returned rc=%d\n",thID,
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
        
            /*
             * Start the job
             */
            rc = vdqm_StartJob(volrec);
            if ( rc < 0 ) {
                log(LOG_ERR,"(ID %d)vdqm_NewVolReq(): vdqm_StartJob() returned error\n",thID);
                volrec->vol.DrvReqID = 0;
                drvrec->drv.VolReqID = 0;
                volrec->drv = NULL;
                drvrec->vol = NULL;
                break;
            }
       } /* end of for (;;) */
    }
    FreeDgnContext(&dgn_context);
    return(0);
}

int vdqm_NewDrvReq(vdqmHdr_t *hdr, vdqmDrvReq_t *DrvReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    vdqm_drvrec_t *drvrec;
    int rc;
    
    if ( hdr == NULL || DrvReq == NULL ) return(-1);
    rc = 0;
    
    /*
     * Reset Device Group Name context
     */
    log(LOG_INFO,"(ID %d)vdqm_NewDrvReq() set context to dgn=%s\n",thID,DrvReq->dgn);
    rc = SetDgnContext(&dgn_context,DrvReq->dgn);
    if ( rc == -1 ) {
        log(LOG_ERR,"(ID %d)vdqm_NewDrvReq() cannot set Dgn context for %s\n",thID,
            DrvReq->dgn);
        return(-1);
    }
        
    /* 
     * Check whether the drive record already exists
     */
    drvrec = NULL;
    rc = GetDrvRecord(dgn_context,DrvReq,&drvrec);
    
    if ( rc < 0 || drvrec == NULL ) {
        /*
         * Drive record did not exist, create it!
         */
        log(LOG_INFO,"(ID %d)vdqm_NewDrvReq() add new drive %s@%s\n",thID,
            DrvReq->drive,DrvReq->server);
        rc = NewDrvRecord(&drvrec);
        if ( rc < 0 || drvrec == NULL ) {
            log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): NewDrvRecord() returned error\n",thID);
            FreeDgnContext(&dgn_context);
            return(-1);
        }
        drvrec->drv = *DrvReq;
        /*
         * Add drive record to drive queue
         */
        rc = AddDrvRecord(dgn_context,drvrec);
        if ( rc < 0 ) {
            log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): AddDrvRecord() returned error\n",thID);
            FreeDgnContext(&dgn_context);
            return(-1);
        }
    }
    /*
     * Update dynamic drive info.
     */
    drvrec->magic = hdr->magic;
    drvrec->drv.recvtime = time(NULL);
    
    /*
     * Reset UNKNOWN status if necessary.
     */
    drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_UNKNOWN;
    /*
     * Verify that new unit status is consistent with the
     * current status of the drive.
     */
    if ( DrvReq->status & VDQM_UNIT_DOWN ) {
        /*
         * Unit configured down. No other status possible
         * First check if there was a running job to re-enter
         * it into the queue.
         */
        if ( drvrec->drv.status & VDQM_UNIT_BUSY ) {
            volrec = drvrec->vol;
            if ( volrec != NULL ) {
                volrec->drv = NULL;
                AddVolRecord(dgn_context,volrec);
            }
            drvrec->vol = NULL;
            *drvrec->drv.volid = '\0';
            drvrec->drv.VolReqID = 0;
            drvrec->drv.jobID = 0;
        }
        drvrec->drv.status = VDQM_UNIT_DOWN;
    } else if ( DrvReq->status & VDQM_UNIT_UP ) {
        /*
         * Unit configured up. Make sure that "down" status is reset.
         */
        drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_DOWN;
        drvrec->drv.status |= DrvReq->status;
    } else {
        if ( !(drvrec->drv.status & VDQM_UNIT_UP) ) {
            /*
             * Unit must be up before anything else is allowed
             */
            log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): unit is not UP\n",thID);
            FreeDgnContext(&dgn_context);
            vdqm_SetError(EVQUNNOTUP);
            return(-1);
        }
        if ( DrvReq->status & VDQM_UNIT_BUSY ) {
            /*
             * Consistency check
             */
            if ( DrvReq->status & VDQM_UNIT_FREE ) {
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
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): cannot free assigned unit %s@%s, jobID=0x%x\n",thID,
                    drvrec->drv.drive,drvrec->drv.server,drvrec->drv.jobID);
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADSTAT);
                return(-1);
            }
            /*
             * Cannot free an unit with tape mounted
             */
            if ( !(DrvReq->status & VDQM_VOL_UNMOUNT) &&
                (*drvrec->drv.volid != '\0') ) {
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): cannot free unit with tape mounted, volid=%s\n",thID,
                    drvrec->drv.volid);
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
                    log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): inconsistent VolReqIDs (0x%x, 0x%x) on ASSIGN\n",thID,
                        DrvReq->VolReqID,drvrec->drv.VolReqID);
                    FreeDgnContext(&dgn_context);
                    vdqm_SetError(EVQBADID);
                    return(-1);
                } else {
                    drvrec->drv.jobID = DrvReq->jobID;
                }
            }
            /*
             * If unit is busy the job IDs must be same
             */
            if ( (drvrec->drv.status & VDQM_UNIT_BUSY) &&
                (DrvReq->status & (VDQM_UNIT_ASSIGN | VDQM_UNIT_RELEASE |
                VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) &&
                (drvrec->drv.jobID != DrvReq->jobID) ) {
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): inconsistent jobIDs (0x%x, 0x%x)\n",thID,
                    DrvReq->jobID,drvrec->drv.jobID);
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADID);
                return(-1);
            }
            
            /*
             * Prevent operations on a free unit. A job must have been
             * started and unit marked busy before it can be used.
             */
            if ( !(DrvReq->status & VDQM_UNIT_BUSY) &&
                (drvrec->drv.status & VDQM_UNIT_FREE) &&
                (DrvReq->status & (VDQM_UNIT_ASSIGN | VDQM_UNIT_RELEASE |
                VDQM_VOL_MOUNT | VDQM_VOL_UNMOUNT)) ) {
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): status 0x%x requested on FREE drive\n",thID,
                    DrvReq->status);
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
                    log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): attempt to re-assign ID=0x%x to an unit assigned to ID=0x%x\n",thID,
                        drvrec->drv.jobID,DrvReq->jobID);
                    FreeDgnContext(&dgn_context);
                    vdqm_SetError(EVQBADID);
                    return(-1);
                }
            }
            /*
             * VDQM_VOL_MOUNT and VDQM_VOL_UNMOUNT are not really unit status
             * values. Their purpose is twofold: 1) input - update the volid field in the
             * drive record (both MOUNT and UNMOUNT) and 2) output - tell client
             * to unmount or keep volume mounted in case of deferred unmount 
             * (UNMOUNT only).
             */
            if ( drvrec->drv.status & VDQM_UNIT_UP )
                drvrec->drv.status |= DrvReq->status & (~VDQM_VOL_MOUNT & ~VDQM_VOL_UNMOUNT);
        }
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
        if ( (DrvReq->status & VDQM_VOL_MOUNT) ) {
            /*
             * A mount volume request. The unit must first have been assigned.
             */
            if ( !(drvrec->drv.status & VDQM_UNIT_ASSIGN) ) {
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): mount request of %s for jobID 0x%x on non-ASSIGNED unit\n",thID,
                    DrvReq->volid,DrvReq->jobID);
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQNOTASS);
                return(-1);
            }
            if ( *DrvReq->volid == '\0' ) {
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): mount request with empty VOLID for jobID 0x%x\n",thID,
                    drvrec->drv.jobID);
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADVOLID);
                return(-1);
            }
            /*
             * Make sure that requested volume and assign volume record are the same
             */
            if ( drvrec->vol != NULL && strcmp(drvrec->vol->vol.volid,DrvReq->volid) ) {
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): inconsistent mount %s (should be %s) for jobID 0x%x\n",thID,
                    DrvReq->volid,drvrec->vol->vol.volid,DrvReq->jobID);
                FreeDgnContext(&dgn_context);
                vdqm_SetError(EVQBADVOLID);
                return(-1);
            }
            strcpy(drvrec->drv.volid,DrvReq->volid);
            drvrec->drv.status |= VDQM_UNIT_BUSY;
        }
        if ( (DrvReq->status & VDQM_VOL_UNMOUNT) ) {
            *drvrec->drv.volid = '\0';
            /*
             * Volume has been unmounted. Reset release status (if set).
             */
            drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
            /*
             * We should also reset UNMOUNT status in the request so that
             * we tell the drive to unmount twice
             */
            DrvReq->status = DrvReq->status & ~VDQM_VOL_UNMOUNT;
        }
        if ((DrvReq->status & VDQM_UNIT_RELEASE) &&
            !(DrvReq->status & VDQM_UNIT_FREE) &&
            (*DrvReq->volid != '\0' || *drvrec->drv.volid != '\0' ) ) {
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
             * previous volume request on this drive
             */
            if ( drvrec->vol != NULL ) {
                rc = DelVolRecord(dgn_context,drvrec->vol);
                free(drvrec->vol);
            }
            drvrec->vol = NULL;
            /*
             * Consistency check: if input request contains a volid it
             * should correspond to the one in the drive record. This
             * is not fatal but should be logged.
             */
            if ( *DrvReq->volid != '\0' && strcmp(DrvReq->volid,drvrec->drv.volid) ) {
                log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): drive %s@%s inconsistent release on %s (should be %s), jobID=%d\n",thID,
                    drvrec->drv.drive,drvrec->drv.server,
                    DrvReq->volid,drvrec->drv.volid,drvrec->drv.jobID);
            }
            /*
             * Fill in current volid in case we need to request an unmount.
             */
            if ( *DrvReq->volid == '\0' ) strcpy(DrvReq->volid,drvrec->drv.volid);
            /*
             * If a volume is mounted but current job ended: check if there
             * are any other valid request for the same volume.
             */
            rc = AnyVolRecForMountedVol(dgn_context,drvrec,&volrec);
            if ( rc == -1 || volrec == NULL ) {
                if ( rc == -1 ) 
                    log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): AnyVolRecForVolid() returned error\n",thID);
                /*
                 * No, there wasn't another job for that volume. Tell the
                 * drive to unmount the volume
                 */
                DrvReq->status  = VDQM_VOL_UNMOUNT;
            } else {
                volrec->drv = drvrec;
                drvrec->vol = volrec;
                volrec->vol.DrvReqID = drvrec->drv.DrvReqID;
                drvrec->drv.VolReqID = volrec->vol.VolReqID;
                drvrec->drv.status = drvrec->drv.status & ~VDQM_UNIT_RELEASE;
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
                    log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): DelVolRecord() returned error\n",thID);
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
         * no free suitable drive
         */
        for (;;) {
            /*
             * If volrec already assigned by AnyVolRecForMountedVol()
             * we skip to start request
             */
            if ( volrec == NULL ) {
                rc = AnyVolRecForDgn(dgn_context);
                if ( rc < 0 ) {

                    log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): AnyVolRecForDgn() returned error\n",thID);
                    break;
                }
                if ( rc == 0 ) break;
                if ( rc == 1 ) {
                    rc = SelectVolAndDrv(dgn_context,&volrec,&drvrec);
                    if ( rc == -1 || volrec == NULL || drvrec == NULL ) {
                        log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): SelectVolAndDrv() returned rc=%d\n",thID,
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
                    log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): vdqm_StartJob() returned error\n",thID);
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
            log(LOG_ERR,"(ID %d)vdqm_NewDrvReq(): drive is DOWN\n",thID);
            FreeDgnContext(&dgn_context);
            vdqm_SetError(EVQUNNOTUP);
            return(-1);
        }
    }
    FreeDgnContext(&dgn_context);
    return(0);
}

int vdqm_GetQueuePos(vdqmVolReq_t *VolReq) {
    dgn_element_t *dgn_context;
    vdqm_volrec_t *volrec;
    int rc;
    
    if ( VolReq == NULL ) return(-1);
    
    /*
     * Set Device Group Name context
     */
    rc = SetDgnContext(&dgn_context,VolReq->dgn);
    if ( rc == -1 ) return(-1);
    
    /*
     * Verify that the request exists
     */
    volrec = NULL;
    rc = GetVolRecord(dgn_context,VolReq,&volrec);
    if ( volrec == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_GetQueuePos() request not found\n",thID);
        FreeDgnContext(&dgn_context);
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
            log(LOG_DEBUG,"(ID %d)vdqm_GetVolQueue(NULL)\n",thID);
            rc = NextDgnContext(&dgn_context);
        } else {
            /* 
             * If caller has asked for a specific DGN and
             * we already have had its context, we are here
             * because the end of queue for that DGN has been
             * reached. Context was freed in previous loop so
             * it does not need to be done again.
             */
            if ( dgn_context != NULL ) dgn_context = NULL;
            else {
               log(LOG_DEBUG,"(ID %d)vdqm_GetVolQueue(%s)\n",thID,dgn);
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
            log(LOG_DEBUG,"(ID %d)vdqm_GetDrvQueue(NULL)\n",thID);
            rc = NextDgnContext(&dgn_context);
        } else {
            /* 
             * If caller has asked for a specific DGN and
             * we already have had its context, we are here
             * because the end of queue for that DGN has been
             * reached. Context was freed in previous loop so
             * it does not need to be done again.
             */
            if ( dgn_context != NULL ) dgn_context = NULL;
            else {
                log(LOG_DEBUG,"(ID %d)vdqm_GetDrvQueue(%s)\n",thID,dgn);
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


