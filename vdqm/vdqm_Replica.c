/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_Replica.c,v $ $Revision: 1.11 $ $Date: 2000/04/28 11:41:23 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_Replica.c - Update VDQM replica database (server only).
 */

#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#if !defined(_WIN32)
#include <regex.h>
#endif /* _WIN32 */

#include <osdep.h>
#include <net.h>
#include <log.h>
#include <serrno.h>
#include <Castor_limits.h>
#include <Cthread_api.h>
#include <vdqm_constants.h>
#include <vdqm.h>

#if defined(_REENTRANT) || defined(_THREAD_SAFE)
#define strtok(X,Y) strtok_r(X,Y,&last)
#endif /* _REENTRANT || _THREAD_SAFE */


static int success = 0;
static int failure = -1;
static int replication_ON = 0;
static int retry_replication = VDQM_REPLICA_RETRIES;

extern dgn_element_t *dgn_queues;
extern int nb_dgn;
extern int hold;
extern char *getconfent _PROTO((char *, char *, int));
/*
 * Only allow access to dgn queues if server is held. Otherwise
 * we risk to have concurrent access with QueueOps. As soon as
 * hold == 0, it means that we have been put in production and if
 * we are a secondary replica we should not accept any replication
 * update until we're back in hold status (-> primary server has
 * been restarted).
 */
#define INVALID_DGN_CONTEXT (dgn_context == NULL && hold == 1)

static vdqmReplicaList_t *ReplicaList = NULL;
static int nb_Replicas = 0;
void *ReplicaList_lock = NULL;

typedef struct ReqPipe {
       int update;        /* 0=drv only, 1=vol only, 2=both drv&vol */
       vdqmHdr_t drvhdr;
       vdqmDrvReq_t drv;
       vdqmHdr_t volhdr;
       vdqmVolReq_t vol;
} ReqPipe_t;

static struct vdqmReplicationPipe {
    void *lock;
    int nb_piped;
    ReqPipe_t pipe[VDQM_REPLICA_PIPELEN];
} ReplicationPipe;

/*
 * Some QueueOp primitives without DGN locks
 */
static int R_SetDgnContext(dgn_element_t **dgn_context, const char *dgn) {
    dgn_element_t *tmp = NULL;
    int found,rc;
 
    if ( dgn == NULL ) return(-1);
    if ( hold == 0 ) {
        log(LOG_ERR,"R_SetDgnContext(): replication request received during production mode!\n");
        return(-1);
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
        if ( tmp == NULL ) return(-1);
        nb_dgn++;
        strcpy(tmp->dgn,dgn);
        tmp->dgnindx = nb_dgn;
        tmp->drv_queue = NULL;
        tmp->vol_queue = NULL;
        CLIST_INSERT(dgn_queues,tmp);
        rc = vdqm_InitDgnQueue(tmp);
        if ( rc == -1 ) {
            log(LOG_ERR,"R_SetDgnContext() InitDgnQueue(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
    }

    *dgn_context = tmp;
    return(0);
}

static int R_NewVolRecord(vdqm_volrec_t **volrec) {
    
    if ( volrec == NULL ) return(-1);
    *volrec = (vdqm_volrec_t *)calloc(1,sizeof(vdqm_volrec_t));
    if ( *volrec == NULL ) return(-1);
    return(0);
}

static int R_NewDrvRecord(vdqm_drvrec_t **drvrec) {
    
    if ( drvrec == NULL ) return(-1);
    *drvrec = (vdqm_drvrec_t *)calloc(1,sizeof(vdqm_drvrec_t));
    if ( *drvrec == NULL ) return(-1);
    return(0);
}

static int R_GetVolRecord(const dgn_element_t *dgn_context,
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
        *volrec = tmprec;
    } else {
        *volrec = NULL;
    }
    return(0);
}

static int R_FindVolRecord(const dgn_element_t *dgn_context,
                           vdqmDrvReq_t *drv, vdqm_volrec_t **volrec) {
    vdqm_volrec_t *tmprec;
    int found = 0;

    if ( drv == NULL || volrec == NULL || INVALID_DGN_CONTEXT ) return(-1);

    CLIST_ITERATE_BEGIN(dgn_context->vol_queue,tmprec) {
        if ( (drv->DrvReqID>0 && tmprec->drv != NULL &&
              drv->DrvReqID == tmprec->drv->drv.DrvReqID) ||
             (drv->VolReqID>0 && drv->VolReqID == tmprec->vol.VolReqID) ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->vol_queue,tmprec);

    if ( found ) *volrec = tmprec;
    else {
        *volrec = NULL;
    }

    return(0);
}

static int R_FindDrvRecord(const dgn_element_t *dgn_context,
                           vdqmVolReq_t *vol, vdqm_drvrec_t **drvrec) {
    vdqm_drvrec_t *tmprec;
    int found = 0;

    if ( vol == NULL || drvrec == NULL || INVALID_DGN_CONTEXT ) return(-1);

    CLIST_ITERATE_BEGIN(dgn_context->drv_queue,tmprec) {
        if ( (vol->VolReqID>0 && tmprec->vol != NULL &&
              vol->VolReqID == tmprec->vol->vol.VolReqID) ||
             (vol->DrvReqID>0 && vol->DrvReqID == tmprec->drv.DrvReqID) ) {
            found = 1;
            break;
        }
    } CLIST_ITERATE_END(dgn_context->drv_queue,tmprec);
    
    if ( found ) *drvrec = tmprec;
    else {
        *drvrec = NULL;
    }

    return(0);
}

static int R_GetDrvRecord(const dgn_element_t *dgn_context,
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
        *drvrec = tmprec;
    } else {
        *drvrec = NULL;
    }
    return(0);
}

int vdqm_ReplVolReq(vdqmHdr_t *hdr, vdqmVolReq_t *vol) {
    int rc;
    vdqm_volrec_t *volrec = NULL;
    vdqm_drvrec_t *drvrec = NULL;
    dgn_element_t *dgn_context = NULL;

    if ( hdr == NULL || vol == NULL ) return(-1);
    (void) vdqm_NewVolReqID(&vol->VolReqID);
    rc = R_SetDgnContext(&dgn_context, vol->dgn);
    if ( rc == -1 ) return(-1);

    rc = R_GetVolRecord(dgn_context, vol, &volrec);    
    if ( rc == -1 ) return(-1);
    if ( hdr->reqtype == VDQM_DEL_VOLREQ ) {
        if ( volrec == NULL ) {
            log(LOG_ERR,"vdqm_ReplVolReq() attempt to delete non-existing request (VID:%s, VolReqID: %d)\n",
                vol->volid,vol->VolReqID);
            return(-1);
        }
        log(LOG_DEBUG,"vdqm_ReplVolReq() Remove volume record (VolReqID: %d)\n",
            vol->VolReqID);
        CLIST_DELETE(dgn_context->vol_queue,volrec);
        /*
         * Check if there's an attached drive record. We only need to
         * detach the deleted volume record from it. The rest of the
         * drive information should be updated automatically through
         * another replica request.
         */
        if ( volrec->drv != NULL ) {
            volrec->drv->vol = NULL;
            volrec->drv = NULL;
        }
        free(volrec);
        return(0);
    }
        
    if ( volrec == NULL ) {
        rc = R_NewVolRecord(&volrec);
        if ( rc == - 1 || volrec == NULL ) {
            log(LOG_ERR,"vdqm_ReplVolReq() R_NewVolRecord(): %s\n",
                sstrerror(errno));
            return(-1);
        }
        log(LOG_DEBUG,"vdqm_ReplVolReq() Add new volume record (VolReqID: %d)\n",
            vol->VolReqID);
        CLIST_INSERT(dgn_context->vol_queue,volrec);
    } else log(LOG_DEBUG,"vdqm_ReplVolReq() Update existing volume record (VolReqID: %d)\n",
               vol->VolReqID);

    volrec->vol = *vol;

    /*
     * Check if it should be attached to a drive. Only the links need
     * to be updated. The request content is updated through replication
     * requests.
     */
    rc = R_FindDrvRecord(dgn_context, vol, &drvrec);
    if ( drvrec != NULL ) {
        drvrec->vol = volrec;
        volrec->drv = drvrec;
    } else {
        volrec->drv = NULL;
    }
    return(0);
}

int vdqm_ReplDrvReq(vdqmHdr_t *hdr, vdqmDrvReq_t *drv) {
    int rc;
    vdqm_drvrec_t *drvrec = NULL;
    vdqm_volrec_t *volrec = NULL;
    dgn_element_t *dgn_context = NULL;

    if ( hdr == NULL || drv == NULL ) return(-1);
    (void) vdqm_NewDrvReqID(&drv->DrvReqID);
    rc = R_SetDgnContext(&dgn_context, drv->dgn);
    if ( rc == -1 ) return(-1);

    rc = R_GetDrvRecord(dgn_context, drv, &drvrec);
    if ( rc == -1 ) return(-1);
    if ( hdr->reqtype == VDQM_DEL_DRVREQ ) {
        if ( drvrec == NULL ) {
            log(LOG_ERR,"vdqm_ReplDrvReq() attempt to delete non-existing request (DrvReqID: %d)\n",
                drv->DrvReqID);
            return(-1);
        }
        log(LOG_DEBUG,"vdqm_ReplDrvReq() Remove drive record (DrvReqID: %d)\n",
            drv->DrvReqID);
        CLIST_DELETE(dgn_context->drv_queue,drvrec);
        /*
         * Detach linked volume records. As usual only the links need to
         * be updated. The remaining info. is updated through replication
         * requests.
         */
        if ( drvrec->vol != NULL ) {
            drvrec->vol->drv = NULL;
            drvrec->vol = NULL;
        }
        free(drvrec);
        return(0);
    }

    if ( drvrec == NULL ) {
        rc = R_NewDrvRecord(&drvrec);
        if ( rc == - 1 || drvrec == NULL ) {
            log(LOG_ERR,"vdqm_ReplDrvReq() R_NewDrvRecord(): %s\n",
                sstrerror(errno));
            return(-1);
        }
        log(LOG_DEBUG,"vdqm_ReplDrvReq() Add new drive record (DrvReqID: %d)\n",
            drv->DrvReqID);
        CLIST_INSERT(dgn_context->drv_queue,drvrec);
    } else log(LOG_DEBUG,"vdqm_ReplDrvReq() Update existing drive record (DrvReqID: %d)\n",
               drv->DrvReqID);

    if ( strcmp(drv->dedicate,drvrec->drv.dedicate) != 0 ) {
        if ( drv->dedicate == '\0' ) {
            rc = vdqm_ResetDedicate(drvrec);
        } else {
            strcpy(drvrec->drv.dedicate,drv->dedicate);
            rc = vdqm_SetDedicate(drvrec);
        }
    }

    drvrec->drv = *drv;
    /*
     * Check if there is (already) an attached volume request. This can
     * happen if the replication updates arrive out of sequence.
     */
    rc = R_FindVolRecord(dgn_context,drv,&volrec);
    if ( volrec != NULL ) {
        volrec->drv = drvrec;
        drvrec->vol = volrec;
    } else {
        drvrec->vol = NULL;
    }

    return(0);
}

/*
 * Operations on the list of replication clients
 */
static int InitReplicaList() {
    int rc;

    /*
     * Initialise fast lock on list of replication clients
     */
    rc = Cthread_mutex_lock(&nb_Replicas);
    if ( rc == -1 ) {
        log(LOG_ERR,"InitReplicaList() Cthread_mutex_lock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    nb_Replicas = 0;
    rc = Cthread_mutex_unlock(&nb_Replicas);
    if ( rc == -1 ) {
        log(LOG_ERR,"InitReplicaList() Cthread_mutex_unlock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    if ( ReplicaList_lock == NULL ) {
        ReplicaList_lock = Cthread_mutex_lock_addr(&nb_Replicas);
        if ( ReplicaList_lock == NULL ) {
            log(LOG_ERR,"InitReplicaList() Cthread_mutex_lock_addr(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
    }

    /*
     * Initialise fast lock on replication request pipe
     */
    rc = Cthread_mutex_lock(&ReplicationPipe.nb_piped);
    if ( rc == -1 ) {
        log(LOG_ERR,"InitReplicaList() Cthread_mutex_lock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    ReplicationPipe.nb_piped = 0;
    rc = Cthread_mutex_unlock(&ReplicationPipe.nb_piped);
    if ( rc == -1 ) {
        log(LOG_ERR,"InitReplicaList() Cthread_mutex_unlock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    if ( ReplicationPipe.lock == NULL ) {
        ReplicationPipe.lock = Cthread_mutex_lock_addr(&ReplicationPipe.nb_piped);
        if ( ReplicationPipe.lock == NULL ) {
            log(LOG_ERR,"InitReplicaList() Cthread_mutex_lock_addr(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
    }
    return(0);
}

static int LockReplicaList() {
    return(Cthread_mutex_lock_ext(ReplicaList_lock));
}

static int UnlockReplicaList() {
    return(Cthread_mutex_unlock_ext(ReplicaList_lock));
}

static int CmpReplica(vdqmReplica_t *rp1, vdqmReplica_t *rp2) {
    if ( rp1 == NULL || rp2 == NULL ) return(0);

    if ( rp1->nw.accept_socket == rp2->nw.accept_socket &&
         rp1->nw.listen_socket == rp2->nw.listen_socket &&
         rp1->nw.connect_socket == rp2->nw.connect_socket ) return(1);
    else return(0);
}

static int FindReplicaRecord(vdqmReplica_t *Replica, vdqmReplicaList_t **rpl) {
    vdqmReplicaList_t *tmp_rpl;
    int found = 0;

    if ( rpl != NULL ) *rpl = NULL;

    CLIST_ITERATE_BEGIN(ReplicaList,tmp_rpl) {
        if ( CmpReplica(&tmp_rpl->Replica,Replica) == 1 ) {
            found = 1;
            *rpl = tmp_rpl;
            break;
        }
    } CLIST_ITERATE_END(ReplicaList,tmp_rpl); 
    return(found);
}

static int NewReplicaRecord(vdqmReplicaList_t **rpl) {

    if ( rpl == NULL || *rpl != NULL ) {
        serrno = EINVAL;
        return(-1);
    }
    (*rpl) = (vdqmReplicaList_t *)calloc(1,sizeof(vdqmReplicaList_t));
    if ( (*rpl) == NULL ) return(-1);
    return(0);
}

static int CheckReplica(vdqmReplica_t *Replica) {
    int rc;
    char *p, *q;
    char *last = NULL;

    if ( Replica == NULL ) return(-1);
    log(LOG_DEBUG,"CheckReplica() check %s\n",Replica->host);
    p = getconfent("VDQM","HOST",1);
    if ( p == NULL ) {
        log(LOG_ERR,"CheckReplica() no replica host configured!\n");
        return(-1);
    }

    q = strtok(p," \t");
    while ( q != NULL ) {
        if ( strcmp(q,Replica->host) == 0  ) break;
        q = strtok(NULL," \t");
    }
    if ( q == NULL ) {
        log(LOG_ERR,"CheckReplica() host %s is not configured as replica\n",
            Replica->host);
        return(-1);
    }

    return(0);
}

int vdqm_CheckReplicaHost(vdqmnw_t *nw) {
    int rc;
    vdqmReplica_t Replica;

    if ( nw == NULL ) return(-1);
    rc = vdqm_GetReplica(nw,&Replica);
    if ( rc == -1 ) return(-1);
    return(CheckReplica(&Replica));
}

static int DeleteReplica(vdqmReplica_t *Replica) {
    int rc, save_serrno;
    vdqmReplicaList_t *rpl = NULL;

    rc = LockReplicaList();
    if ( rc == -1 ) {
        save_serrno = serrno;
        log(LOG_ERR,"DeleteReplica() LockReplicaList(): %s\n",
            sstrerror(serrno));
        vdqm_SetError(save_serrno);
        return(-1);
    }
    rc = FindReplicaRecord(Replica,&rpl);
    if ( rc != 1 || rpl == NULL ) {
        log(LOG_ERR,"DeleteReplica() could not find replication record\n");
        vdqm_SetError(ENOENT);
        (void)UnlockReplicaList();
        return(-1);
    }
    CLIST_DELETE(ReplicaList,rpl);
    rc = UnlockReplicaList();
    if ( rc == -1 ) {
        save_serrno = serrno;
        log(LOG_ERR,"DeleteReplica() UnlockReplicaList(): %s\n",
            sstrerror(save_serrno));
        vdqm_SetError(save_serrno);
        return(-1);
    }
    return(0);
}

int vdqm_AddReplica(vdqmnw_t *nw, vdqmHdr_t *hdr) {
    int save_serrno, rc;
    vdqmReplicaList_t *rpl = NULL;
    vdqmReplica_t _Replica, *Replica;

    if ( nw == NULL ) {
        log(LOG_ERR,"vdqm_AddReplica() called with NULL connection!\n");
        vdqm_SetError(SEINTERNAL);
        return(-1);
    }
    if ( replication_ON == 0 ) {
        log(LOG_ERR,"vdqm_AddReplica() replication not started\n");
        return(1);
    }
    _Replica.nw.listen_socket = _Replica.nw.connect_socket = 
        _Replica.nw.accept_socket = INVALID_SOCKET;
    Replica = &_Replica;
    _Replica.nw = *nw;
    rc = vdqm_GetReplica(nw,Replica);
    Replica->failed = 0;
    if ( rc == -1 ) {
        save_serrno = serrno;
        log(LOG_ERR,"vdqm_AddReplica() vdqm_GetReplica(): %s\n",
            sstrerror(serrno));
        serrno = save_serrno;
        return(-1);
    }
    rc = LockReplicaList();
    if ( rc == -1 ) {
        save_serrno = serrno;
        log(LOG_ERR,"vdqm_AddReplica() LockReplicaList(): %s\n",
            sstrerror(serrno));
        vdqm_SetError(save_serrno);
        return(-1);
    }
    rc = FindReplicaRecord(Replica,NULL);
    if ( rc == 1 ) {
        log(LOG_ERR,"vdqm_AddReplica() replica already exists\n");
        vdqm_SetError(EEXIST);
        (void)UnlockReplicaList();
        return(-1);
    }

    rc = CheckReplica(Replica);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_AddReplica() replication request from unauthorised node %s\n",
            Replica->host);
        vdqm_SetError(EPERM);
        (void)UnlockReplicaList();
        return(-1);
    }

    rc = NewReplicaRecord(&rpl);
    if ( rc == -1 || rpl == NULL ) {
        save_serrno = serrno;
        log(LOG_ERR,"vdqm_AddReplica() NewReplica(): %s\n",
            sstrerror(save_serrno));
        vdqm_SetError(save_serrno);
        (void)UnlockReplicaList();
        return(-1);
    }
/*
    memcpy(&rpl->Replica,Replica,sizeof(vdqmReplica_t));
*/
    rpl->Replica = _Replica;
    if ( hdr != NULL ) rpl->magic = hdr->magic;
    else rpl->magic = VDQM_MAGIC;

    CLIST_INSERT(ReplicaList,rpl);
    rc = UnlockReplicaList();
    if ( rc == -1 ) {
        save_serrno = serrno;
        log(LOG_ERR,"vdqm_AddReplica() UnlockReplicaList(): %s\n",
            sstrerror(save_serrno));
        vdqm_SetError(save_serrno);
        return(-1);
    }
    log(LOG_DEBUG,"vdqm_AddReplica() replica %s (socket %d) added\n",
        rpl->Replica.host,rpl->Replica.nw.accept_socket);

    return(0);
}

/*
 * Internal replication request pipe operations
 */
static int LockReplicaPipe() {
    return(Cthread_mutex_lock_ext(ReplicationPipe.lock));
}

static int UnlockReplicaPipe() {
    (void)Cthread_cond_broadcast_ext(ReplicationPipe.lock);
    return(Cthread_mutex_unlock_ext(ReplicationPipe.lock));
}

static int PipeRequest(int vol_action, vdqm_volrec_t *vol, 
                       int drv_action, vdqm_drvrec_t *drv) {
    int i,rc, save_serrno;

    log(LOG_DEBUG,"PipeRequest() called\n");
    if ( vol == NULL && drv == NULL ) return(-1);

    if ( ReplicationPipe.nb_piped  >= VDQM_REPLICA_PIPELEN ) {
        log(LOG_ERR,"PipeRequest() Replication failure! pipe is full\n");
        vdqm_SetError(EVQPIPEFULL);
        return(-1);
    }
    i = ReplicationPipe.nb_piped;
    if ( vol != NULL ) {
        ReplicationPipe.pipe[i].volhdr.reqtype = vol_action;
        ReplicationPipe.pipe[i].vol = vol->vol;
    }
    if ( drv != NULL ) {
        ReplicationPipe.pipe[i].drvhdr.reqtype = drv_action;
        ReplicationPipe.pipe[i].drv = drv->drv;
    }
    ReplicationPipe.pipe[i].update = 0;
    if ( vol != NULL ) ReplicationPipe.pipe[i].update = 1; 
    if ( vol != NULL && drv != NULL ) ReplicationPipe.pipe[i].update = 2;
    ReplicationPipe.nb_piped++;

    log(LOG_DEBUG,"PipeRequest() returns with nb_piped=%d\n",
        ReplicationPipe.nb_piped);

    return(0);
}
    

void *vdqm_ReplicationThread(void *arg) {
    int i, rc, failed, save_serrno;
    struct vdqmReplicationPipe l_ReplicationPipe;
    vdqmReplicaList_t *rpl = NULL;

    replication_ON = 1;
    log(LOG_INFO,"vdqm_ReplicationThread() started\n");
    for (;;) {
        rc = LockReplicaPipe();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ReplicationThread() LockReplicaPipe(): %s\n",
                sstrerror(serrno));
            replication_ON = 0;
            return((void *)&failure);
        }
        while ( ReplicationPipe.nb_piped == 0 ) {
            rc = Cthread_cond_wait_ext(ReplicationPipe.lock);
            if ( rc == -1 ) {
                log(LOG_ERR,"vdqm_ReplicationThread() Cthread_cond_wait_ext(): %s\n",
                    sstrerror(serrno));
                replication_ON = 0;
                return((void *)&failure);
            }
            log(LOG_DEBUG,"vdqm_ReplicationThread() woke up with nb_piped=%d\n",
                ReplicationPipe.nb_piped);
        }
        /*
         * Make a local copy of the pipe and release lock so that the 
         * production queue can continue without waiting for us to finish.
         */
        l_ReplicationPipe = ReplicationPipe;
        ReplicationPipe.nb_piped = 0;
        rc = UnlockReplicaPipe();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ReplicationThread() UnlockReplicaPipe(): %s\n",
                sstrerror(serrno));
            replication_ON = 0;
            return((void *)&failure);
        }

        /*
         * Loop over all replication clients and update them one-by-one
         */
        rc = LockReplicaList();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ReplicationThread() Cthread_cond_wait_ext(): %s\n",
                sstrerror(serrno));
            (void)Cthread_mutex_unlock_ext(ReplicationPipe.lock);
            replication_ON = 0;
            return((void *)&failure);
        }

        for (i=0; i<l_ReplicationPipe.nb_piped; i++) {
            failed = 0;
            CLIST_ITERATE_BEGIN(ReplicaList,rpl) {
                if ( l_ReplicationPipe.pipe[i].update == 0 ||
                     l_ReplicationPipe.pipe[i].update == 2 ) {
                    l_ReplicationPipe.pipe[i].drvhdr.magic = rpl->magic;
                    log(LOG_DEBUG,"vdqm_ReplicationThread() send DRV req to %s\n",
                        rpl->Replica.host);
                    rc = vdqm_SendReq(&rpl->Replica.nw,
                                      &l_ReplicationPipe.pipe[i].drvhdr,
                                      NULL,&l_ReplicationPipe.pipe[i].drv);
                    if ( rc == -1 ) {
                        log(LOG_ERR,"vdqm_ReplicationThread() vdqm_SendReq(): %s\n",
                            sstrerror(serrno));
                        rpl->Replica.failed++;
                    }
                }
                if ( l_ReplicationPipe.pipe[i].update == 1 ||
                     l_ReplicationPipe.pipe[i].update == 2 ) {
                    l_ReplicationPipe.pipe[i].volhdr.magic = rpl->magic;
                    log(LOG_DEBUG,"vdqm_ReplicationThread() send VOL req to %s\n",
                        rpl->Replica.host); 
                    rc = vdqm_SendReq(&rpl->Replica.nw,
                                      &l_ReplicationPipe.pipe[i].volhdr,
                                      &l_ReplicationPipe.pipe[i].vol,NULL);
                    if ( rc == -1 ) {
                        log(LOG_ERR,"vdqm_ReplicationThread() vdqm_SendReq():%s\n",
                            sstrerror(serrno));
                        rpl->Replica.failed++;
                    }   
                }
                if ( rpl->Replica.failed > 0 ) failed++;
            } CLIST_ITERATE_END(ReplicaList,rpl);
            /*
             * If some replicas failed their connection probably failed.
             * Close the connection and remove them from the list.
             */
            while ( failed > 0 ) {
                CLIST_ITERATE_BEGIN(ReplicaList,rpl) {
                    if ( rpl->Replica.failed > 0 ) break;
                } CLIST_ITERATE_END(ReplicaList,rpl);
                failed--;
                (void)vdqm_CloseConn(&rpl->Replica.nw);
                (void)DeleteReplica(&rpl->Replica);
            }
        }
        rc = UnlockReplicaList();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ReplicationThread() UnlockReplicaList(): %s\n",
                sstrerror(serrno));
            replication_ON = 0;
            return((void *)&failure);
        }
    } /* for (;;) */
}

void *vdqm_ReplicaListenThread(void *arg) {
    int rc, retry_time;
    int *rc_p = NULL;
    vdqmHdr_t hdr;
    vdqmVolReq_t volreq;
    vdqmDrvReq_t drvreq;
    char primary_host[CA_MAXHOSTNAMELEN+1];
    vdqmnw_t *nw = NULL;

    if ( arg == NULL ) {
        log(LOG_ERR,"vdqm_ReplicaListenThread() called without arguments\n");
        return((void *)&failure);
    }
    log(LOG_INFO,"vdqm_ReplicaListenThread() started\n");
    replication_ON = 1;
    retry_time = 5;
    rc_p = &success;

    strcpy(primary_host,(char *)arg);
    log(LOG_INFO,"vdqm_ReplicaListenThread() using primary host %s\n",
        primary_host);

    while ( retry_replication > 0 ) {
        log(LOG_INFO,"vdqm_ReplicaListenThread() trying to connect to %s\n",
            primary_host);
        rc = vdqm_ConnectToVDQM(&nw,primary_host);
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ReplicaListenThread() cannot connect to primary VDQM\n");
            retry_replication--;
            if ( retry_replication > 0 ) sleep(retry_time);
            else rc_p = &failure;
            continue;
        }
        hdr.magic = VDQM_MAGIC;
        hdr.reqtype = VDQM_REPLICA;
        hdr.len = 0;
        rc = vdqm_SendReq(nw,&hdr,NULL,NULL);
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ReplicaListenThread() vdqm_SendReq(): %s\n",
                sstrerror(serrno));
            (void)vdqm_CloseConn(nw);
            retry_replication--;
            if ( retry_replication > 0 ) sleep(retry_time);
            else rc_p = &failure;
            continue;
        }

        for (;;) { 
            rc = vdqm_Listen(nw);
            if ( rc == -1 ) {
                log(LOG_ERR,"vdqm_ReplicaListenThread() vdqm_Listen(): %s\n",
                    sstrerror(serrno));
                (void)vdqm_CloseConn(nw);
                break;
            }
            rc = vdqm_RecvReq(nw,&hdr,&volreq,&drvreq);
            if ( rc == -1 ) {
                log(LOG_ERR,"vdqm_ReplicaListenThread() vdqm_RecvReq(): %s\n",
                    sstrerror(serrno));
                (void)vdqm_CloseConn(nw);
                break;
            }
            log(LOG_DEBUG,"vdqm_ReplicaListenThread() received 0x%x request\n",
                hdr.reqtype);
            if ( hdr.reqtype == VDQM_VOL_REQ || 
                 hdr.reqtype == VDQM_DEL_VOLREQ ) {
                rc = vdqm_ReplVolReq(&hdr,&volreq);
                if ( rc == -1 ) {
                    log(LOG_ERR,"vdqm_ReplicaListenThread() vdqm_NewReplReq(): %s\n",
                        sstrerror(serrno));
                    (void)vdqm_CloseConn(nw);
                    break;
                }
            }
            if ( hdr.reqtype == VDQM_DRV_REQ || 
                 hdr.reqtype == VDQM_DEL_DRVREQ ) {
                rc = vdqm_ReplDrvReq(&hdr,&drvreq);
                if ( rc == -1 ) {
                    log(LOG_ERR,"vdqm_ReplicaListenThread() vdqm_ReplDrvReq(): %s\n",
                        sstrerror(serrno));
                    (void)vdqm_CloseConn(nw);
                    break;
                }
            }
        } /* for (;;) */
        retry_replication--;
        if ( retry_replication > 0 ) sleep(retry_time);
        log(LOG_INFO,"vdqm_ReplicaListenThread() retry nb %d\n",
            VDQM_REPLICA_RETRIES-retry_replication);
    } /* while ( retry_replication > 0 ) */
    if ( nw != NULL ) free(nw);
    replication_ON = 0;
    hold = 0;
    log(LOG_INFO,"vdqm_ReplicaListenThread() exits because retry limit exhausted\n");
    return((void *)rc_p);
}

int vdqm_StartReplicaThread() {
    int rc, len;
    int *rc_p = NULL;
    extern char *getconfent(char *, char *, int);
    char *p, *q, l_hostname[CA_MAXHOSTNAMELEN+1];
    char *primary_host = NULL;
    char *secondary_host = NULL;
    char *last = NULL;

    /*
     * Determine whether we are a primary server or not.
     */
    len = sizeof(l_hostname);
    rc = gethostname(l_hostname,len);
    if ( rc == SOCKET_ERROR ) {
        log(LOG_ERR,"vdqm_StartReplicaThread() gethostname(): %s\n",
            neterror());
        serrno = SECOMERR;
        return(-1);
    }
    /*
     * Remove domain
     */
    q = strchr(l_hostname,'.');
    if ( q != NULL ) q = '\0';

    if ( (p = getconfent("VDQM","HOST",1)) == NULL ) {
        log(LOG_ERR,"vdqm_StartReplicaThread() No primary host configured\n");
        return(-1);
    }
    q = strtok(p," \t\n");
    primary_host = (char *)calloc(1,CA_MAXHOSTNAMELEN+1);
    if ( q == NULL || primary_host == NULL ) {
        log(LOG_ERR,"vdqm_StartReplicaThread() internal error\n");
        if ( primary_host != NULL ) free(primary_host);
        serrno = SEINTERNAL;
        return(-1);
    }
    strcpy(primary_host,q);

    if ( strcmp(primary_host,l_hostname) != 0 ) {
        log(LOG_INFO,"vdqm_StartReplicaThread() host (%s) is not primary\n",
            l_hostname);
        while ( (q = strtok(NULL," \t\n")) != NULL &&
                strcmp(q,l_hostname) != 0 ); 
        if ( q == NULL ) {
            log(LOG_ERR,"vdqm_StartReplicaThread() host (%s) not replica\n",
                l_hostname);
            return(-1);
        } else {
            /* 
             * This is a secondary replica server. Set hold status.
             */
            log(LOG_INFO,"vdqm_StartReplicaThread() server in replica mode. Hold status set!\n");
            hold = 1;
            retry_replication = VDQM_REPLICA_RETRIES;
            rc = Cthread_create_detached(
                                 (void *(*)(void *))vdqm_ReplicaListenThread,
                                 (void *)primary_host);
            if ( rc == -1 ) {
                log(LOG_ERR,"vdqm_StartReplicaThread() Cthread_create_detached(): %s\n",
                    sstrerror(serrno));
                return(-1);
            }
            return(0);
        }
    } 
    free(primary_host);

    /*
     * Primary server. We are starting up! try to get queues from 
     * replica.
     */
    secondary_host = (char *)calloc(1,CA_MAXHOSTNAMELEN+1);
    while ( (q = strtok(NULL," \t\n")) != NULL )  {
        strcpy(secondary_host,q);
        log(LOG_INFO,"vdqm_StartReplicaThread() trying to get queues from %s\n",
            secondary_host);
        retry_replication = hold = 1;
        rc_p = (int *)vdqm_ReplicaListenThread((void *)secondary_host);
        if ( rc_p == NULL || *rc_p == failure ) continue;
        /*
         * Repeat request to force secondary server to re-enter replication mode
         */
        retry_replication = hold = 1;
        log(LOG_INFO,"vdqm_ReplicaListenThread() force %s to re-enter replica mode\n",
            secondary_host);
        (void)vdqm_ReplicaListenThread(secondary_host);
        break;
    }
    hold = 0;
    retry_replication = VDQM_REPLICA_RETRIES;
    free(secondary_host);

    rc = InitReplicaList();
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_StartReplicaThread() InitReplicaList(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    rc = Cthread_create_detached((void *(*)(void *))vdqm_ReplicationThread,
                                 NULL);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_StartReplicaThread() Cthread_create_detached(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    log(LOG_INFO,"vdqm_StartReplicaThread() Replication thread started\n");
    return(0);
}

int vdqm_UpdateReplica(dgn_element_t *dgn_context) {
    int rc;
    vdqm_volrec_t *vol;
    vdqm_drvrec_t *drv;
    int vol_update, drv_update;

    if ( dgn_context == NULL ) return(-1);
    if ( replication_ON == 0 ) return(0);

    for (;;) {
        vol_update = drv_update = 0;
        CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
            if ( vol->update == 1 ) {
                log(LOG_DEBUG,"vdqm_UpdateReplica() update for VolReqID=%d\n",
                    vol->vol.VolReqID);
                vol_update = 1;
                break;
            }
        } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
        CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
            if ( drv->update == 1 ) {
                log(LOG_DEBUG,"vdqm_UpdateReplica() update for DrvReqID=%d\n",
                    drv->drv.DrvReqID);
                drv_update = 1;
                break;
            }
        } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
        if ( vol_update == 0 && drv_update == 0 ) break;
        if ( vol_update == 0 ) vol = NULL;
        if ( drv_update == 0 ) drv = NULL;
        if ( vol != NULL ) vol->update = 0;
        if ( drv != NULL ) drv->update = 0;
        rc = LockReplicaPipe();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_UpdateReplica() LockReplicaPipe(): %s\n",
                sstrerror(serrno));
            break;
        }
        rc = PipeRequest(VDQM_VOL_REQ,vol,VDQM_DRV_REQ,drv);
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_UpdateReplica() failed to pipe requests\n");
            (void)UnlockReplicaPipe();
            break;
        }
        rc = UnlockReplicaPipe();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_UpdateReplica() UnlockReplicaPipe(): %s\n",
                sstrerror(serrno));
            break;
        }
    }
    return(rc);
}

int vdqm_DeleteFromReplica(vdqmVolReq_t *VolReq, vdqmDrvReq_t *DrvReq) {
    vdqm_volrec_t *volrec, vol;
    vdqm_drvrec_t *drvrec, drv;
    int rc, vol_update, drv_update;

    if ( replication_ON == 0 ) return(0);
    volrec = NULL;
    drvrec = NULL;
    vol_update = drv_update = 0;
    if ( VolReq != NULL ) {
        vol_update = 1;
        vol.vol = *VolReq;
        volrec = &vol;
        log(LOG_DEBUG,"vdqm_DeleteFromReplica() delete VolReqID=%d\n",
            VolReq->VolReqID);
    }
    if ( DrvReq != NULL ) {
        drv_update = 1;
        drv.drv = *DrvReq;
        drvrec = &drv;
        log(LOG_DEBUG,"vdqm_DeleteFromReplica() delete DrvReqID=%d\n",
            DrvReq->DrvReqID);
    }
    if ( vol_update != 0 || drv_update != 0 ) {
        rc = LockReplicaPipe();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_DeleteFromReplica() LockReplicaPipe(): %s\n",
                sstrerror(serrno));
            return(-1);;
        }
        rc = PipeRequest(VDQM_DEL_VOLREQ,volrec,VDQM_DEL_DRVREQ,drvrec);
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_DeleteFromReplica() failed to pipe requests\n");
            (void)UnlockReplicaPipe();
            return(-1);
        }
        rc = UnlockReplicaPipe();
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_DeleteFromReplica() UnlockReplicaPipe(): %s\n",
                sstrerror(serrno));
            return(-1);
        }
    }
    return(0);
}

int vdqm_DumpQueues(vdqmnw_t *nw) {
    int rc;
    dgn_element_t *dgn_context = NULL;
    vdqm_volrec_t *vol = NULL;
    vdqm_drvrec_t *drv = NULL;

    log(LOG_DEBUG,"vdqm_DumpQueues() called\n");
    if ( nw == NULL ) {
        serrno = EINVAL;
        return(-1); 
    }
    CLIST_ITERATE_BEGIN(dgn_queues,dgn_context) {
        CLIST_ITERATE_BEGIN(dgn_context->vol_queue,vol) {
            log(LOG_DEBUG,"vdqm_DumpQueues() send Volume record, VolReqID %d\n",
                vol->vol.VolReqID);
            rc = vdqm_SendReq(nw,NULL,&vol->vol,NULL);
            if ( rc == -1 ) {
                log(LOG_ERR,"vdqm_DumpQueues() vdqm_SendReq(vol): %s\n",
                    sstrerror(serrno));
                return(-1);
            }
        } CLIST_ITERATE_END(dgn_context->vol_queue,vol);
        CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
            log(LOG_DEBUG,"vdqm_DumpQueues() send Drive record, DrvReqID %d\n",
                drv->drv.DrvReqID);
            rc = vdqm_SendReq(nw,NULL,NULL,&drv->drv);
            if ( rc == -1 ) {
                log(LOG_ERR,"vdqm_DumpQueues() vdqm_SendReq(drv): %s\n",
                    sstrerror(serrno));
                return(-1);
            }
        } CLIST_ITERATE_END(dgn_context->drv_queue,drv);
    } CLIST_ITERATE_END(dgn_queues,dgn_context);
    log(LOG_DEBUG,"vdqm_DumpQueues() finished\n");
    return(0);
}
