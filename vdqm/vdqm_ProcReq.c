/*
 * Copyright (C) 1999-2002 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_ProcReq.c,v $ $Revision: 1.19 $ $Date: 2003/02/18 14:01:18 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * vdqm_ProcReq.c - Receive and process VDQM client request (server only).
 */

#include <stdlib.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */
#include <osdep.h>
#include <log.h>
#include <net.h>
#include <serrno.h>
#include <Castor_limits.h>
#include <Cthread_api.h>
#include <vdqm_constants.h>
#include <vdqm.h>

const int return_status = 0;
static int error_key;
int hold = 0;
void *hold_lock = NULL;
int nbReqs = 0;
void *nbReqs_lock = NULL;

extern int vdqm_PipeHangup();


int vdqm_InitProcReq() {

    if ( Cthread_mutex_lock(&hold)==-1 ) {
        log(LOG_ERR,"vdqm_InitProcReq() Cthread_mutex_lock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    if ( Cthread_mutex_unlock(&hold)==-1 ) {
        log(LOG_ERR,"vdqm_InitProcReq() Cthread_mutex_unlock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    if ( (hold_lock=Cthread_mutex_lock_addr(&hold))==NULL ) {
        log(LOG_ERR,"vdqm_InitProcReq() Cthread_mutex_lock_addr(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    if ( Cthread_mutex_lock(&nbReqs)==-1 ) {
        log(LOG_ERR,"vdqm_InitProcReq() Cthread_mutex_lock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    if ( Cthread_mutex_unlock(&nbReqs)==-1 ) {
        log(LOG_ERR,"vdqm_InitProcReq() Cthread_mutex_unlock(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    if ( (nbReqs_lock=Cthread_mutex_lock_addr(&nbReqs))==NULL ) {
        log(LOG_ERR,"vdqm_InitProcReq() Cthread_mutex_lock_addr(): %s\n",
            sstrerror(serrno));
        return(-1);
    }

    return(0);
}

int vdqm_ReqStarted() {
    if ( Cthread_mutex_lock_ext(nbReqs_lock)==-1 ) {
        log(LOG_ERR,"vdqm_ReqStarted() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    nbReqs++;
    if ( Cthread_mutex_unlock_ext(nbReqs_lock)==-1 ) {
        log(LOG_ERR,"vdqm_ReqStarted() Cthread_mutex_unlock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    return(0);
}

int vdqm_ReqEnded() {
    int rc = 0;
    if ( Cthread_mutex_lock_ext(nbReqs_lock)==-1 ) {
        log(LOG_ERR,"vdqm_ReqEnded() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    nbReqs--;
    if ( Cthread_cond_broadcast_ext(nbReqs_lock)==-1 ) {
        log(LOG_ERR,"vdqm_ReqEnded() Cthread_cond_broadcast_ext(): %s\n",
            sstrerror(serrno));
        rc = -1;
    }
    if ( Cthread_mutex_unlock_ext(nbReqs_lock)==-1 ) {
        log(LOG_ERR,"vdqm_ReqEnded() Cthread_mutex_unlock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    return(rc);
}

int vdqm_WaitForReqs(int howmany) {
    int rc = 0;
    if ( Cthread_mutex_lock_ext(nbReqs_lock)==-1 ) {
        log(LOG_ERR,"vdqm_WaitForReqs() Cthread_mutex_lock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    while ( rc == 0 && nbReqs > howmany ) {
        if ( (rc = Cthread_cond_wait_ext(nbReqs_lock)) == -1 ) {
            log(LOG_ERR,"vdqm_WaitForReqs() Cthread_cond_wait_ext(): %s\n",
                sstrerror(serrno));
        }
    }
    if ( Cthread_mutex_unlock_ext(nbReqs_lock)==-1 ) {
        log(LOG_ERR,"vdqm_WaitForReqs() Cthread_mutex_unlock_ext(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    return(rc);
}

void *vdqm_ProcReq(void *arg) {
    vdqmVolReq_t volumeRequest;
    vdqmDrvReq_t driveRequest;
    vdqmHdr_t hdr;
    vdqmnw_t *client_connection;
    char dgn[CA_MAXDGNLEN+1];
    char server[CA_MAXHOSTNAMELEN+1];
    void *vol_place_holder,*drv_place_holder,*dgn_place_holder;
    int reqtype, rc, i, save_serrno;
    char req_strings[][20] = VDQM_REQ_STRINGS;
    char *req_string;
    int req_values[] = VDQM_REQ_VALUES;
    extern int vdqm_shutdown;
    int queues_locked = 0;
    
    log(LOG_DEBUG,"vdqm_ProcReq() called with arg=0x%lx\n",arg);
    client_connection = (vdqmnw_t *)arg;
    if ( client_connection == NULL ) return((void *)&return_status);
    
    memset(&volumeRequest,'\0',sizeof(volumeRequest));
    memset(&driveRequest,'\0',sizeof(driveRequest));
    reqtype = vdqm_RecvReq(client_connection,&hdr,&volumeRequest,
        &driveRequest);
    i=0;
    while (req_values[i] != -1 && req_values[i] != reqtype) i++;
    req_string = req_strings[i];
	if ((reqtype != VDQM_GET_VOLQUEUE) &&
		(reqtype != VDQM_GET_DRVQUEUE) &&
		(reqtype != VDQM_PING)) {
		log(LOG_INFO,"vdqm_ProcReq(): new %s request\n",req_string);
	}
    if ( !VDQM_VALID_REQTYPE(reqtype) ) {
        log(LOG_ERR,"vdqm_ProcReq(): invalid request 0x%x\n",
            reqtype);
    } else {
        rc = 0;
        if ( Cthread_mutex_lock_ext(hold_lock)==-1 ) {
            log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_lock_ext(): %s\n",
                sstrerror(serrno));
            return((void *)&return_status);
        }
        if ( reqtype == VDQM_SHUTDOWN ) {
            log(LOG_INFO,"vdqm_ProcReq(): shutdown server requested\n");
            hold = vdqm_shutdown = 1;
            vdqm_PipeHangup();
            rc = 1;
        }
        if ( reqtype == VDQM_HOLD ) {
            if ( !hold ) log(LOG_INFO,"vdqm_ProcReq(): set server HOLD status\n");
            hold = 1;
            rc = 1;
        }
        if ( reqtype == VDQM_RELEASE ) {
            if ( hold ) log(LOG_INFO,"vdqm_ProcReq(): set server RELEASE status\n");
            hold = 0;
            rc = 1;
        }
        if ( hold && reqtype != VDQM_REPLICA ) {
            log(LOG_INFO,"vdqm_ProcReq(): server in HOLD status\n");
            vdqm_SetError(EVQHOLD);
            rc = -1;
        }
        if ( Cthread_mutex_unlock_ext(hold_lock)==-1 ) {
            log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_unlock_ext(): %s\n",
                sstrerror(serrno));
            return((void *)&return_status);
        }

        vdqm_ReqStarted();
#if DEBUG
		{
			extern dgn_element_t *dgn_queues;
			dgn_element_t *dgn_context = NULL;
			vdqm_drvrec_t *drv = NULL;
			CLIST_ITERATE_BEGIN(dgn_queues,dgn_context) {
				CLIST_ITERATE_BEGIN(dgn_context->drv_queue,drv) {
					log(LOG_INFO,"vdqm_DumpQueues() [Debug] %s@%s : is_uid=%d, uid=%d, is_gid=%d, gid=%d, is_name=%d\n", drv->drv.drive, drv->drv.server, drv->drv.is_uid, drv->drv.uid, drv->drv.is_gid, drv->drv.gid, drv->drv.is_name);
				} CLIST_ITERATE_END(dgn_context->drv_queue,drv);
			} CLIST_ITERATE_END(dgn_queues,dgn_context);
		}
#endif
        if ( !rc ) {
            switch (reqtype) {
            case VDQM_VOL_REQ:
                rc = vdqm_NewVolReq(&hdr,&volumeRequest);
                break;
            case VDQM_DRV_REQ:
                rc = vdqm_NewDrvReq(&hdr,&driveRequest);
                break;
            case VDQM_DEL_VOLREQ:
                rc = vdqm_DelVolReq(&volumeRequest);
                break;
            case VDQM_DEL_DRVREQ:
                rc = vdqm_DelDrvReq(&driveRequest);
                /* Dumping the list of drives to file */
                if (rc == 0) {
                    log(LOG_DEBUG, "vdqm_ProcReq - NOW SAVING THE REDUCED LIST OF DRIVES TO FILE\n");
                    if (vdqm_save_queue() < 0) {
                        log(LOG_ERR, "Could not save drive list\n");
                    }
                }
                break;
            case VDQM_DEDICATE_DRV:
                rc = vdqm_DedicateDrv(&driveRequest);
                /* Dumping the list of drives to file */
                if (rc == 0) {
                    log(LOG_DEBUG, "vdqm_ProcReq - NOW SAVING THE LIST OF DRIVES WITH NEW DEDICATION TO FILE\n");
                    if (vdqm_save_queue() < 0) {
                        log(LOG_ERR, "Could not save drive list\n");
                    }
                }
                break;
            case VDQM_REPLICA:
                /*
                 * Make sure new requests will wait
                 */
                if ( Cthread_mutex_lock_ext(hold_lock)==-1 ) {
                    log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_lock_ext(): %s\n",
                        sstrerror(serrno));
                    return((void *)&return_status);
                }
                rc = vdqm_CheckReplicaHost(client_connection);
                if ( rc == 0 ) {
                    /*
                     * Wait for all threads except ourselves before
                     * locking the queues. Otherwise we risk a deadlock.
                     */
                    if ( !hold) (void)vdqm_WaitForReqs(1);
                    rc = vdqm_LockAllQueues();
                    if ( rc == 0 ) queues_locked = 1;
                }
                if ( rc == 0 && !hold ) rc = vdqm_DumpQueues(client_connection);

                if ( rc == 0 )
                    rc = vdqm_AddReplica(client_connection,&hdr);
                save_serrno = serrno;
                if ( rc == 0 ) 
                    log(LOG_INFO,"vdqm_ProcReq(): replica client added\n");
                else {
                    if ( rc == -1 ) {
                        log(LOG_ERR,"vdqm_ProcReq(): failed to add replica\n");
                    } else if ( rc == 1 ) {
                        log(LOG_INFO,"vdqm_ProcReq() re-enter replication mode\n");
                        hold = 1;
                        /*
                         * Inform main server that we are back in
                         * replication mode
                         */
                        (void)vdqm_Hangup(client_connection);
                        (void)vdqm_RecvAckn(client_connection);
                        log(LOG_INFO,"vdqm_ProcReq() replication mode entered\n");
                        rc = vdqm_StartReplicaThread();
                        if ( rc == -1 ) {
                            log(LOG_ERR,"vdqm_ProcReq() vdqm_StartReplicaThread(): %s\n",sstrerror(serrno));
                            /*
                             * If a secondary replica server can't start
                             * its replication thread, there is not much else
                             * for it to do than exit.
                             */
                            log(LOG_ERR,"**** VDQM replica failed to start ****\n");
                            exit(1);
                        }
                    }
                    (void)vdqm_CloseConn(client_connection);
                }
                if ( queues_locked == 1 ) {
                    queues_locked = 0;
                    (void)vdqm_UnlockAllQueues();
                }
                if ( Cthread_mutex_unlock_ext(hold_lock)==-1 ) {
                    log(LOG_ERR,"vdqm_ProcReq() Cthread_mutex_unlock_ext(): %s\n",
                        sstrerror(serrno));
                    return((void *)&return_status);
                }
                vdqm_ReqEnded();
                (void)vdqm_ReturnPool(client_connection);
                return((void *)&return_status);
                break;
            case VDQM_GET_VOLQUEUE:
                *dgn = '\0';
                *server = '\0';
                vol_place_holder = dgn_place_holder = NULL;
                if ( *volumeRequest.dgn != '\0' ) strcpy(dgn,volumeRequest.dgn);
                if ( *volumeRequest.server != '\0' ) strcpy(server,volumeRequest.server);
                do {
                    rc = vdqm_GetVolQueue(dgn,&volumeRequest,
                        &dgn_place_holder,&vol_place_holder);
                    if ( rc != -1 && ((*server == '\0') || 
                        ((*server != '\0') && 
                         (strcmp(server,volumeRequest.server) == 0))) )
                        rc = vdqm_SendReq(client_connection,&hdr,
                            &volumeRequest,&driveRequest);
                } while ( rc != -1 );
                vdqm_GetVolQueue(dgn,NULL,
                    &dgn_place_holder,&vol_place_holder);
                volumeRequest.VolReqID = -1;
                rc = vdqm_SendReq(client_connection,&hdr,
                    &volumeRequest,&driveRequest);
                break;
            case VDQM_GET_DRVQUEUE:
                *dgn = '\0';
                *server = '\0';
                drv_place_holder = dgn_place_holder = NULL;
                if ( *driveRequest.dgn != '\0' ) strcpy(dgn,driveRequest.dgn);
                if ( *driveRequest.server != '\0' ) strcpy(server,driveRequest.server);
                do {
                    rc = vdqm_GetDrvQueue(dgn,&driveRequest,
                        &dgn_place_holder,&drv_place_holder);
                    if ( rc != -1 && ((*server == '\0') ||
                        ((*server != '\0') &&
                         (strcmp(server,driveRequest.server) == 0))) ) 
                        rc = vdqm_SendReq(client_connection,&hdr,
                            &volumeRequest,&driveRequest);
                } while ( rc != -1 );
                vdqm_GetDrvQueue(dgn,NULL,
                    &dgn_place_holder,&drv_place_holder);
                driveRequest.DrvReqID = -1;
                rc = vdqm_SendReq(client_connection,&hdr,
                    &volumeRequest,&driveRequest);
                break;
            case VDQM_PING:
                rc = vdqm_GetQueuePos(&volumeRequest);
                (void) vdqm_AcknPing(client_connection,rc);
                (void) vdqm_CloseConn(client_connection);
                /* log(LOG_INFO,"vdqm_ProcReq(): end of %s request\n",req_string); */
                vdqm_ReqEnded();
                vdqm_ReturnPool(client_connection);
                return((void *)&return_status);
                break;
            case VDQM_HANGUP:
                (void) vdqm_RecvAckn(client_connection);
                log(LOG_INFO,"vdqm_ProcReq(): hangup request acknowledged\n");
                (void)vdqm_CloseConn(client_connection);
                vdqm_ReqEnded();
                vdqm_ReturnPool(client_connection);
                return((void *)&return_status);
                break;
            default:
                log(LOG_ERR,"vdqm_ProcReq(): no action for request %s\n",
                    req_string);
                break;
            }
        }
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ProcReq(): request processing error\n");
            if ( reqtype == VDQM_PING ) 
                (void) vdqm_AcknPing(client_connection,rc);
            else (void)vdqm_AcknRollback(client_connection);
            rc = 0;
        } else {
            rc = vdqm_AcknCommit(client_connection);
            if ( rc != -1 ) {
                rc = vdqm_SendReq(client_connection,
                    &hdr,&volumeRequest,&driveRequest);
                if ( rc != -1 ) rc = vdqm_RecvAckn(client_connection);
            }
            if ( rc == -1 )
                log(LOG_ERR,"vdqm_ProcReq(): client error on commit\n");
        }
        /*
         * Commit failed somewhere. If it was a volume request we must
         * remove it since client is either dead or unreachable.
         */
        if ( rc == -1 ) {
           switch (reqtype) {
           case VDQM_VOL_REQ:
                log(LOG_INFO,"vdqm_ProcReq() remove volume request from queue\n");
                (void)vdqm_DelVolReq(&volumeRequest); 
                break;
           case VDQM_DRV_REQ:
                log(LOG_INFO,"vdqm_ProcReq() rollback drive request\n");
                (void)vdqm_QueueOpRollback(NULL,&driveRequest);
                break;
           default:
                break;
           }
        }
    }
    (void) vdqm_CloseConn(client_connection);
	if ((reqtype != VDQM_GET_VOLQUEUE) &&
		(reqtype != VDQM_GET_DRVQUEUE) &&
		(reqtype != VDQM_PING)) {
		log(LOG_INFO,"vdqm_ProcReq(): end of %s request\n",req_string);
	}
    vdqm_ReqEnded();
    vdqm_ReturnPool(client_connection);
    return((void *)&return_status);
}

void *vdqm_OnRollbackThread(void *arg) {
    int rc;
    log(LOG_INFO,"vdqm_OnRollbackThread() started\n");
    rc = vdqm_OnRollback();
    log(LOG_ERR,"vdqm_OnRollbackThread() unexpected return from vdqm_OnRollback(), rc = %d\n",rc);
    return((void *)&return_status);
}
int vdqm_StartRollbackThread() {
    int rc;
    rc = Cthread_create_detached(vdqm_OnRollbackThread,NULL);
    if ( rc == -1 ) {
        log(LOG_ERR,"vdqm_StartRollbackThread() Cthread_create_detached(): %s\n",
            sstrerror(serrno));
        return(-1);
    }
    return(0);
}
static int vdqm_Error(int eval) {
    int *ts_eval;
    int rc;

    rc = Cthread_getspecific(&error_key,(void **)&ts_eval);
    if ( rc == -1 || ts_eval == NULL ) {
        ts_eval = (int *)calloc(1,sizeof(int));
        rc = Cthread_setspecific(&error_key,(void *)ts_eval);
    }
    if ( rc == -1 || ts_eval == NULL ) return(-1);
    rc = *ts_eval;
    if ( eval >= 0 ) *ts_eval = eval;
    return(rc);
}

int vdqm_GetError() {
    int rc;
    rc = vdqm_Error(-1);
    if ( rc == -1 ) return(SEINTERNAL);
    else return(rc);
}

int vdqm_SetError(int eval) {
    return(vdqm_Error(eval));
}

