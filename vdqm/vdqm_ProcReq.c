/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: vdqm_ProcReq.c,v $ $Revision: 1.6 $ $Date: 1999/12/08 10:49:21 $ CERN IT-PDP/DM Olof Barring";
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
static int hold = 0;

void *vdqm_ProcReq(void *arg) {
    vdqmVolReq_t volumeRequest;
    vdqmDrvReq_t driveRequest;
    vdqmHdr_t hdr;
    vdqmnw_t *client_connection;
    char dgn[CA_MAXDGNLEN+1];
    void *vol_place_holder,*drv_place_holder,*dgn_place_holder;
    int reqtype,rc,i;
    char req_strings[][20] = VDQM_REQ_STRINGS;
    char *req_string;
    int req_values[] = VDQM_REQ_VALUES;
    extern int vdqm_shutdown;
    
    client_connection = (vdqmnw_t *)arg;
    if ( client_connection == NULL ) return((void *)&return_status);
    
    memset(&volumeRequest,'\0',sizeof(volumeRequest));
    memset(&driveRequest,'\0',sizeof(driveRequest));
    reqtype = vdqm_RecvReq(client_connection,&hdr,&volumeRequest,
        &driveRequest);
    i=0;
    while (req_values[i] != -1 && req_values[i] != reqtype) i++;
    req_string = req_strings[i];
    log(LOG_INFO,"vdqm_ProcReq(): new %s request\n",req_string);
    if ( !VDQM_VALID_REQTYPE(reqtype) ) {
        log(LOG_ERR,"vdqm_ProcReq(): invalid request 0x%x\n",
            reqtype);
    } else {
        rc = 0;
        Cthread_mutex_lock(&hold);
        if ( reqtype == VDQM_SHUTDOWN ) {
            log(LOG_INFO,"vdqm_ProcReq(): shutdown server requested\n");
            hold = vdqm_shutdown = 1;
            rc = 1;
        }
        if ( reqtype == VDQM_HOLD ) {
            if ( !hold ) log(LOG_INFO,"vdqm_ProcReq(): server RELEASE\n");
            hold = 1;
            rc = 1;
        }
        if ( reqtype == VDQM_RELEASE ) {
            if ( hold ) log(LOG_INFO,"vdqm_ProcReq(): server RELEASE\n");
            hold = 0;
            rc = 1;
        }
        if ( hold ) {
            log(LOG_INFO,"vdqm_ProcReq(): server in HOLD\n");
            rc = -1;
        }
        Cthread_mutex_unlock(&hold);

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
                break;
            case VDQM_GET_VOLQUEUE:
                *dgn = '\0';
                vol_place_holder = dgn_place_holder = NULL;
                if ( *volumeRequest.dgn != '\0' ) strcpy(dgn,volumeRequest.dgn);
                do {
                    rc = vdqm_GetVolQueue(dgn,&volumeRequest,
                        &dgn_place_holder,&vol_place_holder);
                    if ( rc != -1 )
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
                drv_place_holder = dgn_place_holder = NULL;
                if ( *driveRequest.dgn != '\0' ) strcpy(dgn,driveRequest.dgn);
                do {
                    rc = vdqm_GetDrvQueue(dgn,&driveRequest,
                        &dgn_place_holder,&drv_place_holder);
                    if ( rc != -1 )
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
                break;
            default:
                log(LOG_ERR,"vdqm_ProcReq(): no action for request %s\n",
                    req_string);
                break;
            }
        }
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ProcReq(): request processing error\n");
            (void)vdqm_AcknRollback(client_connection);
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
    log(LOG_INFO,"vdqm_ProcReq(): end of %s request\n",req_string);
    vdqm_ReturnPool(client_connection);
    return((void *)&return_status);
}

void *vdqm_OnRollbackThread(void *arg) {
    int rc;
    log(LOG_INFO,"vdqm_OnRollbackThread() started\n");
    rc = vdqm_OnRollback();
    log(LOG_ERR,"vdqm_OnRollbackThread() unexpected return from vdqm_OnRollback(), rc = %d\n",rc);
    return;
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

