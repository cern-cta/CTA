/*
 * $Id: vdqm_ProcReq.c,v 1.2 1999/07/31 14:50:18 obarring Exp $
 * $Log: vdqm_ProcReq.c,v $
 * Revision 1.2  1999/07/31 14:50:18  obarring
 * Add vdqm_SetError(), vdqm_GetError()
 *
 * Revision 1.1  1999/07/27 09:21:04  obarring
 * First version
 *
 */

/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * vdqm_ProcReq.c - Receive and process VDQM client request (server only).
 */

#ifndef lint
static char cvsId[] = "$Id";
#endif /* not lint */

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
    int reqtype,rc,thID;
    extern int vdqm_shutdown;
    
    client_connection = (vdqmnw_t *)arg;
    if ( client_connection == NULL ) return((void *)&return_status);
    
    thID = Cthread_self();
    
    
    memset(&volumeRequest,'\0',sizeof(volumeRequest));
    memset(&driveRequest,'\0',sizeof(driveRequest));
    reqtype = vdqm_RecvReq(client_connection,&hdr,&volumeRequest,
        &driveRequest);
    log(LOG_INFO,"vdqm_ProcReq(): thread %d: new 0x%x request, connection=0x%x\n",
        thID,reqtype,client_connection);
    if ( !VDQM_VALID_REQTYPE(reqtype) ) {
        log(LOG_ERR,"vdqm_ProcReq(): thread %d: invalid request 0x%x\n",
            thID,reqtype);
    } else {
        rc = 0;
        Cthread_mutex_lock(&hold);
        if ( reqtype == VDQM_SHUTDOWN ) {
            log(LOG_INFO,"vdqm_ProcReq(): shutdown server requested\n");
            hold = vdqm_shutdown = 1;
            rc = 1;
        }
        if ( reqtype == VDQM_HOLD ) {
            if ( !hold ) log(LOG_INFO,"vdqm_ProcReq(): thread %d: server RELEASE\n",
                thID);
            hold = 1;
            rc = 1;
        }
        if ( reqtype == VDQM_RELEASE ) {
            if ( hold ) log(LOG_INFO,"vdqm_ProcReq(): thread %d: server RELEASE\n",
                thID);
            hold = 0;
            rc = 1;
        }
        if ( hold ) {
            log(LOG_INFO,"vdqm_ProcReq(): thread %d: server in HOLD\n",
                thID);
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
                log(LOG_ERR,"vdqm_ProcReq(): thread %d: no action for request 0x%x\n",
                    thID,reqtype);
                break;
            }
        }
        if ( rc == -1 ) {
            log(LOG_ERR,"vdqm_ProcReq(): thread %d: request processing error\n",
                thID);
            (void)vdqm_AcknRollback(client_connection);
        } else {
            rc = vdqm_AcknCommit(client_connection);
            if ( rc != -1 ) {
                rc = vdqm_SendReq(client_connection,
                    &hdr,&volumeRequest,&driveRequest);
                if ( rc != -1 ) rc = vdqm_RecvAckn(client_connection);
            }
            if ( rc == -1 )
                log(LOG_ERR,"vdqm_ProcReq(): thread %d: client error on commit\n",
                thID);
        }
    }
    (void) vdqm_CloseConn(client_connection);
    log(LOG_INFO,"vdqm_ProcReq(): thread %d: end of 0x%x request\n",
        thID,reqtype);
    vdqm_ReturnPool(client_connection);
    return((void *)&return_status);
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

