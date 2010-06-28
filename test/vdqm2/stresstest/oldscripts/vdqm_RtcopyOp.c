#include <stdlib.h>
#include <time.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <Castor_limits.h>
#include <net.h>
#include <log.h>
#include <Cthread_api.h>
#include <vdqm_constants.h>
#include <vdqm.h>

#if !defined(linux)
extern char *sys_errlist[];
#else /* linux */
#include <stdio.h>   /* Contains definition of sys_errlist[] */
#endif /* linux */

#define RTCOPYPORT 8889
/*
 * Thread return constants
 */
const int success = 0;
const int failure = -1;

typedef struct rtcpd_arg {
    vdqmVolReq_t *VolReq;
    vdqmDrvReq_t *DrvReq;
} RtcopyArg_t;

int vdqm_GetRTCPPort() {
    struct servent *sp;
    static int rtcp_port = -1;
    int port;
    char *p;

    Cthread_mutex_lock(&rtcp_port);
    if ( rtcp_port == -1 ) {
        if ( (p = getenv("RTCP_PORT")) != (char *)NULL ||
             (p = getenv("RTCOPYPORT")) != (char *)NULL ) {
            rtcp_port = atoi(p);
        } else if ( (sp = getservbyname("rtcopy","tcp")) != 
            (struct servent *)NULL ) {
            rtcp_port = (int)ntohs(sp->s_port);
        } else {
#if defined(RTCOPYPORT)
            rtcp_port = RTCOPYPORT;
#endif /* RTCOPYPORT */
        }
    }
    port = rtcp_port;
    Cthread_mutex_unlock(&rtcp_port);
    return(port);
}

void *vdqm_SendJobToRTCP(void *arg) {
    RtcopyArg_t *req;
    int thID, rc;
    SOCKET s;

    thID = Cthread_self();
    log(LOG_INFO,"vdqm_SendJobToRTCP() started, thread id=%d\n",thID);

    req = (struct rtcpd_arg *)arg;
    if ( req == NULL ) return((void *)&failure);

    rc = vdqm_ConnectToRTCP(&s,req->DrvReq->server);
    if ( rc == -1 ) {
        log(LOG_ERR,"(ID %d)vdqm_SendJobToRTPCP() vdqm_ConnectToRTCP(%s)\n",
            thID,req->DrvReq->server);
    } else {
        rc = vdqm_SendToRTCP(s,req->VolReq,req->DrvReq);
        shutdown(s,SD_BOTH);
        closesocket(s);
    }
    if ( rc != -1 ) {
        log(LOG_INFO,"(ID %d)vdqm_SendJobToRTCP() VolReqID %d started as JobID %d on %s@%s\n",
            thID,req->VolReq->VolReqID,req->DrvReq->jobID,
            req->DrvReq->drive,req->DrvReq->server);
    } else {
        log(LOG_ERR,"(ID %d)vdqm_SendJobToRTCP() VolReqID %d could not start on %s@%s\n",
            thID,req->VolReq->VolReqID,
            req->DrvReq->drive,req->DrvReq->server);
    }
    if ( rc == -1 ) vdqm_QueueOpRollback(req->VolReq,req->DrvReq);
    free(req->VolReq);
    free(req->DrvReq);
    free(req);
    if ( rc == -1 ) return((void *)&failure);
    else return((void *)&success);
}

int vdqm_StartJob(vdqm_volrec_t *vol) {
    int rc, thID;
    RtcopyArg_t *arg;

    thID = Cthread_self();
    log(LOG_INFO,"(ID %d)Start job id %d, volid %s on %s@%s\n",thID,
        vol->vol.VolReqID,vol->vol.volid,vol->drv->drv.drive,vol->drv->drv.server);
    vol->drv->drv.status = vol->drv->drv.status & ~VDQM_UNIT_FREE;
    vol->drv->drv.status |= VDQM_UNIT_BUSY;
    /*
     * Make a copy of Volume and Drive request structures and
     * kick off a thread to do the job. This is to avoid that
     * we hang in the connection with RTCOPY and hold the DGN
     * context for ever. It is not completely threadsafe because
     * a new request (once we released the DGN context) might decide
     * to e.g. put the drive down or the volume request client kills 
     * his request. However, at this point the request is commited to
     * RTCOPY who will either discover the drive is down or the client
     * has disappeared. RTCOPY should be able to handle that.
     */
    if ( (arg = (RtcopyArg_t *)malloc(sizeof(RtcopyArg_t))) == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_StartJob() malloc(RTCP arg): %s\n",thID,
            ERRTXT);
        return(-1);
    }
    if ( (arg->VolReq = (vdqmVolReq_t *)malloc(sizeof(vdqmVolReq_t))) == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_StartJob() malloc(VolReq): %s\n",thID,
            ERRTXT);
        free(arg);
        return(-1);
    }
    if ( (arg->DrvReq = (vdqmDrvReq_t *)malloc(sizeof(vdqmDrvReq_t))) == NULL ) {
        log(LOG_ERR,"(ID %d)vdqm_StartJob() malloc(DrvReq): %s\n",thID,
            ERRTXT);
        free(arg->VolReq);
        free(arg);
        return(-1);
    }
    *(arg->VolReq) = (vol->vol);
    *(arg->DrvReq) = (vol->drv->drv);
    /* 
     * The arg pointer will be freed in vdqm_SendToRTCP()
     */
    rc = Cthread_create_detached(vdqm_SendJobToRTCP,(void *)arg);
    if ( rc == -1 ) {
        log(LOG_ERR,"(ID %d)vdqm_StartJob() Cthread_Create_detached() returned error\n",thID);
        free(arg->VolReq);
        free(arg->DrvReq);
        free(arg);
    }
    return(rc);
}
