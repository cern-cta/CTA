/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcp_ClientListen.c - Keep connection with client (to receive ABORT)
 */

#include <stdlib.h>
#include <time.h>
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#include <errno.h>
#include <signal.h>

#include <Castor_limits.h>
#include <osdep.h>
#include <net.h>
#include <serrno.h>
#include <log.h>
#include <Cthread_api.h>
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_api.h>
#include <rtcp_server.h>
#include "tplogger_api.h"

extern int success;
extern int failure;
extern int AbortFlag;
extern int SHIFTclient;
extern int Dumptape;
static int wait_to_be_joined = FALSE;
extern int rtcp_InitLog(char *, FILE *, FILE *, SOCKET *);

void *rtcpd_CLThread(void *arg) {
    static SOCKET client_socket;
    rtcpHdr_t hdr;
    int stop_request;
    int rc, nb_pings;

    if ( arg == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_CLThread() NULL argument\n");
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 2, 
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                         "Message", TL_MSG_PARAM_STR, "NULL argument" ); 
        return((void *)&failure);
    }
    client_socket = *(SOCKET *)arg;
    free(arg);

    nb_pings = stop_request = 0;
    while ( stop_request == 0 ) {
        memset(&hdr,'\0',sizeof(hdr));
        rc = rtcp_Listen(client_socket,NULL,-1,RTCP_ACCEPT_FROM_CLIENT);
        if ( rc == -1 ) {
            rtcp_log(LOG_ERR,"rtcpd_CLThread() rtcpd_Listen(): %s\n",
                sstrerror(serrno));
            tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                             "func"   , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                             "Message", TL_MSG_PARAM_STR, "rtcpd_Listen",
                             "Error",   TL_MSG_PARAM_STR, sstrerror(serrno) ); 
            stop_request = 1;
        } else {
            rc = rtcp_RecvReq(&client_socket,&hdr,NULL,NULL,NULL);
            if ( rc == -1 ) {
                rtcp_log(LOG_ERR,"rtcp_CLThread() rtcp_RecvReq(): %s\n",
                    sstrerror(serrno));
                tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                                 "func"   , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                 "Message", TL_MSG_PARAM_STR, "rtcp_RecvReq",
                                 "Error",   TL_MSG_PARAM_STR, sstrerror(serrno) ); 
                stop_request = 1;
            } else {
                if ( hdr.magic == RTCOPY_MAGIC_SHIFT ) {
                    switch (hdr.reqtype) {
                    case RQST_PING:
                        rc = rtcp_SendOldCAckn(&client_socket, &hdr);
                        break;
                    case RQST_ABORT:
                        stop_request = 1;
                        rc = rtcp_SendOldCAckn(&client_socket, &hdr);
                        break;
                    default:
                        /*
                         * Unexpected request type. 
                         */
                        rtcp_log(LOG_ERR,"rtcp_CLThread() unexpected SHIFT request 0x%x\n",
                                 hdr.reqtype);
                        {
                                char __reqtype[32];
                                sprintf( __reqtype, "0x%x", hdr.reqtype );
                                tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                                                 "func"    , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                                 "Message" , TL_MSG_PARAM_STR, "unexpected SHIFT request",
                                                 "Req Type", TL_MSG_PARAM_STR, __reqtype ); 
                        }
                        stop_request = 1;
                        break;
                    }
                } else {
                    switch (hdr.reqtype) {
                    case RTCP_ABORT_REQ:
                        if ( (rtcpd_CheckProcError() & 
                              (RTCP_FAILED | RTCP_RESELECT_SERV)) == 0 ) {
                            rtcp_InitLog(NULL,NULL,NULL,NULL);
                            rtcp_log(LOG_INFO,"request aborted by user\n");
                            tl_rtcpd.tl_log( &tl_rtcpd, 10, 2, 
                                             "func"    , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                             "Message" , TL_MSG_PARAM_STR, "request aborted by user" );
                        }
                        stop_request = 1;
                        break;
                    case RTCP_ENDOF_REQ:
                        rtcp_log(LOG_INFO,"rtcp_CLThread() End Of Request received on main socket\n");
                        tl_rtcpd.tl_log( &tl_rtcpd, TL_LVL_MONITORING, 2, 
                                         "func"    , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                         "Message" , TL_MSG_PARAM_STR, "End Of Request received on main socket" );
                        stop_request = 1;
                        (void)rtcp_SendAckn(&client_socket,hdr.reqtype);
                        break;
                    case RTCP_KILLJID_REQ:
                        rtcp_log(LOG_INFO,"request killed by operator\n");
                        tl_rtcpd.tl_log( &tl_rtcpd, 10, 2, 
                                         "func"    , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                         "Message" , TL_MSG_PARAM_STR, "request killed by operator" );
                        stop_request = 1;
                        break;
                    case RTCP_RSLCT_REQ:
                        rtcp_log(LOG_INFO,"reselect server request by operator\n");
                        tl_rtcpd.tl_log( &tl_rtcpd, 10, 2, 
                                         "func"    , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                         "Message" , TL_MSG_PARAM_STR, "reselect server request by operator" );
                        stop_request = 1;
                        break;
                    case RTCP_PING_REQ:
                        rtcp_log(LOG_DEBUG,"rtcp_CLThread() ping request from client\n");
                        tl_rtcpd.tl_log( &tl_rtcpd, 11, 2, 
                                         "func"    , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                         "Message" , TL_MSG_PARAM_STR, "ping request from client" );
                        /*
                         * If we are waited for it means that request has
                         * finished and client should normally have sent an
                         * ENDOF_REQ (or ABORT) long time ago. Something has
                         * gone wrong in the communication since client is
                         * still sending pings. Let a few pings go through to
                         * be sure.
                         * Thread unsafe usage of wait_to_be_joined is harmless.
                         */
                        if ( wait_to_be_joined == TRUE ) {
                            nb_pings++;
                            if ( nb_pings > 1 ) {
                                rtcp_log(LOG_ERR,"rtcp_CLThread() client has not noticed end of request\n");
                                tl_rtcpd.tl_log( &tl_rtcpd, 3, 2, 
                                                 "func"    , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                                 "Message" , TL_MSG_PARAM_STR, "client has not noticed end of request" );
                                stop_request = 1;
                            }
                        }
                        break;
                    default:
                        rtcp_log(LOG_ERR,"rtcp_CLThread() unexpected request %d\n",
                            hdr.reqtype);
                        tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                                         "func"   , TL_MSG_PARAM_STR, "rtcpd_CLThread", 
                                         "Message", TL_MSG_PARAM_STR, "unexpected request",
                                         "Request", TL_MSG_PARAM_INT, hdr.reqtype );
                        stop_request = 1;
                        break;
                    }
                }
            }
        }
    }
    if ( rc == -1 || hdr.reqtype != RTCP_ENDOF_REQ ) {
        if ( SHIFTclient == FALSE ) {
            if ( stop_request == 1 ) {
                if ( hdr.reqtype != RTCP_KILLJID_REQ &&
                     hdr.reqtype != RTCP_RSLCT_REQ ) AbortFlag = 1;
                else AbortFlag = 2;
            }
            if ( hdr.reqtype != RTCP_RSLCT_REQ )
                rtcpd_SetProcError(RTCP_FAILED | RTCP_USERR);
            else rtcpd_SetProcError(RTCP_RESELECT_SERV);
            rtcpd_CtapeKill();
        } else {
            rtcp_log = (void (*)(int, const char *, ...))log;
            if ( stop_request == 1 ) rtcpc_kill();
        }
        if ( AbortFlag != 0 && Dumptape == TRUE ) exit(0);
    }

    return((void *)&success);
}

int rtcpd_ClientListen(SOCKET s) {
    int rc, *client_socket, save_serrno;

    if ( s == INVALID_SOCKET ) {
        serrno = EINVAL;
        return(-1);
    }

    client_socket = (SOCKET *)malloc(sizeof(SOCKET));
    if ( client_socket == NULL ) {
        serrno = errno;
        rtcp_log(LOG_ERR,"rtcpd_ClientListen() malloc(): %s\n",
            sstrerror(errno));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_ClientListen", 
                         "Message", TL_MSG_PARAM_STR, "malloc",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(errno) );
        return(-1);
    }
    *client_socket = s;
    rc = Cthread_create(rtcpd_CLThread,(void *)client_socket);
    if ( rc == -1 ) {
        save_serrno = serrno;
        rtcp_log(LOG_ERR,"rtcpd_ClientListen() Cthread_create(): %s\n",
            sstrerror(serrno));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_ClientListen", 
                         "Message", TL_MSG_PARAM_STR, "Cthread_create",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(errno) );
        serrno = save_serrno;
        return(-1);
    }

    return(rc);
}


int rtcpd_WaitCLThread(int CLThId, int *status) {
    int rc,*_status;

    rtcp_log(LOG_DEBUG,"rtcpd_WaitCLThread() waiting for Client Listen thread (%d)\n",CLThId);
    tl_rtcpd.tl_log( &tl_rtcpd, 11, 3, 
                     "func"     , TL_MSG_PARAM_STR, "rtcpd_WaitCLThread", 
                     "Message"  , TL_MSG_PARAM_STR, "waiting for Client Listen thread",
                     "Thread ID", TL_MSG_PARAM_INT, CLThId );

    _status = NULL;
    wait_to_be_joined = TRUE;
    rc = Cthread_join(CLThId,&_status);
    if ( rc == -1 ) {
        rtcp_log(LOG_ERR,"rtcpd_WaitCLThread() Cthread_join(): %s\n",
            sstrerror(serrno));
        tl_rtcpd.tl_log( &tl_rtcpd, 3, 3, 
                         "func"   , TL_MSG_PARAM_STR, "rtcpd_WaitCLThread", 
                         "Message", TL_MSG_PARAM_STR, "Cthread_join",
                         "Error"  , TL_MSG_PARAM_STR, sstrerror(serrno) );
    } else {
        if ( status != NULL && _status != NULL ) *status = *_status;
    }
    return(rc);
}
