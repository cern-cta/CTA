/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpd_tpdump.c,v $ $Revision: 1.1 $ $Date: 2000/02/29 15:13:27 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

/*
 * rtcpd_tpdump.c - RTCOPY server dumptape 
 */


#if defined(_WIN32)
#include <winsock2.h>
#include <io.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/time.h>
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#endif /* _WIN32 */
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>
#include <sys/stat.h>

#include <pwd.h>
#include <Castor_limits.h>
#include <Cglobals.h>
#include <log.h>
#include <osdep.h>
#include <net.h>
#include <sacct.h>
#include <Cthread_api.h>
#include <vdqm_api.h>
#include <Ctape_constants.h>
#include <Ctape.h>
#include <Ctape_api.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>

#define TP_STATUS(X) (proc_stat.tapeIOstatus.current_activity = (X))
extern processing_status_t proc_stat;

int dmp_usrmsg(int dmpmsg_level, char *format, ...) {
    va_list args;
    char msgtxtbuf[RTCP_SHIFT_BUFSZ];

    va_start(args,format);
    vsprintf(msgtxtbuf,format,args);
    va_end(args);

    if ( dmpmsg_level == MSG_OUT ) {
        rtcp_log(LOG_NOTICE,msgtxtbuf);
    } else {
        rtcp_log(LOG_ERR,msgtxtbuf);
    }
    return(0);
}

#define CHECK_PROC_ERR(X,Y,Z) { \
    save_errno = errno; \
    save_serrno = serrno; \
    if ( rc == -1 || (rtcpd_CheckProcError() & RTCP_FAILED) != 0 ) { \
        rtcp_log(LOG_ERR,"rtcpd_tpdump() %s, errno=%d, serrno=%d\n",(Z), \
                 save_errno,save_serrno); \
        if ( rc == -1 ) { \
            rtcpd_SetProcError(RTCP_FAILED); \
            (void) tellClient(client_socket,X,Y,-1); \
            (void) rtcp_WriteAccountRecord(client,tape,NULL,RTCPEMSG); \
        } \
        rtcpd_DmpEnd(); \
        (void) rtcpd_Release((X),NULL); \
        (void) tellClient(client_socket,NULL,NULL,-1); \
        rtcp_CloseConnection(client_socket); \
        free(client_socket); \
        serrno = save_serrno > 0 ? save_serrno : save_errno; \
        return(-1); \
    }}

int rtcpd_tpdump(rtcpClientInfo_t *client, tape_list_t *tape) {
    char *msgtxtbuf = NULL;
    int rc,severity,save_errno,save_serrno;
    tape_list_t *tl = NULL;
    file_list_t *fl = NULL;
    rtcpFileRequest_t *filereq = NULL;
    SOCKET *client_socket = NULL;
    char lbltyp[CA_MAXLBLTYPLEN+1];

    proc_stat.tapeIOstatus.nbbytes = 0;
    TP_STATUS(RTCP_PS_NOBLOCKING);

    client_socket = (SOCKET *)malloc(sizeof(SOCKET));
    if ( client_socket == NULL ) {
        rtcp_log(LOG_ERR,"rtcpd_tpdump() malloc(SOCKET): %s\n",
                 sstrerror(serrno));
        return(-1);
    }
    *client_socket = INVALID_SOCKET;
    rc = rtcpd_ConnectToClient(client_socket,
                               client->clienthost,
                               &client->clientport);

    msgtxtbuf = (char *)calloc(1,CA_MAXLINELEN+1);
    rtcp_InitLog(msgtxtbuf,NULL,NULL,client_socket);

    /*
     * Initialize Ctape error message buffer
     */
    rc = rtcpd_CtapeInit();
    CHECK_PROC_ERR(tape,NULL,"rtcpd_CtapeInit() error");
    /*
     * We must add the first file list element here since
     * rtcpd_Mount() needs it to store the tape path
     */
    tl = tape;
    rc = rtcp_NewFileList(&tl,&fl,WRITE_DISABLE);
    if ( fl == NULL ) rc = -1;
    CHECK_PROC_ERR(tape,NULL,"rtcp_NewFileList() error"); 

    filereq = &fl->filereq;
    filereq->position_method = TPPOSIT_FSEQ;
    filereq->tape_fseq = 1;
    fl->tape_fsec = 1;
    filereq->blockid = 0;
    filereq->check_fid = CHECK_FILE;
    *filereq->fid = '\0';
    strcpy(filereq->recfm,"U");
    filereq->blocksize = 0;
    filereq->recordlength = 0;
    filereq->retention = 0;
    filereq->tp_err_action = 0;
    filereq->concat = NOCONCAT;
    filereq->convert = tape->dumpreq.convert;
    filereq->err.severity = RTCP_OK;

    /*
     * Reserve the unit 
     */
    TP_STATUS(RTCP_PS_RESERVE);
    rc = rtcpd_Reserve(tape);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    if ( rc == -1 ) {
        (void)rtcpd_Deassign(-1,&tape->tapereq);
    }
    CHECK_PROC_ERR(tape,NULL,"rtcpd_Reserv() error");

    /*
     * Mount the volume, temporary change label to "blp"
     */
    strcpy(lbltyp,tape->tapereq.label);
    strcpy(tape->tapereq.label,"blp");
    TP_STATUS(RTCP_PS_MOUNT);
    rc = rtcpd_Mount(tape);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    strcpy(tape->tapereq.label,lbltyp);
    if ( rc == -1 ) (void)rtcpd_Deassign(-1,&tape->tapereq);
    CHECK_PROC_ERR(tape,NULL,"rtcpd_Mount() error");

    /*
     * Position the volume
     */
    TP_STATUS(RTCP_PS_POSITION);
    rc = rtcpd_Position(tape,tape->file);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    CHECK_PROC_ERR(tape,tape->file,"rtcpd_Position() error");

    TP_STATUS(RTCP_PS_POSITION);
    rc = rtcpd_Info(tape,tape->file);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    CHECK_PROC_ERR(tape,tape->file,"rtcpd_Info() error");

    /*
     * Initialise the dumptape routines
     */
    rc = rtcpd_DmpInit(tape);
    CHECK_PROC_ERR(tape,NULL,"rtcpd_DmpInit() error");

    /*
     * Loop on the files and dump as many as requested by the client.
     */
    rc = 0;
    while ( rc == 0 ) {
        rc = rtcpd_DmpFile(tape,tape->file->prev);
        CHECK_PROC_ERR(tape,NULL,"rtcpd_dmpfil() error");
        tellClient(client_socket,NULL,tape->file->prev,0);
        if ( rc != 0 ) break;

        tl = tape;
        (void) rtcp_NewFileList(&tl,&fl,WRITE_DISABLE);
        if ( fl == NULL ) {
            rtcp_log(LOG_ERR,"rtcpd_tpdump() rtcp_NewFileList(): %s\n",sstrerror(serrno));
            rc = -1;
            break;
        }
        fl->filereq.err.severity = RTCP_OK;
    }
    if ( rc != -1 ) rc = 0;
    tellClient(client_socket,NULL,NULL,rc);
    rtcp_CloseConnection(client_socket);
    free(client_socket);
    free(msgtxtbuf);
    (void) rtcpd_DmpEnd();

    TP_STATUS(RTCP_PS_RELEASE);
    (void) rtcpd_Release(tape,NULL);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    return(rc);
}
