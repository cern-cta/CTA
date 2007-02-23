/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcpd_tpdump.c - RTCOPY server dumptape 
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
#include <io.h>
extern char *geterr();
#else /* _WIN32 */
#include <sys/param.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>
#include <netinet/in.h>                 /* Internet data types          */
#include <sys/time.h>
#endif /* _WIN32 */
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
#include <Cuuid.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <serrno.h>
#include <stdio.h>

#define TP_STATUS(X) (proc_stat.tapeIOstatus.current_activity = (X))
extern processing_status_t proc_stat;
extern int AbortFlag;

void dmp_usrmsg(int dmpmsg_level, char *format, ...) {
    va_list args;
    char msgtxtbuf[RTCP_SHIFT_BUFSZ];

    va_start(args,format);
    vsprintf(msgtxtbuf,format,args);
    va_end(args);
    /*
     * Make sure the string is terminated
     */
    msgtxtbuf[RTCP_SHIFT_BUFSZ-1] = '\0'; 

    if ( dmpmsg_level == MSG_OUT ) {
        rtcp_log(LOG_NOTICE,"%s",msgtxtbuf);
    } else {
        rtcp_log(LOG_ERR,"%s",msgtxtbuf);
    }
    return;
}

#define CHECK_PROC_ERR(X,Y,Z) { \
    save_errno = errno; \
    save_serrno = serrno; \
    if ( AbortFlag != 0 ) rtcp_InitLog(NULL,NULL,NULL,NULL); \
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
    int rc,save_errno,save_serrno;
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
    Ctape_dmpmsg = (void (*) _PROTO((int, const char *, ...)))dmp_usrmsg;

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
    filereq->tape_fseq = 0;
    fl->tape_fsec = 1;
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
        (void)rtcpd_Deassign(-1,&tape->tapereq,NULL);
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
    if ( rc == -1 ) (void)rtcpd_Deassign(-1,&tape->tapereq,NULL);
    CHECK_PROC_ERR(tape,NULL,"rtcpd_Mount() error");

    TP_STATUS(RTCP_PS_POSITION);
    rc = rtcpd_Info(tape,tape->file);
    TP_STATUS(RTCP_PS_NOBLOCKING);
    *(tape->file->filereq.err.errmsgtxt) = '\0';
    tape->file->filereq.err.severity = RTCP_OK;
    tape->file->filereq.err.errorcode = 0;

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
        tellClient(client_socket,NULL,tape->file->prev,rc);
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
