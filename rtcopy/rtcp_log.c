
/*
 * Copyright (C) 1999 by CERN IT-PDP/DM
 * All rights reserved
 */

/*
 * rtcp_log.c - Common entry points for RTCOPY logging
 */

#ifndef lint
static char cvsId[] = "@(#)$RCSfile: rtcp_log.c,v $ $Revision: 1.6 $ $Date: 1999/12/28 15:19:18 $ CERN IT-PDP/DM Olof Barring";
#endif /* not lint */

#if defined(_WIN32)
#include <winsock2.h>
#include <time.h>
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
#include <stdio.h>
#include <string.h>

#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <Cglobals.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#if defined(RTCP_SERVER)
#include <rtcp_server.h>
#endif /* RTCP_SERVER */


#if !defined(linux)
extern char *sys_errlist[];
#endif /* linux */

static int errmsg_key = -1;
static int out_key = -1;
static int err_key = -1;
static int client_socket_key = -1;

void DLL_DECL (*rtcp_log) _PROTO((int, const char *, ...)) = NULL;

void rtcpc_SetErrTxt(int level, char *format, ...) {
    va_list args;
    char *p;
    char *msgbuf = NULL;
    char **msgbuf_p;
    int loglevel = LOG_INFO;
    FILE **err_p, **out_p;
    SOCKET **client_socket_p;

    err_p = out_p = NULL;
    client_socket_p = NULL;
    Cglobals_get(&errmsg_key,(void *)&msgbuf_p,sizeof(char *));
    if ( msgbuf_p == NULL ) return;
    msgbuf = *msgbuf_p;
    if ( msgbuf == NULL ) return;
    if ( (p = getenv("RTCOPY_LOGLEVEL")) != NULL ) {
        loglevel = atoi(p);
    }
    Cglobals_get(&out_key,(void *)&out_p,sizeof(FILE *));
    Cglobals_get(&err_key,(void *)&err_p,sizeof(FILE *));
    Cglobals_get(&client_socket_key,(void *)&client_socket_p,sizeof(SOCKET *));
    if ( level <= loglevel ) {
        va_start(args,format);
        vsprintf(msgbuf,format,args);
        va_end(args);

        if ( level >= LOG_INFO && out_p != NULL && *out_p != NULL )
            fprintf(*out_p,msgbuf);
        else if ( err_p != NULL && *err_p != NULL ) 
            fprintf( *err_p,msgbuf);

        if ( client_socket_p != NULL && *client_socket_p != NULL )
            rtcp_ClientMsg(*client_socket_p,msgbuf);
    }
    return;
}

int rtcp_InitLog(char *msgbuf, FILE *out, FILE *err, SOCKET *client_socket) {
    char *p = NULL;
    char **msgbuf_p = NULL;
    int loglevel = LOG_INFO;
    FILE **out_p, **err_p;
    SOCKET **client_socket_p;

    if ( (p = getenv("RTCOPY_LOGLEVEL")) != NULL ) {
        loglevel = atoi(p);
    }
#if defined(RTCP_SERVER)
    if ( msgbuf == NULL && out == NULL && err == NULL && client_socket == NULL ) {
        initlog("rtcopyd",loglevel,RTCOPY_LOGFILE);
        rtcp_log = (void (*)(int, const char *, ...))log;
        return(0);
    }
#endif /* RTCP_SERVER */
    if ( p == NULL ) {
        if ( out == NULL ) loglevel = LOG_ERR;
        else loglevel = LOG_INFO;
    }
    Cglobals_get(&errmsg_key,(void **)&msgbuf_p,sizeof(char *));
    if ( msgbuf != NULL && msgbuf_p == NULL ) return(-1);
    else *msgbuf_p = msgbuf;

    Cglobals_get(&out_key,(void **)&out_p,sizeof(FILE *));
    if ( out != NULL && out_p == NULL ) return(-1);
    else *out_p = out;

    Cglobals_get(&err_key,(void **)&err_p,sizeof(FILE *));
    if ( err != NULL && err_p == NULL ) return(-1);
    else *err_p = err;

    Cglobals_get(&client_socket_key,(void **)&client_socket_p,sizeof(SOCKET *));
    if ( client_socket != NULL && client_socket_p == NULL ) return(-1);
    else *client_socket_p = client_socket;

    rtcp_log = (void (*)(int, const char *, ...))rtcpc_SetErrTxt;
    return(0);
}

#if defined(RTCP_SERVER)
/*
 * Append a message to the client error message structure for
 * current tape/file request
 */
void rtcpd_AppendClientMsg(tape_list_t *tape, file_list_t *file,
                          char *format, ...) {
    va_list args;
    rtcpTapeRequest_t *tapereq;
    rtcpFileRequest_t *filereq;
    char tmpbuf[CA_MAXLINELEN+1];
    char *buf;
    int l_buf;

    buf = NULL;
    if ( format == NULL || *format == '\0' ) return;
    if ( file != NULL ) {
        filereq = &file->filereq;
        buf = filereq->err.errmsgtxt;
    } else if ( tape != NULL ) {
        tapereq = &tape->tapereq;
        buf = tapereq->err.errmsgtxt;
    }

    if ( buf != NULL ) {
        va_start(args,format);
        vsprintf(tmpbuf,format,args);
        va_end(args);
        if ( tmpbuf[strlen(tmpbuf)-1] != '\0' )
            strcat(tmpbuf,"\n");
        rtcp_log(LOG_ERR,tmpbuf);
        l_buf = strlen(buf) + strlen(tmpbuf);
        if ( l_buf < CA_MAXLINELEN ) strcat(buf,tmpbuf);
    }
}
#endif /* RTCP_SERVER */
