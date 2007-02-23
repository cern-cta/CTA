
/*
 * Copyright (C) 1999-2004 by CERN IT
 * All rights reserved
 */

/*
 * rtcp_log.c - Common entry points for RTCOPY logging
 */

#include <stdlib.h>
#include <time.h>
#if defined(_WIN32)
#include <winsock2.h>
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
#include <serrno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <Cuuid.h>
#include <osdep.h>
#include <Cglobals.h>
#include <rtcp_constants.h>
#include <rtcp.h>
#if defined(RTCP_SERVER)
#include <rtcp_server.h>
char *rtcpd_logfile = RTCOPY_LOGFILE;
#endif /* RTCP_SERVER */

#define SAVE_ERRNO int save_errno, save_serrno; save_errno = errno; save_serrno = serrno;
#define RESTORE_ERRNO {serrno = save_serrno; errno = save_errno;}

static int errmsg_key = -1;
static int out_key = -1;
static int err_key = -1;
static int client_socket_key = -1;
int tpread_command = FALSE;
int loglevel = LOG_INFO;

void DLL_DECL (*rtcp_log) _PROTO((int, const char *, ...)) = NULL;

void rtcpc_SetErrTxt(int level, char *format, ...) {
    va_list args;
    char *p;
    char *msgbuf = NULL;
    char **msgbuf_p;
    int loglevel = LOG_INFO;
    FILE **err_p, **out_p;
    SOCKET **client_socket_p;

    SAVE_ERRNO

    err_p = out_p = NULL;
    client_socket_p = NULL;
    Cglobals_get(&errmsg_key,(void *)&msgbuf_p,sizeof(char *));
    if ( msgbuf_p == NULL ) {RESTORE_ERRNO; return;}
    msgbuf = *msgbuf_p;
    if ( msgbuf == NULL ) {RESTORE_ERRNO; return;}
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

        /*
         * tpread/tpwrite command output for client log messages
         * (like "selecting server ...").
         */
        if ( level == LOG_INFO && (strncmp(msgbuf," CP",3) != 0 &&
             strncmp(msgbuf," DUMP",5) != 0 && 
             *msgbuf != '\0' && *msgbuf != '\n') ) {
            if ( tpread_command == TRUE ) log(LOG_INFO,"%s",msgbuf);
            RESTORE_ERRNO;
            return;
        }

        if ( client_socket_p != NULL && *client_socket_p != NULL ) {
            if ( level <= LOG_ERR ) log(level,"%s",msgbuf);
            rtcp_ClientMsg(*client_socket_p,msgbuf);
        }
 
        if ( out_p != NULL && *out_p != NULL && 
             err_p != NULL && *err_p != NULL ) {
            if ( loglevel >= LOG_INFO ) fprintf(*out_p,"%s",msgbuf);
            else fprintf(*err_p,"%s",msgbuf);
        } else {
            if ( out_p != NULL && *out_p != NULL ) fprintf(*out_p,"%s",msgbuf);
            else if ( err_p != NULL && *err_p != NULL ) fprintf(*err_p,"%s",msgbuf);
        }

    }
    RESTORE_ERRNO;
    return;
}

int rtcp_InitLog(char *msgbuf, FILE *out, FILE *err, SOCKET *client_socket) {
    char *p = NULL;
    char **msgbuf_p = NULL;
    FILE **out_p, **err_p;
    SOCKET **client_socket_p;

    SAVE_ERRNO

    if ( (p = getenv("RTCOPY_LOGLEVEL")) != NULL ) {
        loglevel = atoi(p);
    }
#if defined(RTCP_SERVER)
    if ( out == NULL && err == NULL ) {
        initlog("rtcopyd",loglevel,rtcpd_logfile);
        if ( msgbuf == NULL && client_socket == NULL ) {
            rtcp_log = (void (*)(int, const char *, ...))log;
            RESTORE_ERRNO;
            return(0);
        }
    }
#endif /* RTCP_SERVER */
    if ( p == NULL ) {
        if ( out == NULL ) loglevel = LOG_ERR;
        else loglevel = LOG_INFO;
    }
    Cglobals_get(&errmsg_key,(void **)&msgbuf_p,sizeof(char *));
    if ( msgbuf != NULL && msgbuf_p == NULL ) {RESTORE_ERRNO; return(-1);}
    else *msgbuf_p = msgbuf;

    Cglobals_get(&out_key,(void **)&out_p,sizeof(FILE *));
    if ( out != NULL && out_p == NULL ) {RESTORE_ERRNO; return(-1);}
    else *out_p = out;

    Cglobals_get(&err_key,(void **)&err_p,sizeof(FILE *));
    if ( err != NULL && err_p == NULL ) {RESTORE_ERRNO; return(-1);}
    else *err_p = err;

    Cglobals_get(&client_socket_key,(void **)&client_socket_p,sizeof(SOCKET *));
    if ( client_socket != NULL && client_socket_p == NULL ) {
      RESTORE_ERRNO; 
      return(-1);
    }
    else *client_socket_p = client_socket;

    rtcp_log = (void (*)(int, const char *, ...))rtcpc_SetErrTxt;
    RESTORE_ERRNO;
    return(0);
}

/*
 * Useful routine for printing binary quantities (e.g. uuids) in hex
 * The returned string must be free'd by the caller
 */
char *rtcp_voidToString(void *in, int len) {
    int i, printlen;
    u_char *c;
    char *str, *buf = NULL;

    printlen = 2*len + len/4 + 1;
    buf = (char *)malloc(printlen);
    if ( buf == NULL ) return(NULL);
    c = (u_char *)in;
    str = buf;
    for ( i=0; i<len; i++ ) {
         sprintf(buf,"%.2x",*c++);
         buf += 2;
         if ( ((i+1) % 4 == 0) && (i+1 < len) ) *buf++ = '-';
    }
    return(str);
}

/*
 * Dual to rtcp_voidToString(). Transform a hex string back to
 * binary quantity
 */
int rtcp_stringToVoid(char *str, void *out, int len)
{
  u_char *outCp;
  u_long num;
  char current_char[3], *p;
  int i;

  if ( str == NULL || out == NULL || len < 0 ) {
    serrno = EINVAL;
    return(-1);
  }
  
  outCp = (u_char *)malloc(len);
  if ( outCp == NULL ) return(-1);
  p = str;
  i=0;
  while ( (*p != '\0') && (*(p+1) != '\0') ) {
    if ( *p == '-' ) {
      p++;
      continue;
    }
    if ( i>len ) {
      free(outCp);
      serrno = E2BIG;
      return(-1);
    }
    current_char[0] = *p;
    current_char[1] = *(p+1);
    current_char[2] = '\0';
    num = strtol(current_char,(char **)0,16);
    outCp[i++] = num & 0xff;
    p+=2;
  }
  memcpy(out,outCp,len);
  free(outCp);
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

    SAVE_ERRNO

    buf = NULL;
    rtcp_log(LOG_DEBUG,"rtcpd_AppendClientMsg() called\n");
    if ( format == NULL || *format == '\0' ) {RESTORE_ERRNO; return;}
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
        if ( tmpbuf[strlen(tmpbuf)-1] != '\n' ) strcat(tmpbuf,"\n");
        rtcp_log(LOG_DEBUG,"rtcpd_AppendClientMsg() txtbuf=%s\n",tmpbuf);
        if ( *tmpbuf != '\0' ) rtcp_log(LOG_ERR,tmpbuf);
        l_buf = strlen(buf) + strlen(tmpbuf);
        if ( l_buf < CA_MAXLINELEN ) strcat(buf,tmpbuf);
    }
    RESTORE_ERRNO;
    return;
}
#endif /* RTCP_SERVER */
