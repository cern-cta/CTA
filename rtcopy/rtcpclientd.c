/*
 *
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: rtcpclientd.c,v $ $Revision: 1.15 $ $Release$ $Date: 2004/11/02 11:30:29 $ $Author: obarring $
 *
 *
 *
 * @author Olof Barring
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpclientd.c,v $ $Revision: 1.15 $ $Release$ $Date: 2004/11/02 11:30:29 $ Olof Barring";
#endif /* not lint */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#if defined(_WIN32)
#include <winsock2.h>
extern char *geterr();
WSADATA wsadata;
#else /* _WIN32 */
#include <unistd.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <wait.h>
#include <sys/time.h>
#endif /* _WIN32 */
#include <sys/stat.h>
#include <errno.h>
#include <patchlevel.h>
#include <Castor_limits.h>
#include <log.h>
#include <net.h>
#include <osdep.h>
#include <trace.h>
#include <serrno.h>
#include <Cgetopt.h>
#include <Cinit.h>
#include <Cuuid.h>
#include <Cgrp.h>
#include <Cpwd.h>
#include <Cnetdb.h>
#include <dlf_api.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <castor/Constants.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld.h>
#include <rtcpcld_messages.h>

typedef struct rtcpcld_RequestList {
  tape_list_t *tape;
  int pid;       /* PID of vidChild if request has started, -1 otherwise */
  time_t startTime;
  time_t lastVdqmPingTime;
  struct rtcpcld_RequestList *next;
  struct rtcpcld_RequestList *prev;
} rtcpcld_RequestList_t;
rtcpcld_RequestList_t *requestList = NULL;

extern int rtcp_InitLog _PROTO((char *, FILE *, FILE *, SOCKET *));
extern int Cinitdaemon _PROTO((char *, void (*)(int)));
extern int Cinitservice _PROTO((char *, void (*)(int)));
int Debug = FALSE;
int inChild = 0;
Cuuid_t mainUuid;
Cuuid_t childUuid;
extern int use_port;
extern char *getconfent _PROTO((char *, char *, int));
extern int (*rtcpc_ClientCallback) _PROTO((
                                           rtcpTapeRequest_t *, 
                                           rtcpFileRequest_t *
                                           ));

static int getVIDFromRtcpd(
                           origSocket,
                           tapereq
                           )
     SOCKET *origSocket;
     rtcpTapeRequest_t *tapereq;
{
  rtcpHdr_t hdr;
  int rc, acknMsg;

  if ( tapereq == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  hdr.magic = RTCOPY_MAGIC;
  hdr.reqtype = RTCP_TAPEERR_REQ;
  hdr.len = -1;
  *tapereq->vid = '\0';
  acknMsg = 0;
  rc = rtcp_SendReq(
                    origSocket,
                    &hdr,
                    NULL,
                    tapereq,
                    NULL
                    );
  if ( rc != -1 ) {
    acknMsg = 1;
    rc = rtcp_RecvAckn(
                       origSocket,
                       hdr.reqtype
                       );
  }
  if ( rc == -1 ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    (acknMsg == 0 ? "rtcp_SendReq()" : "rtcp_RecvAckn()"),
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }
  acknMsg = 0;
  rc = rtcp_RecvReq(
                    origSocket,
                    &hdr,
                    NULL,
                    tapereq,
                    NULL
                    );
  if ( rc != -1 ) {
    acknMsg = 1;
    rc = rtcp_SendAckn(
                       origSocket,
                       hdr.reqtype
                       );
  }
  if ( rc == -1 ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    (acknMsg == 0 ? "rtcp_RecvReq()" : "rtcp_SendAckn()"),
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }
  if ( tapereq->VolReqID <= 0 ) {
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_NOVOLREQID,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS,
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }
  return(0);
}

static int getTapeRequestItem(
                              VolReqID, 
                              vid, 
                              side, 
                              mode, 
                              pid, 
                              item
                              )
     int VolReqID, side, mode, pid;     
     char *vid;
     rtcpcld_RequestList_t **item;
{
  rtcpcld_RequestList_t *iterator;
  int found = 0;

  if ( item == NULL || (VolReqID < 0 && vid == NULL && pid < 0) ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Invalid parameter",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = EINVAL;
    return(-1);
  }

  *item = NULL;
  CLIST_ITERATE_BEGIN(requestList,iterator) {
    if ( VolReqID >= 0 ) {
      if ( iterator->tape->tapereq.VolReqID == VolReqID ) {
        found = 1;
        break;
      }
    } else if ( vid != NULL ) {
      if ( (strcmp(iterator->tape->tapereq.vid,vid) == 0) &&
           (iterator->tape->tapereq.side == side) &&
           (iterator->tape->tapereq.mode == mode) ) {
        found = 1;
        break;
      }
    } else if ( pid > 0 ) {
      if ( iterator->pid == pid ) {
        found = 1;
        break;
      }
    }
  } CLIST_ITERATE_END(requestList,iterator);
  if ( found == 1 ) *item = iterator;
  else {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+5,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Could not find tape request",
                    "",
                    DLF_MSG_PARAM_TPVID,
                    vid,
                    "SIDE",
                    DLF_MSG_PARAM_INT,
                    side,
                    "MODE",
                    DLF_MSG_PARAM_INT,
                    mode,
                    "WORKPID",
                    DLF_MSG_PARAM_INT,
                    pid,
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }
  return(0);
}

static int getTapeRequest(
                          VolReqID, 
                          vid, 
                          side, 
                          mode, 
                          pid, 
                          tape
                          )
     int VolReqID, side, mode, pid;
     char *vid;
     tape_list_t **tape;
{
  rtcpcld_RequestList_t *item = NULL;
  int rc;
  if ( tape == NULL ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Invalid parameter",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = EINVAL;
    return(-1);
  }
  
  rc = getTapeRequestItem(
                          VolReqID,
                          vid,
                          side,
                          mode,
                          pid,
                          &item
                          );
  if ( rc == 0 && item != NULL ) {
    *tape = item->tape;
  }
  return(rc);
}

static int updateRequestList(
                             tape, 
                             pid
                             )
     tape_list_t *tape;
     int pid;
{
  rtcpcld_RequestList_t *iterator;
  int found = 0;

  if ( tape == NULL ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Invalid parameter",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = EINVAL;
    return(-1);
  }

  CLIST_ITERATE_BEGIN(requestList,iterator) {
    if ( (strcmp(iterator->tape->tapereq.vid,tape->tapereq.vid) == 0) &&
         (iterator->tape->tapereq.side == tape->tapereq.side) &&
         (iterator->tape->tapereq.mode == tape->tapereq.mode) ) {
      found = 1;
      break;
    }
  } CLIST_ITERATE_END(requestList,iterator);

  if ( found == 1 ) {
    if ( pid > 0 ) iterator->pid = pid;
    iterator->lastVdqmPingTime = time(NULL);
  } else {
    iterator = (rtcpcld_RequestList_t *)calloc(
                                               1,
                                               sizeof(rtcpcld_RequestList_t)
                                               );
    if ( iterator == NULL ) {
      (void)dlf_write(
                      mainUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "calloc()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      strerror(errno),
                      RTCPCLD_LOG_WHERE
                      );
      return(-1);
    }
    iterator->tape = tape;
    iterator->pid = pid;
    iterator->startTime = time(NULL);
    iterator->lastVdqmPingTime = time(NULL);
    CLIST_INSERT(requestList,iterator);
  }
  return(found);
}

static void logConnection(
                         s
                         )
     SOCKET s;
{
    char remhost[CA_MAXHOSTNAMELEN+1];
    struct sockaddr_in from;
    struct hostent *hp;
    int fromlen,rc;

    if ( s == INVALID_SOCKET ) return;

    fromlen = sizeof(from);
    rc = getpeername(s,(struct sockaddr *)&from,&fromlen);
    if ( rc == SOCKET_ERROR ) {
      (void)dlf_write(
                      mainUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "getpeername()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      strerror(errno),
                      RTCPCLD_LOG_WHERE
                      );
      return;
    }

    hp = Cgethostbyaddr((void *)&(from.sin_addr),sizeof(struct in_addr),
                        from.sin_family);
    if ( hp == NULL ) {
      (void)dlf_write(
                      mainUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "Cgethostbyaddr()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      sstrerror(serrno),
                      RTCPCLD_LOG_WHERE
                      );
      return;
    }

    strncpy(remhost,hp->h_name,sizeof(remhost)-1);

    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_SYSTEM,
                    RTCPCLD_MSG_NEWCONNECT,
                    (struct Cns_fileid *)NULL,
                    1,
                    "REMHOST",
                    DLF_MSG_PARAM_STR,
                    remhost
                    );
     return;
}

static int checkVdqmReqs() 
{
  return(0);
}

void  wait4Worker(
                     signo
                     )
  int signo; 
{
  return;
}

void checkWorkerExit() 
{
  int pid, status, rc = 0, sig=0, value = 0, stopped = 0;
  rtcpcld_RequestList_t *item = NULL;
  
  while ((pid = waitpid(-1,&status,WNOHANG)) > 0) {  
    if (WIFEXITED(status)) {
      value = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      sig = 1;
      value = WTERMSIG(status);
    } else {
      value = 1;
      stopped = 1;
    }
    rc = getTapeRequestItem(
                            -1,        /* VolReqID */
                            NULL,      /* VID */
                            -1,        /* side */
                            -1,        /* mode */
                            pid,
                            &item
                            );
    if ( rc == 0 && item != NULL ) {
      (void)dlf_write(
                      mainUuid,
                      DLF_LVL_SYSTEM,
                      (item->tape->tapereq.mode == WRITE_ENABLE ?
                       RTCPCLD_MSG_MIGRATOR_ENDED : 
                       RTCPCLD_MSG_RECALLER_ENDED),
                      (struct Cns_fileid *)NULL,
                      5,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      item->tape->tapereq.vid,
                      "MODE",
                      DLF_MSG_PARAM_INT,
                      item->tape->tapereq.mode,
                      "VOLREQID",
                      DLF_MSG_PARAM_INT,
                      item->tape->tapereq.VolReqID,
                      "WORKPID",
                      DLF_MSG_PARAM_INT,
                      pid,
                      (stopped == 1 ? "STOPPED" : 
                       (sig > 0 ? "SIGNAL" : "STATUS")),
                      DLF_MSG_PARAM_INT,
                      value
                      );
      if ( value != 0 ) {
        /*
         * Reset BUSY status in case child terminated
         * without cleanup
         */
        if ( item->tape->tapereq.mode == WRITE_ENABLE ) {
          (void)rtcpcld_updateTape(
                                   item->tape,
                                   NULL,
                                   1,
                                   0
                                   );
        }
        (void)rtcpcld_setVIDFailedStatus(item->tape);
      }
      CLIST_DELETE(requestList,item);
      (void)rtcpcld_cleanupTape(item->tape);
      free(item);
    }
  }
  return;
}

static int startWorker(
                       s,
                       tape
                       ) 
     SOCKET *s;
     tape_list_t *tape;
{
  ID_TYPE key;
  int usePipe = 0;
  int rc, c, i, argc, maxfds;
  char **argv, volReqIDStr[16], sideStr[16], tStartRequestStr[16], keyStr[32];
  char usePipeStr[16], startFseqStr[16];
  char cmd[CA_MAXLINELEN+1], cmdline[CA_MAXLINELEN+1];

  if ( (s == NULL) || (*s == INVALID_SOCKET) || 
       (tape == NULL) || (tape->dbRef == NULL) || (tape->dbRef->key == 0) ||
       (*tape->tapereq.vid == '\0') ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( tape->tapereq.mode == WRITE_ENABLE ) {
    if ( tape->file == NULL ) {
      serrno = EINVAL;
      return(-1);
    }
    sprintf(cmd,"%s/%s",BIN,RTCPCLD_MIGRATOR_CMD);
  } else {
    sprintf(cmd,"%s/%s",BIN,RTCPCLD_RECALLER_CMD);
  }

  key = tape->dbRef->key;
  
#ifndef _WIN32
#if defined(SOLARIS) || (defined(__osf__) && defined(__alpha)) || defined(linux) || defined(sgi)
  maxfds = getdtablesize();
#else
  maxfds = _NFILE;
#endif
  for (c=0; c<maxfds; c++) if ( c != *s ) close(c);
  /*
   * In case filedescriptor 0 is reserved by Insure
   */
  for (c=0; (c<maxfds) && (((rc = dup2(*s,c)) == -1) && (errno == EBADF)); c++);
  if ( rc == -1 ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "dup2()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    strerror(errno),
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }
  usePipe = c;
  /*
   * Count number of arguments. The first 9 are always provided
   */
  argc=9; 
  /* 
   * argv[0] = cmd plus the require options
   * -U <uuid> -V <vid> -s <side> -r/-w -k <key>
   */
  if ( *tape->tapereq.dgn != '\0' ) argc+=2;
  if ( tape->tapereq.mode == WRITE_ENABLE ) argc+=2;
  if ( *tape->tapereq.density != '\0' ) argc+=2;
  if ( tape->tapereq.VolReqID > 0 ) argc+=2;
  if ( *tape->tapereq.label != '\0' ) argc+=2;
  if ( *tape->tapereq.unit != '\0' ) argc+=2;
  if ( tape->tapereq.TStartRequest > 0 ) argc+=2;
  if ( usePipe > 0 ) argc+=2;
  argc++; /* One extra for the terminating NULL element */
  argv = (char **)calloc(argc,sizeof(char *));
  if ( argv == NULL ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "calloc()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    strerror(errno),
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }
  argv[0] = cmd;
  argv[1] = "-V";
  argv[2] = tape->tapereq.vid;
  argv[3] = "-s";
  sprintf(sideStr,"%d",tape->tapereq.side);
  argv[4] = sideStr;
  argv[5] = "-U";
  argv[6] = rtcp_voidToString((void *)&mainUuid,sizeof(mainUuid));
  argv[7] = "-k";
  u64tostr(key,keyStr,-sizeof(keyStr));
  argv[8] = keyStr;
  c = 9;
  if ( *tape->tapereq.dgn != '\0' ) {
    argv[c++] = "-g";
    argv[c++] = tape->tapereq.dgn;
  }
  if ( tape->tapereq.mode == WRITE_ENABLE ){
    argv[c++] = "-f";
    sprintf(startFseqStr,"%d",tape->file->filereq.tape_fseq);
    argv[c++] = startFseqStr;
  }
  if ( *tape->tapereq.density != '\0' ) {
    argv[c++] = "-d";
    argv[c++] = tape->tapereq.density;
  }
  if ( tape->tapereq.VolReqID > 0 ) {
    argv[c++] = "-i";
    sprintf(volReqIDStr,"%d",tape->tapereq.VolReqID);
    argv[c++] = volReqIDStr;
  }
  if ( *tape->tapereq.label != 0 ) {
    argv[c++] = "-l";
    argv[c++] = tape->tapereq.label;
  }
  if ( *tape->tapereq.unit != 0 ) {
    argv[c++] = "-u";
    argv[c++] = tape->tapereq.unit;
  }
  if ( tape->tapereq.TStartRequest > 0 ) {
    argv[c++] = "-T";
    sprintf(tStartRequestStr,"%d",tape->tapereq.TStartRequest);
    argv[c++] = tStartRequestStr;
  }
  if ( usePipe > 0 ) {
    argv[c++] = "-S";
    sprintf(usePipeStr,"%d",usePipe);
    argv[c++] = usePipeStr;
  }
  argv[c] = NULL;

  c = 0;
  *cmdline = '\0';
  for (i=0; (i<CA_MAXLINELEN) && (c<argc);) {
    if ( argv[c] != NULL ) {
      strcat(cmdline,argv[c]);
      strcat(cmdline," ");
    }
    c++;
    i = strlen(cmdline)+1;
  }
  (void)dlf_write(
                  mainUuid,
                  DLF_LVL_SYSTEM,
                  RTCPCLD_MSG_EXECCMD,
                  (struct Cns_fileid *)NULL,
                  1,
                  "COMMAND",
                  DLF_MSG_PARAM_STR,
                  cmdline
                  );
  execv(cmd,argv);
  /*
   * If we got here something went very wrong
   */
  (void)dlf_write(
                  mainUuid,
                  DLF_LVL_ERROR,
                  RTCPCLD_MSG_SYSCALL,
                  (struct Cns_fileid *)NULL,
                  RTCPCLD_NB_PARAMS+2,
                  "SYSCALL",
                  DLF_MSG_PARAM_STR,
                  "execv()",
                  "ERROR_STR",
                  DLF_MSG_PARAM_STR,
                  strerror(errno),
                  RTCPCLD_LOG_WHERE
                  );
  exit(SYERR);
#endif /* _WIN32 */
  
  return(0);
}

int rtcpcld_main(
                 main_args
                 )
     struct main_args *main_args;
{
  int rc, pid, maxfd, cnt, i, port, len, save_errno;
  fd_set rd_set, wr_set, ex_set, rd_setcp, wr_setcp, ex_setcp;
  struct sockaddr_in sin ; /* Internet address */ 
  struct timeval timeout, timeout_cp;
  char serverName[CA_MAXHOSTNAMELEN+1];
  SOCKET *rtcpdSocket = NULL;
  SOCKET *notificationSocket = NULL;
  SOCKET acceptSocket = INVALID_SOCKET;
  tape_list_t **tapeArray, *tape;
  rtcpTapeRequest_t tapereq;
  struct sigaction sa;

  /* Initializing the C++ log */
  /* Necessary at start of program and after any fork */
  C_BaseObject_initLog("NewStagerLog", SVC_NOMSG);

#if !defined(_WIN32)
  signal(SIGPIPE,SIG_IGN);
#endif /* _WIN32 */

  gethostname(serverName,CA_MAXHOSTNAMELEN);
#if defined(__DATE__) && defined (__TIME__)
  (void)dlf_write(
                  mainUuid,
                  DLF_LVL_SYSTEM,
                  RTCPCLD_MSG_STARTUP,
                  (struct Cns_fileid *)NULL,
                  3,
                  "GENERATED_DATE",DLF_MSG_PARAM_STR,__DATE__,
                  "GENERATED_TIME",DLF_MSG_PARAM_STR,__TIME__,
                  "SERVER",DLF_MSG_PARAM_STR,serverName
                  );
#endif /* __DATE__ && __TIME__ */

  serrno = errno = 0;
  rc = rtcpcld_InitNW(&rtcpdSocket);
  if ( rc == -1 ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ALERT,
                    RTCPCLD_MSG_INITNW,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    return(1);
  }
  len = sizeof(sin);
  rc = getsockname(*rtcpdSocket,(struct sockaddr *)&sin,&len);
  if ( rc == SOCKET_ERROR ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "getsockname()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    strerror(errno),
                    RTCPCLD_LOG_WHERE
                    );
    return(1);
  }
  port = ntohs(sin.sin_port);

  serrno = errno = 0;
  rc = rtcpcld_initNotify(&notificationSocket);
  if ( rc == -1 ) {
    (void)dlf_write(
                    mainUuid,
                    DLF_LVL_ALERT,
                    RTCPCLD_MSG_INITNW,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    return(1);
  }

  FD_ZERO(&rd_set);
  FD_ZERO(&wr_set);
  FD_ZERO(&ex_set);
  FD_SET(*rtcpdSocket,&rd_set);
  FD_SET(*notificationSocket,&rd_set);
  timeout.tv_sec = RTCPCLD_CATPOLL_TIMEOUT;
  timeout.tv_usec = 0;
  timeout_cp.tv_sec = timeout_cp.tv_usec = 0;
  maxfd = 0;
#ifndef _WIN32
  maxfd = *rtcpdSocket;
  if ( maxfd < *notificationSocket ) maxfd = *notificationSocket;
  maxfd++;
  memset(&sa,'\0',sizeof(sa));
  sa.sa_handler = (void (*)(int))wait4Worker;
  (void)sigaction(SIGCHLD,&sa,NULL);
#endif /* _WIN32 */
  rc = 0;
  /*
   * Main infinite processing loop. Wake-up on either notify (UDP)
   * or catalogue poll timeout (RTCPCLD_CATPOLL_TIMEOUT), or
   * migration/recall child exit (EINTR).
   */
  for (;;) {
    rd_setcp = rd_set;
    wr_setcp = wr_set;
    ex_setcp = ex_set;
    errno = serrno = 0;
    rc = select(
                maxfd, 
                &rd_setcp, 
                &wr_setcp, 
                &ex_setcp, 
                &timeout_cp
                );
    save_errno = errno;
    timeout_cp = timeout;
    checkWorkerExit();
    if ( rc < 0 ) {
      /*
       * Error, probably a child exit. If so, ignore and continue, otherwise
       * log the error and continue.
       */
      if ( save_errno == EINTR ) continue;
      errno = save_errno;
      (void)dlf_write(
                      mainUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_SYSCALL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "select()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      strerror(errno),
                      RTCPCLD_LOG_WHERE
                      );
      continue;
    } else if ( (rc > 0) && FD_ISSET(*rtcpdSocket,&rd_setcp) ) {
      /*
       * Got a connection from a tape server (rtcpd). Start the
       * tape migrator/recaller client.
       */
      errno = serrno = 0;
      rc = rtcp_Listen(
                       *rtcpdSocket,
                       &acceptSocket,
                       -1,
                       RTCP_ACCEPT_FROM_SERVER
                       );
      if ( rc == -1 ) {
        (void)dlf_write(
                        mainUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_LISTEN,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+1,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        continue;
      }
      /*
       * Send an empty tapereq to tell the server that we want to 
       * know the real request
       */
      logConnection(acceptSocket);
      errno = serrno = 0;
      memset(&tapereq,'\0',sizeof(tapereq));
      rc = getVIDFromRtcpd(
                           &acceptSocket,
                           &tapereq
                           );
      if ( rc == -1 ) {
        closesocket(acceptSocket);
        continue;
      }
      tape = NULL;
      /*
       * Find the request in our internal table
       */
      rc = getTapeRequest(
                          tapereq.VolReqID,
                          tapereq.vid,
                          tapereq.side,
                          tapereq.mode,
                          -1,
                          &tape
                          );
      if ( rc == -1 || tape == NULL ) {
        /*
         * Not found or error
         */
        closesocket(
                    acceptSocket
                    );
        continue;
      }
      strcpy(
             tape->tapereq.unit,
             tapereq.unit
             );
#if !defined(_WIN32)
      pid = (int)fork();
#else  /* !_WIN32 */
      pid = 0;
#endif /* _WIN32 */
      if ( pid == -1 ) {
        (void)dlf_write(
                        mainUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_SYSCALL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "SYSCALL", 
                        DLF_MSG_PARAM_STR, 
                        "fork()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        strerror(errno),
                        RTCPCLD_LOG_WHERE
                        );
        continue;
      }
      if ( pid != 0 ) {
        /*
         * Parent, update the internal request list with the pid of
         * the newly forked child.
         */
        (void)dlf_write(
                        mainUuid,
                        DLF_LVL_SYSTEM,
                        RTCPCLD_MSG_REQSTARTED,
                        (struct Cns_fileid *)NULL,
                        4,
                        "",
                        DLF_MSG_PARAM_TPVID,
                        tape->tapereq.vid,
                        "MODE",
                        DLF_MSG_PARAM_INT,
                        tape->tapereq.mode,
                        "VOLREQID",
                        DLF_MSG_PARAM_INT,
                        tape->tapereq.VolReqID,
                        "WORKPID",
                        DLF_MSG_PARAM_INT,
                        pid
                        );
        closesocket(acceptSocket);
        rc = updateRequestList(
                               tape,
                               pid
                               );
        continue; /* Parent */
      }

      /*
       * Child, kick off the migrator/recaller program and exit.
       */
      inChild = 1;
#if !defined(_WIN32)
      signal(SIGPIPE,SIG_IGN);
      closesocket(*rtcpdSocket);
      free(rtcpdSocket);
      rtcpdSocket = NULL;
#endif /* !_WIN32 */
      rc = startWorker(&acceptSocket,tape);
      if ( rc == -1 ) {
        (void)dlf_write(
                        childUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_WFAILED,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+1,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        rc = rtcpcld_updateTapeStatus(tape, TAPE_WAITDRIVE, TAPE_FAILED);
      }
      /*
       * break out from for(;;), cleanup and exit
       */
      break;    
    } else {
      /*
       * Notification or poll timeout. In either case, we check the catalogue
       * for new stuff to do (Tape or Stream).
       */
      if ( rc > 0 && FD_ISSET(*notificationSocket, &rd_setcp) ) {
        /*
         * We got notified. Message is not important since we will
         * check with the catalogue to find out what to do.
         */
        (void)rtcpcld_getNotify(*notificationSocket);
      }
      tapeArray = NULL;
      cnt = 0;
      rc = rtcpcld_getTapesToDo(&tapeArray,&cnt);
      if ( rc == -1 ) {
        (void)dlf_write(
                        mainUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_CATALOGUE,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+1,
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        continue;
      }
      for ( i=0; i<cnt; i++ ) {
        tape = tapeArray[i];
        tape->tapereq.TStartRequest = (int)time(NULL);
        /*
         * Add tape to internal list. This will automatically check
         * if the request is already there and if so, prevent that
         * the same tape+mode is submitted twice to VDQM.
         */
        rc = updateRequestList(
                               tape,
                               -1
                               );
        if ( rc == 0 ) {
          /*
           * Request wasn't found --> new request. Send it to VDQM
           */
          rc = vdqm_SendVolReq(
                               NULL,
                               &(tape->tapereq.VolReqID),
                               tape->tapereq.vid,
                               tape->tapereq.dgn,
                               NULL,
                               NULL,
                               tape->tapereq.mode,
                               port
                               );
          if ( rc == -1 ) {
            (void)dlf_write(
                            mainUuid,
                            DLF_LVL_ERROR,
                            RTCPCLD_MSG_VDQM,
                            (struct Cns_fileid *)NULL,
                            RTCPCLD_NB_PARAMS+1,
                            "ERROR_STR", 
                            DLF_MSG_PARAM_STR,
                            sstrerror(serrno),
                            RTCPCLD_LOG_WHERE
                            );
            continue;
          } else {
            (void)dlf_write(
                            mainUuid,
                            DLF_LVL_SYSTEM,
                            RTCPCLD_MSG_VDQMINFO,
                            (struct Cns_fileid *)NULL,
                            3,
                            "VID",
                            DLF_MSG_PARAM_STR,
                            tape->tapereq.vid,
                            "MODE",
                            DLF_MSG_PARAM_INT,
                            tape->tapereq.mode,
                            "VDQMRQID",
                            DLF_MSG_PARAM_INT,
                            tape->tapereq.VolReqID
                            );
          }
        }
      }
      if ( tapeArray != NULL ) free(tapeArray);
    }
    (void)checkVdqmReqs();
  }
  (void)rtcp_CleanUp(&rtcpdSocket,rc);
  return(0);
}

static int runAsUser(
                     myUser,
                     myGroup
                     )
     char *myUser, *myGroup;
{
  struct passwd *pw = NULL;
  struct group *grp = NULL;
  gid_t gid;
  int rc;
  
  if ( myUser == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  
  pw = Cgetpwnam(myUser);
  if ( pw == NULL ) return(-1);
  gid = pw->pw_gid;

  if ( myGroup != NULL ) {
    grp = Cgetgrnam(myGroup);
    if ( grp == NULL ) return(-1);
    gid = grp->gr_gid;
  }

  rc = setgid(gid);
  if ( rc == -1 ) return(-1);
  
  rc = setuid(pw->pw_uid);
  if ( rc == -1 ) return(-1);
  
  if ( (pw->pw_uid != getuid()) || (gid != getgid()) ) {
    serrno = EPERM;
    return(-1);
  }
  
  return(0);
}

static void usage(
                  cmd
                  )
     char *cmd;
{
  printf("Usage: %s [-d] [-f facilityName] [-p port] [-u uid] [-g gid]\n",cmd);
  return;
}

int main(
         argc,
         argv
         )
     int argc;
     char *argv[];
{
  int c, rc;
  char *myUser, *myGroup;
  char *rtcpcldFacilityName = RTCPCLIENTD_FACILITY_NAME;


#ifdef RTCPCLD_USER
  myUser = RTCPCLD_USER;
#else /* RTCPCLD_USER */
  myUser = NULL;
#endif /* RTCPCLD_USER */

#ifdef RTCPCLD_GROUP
  myGroup = RTCPCLD_GROUP;
#else /* RTCPCLD_GROUP */
  myGroup = NULL;
#endif /* RTCPCLD_GROUP */

  Coptind = 1;
  Copterr = 1;

  while ( (c = Cgetopt(argc, argv, "df:p:u:g:")) != -1 ) {
    switch (c) {
    case 'd':
      Debug = TRUE;
      break;
    case 'f':
      rtcpcldFacilityName = strdup(Coptarg);
      if ( rtcpcldFacilityName == NULL ) {
        fprintf(stderr,"main() strdup(): %s\n",
                strerror(errno));
        return(1);
      }
      break;
    case 'g':
      myGroup = Coptarg;
      break;
    case 'p':
      use_port = atoi(Coptarg);
      break;
    case 'u':
      myUser = Coptarg;
      break;
    default:
      usage(argv[0]);
      break;
    }
  }

  rc = runAsUser(myUser,myGroup);
  if ( rc == -1 ) {
    c = (serrno > 0 ? serrno : errno);
    fprintf(stderr,"Failed to start as user (%s,%s): %s\n",
            (myUser != NULL ? myUser : "(null)"),
            (myGroup != NULL ? myGroup : "(null)"),
            sstrerror(c));
    usage(argv[0]);
    return(1);
  }
  
  Cuuid_create(&mainUuid);

  (void)rtcpcld_initLogging(rtcpcldFacilityName);

  if ( Debug == TRUE ) {
    rc = rtcpcld_main(NULL);
  } else {
#if defined(_WIN32)
    /*
     * Windows
     */
    if ( Cinitservice("rtcpcld",rtcpcld_main) == -1 ) exit(1);
#else /* _WIN32 */
    /*
     * UNIX
     */
    if ( Cinitdaemon("rtcpcld",SIG_IGN) == -1 ) exit(1);
#endif /* _WIN32 */
    rc = rtcpcld_main(NULL);
  }
  if ( rc != 0 ) exit(1);
  else exit(0);
}

