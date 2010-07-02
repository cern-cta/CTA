/*
 *
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: rtcpclientd.c,v $ $Revision: 1.50 $ $Release$ $Date: 2009/08/18 09:43:01 $ $Author: waldron $
 *
 *
 *
 * @author Olof Barring
 */

#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/types.h>                  /* Standard data types          */
#include <netdb.h>                      /* Network "data base"          */
#include <sys/socket.h>                 /* Socket interface             */
#include <netinet/in.h>                 /* Internet data types          */
#include <signal.h>
#include <wait.h>
#include <sys/time.h>
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
#include <Cthread_api.h>
#include <Cuuid.h>
#include <Cgrp.h>
#include <Cpwd.h>
#include <Cnetdb.h>
#include <u64subr.h>
#include <dlf_api.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <castor/Constants.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <castor/stager/TapeCopy.h>
#include <castor/stager/ITapeSvc.h>
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

static int port = -1;
static pid_t tapeErrorHandlerPid = 0;
static char *cmdName = NULL;
static sigset_t signalset;
static int shutdownServiceFlag = 0;
static char *rtcpcldFacilityName = RTCPCLIENTD_FACILITY_NAME;
static void startTapeErrorHandler (void);
extern int rtcp_InitLog (char *, FILE *, FILE *, SOCKET *);
extern int Cinitdaemon (char *, void (*)(int));
extern int Cinitservice (char *, void (*)(int));
int Debug = FALSE;
int inChild = 0;
Cuuid_t mainUuid;
Cuuid_t childUuid;
extern int use_port;
extern char *getconfent (char *, char *, int);
extern int (*rtcpc_ClientCallback) (
				    rtcpTapeRequest_t *,
				    rtcpFileRequest_t *
				    );

static int getVIDFromRtcpd(SOCKET *origSocket,
			   rtcpTapeRequest_t *tapereq)
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_NOVOLREQID),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INTERNAL),
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
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
    socklen_t fromlen;
    int rc;

    if ( s == INVALID_SOCKET ) return;

    fromlen = sizeof(from);
    rc = getpeername(s,(struct sockaddr *)&from,&fromlen);
    if ( rc == SOCKET_ERROR ) {
      (void)dlf_write(
                      mainUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_NEWCONNECT),
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
  rtcpcld_RequestList_t *iterator;
  tape_list_t *tape;
  int vdqmPingInterval = RTCPCLD_VDQMPOLL_TIMEOUT;
  time_t now;
  int rc = 0, save_serrno;

  now = time(NULL);
  CLIST_ITERATE_BEGIN(requestList,iterator) {
    if ( (iterator->pid <= 0) &&
         (now - iterator->lastVdqmPingTime > vdqmPingInterval) ) {
      tape = iterator->tape;
      iterator->lastVdqmPingTime = now;
      (void)dlf_write(
                      mainUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_VDQM_PING),
                      (struct Cns_fileid *)NULL,
                      4,
                      "",
                      DLF_MSG_PARAM_TPVID,
                      tape->tapereq.vid,
                      "MODE",
                      DLF_MSG_PARAM_INT,
                      tape->tapereq.mode,
                      "VDQMRQID",
                      DLF_MSG_PARAM_INT,
                      tape->tapereq.VolReqID,
                      "DGN",
                      DLF_MSG_PARAM_STR,
                      tape->tapereq.dgn
                      );
      rc = vdqm_PingServer(NULL,tape->tapereq.dgn,tape->tapereq.VolReqID);
      if ( rc == -1 ) {
        save_serrno = serrno;
        if ( (save_serrno != EVQHOLD) && (save_serrno != SECOMERR) ) {
          (void)dlf_write(
                          mainUuid,
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_VDQM_LOST),
                          (struct Cns_fileid *)NULL,
                          4,
                          "",
                          DLF_MSG_PARAM_TPVID,
                          tape->tapereq.vid,
                          "MODE",
                          DLF_MSG_PARAM_INT,
                          tape->tapereq.mode,
                          "VDQMRQID",
                          DLF_MSG_PARAM_INT,
                          tape->tapereq.VolReqID,
                          "DGN",
                          DLF_MSG_PARAM_STR,
                          tape->tapereq.dgn
                          );
          tape->tapereq.VolReqID = 0;
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
                            RTCPCLD_LOG_MSG(RTCPCLD_MSG_VDQM),
                            (struct Cns_fileid *)NULL,
                            RTCPCLD_NB_PARAMS+1,
                            "ERROR_STR",
                            DLF_MSG_PARAM_STR,
                            sstrerror(serrno),
                            RTCPCLD_LOG_WHERE
                            );
          } else {
            (void)dlf_write(
                            mainUuid,
                            RTCPCLD_LOG_MSG(RTCPCLD_MSG_VDQMINFO),
                            (struct Cns_fileid *)NULL,
                            3,
                            "",
                            DLF_MSG_PARAM_TPVID,
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
      /*
       * Only do one at a time so that we don't overload
       * VDQM with pings
       */
      break;
    }
  } CLIST_ITERATE_END(requestList,iterator);
  return(0);
}

static void checkWorkerExit(int shutDownFlag)
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
    if ( pid == tapeErrorHandlerPid ) {
      tapeErrorHandlerPid = 0;
      continue;
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
                      RTCPCLD_LOG_MSG((item->tape->tapereq.mode == WRITE_ENABLE ?
                                     RTCPCLD_MSG_MIGRATOR_ENDED :
                                     RTCPCLD_MSG_RECALLER_ENDED)),
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
      /*
       * Reset BUSY status in VMGR
       */
      if ( item->tape->tapereq.mode == WRITE_ENABLE ) {
        (void)rtcpcld_updateTape(
                                 item->tape,
                                 NULL,
                                 1,
                                 0,
                                 NULL
                                 );
        /*
         * Reset the stream or delete it in case it has finished
         */
        if ( rtcpcld_returnStream(item->tape) == -1 ) {
          LOG_SYSCALL_ERR("rtcpcld_returnStream()");
        }
        if ( value != 0 ) {
          (void)rtcpcld_setVIDFailedStatus(item->tape);
        }
      } else if ( (item->tape->tapereq.mode == WRITE_DISABLE) &&
                  (value != 0) ) {
        /*
         * Restore segments that are left in SELECTED status. This
         * will allow another recaller to pick them up later. If there
         * were such segments the tape status is reset to PENDING
         */
        rc = rtcpcld_restoreSelectedSegments(item->tape);
        if ( rc == -1 ) {
          LOG_SYSCALL_ERR("rtcpcld_restoreSelectedSegments()");
        } else if ( rc == 0 ) {
          /*
           * If no segments were restored and the recaller had failed
           * we set the tape status to FAILED.
           */
          (void)rtcpcld_setVIDFailedStatus(item->tape);
        }
      }
      /*
       * In all cases
       */
      CLIST_DELETE(requestList,item);
      (void)rtcpcld_cleanupTape(item->tape);
      free(item);
    }
    if ((shutDownFlag == 0) && (value != 0) ) {
      startTapeErrorHandler();
    }
  }
  return;
}

static void shutdownService (
			     int
			     );

static void sigchld_handler(int signo) {
  (void)signo;
  return;
}

static void *signal_handler(void *arg) {
  int signal;

  (void)arg;
  while (1) {
    if (sigwait(&signalset, &signal) == 0) {
      if ((signal == SIGINT)  ||
	  (signal == SIGTERM) ||
	  (signal == SIGABRT)) {
	shutdownService(signal);
      }	else if (signal == SIGCHLD) {
	checkWorkerExit(shutdownServiceFlag);
      } else {
	(void)dlf_write(
			mainUuid,
			RTCPCLD_LOG_MSG(RTCPCLD_MSG_IGNORE_SIGNAL),
			(struct Cns_fileid *)NULL,
			1,
			"SIGNAL",
			DLF_MSG_PARAM_INT,
			signal
			);
      }
    }
  }
  return NULL;
}

static void shutdownService(
                            signo
                            )
     int signo;
{
  rtcpcld_RequestList_t *iterator = NULL;
  int i, j;

  shutdownServiceFlag = 1;
  CLIST_ITERATE_BEGIN(requestList,iterator) {
    if (iterator->pid > 0) {
      kill(iterator->pid,signo);
    } else if (iterator->tape != NULL) {
      if (rtcpcld_returnStream(iterator->tape) == -1) {
        LOG_SYSCALL_ERR("rtcpcld_returnStream()");
      }
    }
  } CLIST_ITERATE_END(requestList,iterator);
  // wait a while for all forked children to end
  for (i = 0, j = 0; i < 30; i++, j = 0) {
    (void)checkWorkerExit(shutdownServiceFlag);
    CLIST_ITERATE_BEGIN(requestList,iterator) {
      if (iterator->pid > 0) {
      	j++;
      }
    } CLIST_ITERATE_END(requestList,iterator);
    if (j == 0) {
      break;  /* break out, all processes ended */
    }
  }
  sleep(1);
  (void)checkWorkerExit(shutdownServiceFlag);
  (void)dlf_write(
                  mainUuid,
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_SHUTDOWN),
                  (struct Cns_fileid *)NULL,
                  1,
                  "SIGNAL",
                  DLF_MSG_PARAM_INT,
                  signo
                  );
  dlf_shutdown();
  exit(0);
}

static void startTapeErrorHandler()
{
  char cmd[CA_MAXLINELEN+1], cmdline[CA_MAXLINELEN+1];
  char **argv = NULL;
  int argc, c, maxfds, i, rc;
  pid_t pid;
  sigset_t new_set, old_set;

  /*
   * Never run more than one at a time
   */
  if ( tapeErrorHandlerPid > 0 ) return;

  pid = fork();
  if ( pid > 0 ) {
    /*
     * Parent
     */
    tapeErrorHandlerPid = pid;
  } else if ( pid == 0 ) {
    /*
     * Child
     */
    sprintf(cmd,"%s/%s",BIN,TAPEERRORHANDLER_CMD);
#if defined(linux)
    maxfds = getdtablesize();
#else
    maxfds = _NFILE;
#endif
    for (c=0; c<maxfds; c++) close(c);
    argc = 2; /* One extra for the terminating NULL element */
    argv = (char **)calloc(argc,sizeof(char *));
    if ( argv == NULL ) {
      (void)dlf_write(
                      mainUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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

      return;
    }
    argv[0] = cmd;
    argv[1] = NULL;
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_EXECCMD),
                    (struct Cns_fileid *)NULL,
                    1,
                    "COMMAND",
                    DLF_MSG_PARAM_STR,
                    cmdline
                    );

    /* reset signal handler */
    sigfillset(&new_set);
    rc = pthread_sigmask(SIG_UNBLOCK, &new_set, &old_set);
    if (rc != 0) {
      (void)dlf_write(
                      mainUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "pthread_sigmask()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      strerror(errno),
                      RTCPCLD_LOG_WHERE
                      );
      return;
    }

    execv(cmd,argv);

    /* restore original sigmask */
    rc = pthread_sigmask(SIG_BLOCK, &old_set, NULL);
    if (rc != 0) {
      (void)dlf_write(
                      mainUuid,
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "pthread_sigmask()",
                      "ERROR_STR",
                      DLF_MSG_PARAM_STR,
                      strerror(errno),
                      RTCPCLD_LOG_WHERE
                      );
      return;
    }

    /*
     * If we got here something went very wrong
     */
    (void)dlf_write(
                    mainUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
    dlf_shutdown();
    exit(SYERR);
  } else {
    /*
     * fork() failed
     */
    (void)dlf_write(
                    mainUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
  sigset_t new_set, old_set;

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

#if defined(linux)
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
  u64tostr(key,keyStr,(int)(-sizeof(keyStr)));
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
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_EXECCMD),
                  (struct Cns_fileid *)NULL,
                  1,
                  "COMMAND",
                  DLF_MSG_PARAM_STR,
                  cmdline
                  );

  /* reset signal handler */
  sigfillset(&new_set);
  rc = pthread_sigmask(SIG_UNBLOCK, &new_set, &old_set);
  if (rc != 0) {
    (void)dlf_write(
                    mainUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "pthread_sigmask()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    strerror(errno),
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }

  execv(cmd,argv);

  /* restore original sigmask */
  rc = pthread_sigmask(SIG_BLOCK, &old_set, NULL);
  if (rc != 0) {
    (void)dlf_write(
                    mainUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "pthread_sigmask()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    strerror(errno),
                    RTCPCLD_LOG_WHERE
                    );
    return(-1);
  }

  /*
   * If we got here something went very wrong
   */
  (void)dlf_write(
                  mainUuid,
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
  dlf_shutdown();
  exit(SYERR);

  return(0);
}

int rtcpcld_main() {
  int rc, pid, maxfd, cnt, i, save_errno;
  socklen_t len;
  int timeout_secs = RTCPCLD_CATPOLL_TIMEOUT;
  int tapeErrorHandler_checkInterval = RTCPCLD_TPERRCHECK_INTERVAL;
  fd_set rd_set, wr_set, ex_set, rd_setcp, wr_setcp, ex_setcp;
  struct sockaddr_in sin ; /* Internet address */
  time_t now = 0, tapeErrorHandler_lastCheck = 0;
  struct timeval timeout, timeout_cp;
  SOCKET *rtcpdSocket = NULL;
  SOCKET *notificationSocket = NULL;
  SOCKET acceptSocket = INVALID_SOCKET;
  tape_list_t **tapeArray, *tape;
  rtcpTapeRequest_t tapereq;
  char *p = NULL;

  signal(SIGPIPE, SIG_IGN);
  signal(SIGXFSZ, SIG_IGN);
  signal(SIGCHLD, sigchld_handler);

  sigemptyset(&signalset);
  sigaddset(&signalset, SIGINT);
  sigaddset(&signalset, SIGTERM);
  sigaddset(&signalset, SIGHUP);
  sigaddset(&signalset, SIGABRT);
  sigaddset(&signalset, SIGCHLD);
  sigaddset(&signalset, SIGPIPE);

  rc = pthread_sigmask(SIG_BLOCK, &signalset, NULL);
  if (rc != 0) {
    return(1);
  }

  rc = Cthread_create_detached((void *)signal_handler, NULL);
  if (rc < 0) {
    return(1);
  }

  (void)rtcpcld_initLogging(rtcpcldFacilityName);

#if defined(__DATE__) && defined (__TIME__)
  (void)dlf_write(
                  mainUuid,
                  RTCPCLD_LOG_MSG(RTCPCLD_MSG_STARTUP),
                  (struct Cns_fileid *)NULL,
                  3,
                  "GENERATED_DATE",DLF_MSG_PARAM_STR,__DATE__,
                  "GENERATED_TIME",DLF_MSG_PARAM_STR,__TIME__,
                  "SERVICE",DLF_MSG_PARAM_STR,
                  (cmdName != NULL ? cmdName : "(null)")
                  );
#endif /* __DATE__ && __TIME__ */

  /* Cleanup the stager database */
  rc = rtcpcld_cleanup();
  if ( rc == -1 ) {
    return(-1);
  }

  serrno = errno = 0;
  rc = rtcpcld_InitNW(&rtcpdSocket);
  if ( rc == -1 ) {
    (void)dlf_write(
                    mainUuid,
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INITNW),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INITNW),
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
                    RTCPCLD_LOG_MSG(RTCPCLD_MSG_INITNW),
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    return(1);
  }

  if ( (p = getconfent("rtcpcld","TIMEOUT",0)) != NULL ) {
    timeout_secs = atoi(p);
  }

  FD_ZERO(&rd_set);
  FD_ZERO(&wr_set);
  FD_ZERO(&ex_set);
  FD_SET(*rtcpdSocket,&rd_set);
  FD_SET(*notificationSocket,&rd_set);
  timeout.tv_sec = timeout_secs;
  timeout.tv_usec = 0;
  timeout_cp.tv_sec = timeout_cp.tv_usec = 0;
  maxfd = 0;
  maxfd = *rtcpdSocket;
  if ( maxfd < *notificationSocket ) maxfd = *notificationSocket;
  maxfd++;
  rc = 0;

  /*
   * Fork a tape error handler to retry possible failed segments
   * from last shutdown
   */
  startTapeErrorHandler();
  /*
   * Main infinite processing loop. Wake-up on either notify (UDP)
   * or catalogue poll timeout (RTCPCLD_CATPOLL_TIMEOUT), or
   * migration/recall child exit (EINTR).
   */
  for (;;) {
    rd_setcp = rd_set;
    wr_setcp = wr_set;
    ex_setcp = ex_set;
    /*
     * Select
     */
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

    if (shutdownServiceFlag) {
      continue;
    }

    if ( rc < 0 ) {
      /*
       * Error, probably a child exit. If so, ignore and continue, otherwise
       * log the error and loop.
       */
      if ( save_errno != EINTR ) {
        errno = save_errno;
        (void)dlf_write(
                        mainUuid,
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
      }
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
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_LISTEN),
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

      pid = (int)fork();
      if ( pid == -1 ) {
        (void)dlf_write(
                        mainUuid,
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_SYSCALL),
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
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_REQSTARTED),
                        (struct Cns_fileid *)NULL,
                        4,
                        "",
                        DLF_MSG_PARAM_TPVID,
                        tape->tapereq.vid,
                        "MODE",
                        DLF_MSG_PARAM_INT,
                        tape->tapereq.mode,
                        "VDQMID",
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
        sleep(2); /* Avoid starting all migrators concurrently, it may cause DB lock contention */
        continue; /* Parent */
      }

      /*
       * Child, kick off the migrator/recaller program and exit.
       */
      inChild = 1;
      signal(SIGPIPE,SIG_IGN);
      closesocket(*rtcpdSocket);
      free(rtcpdSocket);
      rtcpdSocket = NULL;
      rc = startWorker(&acceptSocket,tape);
      if ( rc == -1 ) {
        (void)dlf_write(
                        childUuid,
                        RTCPCLD_LOG_MSG(RTCPCLD_MSG_WFAILED),
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
    }
    /*
     * Always check the catalogue for new stuff to do (Tape or Stream).
     */
    if ( (rc > 0) && FD_ISSET(*notificationSocket, &rd_setcp) ) {
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
                      RTCPCLD_LOG_MSG(RTCPCLD_MSG_CATALOGUE),
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
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_VDQM),
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
                          RTCPCLD_LOG_MSG(RTCPCLD_MSG_VDQMINFO),
                          (struct Cns_fileid *)NULL,
                          3,
                          "",
                          DLF_MSG_PARAM_TPVID,
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
    (void)checkVdqmReqs();
    now = time(NULL);
    if ( (now - tapeErrorHandler_lastCheck) >
         tapeErrorHandler_checkInterval ) {
      /*
       * The tperrhandler is forked when a recaller or migrator exits with errors.
       * However, sometimes the recaller/migrator may exit with success despite failed
       * segments so we do an extra check every now and then...
       */
      tapeErrorHandler_lastCheck = now;
      startTapeErrorHandler();
    }
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
  printf("Usage: %s [-c configurationFile] [-d] [-f facilityName] [-p port] [-u uid] [-g gid] [-h]\n",cmd);
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

  cmdName = argv[0];
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

  while ( (c = Cgetopt(argc, argv, "c:df:p:u:g:h")) != -1 ) {
    switch (c) {
    case 'c':
      {
        FILE *fp = fopen(Coptarg, "r");
        if(fp) {
          // The file exists
          fclose(fp);
        } else {
          // The file does not exist
          fprintf(stderr, "Configuration file '%s' does not exist\n", Coptarg);
          return(1);
        }
      }
      setenv("PATH_CONFIG", Coptarg, 1);
      break;
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
    case 'h':
      usage(argv[0]);
      return(0);
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

  if (Debug == TRUE) {
    rc = rtcpcld_main();
  } else {
    /*
     * UNIX
     */
    if (Cinitdaemon("rtcpclientd",SIG_IGN) == -1) {
      dlf_shutdown();
      exit(1);
    }
    rc = rtcpcld_main();
  }
  dlf_shutdown();
  if ( rc != 0 ) exit(1);
  else exit(0);
}

