/******************************************************************************
 *                      rtcpcldcommon.c
 *
 * This file is part of the Castor project.
 * See http://castor.web.cern.ch/castor
 *
 * Copyright (C) 2003  CERN
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 *
 * @(#)$RCSfile: rtcpcldcommon.c,v $ $Revision: 1.14 $ $Release$ $Date: 2004/10/12 08:37:34 $ $Author: obarring $
 *
 * 
 *
 * @author Olof Barring
 *****************************************************************************/

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldcommon.c,v $ $Revision: 1.14 $ $Release$ $Date: 2004/10/12 08:37:34 $ Olof Barring";
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
#include <sys/time.h>
#endif /* _WIN32 */
#include <sys/stat.h>
#include <errno.h>
#include <netdb.h>
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
#include <Cpwd.h>
#include <Cnetdb.h>
#include <dlf_api.h>
#include <rtcp_constants.h>
#include <vdqm_api.h>
#include <Cnetdb.h>
#include <castor/stager/TapeStatusCodes.h>
#include <castor/stager/SegmentStatusCodes.h>
#include <rtcp.h>
#include <rtcp_server.h>
#include <rtcp_api.h>
#include <rtcpcld_constants.h>
#include <rtcpcld.h>
#define RTCPCLD_COMMON
#include <rtcpcld_messages.h>
extern char *getconfent _PROTO((char *, char *, int));
extern int rtcpc_runReq_ext _PROTO((
                                    rtcpHdr_t *,
                                    rtcpc_sockets_t **,
                                    tape_list_t *,
                                    int (*)(void *(*)(void *), void *)
                                    ));
extern int (*rtcpc_ClientCallback) _PROTO((
                                           rtcpTapeRequest_t *, 
                                           rtcpFileRequest_t *
                                           ));
extern int inChild;
extern Cuuid_t mainUuid;
extern Cuuid_t childUuid;

/*
 * Only used when rtcpcld calls RTCOPY or other libraries
 * that may do internal logging.
 */
void rtcpcld_extlog(
                    int level,
                    CONST char *format, 
                    ...
                    )
{
  int msgNo, dlfLevel;
  char tmpbuf[CA_MAXLINELEN+1], *p;
  va_list args;
  va_start(args,format);
  vsprintf(tmpbuf,format,args);
  va_end(args);
  /*
   * strip off newlines (not good for DLF)
   */
  while ((p = strchr(tmpbuf,'\n')) != NULL) *p = ' ';
  while ((p = strchr(tmpbuf,'\t')) != NULL) *p = ' ';
  if ( level >= LOG_INFO ) {
    msgNo = RTCPCLD_MSG_EXTINFO;
    dlfLevel = DLF_LVL_DEBUG;
  } else {
    msgNo = RTCPCLD_MSG_EXTERR;
    dlfLevel = DLF_LVL_ERROR;
  }
  (void)dlf_write(
                  (inChild == 0 ? mainUuid : childUuid),
                  dlfLevel,
                  msgNo,
                  (struct Cns_fileid *)NULL,
                  1,
                  "RTCP_LOG",
                  DLF_MSG_PARAM_STR,
                  tmpbuf
                  );
  return;
}

int rtcpcld_initLogging(
                        facilityName
                        )
     char *facilityName;
{
  int rc, i;
  char dlfErrBuf[1024];
  extern dlf_facility_info_t g_dlf_fac_info;
  
  
  (void)dlf_seterrbuf(dlfErrBuf,sizeof(dlfErrBuf)-1);
  rc = dlf_init(facilityName);
  
  i=-1;
  while ( *rtcpcldMessages[++i].messageTxt != '\0' ) {
    (void)dlf_add_to_text_list(rtcpcldMessages[i].msgNo,
                               rtcpcldMessages[i].messageTxt,
                               &g_dlf_fac_info.text_list);
    (void)dlf_entertext(
                        facilityName,
                        rtcpcldMessages[i].msgNo,
                        rtcpcldMessages[i].messageTxt
                        );
  }
  rtcp_log = rtcpcld_extlog;
  return(0);
}

static int getServerNotifyAddr(
                               serverHost,
                               serverHostNameLen,
                               serverPort
                               )
     char *serverHost;
     int serverHostNameLen, *serverPort;
{
  char *p;
  struct servent *sp;
  
  if ( serverPort != NULL ) {
    if ( (p = getenv("RTCPCLD_NOTIFY_PORT")) != NULL ) {
      *serverPort = atoi(p);
    } else if ( (p = getconfent("RTCPCLD","NOTIFY_PORT",0)) != NULL ) {
      *serverPort = atoi(p);
    } else if ( (sp = Cgetservbyname("rtcpcld","udp")) != 
                (struct servent *)NULL ) {
      *serverPort = (int)ntohs(sp->s_port);
    } else {
#ifdef RTCPCLD_NOTIFY_PORT
      *serverPort = RTCPCLD_NOTIFY_PORT;
#else /* RTCPCLD_NOTIFY_PORT */
      *serverPort = -1;
#endif /* RTCPCLD_NOTIFY_PORT */
    }
  }
  if ( (serverHost != NULL) && (serverHostNameLen > 0) ) {
    serverHost[serverHostNameLen-1] = '\0';
    if ( (p = getenv("RTCPCLD_NOTIFY_HOST")) != NULL ) {
      strncpy(serverHost,p,serverHostNameLen-1);
    } else if ( (p = getconfent("RTCPCLD","NOTIFY_HOST",0)) != NULL ) {
      strncpy(serverHost,p,serverHostNameLen-1);
    } else {
#ifdef RTCPCLD_NOTIFY_HOST
      strncpy(serverHost,RTCPCLD_NOTIFY_HOST,serverHostNameLen-1);
#else /* RTCPCLD_NOTIFY_PORT */
      *serverHost = '\0';
#endif /* RTCPCLD_NOTIFY_PORT */
    }
  }
  return(0);
}

int rtcpcld_initNotifyByPort(
                             notificationSocket,
                             usePort
                             )
     SOCKET **notificationSocket;
     int *usePort;
{
  struct sockaddr_in sin;
#if defined(_WIN32)
  SOCKET tmp;
#endif /* _WIN32 */
  int rc, len, notificationPort = -1;

  if ( notificationSocket == NULL ) {
    serrno = EINVAL;
    return(-1);
  }

  if ( usePort != NULL ) notificationPort = *usePort;

#if defined(_WIN32)
  /*
   * Do a dummy test to see if WinSock DLL has been initialized
   */
  tmp = socket(AF_INET,SOCK_STREAM,0);
  if ( tmp == INVALID_SOCKET ) {
    if ( GetLastError() == WSANOTINITIALISED ) {
      /* Initialize the WinSock DLL */
      rc = WSAStartup(MAKEWORD(2,0), &wsadata);
      if ( rc ) {
        serrno = SECOMERR;
        return(-1);
      }
    } else {
      serrno = SECOMERR;
      return(-1);
    }
  } else closesocket(tmp);
#endif /* _WIN32 */

  if ( notificationPort == -1 ) {
    getServerNotifyAddr(NULL,-1,&notificationPort);
    if ( notificationPort == -1 ) return(-1);
  }
  
  *notificationSocket = (SOCKET *)malloc(sizeof(SOCKET));
  if ( *notificationSocket == NULL ) {
    serrno = SESYSERR;
    return(-1);
  }
  errno = serrno = 0;
  **notificationSocket = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if ( **notificationSocket == INVALID_SOCKET ) {
    serrno = SECOMERR;
    return(-1);
  }
  memset(&sin,'\0',sizeof(sin));
  sin.sin_family = AF_INET;
  sin.sin_port = htons((unsigned short)notificationPort);
  sin.sin_addr.s_addr = htonl(INADDR_ANY);
  rc = bind(**notificationSocket,(struct sockaddr *)&sin,sizeof(sin));
  if ( rc == -1 ) {
    serrno = SECOMERR;
    return(-1);
  }

  if ( usePort != NULL ) {
    len = sizeof(sin);
    rc = getsockname(**notificationSocket,(struct sockaddr *)&sin,&len);
    if ( rc == SOCKET_ERROR ) {
      serrno = SECOMERR;
      return(-1);
    }
    *usePort = (int)ntohs(sin.sin_port);    
  }
  
  return(0);
}

int rtcpcld_initNotify(
                       notificationSocket
                       )
     SOCKET **notificationSocket;
{
  return(rtcpcld_initNotifyByPort(notificationSocket,NULL));
}

int rtcpcld_getNotifyWithAddr(
                              s,
                              fromAddrStr
                              )
     SOCKET s;
     char *fromAddrStr;
{
  struct sockaddr_in from;
  fd_set rd_set, rd_setcp;
  struct timeval timeout;
  int maxfd, rc, fromlen;
  char buf[CA_MAXLINELEN + 1];
  struct hostent *hp;

  FD_ZERO(&rd_set);
  FD_SET(s,&rd_set);
  maxfd = 0;
#ifndef _WIN32
  maxfd = s + 1;
#endif /* _WIN32 */
  for (;;) {
    rd_setcp = rd_set;
    timeout.tv_sec = 0;
    timeout.tv_usec = 0;
    rc = select(maxfd,&rd_setcp,NULL,NULL,&timeout);
    if ( rc <= 0 ) break;
    fromlen = sizeof(from);
    rc = recvfrom(s,buf,sizeof(buf),0,(struct sockaddr *)&from,&fromlen);
    if ( rc == -1 ) break;
    if ( fromAddrStr != NULL ) {
      hp = Cgethostbyaddr((void *)&(from.sin_addr),sizeof(struct in_addr),
                          from.sin_family);
      if ( hp != NULL ) {
        sprintf(fromAddrStr,"%s:%d",hp->h_name,ntohs(from.sin_port));  
      }
    }
  }
  return(0);
}

int rtcpcld_getNotify(
                      s
                      )
     SOCKET s;
{
  return(rtcpcld_getNotifyWithAddr(s,NULL));
}

int rtcpcld_sendNotifyByAddr(
                             toHost,
                             toPort
                             )
     char *toHost;
     int toPort;
{
  struct sockaddr_in to;
  struct hostent *hp;
  SOCKET s;
  char byteToSend = '\0';
  int rc;

  s = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);
  if ( s == INVALID_SOCKET ) {
    serrno = SECOMERR;
    return(-1);
  }

  hp = Cgethostbyname(toHost);
  if ( hp == NULL ) {
    serrno = SECOMERR;
    return(-1);
  }

  memset(&to,'\0',sizeof(to));
  to.sin_family = AF_INET;
  to.sin_port = htons((unsigned short)toPort);
  to.sin_addr.s_addr = ((struct in_addr *)(hp->h_addr))->s_addr;

  rc = sendto(s,&byteToSend,1,MSG_DONTWAIT,(struct sockaddr *)&to,sizeof(to));
  if ( rc == -1 ) {
    serrno = SECOMERR;
    return(-1);
  }

  closesocket(s);
  return(0);
}

int rtcpcld_sendNotify(
                       toAddrStr
                       )
     char *toAddrStr;
{
  int toPort = -1;
  char toHost[CA_MAXHOSTNAMELEN+1], *p;

  if ( (toAddrStr == NULL) || (*toAddrStr == '\0') ) {
    serrno = EINVAL;
    return(-1);
  }

  /*
   * toAddrStr format is host:port
   */
  strcpy(toHost,toAddrStr);
  p = strstr(toHost,":");
  if ( p == NULL ) {
    serrno = EINVAL;
    return(-1);
  }
  *p = '\0';
  toPort = atoi(++p);

  return(rtcpcld_sendNotifyByAddr(toHost,toPort));
}

int rtcpcld_notifyRtcpclientd() 
{
  char toHost[CA_MAXHOSTNAMELEN+1];
  int toPort, rc;
  
  rc = getServerNotifyAddr(toHost,sizeof(toHost),&toPort);
  if ( rc == -1 ) return(-1);
  return(rtcpcld_sendNotifyByAddr(toHost,toPort));
}

int rtcpcld_setVIDFailedStatus(
                               tape
                               ) 
     tape_list_t *tape;
{
  int failed = 0;

  if ( rtcpcld_updateVIDStatus(tape,
                               TAPE_WAITDRIVE,
                               TAPE_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDStatus(tape,
                               TAPE_WAITMOUNT,
                               TAPE_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDStatus(tape,
                               TAPE_MOUNTED,
                               TAPE_FAILED) == -1 ) failed++;
  if ( failed > 0 ) return(-1);
  else return(0);
}

char *rtcpcld_fixStr(
                     str
                     )
     CONST char *str; 
{
  char *retStr = NULL, *p;
  
  if ( str == NULL ) return(strdup(""));
  retStr = strdup(str);
  if ( retStr == NULL ) return(strdup(""));

  p = retStr;
  while ( (p != NULL) && (*p != '\0') ) {
    if ( !isprint(*p) ) {
      if ( isspace(*p) ) *p = ' ';  /* space includes ' ', \n, \t, ... */
      else *p = '?';
    } else if ( isspace(*p) ) *p = ' '; /* space includes ' ', \n, \t, ... */
    p++;
  }
  return(retStr);
}


int rtcpcld_validBlockid(
                         blockid
                         )
     unsigned char *blockid;
{
  unsigned char nullBlockid[4] = {'\0', '\0', '\0', '\0'};
  
  if ( blockid == NULL ) return(0);
  if ( memcmp(blockid,nullBlockid,sizeof(nullBlockid)) == 0 ) return(0);
  return(1);
}  
      
int rtcpcld_validPosition(
                          fseq,
                          blockid
                          )
     int fseq;
     unsigned char *blockid;
{
  if ( fseq > 0 ) return(1);
  else return(rtcpcld_validBlockid(blockid));
}

int rtcpcld_validPath(
                      path
                      )
     char *path;
{
  if ( (path == NULL) || (*path == '\0') || (strcmp(path,".") == 0) ) return(0);
  else return(1);
}

int rtcpcld_validFilereq(
                         filereq
                         )
     rtcpFileRequest_t *filereq;
{
  int rc;

  if ( filereq == NULL ) return(0);
  rc = rtcpcld_validPosition(
                             filereq->tape_fseq,
                             filereq->blockid
                             );
  if ( rc == 0 ) return(0);
  rc = rtcpcld_validPath(
                         filereq->file_path
                         );
  if ( rc == 0 ) return(0);
  return(1);
}

static int filereqsEqual(
                         filereq1,
                         filereq2
                         )
     rtcpFileRequest_t *filereq1, *filereq2;
{
  if ( rtcpcld_validFilereq(filereq1) == 0 ) return(0);
  if ( rtcpcld_validFilereq(filereq2) == 0 ) return(0);

  if ( filereq1->tape_fseq != filereq2->tape_fseq ) return(0);
  if ( memcmp(filereq1->blockid,
              filereq2->blockid,
              sizeof(filereq1->blockid)) != 0 ) return(0);
  if ( strncmp(filereq1->file_path,
               filereq2->file_path,
               CA_MAXHOSTNAMELEN) != 0 ) return(0);
  return(1);
}


int rtcpcld_findFile(
                     tape,
                     filereq,
                     file
                     )
     tape_list_t *tape;
     rtcpFileRequest_t *filereq;
     file_list_t **file;
{
  file_list_t *fl;
  
  if ( (tape == NULL) || (filereq == NULL) || (file == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  
  CLIST_ITERATE_BEGIN(tape->file,fl) 
    {
      if ( filereqsEqual(&(fl->filereq),filereq) == 1 ) {
        *file = fl;
        return(0);
      }
    }
  CLIST_ITERATE_END(tape->file,fl);

  serrno = ENOENT;
  return(-1);
}

static int updateClientInfo(
                            origSocket,
                            port,
                            tape
                            )
     SOCKET *origSocket;
     int port;
     tape_list_t *tape;
{
  vdqmVolReq_t volReq;
  vdqmDrvReq_t drvReq;
  struct passwd *pw;
  int rc, save_serrno;
  uid_t myUid;
  gid_t myGid;

  if ( origSocket == NULL || 
       *origSocket == INVALID_SOCKET || 
       port < 0 || 
       tape == NULL ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "invalid parameter",
                    RTCPCLD_LOG_WHERE
                    );
    serrno = EINVAL;
    return(-1);
  }
  memset(&volReq,'\0',sizeof(volReq));
  memset(&drvReq,'\0',sizeof(drvReq));

  errno = serrno = 0;
  myUid = geteuid();
  myGid = getegid();
  pw = Cgetpwuid(myUid);
  if ( pw == NULL ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_PWUID,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+1,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = SESYSERR;
    return(-1);
  }
  strncpy(
          volReq.client_name,
          pw->pw_name,
          sizeof(volReq.client_name)-1
          );
  volReq.clientUID = myUid;
  volReq.clientGID = myGid;
  (void)gethostname(
                    volReq.client_host,
                    sizeof(volReq.client_host)-1
                    );
  volReq.client_port = port;
  strncpy(
          drvReq.dgn,
          tape->tapereq.dgn,
          sizeof(drvReq.dgn)-1
          );
  volReq.VolReqID = tape->tapereq.VolReqID;
  strncpy(
          drvReq.drive,
          tape->tapereq.unit,
          sizeof(drvReq.drive)-1
          );
  errno = serrno = 0;
  rc = vdqm_SendToRTCP(
                       *origSocket,
                       &volReq,
                       &drvReq
                       );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "vdqm_SendToRTCP()",
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    sstrerror(serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }
  closesocket(
              *origSocket
              );
  *origSocket = INVALID_SOCKET;
  return(0);
}

static int addPlaceholderFile(
                              tape
                              )
     tape_list_t *tape;
{
  int rc;
  file_list_t *placeHolderFile = NULL;
  
  rc = rtcp_NewFileList(
                        &tape,
                        &placeHolderFile,
                        tape->tapereq.mode
                        );
  if ( (rc != -1) && (placeHolderFile != NULL) ) {
    placeHolderFile->filereq.concat = NOCONCAT;
    placeHolderFile->filereq.convert = ASCCONV;
    strcpy(placeHolderFile->filereq.recfm,"F");
    placeHolderFile->filereq.proc_status = RTCP_REQUEST_MORE_WORK;
  }
  return(rc);
}

int rtcpcld_runWorker(
                      tape,
                      origSocket,
                      myDispatch,
                      myCallback
                      )
     tape_list_t *tape;
     SOCKET *origSocket;
     int (*myDispatch) _PROTO((void *(*)(void *), void *));
     int (*myCallback) _PROTO((rtcpTapeRequest_t *, rtcpFileRequest_t *));
{
  rtcpc_sockets_t *socks;
  rtcpHdr_t hdr;
  tape_list_t *internalTapeList = NULL;
  file_list_t *placeHolderFile = NULL;
  int rc, port = -1, reqId, save_serrno;

  if ( (tape == NULL) || (*tape->tapereq.vid == '\0') ||
       (myCallback == NULL) ) {
    (void)dlf_write(
                    childUuid,
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
  rtcpc_ClientCallback = myCallback;

  (void)dlf_write(
                  mainUuid,
                  DLF_LVL_SYSTEM,
                  (tape->tapereq.mode == WRITE_ENABLE ?
                   RTCPCLD_MSG_MIGRATOR_STARTED :
                   RTCPCLD_MSG_RECALLER_STARTED),
                  (struct Cns_fileid *)NULL,
                  5,
                  "",
                  DLF_MSG_PARAM_UUID,
                  childUuid,
                  "",
                  DLF_MSG_PARAM_TPVID,
                  tape->tapereq.vid,
                  "MODE",
                  DLF_MSG_PARAM_INT,
                  tape->tapereq.mode,
                  "VDQMID",
                  DLF_MSG_PARAM_INT,
                  tape->tapereq.VolReqID
                  );

  /*
   * Check that there still are any requests for the VID
   */
  errno = serrno = 0;
  rc = rtcpcld_anyReqsForVID(
                             tape
                             );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "rtcpcld_anyReqsForVID()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }

  if ( rc == 0 ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_SYSTEM,
                    RTCPCLD_MSG_NOREQS,
                    (struct Cns_fileid *)NULL,
                    0
                    );
    return(0);
  }

  rc = rtcp_NewTapeList(&internalTapeList,NULL,tape->tapereq.mode);
  if ( (rc != -1) && internalTapeList != NULL ) {
    internalTapeList->tapereq = tape->tapereq;
    rc = addPlaceholderFile(internalTapeList);
  }
  
  if ( (rc == -1) || internalTapeList == NULL || 
       internalTapeList->file == NULL ) {
    save_serrno = serrno;
    (void)dlf_write(
                    (inChild == 0 ? mainUuid : childUuid),
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    (internalTapeList == NULL ? 
                     "rtcp_NewTapeList()" : 
                     "rtcp_NewFileList()"),
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }
  /*
   * For migration requests we request the first file before submitting
   * to rtcpd. This assures that the request can immediately start to copy
   * data to the rtcpd memory buffers while waiting for the tape to be
   * mounted
   */
  if ( tape->tapereq.mode == WRITE_ENABLE ) {
    rc = myCallback(
                    &(internalTapeList->tapereq),
                    &(internalTapeList->file->filereq)
                    );
    if ( rc == -1 ) {
      save_serrno = serrno;
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_INTERNAL,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "ERROR_STRING",
                      DLF_MSG_PARAM_STR,
                      "Internal callback error",
                      RTCPCLD_LOG_WHERE
                      );
      serrno = save_serrno;
      return(-1);
    } else {
      if ( internalTapeList->file->filereq.proc_status == RTCP_WAITING ) {
        rc = addPlaceholderFile(internalTapeList);
        if ( rc == -1 ) {
          LOG_SYSCALL_ERR("Cthread_mutex_lock(currentTapeFseq)");
          rc = -1;
        }
      }
    }
  }

  /*
   * Check the request and initialise our private listen socket
   * that we will use for further communication with rtcpd
   */
  errno = serrno = 0;
  rc = rtcpc_InitReq(
                     &socks,
                     &port,
                     internalTapeList
                     );
  if ( rc == -1 ) {
    save_serrno = serrno;
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_SYSCALL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "SYSCALL",
                    DLF_MSG_PARAM_STR,
                    "rtcpc_InitReq()",
                    "ERROR_STR",
                    DLF_MSG_PARAM_STR,
                    sstrerror(save_serrno),
                    RTCPCLD_LOG_WHERE
                    );
    serrno = save_serrno;
    return(-1);
  }

  if ( origSocket != NULL ) {
    /*
     * Send the new address to the rtcpd server. The original vdqm request
     * contained the rtcpclientd port, which we cannot use.
     */
    errno = serrno = 0;
    rc = updateClientInfo(
                          origSocket,
                          port,
                          internalTapeList
                          );
    if ( rc == -1 ) return(-1);  /* error already logged */
  } else {
    /*
     * VDQM not yet called when running in standalone mode.
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
      save_serrno = serrno;
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_VDQM,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+2,
                      "SYSCALL",
                      DLF_MSG_PARAM_STR,
                      "vdqm_SendVolReq()",
                      "ERROR_STR", 
                      DLF_MSG_PARAM_STR,
                      sstrerror(save_serrno),
                      RTCPCLD_LOG_WHERE
                      );
      serrno = save_serrno;
      return(-1);
    }
  }

  /*
   * Put the vwAddress in the database to allow client to send abort
   */
  (void)rtcpcld_setVidWorkerAddress(tape,port);
  
  /*
   * Run the request;
   */

  reqId = tape->tapereq.VolReqID;
  rc = rtcpc_SelectServer(
                          &socks,
                          internalTapeList,
                          NULL,
                          port,
                          &reqId
                          );
  if ( rc == -1 ) return(-1);
  hdr.magic = RTCOPY_MAGIC;
  rc = rtcpc_sendReqList(
                         &hdr,
                         &socks,
                         internalTapeList
                         );
  if ( rc == -1 ) return(-1);
  rc = rtcpc_sendEndOfReq(
                          &hdr,
                          &socks,
                          internalTapeList
                          );
  if ( rc == -1 ) return(-1);
  rc = rtcpc_runReq_ext(
                        &hdr,
                        &socks,
                        internalTapeList,
                        myDispatch
                        );
  return(rc);
}

static int checkArgs(
                     vid,
                     dgn,
                     lbltype,
                     density,
                     unit,
                     vdqmVolReqID
                     )
     char *vid, *dgn, *lbltype, *density, *unit;
     int vdqmVolReqID;
{
  int rc = 0;

  if ( vid == NULL || strlen(vid) > CA_MAXVIDLEN ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "VID wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (vid != NULL ? vid : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }
  if ( (dgn == NULL && vdqmVolReqID > 0) || 
       (dgn != NULL && strlen(dgn) > CA_MAXDGNLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "DGN wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (dgn != NULL ? dgn : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }
  if ( (lbltype == NULL && vdqmVolReqID > 0) || 
       (lbltype != NULL && strlen(lbltype) > CA_MAXLBLTYPLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Label type wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (lbltype != NULL ? lbltype : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }
  if ( (density == NULL && vdqmVolReqID > 0) || 
       (density != NULL && strlen(density) > CA_MAXDENLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Density wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (density != NULL ? density : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }

  if ( (unit == NULL && vdqmVolReqID > 0) || 
       (unit != NULL && strlen(unit) > CA_MAXUNMLEN) ) {
    (void)dlf_write(
                    childUuid,
                    DLF_LVL_ERROR,
                    RTCPCLD_MSG_INTERNAL,
                    (struct Cns_fileid *)NULL,
                    RTCPCLD_NB_PARAMS+2,
                    "ERROR_STRING",
                    DLF_MSG_PARAM_STR,
                    "Tape unit wrongly specified",
                    "VALUE",
                    DLF_MSG_PARAM_STR,
                    (unit != NULL ? unit : "(null)"),
                    RTCPCLD_LOG_WHERE
                    );
    rc = -1;
  }

  return(rc);
}

static int initTapeReq(
                       tape,
                       vid,
                       side,
                       dgn,
                       density,
                       label,
                       unit,
                       vdqmVolReqID
                       )
     tape_list_t *tape;
     char *vid, *dgn, *density, *label, *unit;
     int side, vdqmVolReqID;
{
  if ( (tape == NULL) || (vid == NULL) ) {
    serrno = EINVAL;
    return(-1);
  }
  
  strncpy(
          tape->tapereq.vid,
          vid,
          sizeof(tape->tapereq.vid)-1
          );
  if ( dgn != NULL ) {
    strncpy(
            tape->tapereq.dgn,
            dgn,
            sizeof(tape->tapereq.dgn)-1
            );
  }
  if ( density != NULL ) {
    strncpy(
            tape->tapereq.density,
            density,
            sizeof(tape->tapereq.density)-1
            );
  }
  if ( label != NULL ) {
    strncpy(
            tape->tapereq.label,
            label,
            sizeof(tape->tapereq.label)-1
            );
  }
  if ( unit != NULL ) {
      strncpy(
            tape->tapereq.unit,
            unit,
            sizeof(tape->tapereq.unit)-1
            );
  }

  tape->tapereq.VolReqID = vdqmVolReqID;
  
  return(0);
}

int rtcpcld_parseWorkerCmd(
                           argc,
                           argv,
                           tape,
                           sock
                           )
     int argc;
     char **argv;
     tape_list_t *tape;
     SOCKET *sock;
{
  char *vid = NULL, *dgn = NULL, *lbltype = NULL, *density = NULL;
  char  *unit = NULL, *mainUuidStr = NULL, optstr[3];
  file_list_t *fl;
  int i, vdqmVolReqID = -1, c, rc, side = 0, tStartRequest = 0;
  ID_TYPE key;
  
  if ( (argc < 0) || (argv == NULL) || (tape == NULL) || (sock == NULL) ) {
    serrno =EINVAL;
    return(-1);
  }
  
  Coptind = 1;
  Copterr = 1;

  while ( (c = Cgetopt(argc, argv, "d:f:g:i:k:l:s:S:u:U:T:V:")) != -1 ) {
    switch (c) {
    case 'd':
      /*
       * Tape media density. If not provided, it will be taken from VMGR
       */
      density = Coptarg;
      break;
    case 'f':
      /*
       * Start tape file sequence number (normally only migration)
       */
      rc = addPlaceholderFile(tape);
      fl = tape->file;
      if ( (rc == -1) || (fl == NULL) ) {
        LOG_SYSCALL_ERR("rtcp_NewFileList()");
        return(-1);
      }
      fl->filereq.tape_fseq = atoi(Coptarg);
      fl->filereq.proc_status = RTCP_WAITING;
      break;
    case 'g':
      /* 
       * Device group name. If not provided, it will be taken from VMGR
       */
      dgn = Coptarg;
      break;
    case 'i':
      /*
       * VDQM Volume Request ID. This argument is passed by the rtcpclientd
       * daemon, which starts the VidWorker only when the tape request has
       * started on the tape server. If not provided, the VidWorker will
       * submit its own request to VDQM. This allows for stand-alone running
       * (and debugging) of the VidWorker program.
       */
      vdqmVolReqID = atoi(Coptarg);
      break;
    case 'k':
      key = atoll(Coptarg);
      tape->dbRef = (RtcpDBRef_t *)calloc(1,sizeof(RtcpDBRef_t));
      if ( tape->dbRef == NULL ) {
        LOG_SYSCALL_ERR("calloc()");
        return(-1);
      }
      tape->dbRef->key = key;
      break;
    case 'l':
      /*
       * Tape label type. If not provided, it will be taken from VMGR
       */
      lbltype = Coptarg;
      break;
    case 's':
      /*
       * Media side (forseen for future support for two sided medias).
       * Defaults to 0
       */
      side = atoi(Coptarg);
      break;
    case 'S':
      /*
       * Parent decided to use a different socket than 0 .
       * Useful when running with Insure...
       */
      *sock = atoi(Coptarg);
      break;
    case 'U':
      /*
       * UUID of the rtcpclientd daemon that started this worker
       * Used only for logging of UUID association.
       */
      mainUuidStr = Coptarg;
      serrno = 0;
      rc = rtcp_stringToVoid(
                             mainUuidStr,
                             &mainUuid,
                             sizeof(mainUuid)
                             );
      if ( rc == -1 ) {
        if ( serrno == 0 ) serrno = errno;
        (void)dlf_write(
                        childUuid,
                        DLF_LVL_ERROR,
                        RTCPCLD_MSG_SYSCALL,
                        (struct Cns_fileid *)NULL,
                        RTCPCLD_NB_PARAMS+2,
                        "SYSCALL",
                        DLF_MSG_PARAM_STR,
                        "rtcp_stringToVoid()",
                        "ERROR_STR",
                        DLF_MSG_PARAM_STR,
                        sstrerror(serrno),
                        RTCPCLD_LOG_WHERE
                        );
        return(1);
      }
      break;
    case 'u':
      unit = Coptarg;
      break;
    case 'T':
      tStartRequest = atoi(Coptarg);
      break;
    case 'V':
      /*
       * Tape VID. This argument is required
       */
      vid = Coptarg;
      break;
    default:
      sprintf(optstr,"-%c",c);
      (void)dlf_write(
                      childUuid,
                      DLF_LVL_ERROR,
                      RTCPCLD_MSG_UNKNOPT,
                      (struct Cns_fileid *)NULL,
                      RTCPCLD_NB_PARAMS+1,
                      "OPTION",
                      DLF_MSG_PARAM_STR,
                      optstr,
                      RTCPCLD_LOG_WHERE
                      );
      return(2);
      break;
    }
  }

  rc = checkArgs(
                 vid,
                 dgn,
                 lbltype,
                 density,
                 unit,
                 vdqmVolReqID
                 );
  if ( rc == -1 ) return(-1);
  rc = initTapeReq(
                   tape,
                   vid,
                   side,
                   dgn,
                   density,
                   lbltype,
                   unit,
                   vdqmVolReqID
                   );
  if ( rc == -1 ) return(-1);
  return(0);
}

static void setErrorInfo(
                         tape,
                         errorCode,
                         errMsgTxt
                         )
     tape_list_t *tape;
     char *errMsgTxt;
     int errorCode;
{
  file_list_t *fl;
  int rc = 0, segmFailed = 0;
  
  if ( tape == NULL ) return;
  
  if ( (rc == 1) && (segmFailed == 0) ) {
    if ( tape->tapereq.tprc == 0 ) tape->tapereq.tprc = -1;
    if ( tape->tapereq.err.errorcode == 0 ) 
      tape->tapereq.err.errorcode = errorCode;
    if ( *tape->tapereq.err.errmsgtxt == '\0' ) {
      if ( errMsgTxt != NULL ) {
        strncpy(tape->tapereq.err.errmsgtxt,
                errMsgTxt,
                sizeof(tape->tapereq.err.errmsgtxt)-1);
      } else {
        strncpy(tape->tapereq.err.errmsgtxt,
                sstrerror(errorCode),
                sizeof(tape->tapereq.err.errmsgtxt)-1);
      }
    }
    if ( (tape->tapereq.err.severity == RTCP_OK) ||
         (tape->tapereq.err.severity == 0 ) ) {
      tape->tapereq.err.severity = RTCP_UNERR|RTCP_FAILED;
    }
  }
  return;  
}

int rtcpcld_workerFinished(
                           tape,
                           workerRC,
                           workerErrorCode
                           )
     tape_list_t *tape;
     int workerRC, workerErrorCode;
{
  char *shiftMsg = NULL;
  int retval;

  if ( workerRC == -1 ) {
    (void)rtcp_RetvalSHIFT(tape,NULL,&retval);
    if ( retval == 0 ) retval = UNERR;
  }
  switch (retval) {
  case 0:
    shiftMsg = strdup("command successful");
    break;
  case RSLCT:
    shiftMsg = strdup("Re-selecting another tape server");
    break;
  case BLKSKPD:
    shiftMsg = strdup("command partially successful since blocks were skipped");
    break;
  case TPE_LSZ:
    shiftMsg = strdup("command partially successful: blocks skipped and size limited by -s option");
    break;
  case MNYPARY:
    shiftMsg = strdup("command partially successful: blocks skipped or too many errors on tape");
    break;
  case LIMBYSZ:
    shiftMsg = strdup("command successful");
    break;
  default:
    shiftMsg = strdup("command failed");
    break;
  }  
  (void)dlf_write(
                  childUuid,
                  DLF_LVL_SYSTEM,
                  (tape->tapereq.mode == WRITE_ENABLE ?
                   RTCPCLD_MSG_MIGRATOR_ENDED :
                   RTCPCLD_MSG_RECALLER_ENDED),
                  (struct Cns_fileid *)NULL,
                  6,
                  "",
                  DLF_MSG_PARAM_TPVID,
                  tape->tapereq.vid,
                  "MODE",
                  DLF_MSG_PARAM_INT,
                  tape->tapereq.mode,
                  "VDQMID",
                  DLF_MSG_PARAM_INT,
                  tape->tapereq.VolReqID,
                  "rtcpRC",
                  DLF_MSG_PARAM_INT,
                  workerRC,
                  "serrno",
                  DLF_MSG_PARAM_INT,
                  workerErrorCode,
                  "SHIFTMSG",
                  DLF_MSG_PARAM_STR,
                  shiftMsg
                  );
  if ( workerRC == -1 ) {
    setErrorInfo(tape,workerErrorCode,shiftMsg);
    (void)rtcpcld_setVIDFailedStatus(tape);
  } else {
    (void)rtcpcld_updateVIDStatus(
                                  tape,
                                  TAPE_PENDING,
                                  TAPE_FINISHED
                                  );
    (void)rtcpcld_updateVIDStatus(
                                  tape,
                                  TAPE_WAITDRIVE,
                                  TAPE_FINISHED
                                  );
    (void)rtcpcld_updateVIDStatus(
                                  tape,
                                  TAPE_WAITMOUNT,
                                  TAPE_FINISHED
                                  );
    (void)rtcpcld_updateVIDStatus(
                                  tape,
                                  TAPE_MOUNTED,
                                  TAPE_FINISHED
                                  );
  }
  if ( shiftMsg != NULL ) free(shiftMsg);
  return(retval);
}
