/*
 *
 * Copyright (C) 2004 by CERN/IT/ADC/CA
 * All rights reserved
 *
 * @(#)$RCSfile: rtcpcldcommon.c,v $ $Revision: 1.12 $ $Release$ $Date: 2004/09/21 11:07:57 $ $Author: obarring $
 *
 *
 *
 * @author Olof Barring
 */

#ifndef lint
static char sccsid[] = "@(#)$RCSfile: rtcpcldcommon.c,v $ $Revision: 1.12 $ $Release$ $Date: 2004/09/21 11:07:57 $ Olof Barring";
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
                               TAPE_WAITVDQM,
                               TAPE_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDStatus(tape,
                               TAPE_WAITMOUNT,
                               TAPE_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDStatus(tape,
                               TAPE_MOUNTED,
                               TAPE_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDFileStatus(tape,
                                   SEGMENT_WAITFSEQ,
                                   SEGMENT_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDFileStatus(tape,
                                   SEGMENT_WAITPATH,
                                   SEGMENT_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDFileStatus(tape,
                                   SEGMENT_WAITCOPY,
                                   SEGMENT_FAILED) == -1 ) failed++;
  if ( rtcpcld_updateVIDFileStatus(tape,
                                   SEGMENT_COPYRUNNING,
                                   SEGMENT_FAILED) == -1 ) failed++;

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
