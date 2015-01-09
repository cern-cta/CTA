#include "castor/vdqm/CDevTools.hpp"
#include "h/rtcp_constants.h"
#include "h/vdqm_constants.h"

#include <stdio.h>
#include <arpa/inet.h>
#include <sys/socket.h>


void vdqmCDevToolsPrintIpAndPort(const uint32_t ip, const int port) {
  printf("%d.%d.%d.%d:%d",
    (int)((ip >> 24) & 0x000000FF),
    (int)((ip >> 16) & 0x000000FF),
    (int)((ip >>  8) & 0x000000FF),
    (int)( ip        & 0x000000FF),
    port);
}


const char *castorMagicNb2Str(const uint32_t magic) {
  switch(magic) {
  case 0x00000200 : return "B_RFIO_MAGIC";
  case 0x030E1301 : return "CNS_MAGIC";
  case 0x030E1302 : return "CNS_MAGIC2";
  case 0x030E1303 : return "CNS_MAGIC3";
  case 0x030E1304 : return "CNS_MAGIC4";
  case 0x8CA00001 : return "CSEC_CONTEXT_MAGIC_CLIENT_1";
  case 0x80000000 : return "CSEC_CONTEXT_MAGIC_CLIENT_MASK";
  case 0x0CA00001 : return "CSEC_CONTEXT_MAGIC_SERVER_1";
  case 0x0000CA03 : return "CSEC_TOKEN_MAGIC_1";
  case 0x000CA001 : return "CSP_MSG_MAGIC";
  case 0x07770777 : return "CUPV_MAGIC";
  case 0x0010aa5b : return "C_MAGIC";
  case 0x68767001 : return "DLF_MAGIC";
  case 0x68777001 : return "EXP_MAGIC";
  case 0x00000110 : return "RFIO2TPREAD_MAGIC";
  case 0x00000100 : return "RFIO_MAGIC";
  case 0x120D0301 : return "RMC_MAGIC";
  case 0x00000669 : return "RTCOPY_MAGIC";
  case 0x00000668 : return "RTCOPY_MAGIC_OLD0";
  case 0x00000667 : return "RTCOPY_MAGIC_SHIFT";
  case 0x00000666 : return "RTCOPY_MAGIC_VERYOLD";
  case 0x24140701 : return "STAGER_MAGIC";
  case 0x34140701 : return "STAGER_NOTIFY_MAGIC1";
  case 0x34140702 : return "STAGER_NOTIFY_MAGIC2";
  case 0x13140701 : return "STGMAGIC";
  case 0x13140702 : return "STGMAGIC2";
  case 0x13140703 : return "STGMAGIC3";
  case 0x13140704 : return "STGMAGIC4";
  case 0x00141001 : return "TPMAGIC";
  case 0x00008537 : return "VDQM_MAGIC";
  case 0x766D6701 : return "VMGR_MAGIC";
  case 0x766D6702 : return "VMGR_MAGIC2";
  default         : return "UNKNOWN";
  }
}


const char *rtcpReqTypeToStr(const uint32_t type) {
  switch(type) {
  case RTCP_TAPE_REQ      : return "RTCP_TAPE_REQ";
  case RTCP_FILE_REQ      : return "RTCP_FILE_REQ";
  case RTCP_NOMORE_REQ    : return "RTCP_NOMORE_REQ";
  case RTCP_TAPEERR_REQ   : return "RTCP_TAPEERR_REQ";
  case RTCP_FILEERR_REQ   : return "RTCP_FILEERR_REQ";
  case RTCP_ENDOF_REQ     : return "RTCP_ENDOF_REQ";
  case RTCP_DUMP_REQ      : return "RTCP_DUMP_REQ";
  case RTCP_DUMPTAPE_REQ  : return "RTCP_DUMPTAPE_REQ";
  case RTCP_PING_REQ      : return "RTCP_PING_REQ";
  case RTCP_HAS_MORE_WORK : return "RTCP_HAS_MORE_WORK";
  case VDQM_CLIENTINFO    : return "VDQM_CLIENTINFO";
  default                 : return "UNKNOWN";
  }
}


const char *vdqmReqTypeToStr(const uint32_t type) {
  switch(type) {
  case VDQM_VOL_REQ      : return "VDQM_VOL_REQ";
  case VDQM_DRV_REQ      : return "VDQM_DRV_REQ";
  case VDQM_PING         : return "VDQM_PING";
  case VDQM_CLIENTINFO   : return "VDQM_CLIENTINFO";
  case VDQM_REPLICA      : return "VDQM_REPLICA";
  case VDQM_RESCHEDULE   : return "VDQM_RESCHEDULE";
  case VDQM_ROLLBACK     : return "VDQM_ROLLBACK";
  case VDQM_COMMIT       : return "VDQM_COMMIT";
  case VDQM_DEL_VOLREQ   : return "VDQM_DEL_VOLREQ";
  case VDQM_DEL_DRVREQ   : return "VDQM_DEL_DRVREQ";
  case VDQM_GET_VOLQUEUE : return "VDQM_GET_VOLQUEUE";
  case VDQM_GET_DRVQUEUE : return "VDQM_GET_DRVQUEUE";
  case VDQM_SET_PRIORITY : return "VDQM_SET_PRIORITY";
  case VDQM_DEDICATE_DRV : return "VDQM_DEDICATE_DRV";
  case VDQM_HANGUP       : return "VDQM_HANGUP";
  default                : return "UNKNOWN";
  }
}


const char *castorReqTypeToStr(const uint32_t magic, const uint32_t type) {
  switch(magic) {
  case 0x00000669 : return rtcpReqTypeToStr(type); // RTCOPY_MAGIC
  case 0x00000668 : return rtcpReqTypeToStr(type); // RTCOPY_MAGIC_OLD0
  case 0x00000667 : return rtcpReqTypeToStr(type); // RTCOPY_MAGIC_SHIFT
  case 0x00000666 : return rtcpReqTypeToStr(type); // RTCOPY_MAGIC_VERYOLD
  case 0x00008537 : return vdqmReqTypeToStr(type); // VDQM_MAGIC
  default         : return "UNKNOWN";
  }
}


void printCastorMessage(const int messageWasSent,
  const int messageInNetworkByteOrder, const int socket, void* hdrbuf) {
  uint32_t           magic     = 0;
  uint32_t           reqtype   = 0;
  unsigned int       soutlen   = sizeof(struct sockaddr_in);
  struct sockaddr_in sout;
  unsigned short     localPort = 0;
  unsigned long      localIp   = 0;
  unsigned short     peerPort  = 0;
  unsigned long      peerIp    = 0;

  if(messageInNetworkByteOrder) {
    magic   = ntohl(*((uint32_t *)hdrbuf));
    reqtype = ntohl(*(((uint32_t *)hdrbuf) + 1));
  } else {
    magic   = *((uint32_t *)hdrbuf);
    reqtype = *(((uint32_t *)hdrbuf) + 1);
  }

  if (getsockname(socket, (struct sockaddr*)&sout, &soutlen) < 0) {
    printf("Unable to get local name\n");
    return;
  }

  localPort = ntohs(sout.sin_port);
  localIp   = ntohl(sout.sin_addr.s_addr);

  if(getpeername(socket, (struct sockaddr*)&sout, &soutlen) < 0) {
    printf("Unable to get peer name\n");
    return;
  }

  peerPort = ntohs(sout.sin_port);
  peerIp   = ntohl(sout.sin_addr.s_addr);

  if(messageWasSent) {
    printf("Tx ");
    vdqmCDevToolsPrintIpAndPort(localIp, localPort);
    printf("->");
    vdqmCDevToolsPrintIpAndPort(peerIp, peerPort);
  } else {
    printf("Rx ");
    vdqmCDevToolsPrintIpAndPort(peerIp, peerPort);
    printf("->");
    vdqmCDevToolsPrintIpAndPort(localIp, localPort);
  }

  printf(" %s %s\n", castorMagicNb2Str(magic),
    castorReqTypeToStr(magic,reqtype));
}
