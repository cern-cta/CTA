#include "castor/vdqm/CDevTools.h"

#include <stdio.h>
#include <arpa/inet.h>
#include <sys/socket.h>


void printIpAndPort(const uint32_t ip, const int port) {
  printf("%d.%d.%d.%d:%d",
    (int)((ip >> 24) & 0x000000FF),
    (int)((ip >> 16) & 0x000000FF),
    (int)((ip >>  8) & 0x000000FF),
    (int)( ip        & 0x000000FF),
    port);
}

void printMessage(
  const int messageWasSent, const int messageInNetworkByteOrder,
  const int socket, void* hdrbuf) {
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
    printIpAndPort(localIp, localPort);
    printf("->");
    printIpAndPort(peerIp, peerPort);
  } else {
    printf("Rx ");
    printIpAndPort(peerIp, peerPort);
    printf("->");
    printIpAndPort(localIp, localPort);
  }

  printf(" ");

  switch(magic) {
  case 0x00000200 : printf("B_RFIO_MAGIC")                  ; break;
  case 0x030E1301 : printf("CNS_MAGIC")                     ; break;
  case 0x030E1302 : printf("CNS_MAGIC2")                    ; break;
  case 0x030E1303 : printf("CNS_MAGIC3")                    ; break;
  case 0x030E1304 : printf("CNS_MAGIC4")                    ; break;
  case 0x8CA00001 : printf("CSEC_CONTEXT_MAGIC_CLIENT_1")   ; break;
  case 0x80000000 : printf("CSEC_CONTEXT_MAGIC_CLIENT_MASK"); break;
  case 0x0CA00001 : printf("CSEC_CONTEXT_MAGIC_SERVER_1")   ; break;
  case 0x0000CA03 : printf("CSEC_TOKEN_MAGIC_1")            ; break;
  case 0x000CA001 : printf("CSP_MSG_MAGIC")                 ; break;
  case 0x07770777 : printf("CUPV_MAGIC")                    ; break;
  case 0x0010aa5b : printf("C_MAGIC")                       ; break;
  case 0x68767001 : printf("DLF_MAGIC")                     ; break;
  case 0x68777001 : printf("EXP_MAGIC")                     ; break;
  case 0x00000110 : printf("RFIO2TPREAD_MAGIC")             ; break;
  case 0x00000100 : printf("RFIO_MAGIC")                    ; break;
  case 0x120D0301 : printf("RMC_MAGIC")                     ; break;
  case 0x00000669 : printf("RTCOPY_MAGIC")                  ; break;
  case 0x00000668 : printf("RTCOPY_MAGIC_OLD0")             ; break;
  case 0x00000667 : printf("RTCOPY_MAGIC_SHIFT")            ; break;
  case 0x00000666 : printf("RTCOPY_MAGIC_VERYOLD")          ; break;
  case 0x24140701 : printf("STAGER_MAGIC")                  ; break;
  case 0x34140701 : printf("STAGER_NOTIFY_MAGIC1")          ; break;
  case 0x34140702 : printf("STAGER_NOTIFY_MAGIC2")          ; break;
  case 0x13140701 : printf("STGMAGIC")                      ; break;
  case 0x13140702 : printf("STGMAGIC2")                     ; break;
  case 0x13140703 : printf("STGMAGIC3")                     ; break;
  case 0x13140704 : printf("STGMAGIC4")                     ; break;
  case 0x00141001 : printf("TPMAGIC")                       ; break;
  case 0x00008537 : printf("VDQM_MAGIC")                    ; break;
  case 0x766D6701 : printf("VMGR_MAGIC")                    ; break;
  case 0x766D6702 : printf("VMGR_MAGIC2")                   ; break;
  default         : printf("UNKNOWN");
  }

  printf(" ");

  switch(reqtype) {
  case VDQM_VOL_REQ     : printf("VDQM_VOL_REQ");      break;
  case VDQM_DRV_REQ     : printf("VDQM_DRV_REQ");      break;
  case VDQM_PING        : printf("VDQM_PING");         break;
  case VDQM_CLIENTINFO  : printf("VDQM_CLIENTINFO");   break;
  case VDQM_SHUTDOWN    : printf("VDQM_SHUTDOWN");     break;
  case VDQM_HOLD        : printf("VDQM_HOLD");         break;
  case VDQM_RELEASE     : printf("VDQM_RELEASE");      break;
  case VDQM_REPLICA     : printf("VDQM_REPLICA");      break;
  case VDQM_RESCHEDULE  : printf("VDQM_RESCHEDULE");   break;
  case VDQM_ROLLBACK    : printf("VDQM_ROLLBACK");     break;
  case VDQM_COMMIT      : printf("VDQM_COMMIT");       break;
  case VDQM_DEL_VOLREQ  : printf("VDQM_DEL_VOLREQ");   break;
  case VDQM_DEL_DRVREQ  : printf("VDQM_DEL_DRVREQ");   break;
  case VDQM_GET_VOLQUEUE: printf("VDQM_GET_VOLQUEUE"); break;
  case VDQM_GET_DRVQUEUE: printf("VDQM_GET_DRVQUEUE"); break;
  case VDQM_SET_PRIORITY: printf("VDQM_SET_PRIORITY"); break;
  case VDQM_DEDICATE_DRV: printf("VDQM_DEDICATE_DRV"); break;
  case VDQM_HANGUP      : printf("VDQM_HANGUP");       break;
  default               : printf("UNKNOWN");
  }

  printf("\n");
}
