#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "castor/vdqm/DevTools.hpp"
#include "h/vdqm_constants.h"


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::DevTools::DevTools() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// printVdqmRequestType
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printIp(std::ostream &os, const unsigned long ip)
  throw() {
    os << ((ip >> 24) & 0x000000FF) << "."
       << ((ip >> 16) & 0x000000FF) << "."
       << ((ip >>  8) & 0x000000FF) << "."
       << ( ip        & 0x000000FF);
}


//------------------------------------------------------------------------------
// printVdqmRequestType
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printMagic(std::ostream &os, const uint32_t magic)
  throw() {

  switch(magic) {
  case 0x00000200 : os << "B_RFIO_MAGIC"                  ; break;
  case 0x030E1301 : os << "CNS_MAGIC"                     ; break;
  case 0x030E1302 : os << "CNS_MAGIC2"                    ; break;
  case 0x030E1303 : os << "CNS_MAGIC3"                    ; break;
  case 0x030E1304 : os << "CNS_MAGIC4"                    ; break;
  case 0x8CA00001 : os << "CSEC_CONTEXT_MAGIC_CLIENT_1"   ; break;
  case 0x80000000 : os << "CSEC_CONTEXT_MAGIC_CLIENT_MASK"; break;
  case 0x0CA00001 : os << "CSEC_CONTEXT_MAGIC_SERVER_1"   ; break;
  case 0x0000CA03 : os << "CSEC_TOKEN_MAGIC_1"            ; break;
  case 0x000CA001 : os << "CSP_MSG_MAGIC"                 ; break;
  case 0x07770777 : os << "CUPV_MAGIC"                    ; break;
  case 0x0010aa5b : os << "C_MAGIC"                       ; break;
  case 0x68767001 : os << "DLF_MAGIC"                     ; break;
  case 0x68777001 : os << "EXP_MAGIC"                     ; break;
  case 0x00000110 : os << "RFIO2TPREAD_MAGIC"             ; break;
  case 0x00000100 : os << "RFIO_MAGIC"                    ; break;
  case 0x120D0301 : os << "RMC_MAGIC"                     ; break;
  case 0x00000669 : os << "RTCOPY_MAGIC"                  ; break;
  case 0x00000668 : os << "RTCOPY_MAGIC_OLD0"             ; break;
  case 0x00000667 : os << "RTCOPY_MAGIC_SHIFT"            ; break;
  case 0x00000666 : os << "RTCOPY_MAGIC_VERYOLD"          ; break;
  case 0x24140701 : os << "STAGER_MAGIC"                  ; break;
  case 0x34140701 : os << "STAGER_NOTIFY_MAGIC1"          ; break;
  case 0x34140702 : os << "STAGER_NOTIFY_MAGIC2"          ; break;
  case 0x13140701 : os << "STGMAGIC"                      ; break;
  case 0x13140702 : os << "STGMAGIC2"                     ; break;
  case 0x13140703 : os << "STGMAGIC3"                     ; break;
  case 0x13140704 : os << "STGMAGIC4"                     ; break;
  case 0x00141001 : os << "TPMAGIC"                       ; break;
  case 0x00008537 : os << "VDQM_MAGIC"                    ; break;
  case 0x766D6701 : os << "VMGR_MAGIC"                    ; break;
  case 0x766D6702 : os << "VMGR_MAGIC2"                   ; break;
  default         : os << "UNKNOWN";
  }
}


//------------------------------------------------------------------------------
// printVdqmRequestType
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printVdqmRequestType(std::ostream &os,
  const uint32_t type) throw() {

  switch(type) {
  case VDQM_VOL_REQ     : os << "VDQM_VOL_REQ";      break;
  case VDQM_DRV_REQ     : os << "VDQM_DRV_REQ";      break;
  case VDQM_PING        : os << "VDQM_PING";         break;
  case VDQM_CLIENTINFO  : os << "VDQM_CLIENTINFO";   break;
  case VDQM_SHUTDOWN    : os << "VDQM_SHUTDOWN";     break;
  case VDQM_HOLD        : os << "VDQM_HOLD";         break;
  case VDQM_RELEASE     : os << "VDQM_RELEASE";      break;
  case VDQM_REPLICA     : os << "VDQM_REPLICA";      break;
  case VDQM_RESCHEDULE  : os << "VDQM_RESCHEDULE";   break;
  case VDQM_ROLLBACK    : os << "VDQM_ROLLBACK";     break;
  case VDQM_COMMIT      : os << "VDQM_COMMIT";       break;
  case VDQM_DEL_VOLREQ  : os << "VDQM_DEL_VOLREQ";   break;
  case VDQM_DEL_DRVREQ  : os << "VDQM_DEL_DRVREQ";   break;
  case VDQM_GET_VOLQUEUE: os << "VDQM_GET_VOLQUEUE"; break;
  case VDQM_GET_DRVQUEUE: os << "VDQM_GET_DRVQUEUE"; break;
  case VDQM_SET_PRIORITY: os << "VDQM_SET_PRIORITY"; break;
  case VDQM_DEDICATE_DRV: os << "VDQM_DEDICATE_DRV"; break;
  case VDQM_HANGUP      : os << "VDQM_HANGUP";       break;
  default               : os << "UNKNOWN";
  }
}


//------------------------------------------------------------------------------
// printMessag
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printMessage(std::ostream &os,
  const bool messageWasSent, const bool messageInNetworkByteOrder,
  const int socket, void* hdrbuf) throw (castor::exception::Exception)
{
  uint32_t           magic   = 0;
  uint32_t           reqtype = 0;
  unsigned int       soutlen = sizeof(struct sockaddr_in);
  struct sockaddr_in sout;
  unsigned short     peerPort = 0;
  unsigned long      peerIp   = 0;

  if(messageInNetworkByteOrder) {
    magic   = ntohl(*((uint32_t *)hdrbuf));
    reqtype = ntohl(*(((uint32_t *)hdrbuf) + 1));
  } else {
    magic   = *((uint32_t *)hdrbuf);
    reqtype = *(((uint32_t *)hdrbuf) + 1);
  }

  if(getpeername(socket, (struct sockaddr*)&sout, &soutlen) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to get peer name";
    throw ex;
  }

  peerPort = ntohs(sout.sin_port);
  peerIp   = ntohl(sout.sin_addr.s_addr);

  if(messageWasSent) {
    std::cout << "Sent     to   : ";
  } else {
    std::cout << "Received from : ";
  }
  castor::vdqm::DevTools::printIp(std::cout, peerIp);
  std::cout << ":" << peerPort;
  std::cout << " : ";
  castor::vdqm::DevTools::printMagic(std::cout, magic);
  std::cout << " ";
  castor::vdqm::DevTools::printVdqmRequestType(std::cout, reqtype);
  std::cout << std::endl;
}
