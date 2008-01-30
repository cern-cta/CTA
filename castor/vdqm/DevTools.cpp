#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "castor/vdqm/CDevTools.hpp"
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
// printMessage
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printMessage(std::ostream &os,
  const bool messageWasSent, const bool messageInNetworkByteOrder,
  const int socket, void* hdrbuf) throw (castor::exception::Exception)
{
  uint32_t           magic   = 0;
  uint32_t           reqtype = 0;
  unsigned int       soutlen = sizeof(struct sockaddr_in);
  struct sockaddr_in sout;
  unsigned short     localPort = 0;
  unsigned long      localIp   = 0;
  unsigned short     peerPort = 0;
  unsigned long      peerIp   = 0;

  if(messageInNetworkByteOrder) {
    magic   = ntohl(*((uint32_t *)hdrbuf));
    reqtype = ntohl(*(((uint32_t *)hdrbuf) + 1));
  } else {
    magic   = *((uint32_t *)hdrbuf);
    reqtype = *(((uint32_t *)hdrbuf) + 1);
  }

  if(getsockname(socket, (struct sockaddr*)&sout, &soutlen) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to get local name";
    throw ex;
  }

  localPort = ntohs(sout.sin_port);
  localIp   = ntohl(sout.sin_addr.s_addr);

  if(getpeername(socket, (struct sockaddr*)&sout, &soutlen) < 0) {
    castor::exception::Exception ex(errno);
    ex.getMessage() << "Unable to get peer name";
    throw ex;
  }

  peerPort = ntohs(sout.sin_port);
  peerIp   = ntohl(sout.sin_addr.s_addr);

  if(messageWasSent) {
    std::cout << "Tx ";
    printIp(std::cout, localIp);
    std::cout << ":" << localPort;
    std::cout << "->";
    printIp(std::cout, peerIp);
    std::cout << ":" << peerPort;
  } else {
    std::cout << "Rx ";
    printIp(std::cout, peerIp);
    std::cout << ":" << peerPort;
    std::cout << "->";
    printIp(std::cout, localIp);
    std::cout << ":" << localPort;
  }

  std::cout << " " << castorMagicNb2Str(magic);
  std::cout << " " << castorReqTypeToStr(magic, reqtype);

  std::cout << std::endl;
}


//------------------------------------------------------------------------------
// printTapeDriveStatusBitset
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printTapeDriveStatusBitset(std::ostream &os,
  const int bitset) {

  os << "Drive status bitset: ";

  if(bitset & VDQM_TPD_STARTED)   os << "+TPD_STARTED";
  if(bitset & VDQM_FORCE_UNMOUNT) os << "+FORCE_UNMOUNT";
  if(bitset & VDQM_UNIT_QUERY)    os << "+UNIT_QUERY";
  if(bitset & VDQM_UNIT_MBCOUNT)  os << "+UNIT_MBCOUNT";
  if(bitset & VDQM_UNIT_ERROR)    os << "+UNIT_ERROR";
  if(bitset & VDQM_UNIT_UNKNOWN)  os << "+UNIT_UNKNOWN";
  if(bitset & VDQM_VOL_UNMOUNT)   os << "+VOL_UNMOUNT";
  if(bitset & VDQM_VOL_MOUNT)     os << "+VOL_MOUNT";
  if(bitset & VDQM_UNIT_FREE)     os << "+UNIT_FREE";
  if(bitset & VDQM_UNIT_BUSY)     os << "+UNIT_BUSY";
  if(bitset & VDQM_UNIT_RELEASE)  os << "+UNIT_RELEASE";
  if(bitset & VDQM_UNIT_ASSIGN)   os << "+UNIT_ASSIGN";
  if(bitset & VDQM_UNIT_WAITDOWN) os << "+UNIT_WAITDOWN";
  if(bitset & VDQM_UNIT_DOWN)     os << "+UNIT_DOWN";
  if(bitset & VDQM_UNIT_UP)       os << "+UNIT_UP";

  os << std::endl;
}
