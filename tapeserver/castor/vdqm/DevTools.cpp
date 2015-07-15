#include <errno.h>
#include <string.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#include "castor/vdqm/CDevTools.hpp"
#include "castor/vdqm/DevTools.hpp"
#include "Castor_limits.h"
#include "vdqm_constants.h"
#include "rtcp_constants.h"


//------------------------------------------------------------------------------
// private static member: unitStatusTypes_
//------------------------------------------------------------------------------
castor::vdqm::DevTools::UnitMaskAndStr
  castor::vdqm::DevTools::unitStatusTypes_[] = {
    {VDQM_TPD_STARTED  , "TPD_STARTED"  },
    {VDQM_FORCE_UNMOUNT, "FORCE_UNMOUNT"},
    {VDQM_UNIT_QUERY   , "UNIT_QUERY"   },
    {VDQM_UNIT_MBCOUNT , "UNIT_MBCOUNT" },
    {VDQM_UNIT_ERROR   , "UNIT_ERROR"   },
    {VDQM_UNIT_UNKNOWN , "UNIT_UNKNOWN" },
    {VDQM_VOL_UNMOUNT  , "VOL_UNMOUNT"  },
    {VDQM_VOL_MOUNT    , "VOL_MOUNT"    },
    {VDQM_UNIT_FREE    , "UNIT_FREE "   },
    {VDQM_UNIT_BUSY    , "UNIT_BUSY"    },
    {VDQM_UNIT_RELEASE , "UNIT_RELEASE" },
    {VDQM_UNIT_ASSIGN  , "UNIT_ASSIGN"  },
    {VDQM_UNIT_WAITDOWN, "UNIT_WAITDOWN"},
    {VDQM_UNIT_DOWN    , "UNIT_DOWN"    },
    {VDQM_UNIT_UP      , "UNIT_UP"      },
    {0, NULL}
};


//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::vdqm::DevTools::DevTools() throw() {
  // Do nothing
}


//------------------------------------------------------------------------------
// printIp
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printIp(std::ostream &os, const unsigned long ip)
  throw() {
    os << ((ip >> 24) & 0x000000FF) << "."
       << ((ip >> 16) & 0x000000FF) << "."
       << ((ip >>  8) & 0x000000FF) << "."
       << ( ip        & 0x000000FF);
}


//------------------------------------------------------------------------------
// printSocketDescription
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printSocketDescription(std::ostream &os,
  castor::io::ServerSocket &socket) {

  unsigned short localPort = 0;
  unsigned long  localIp   = 0;
  unsigned short peerPort  = 0;
  unsigned long  peerIp    = 0;

  socket.getPortIp(localPort, localIp);
  socket.getPeerIp(peerPort , peerIp );

  os << "local=";
  castor::vdqm::DevTools::printIp(os, localIp);
  os << ":" << localPort;
  os << " peer=";
  castor::vdqm::DevTools::printIp(os, peerIp);
  os << ":" << peerPort;
}


//------------------------------------------------------------------------------
// printMessage
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printMessage(std::ostream &os,
  const bool messageWasSent, const bool messageInNetworkByteOrder,
  const int socket, void* hdrbuf) 
{
  uint32_t           magic   = 0;
  uint32_t           reqtype = 0;
  uint32_t           len     = 0;
  unsigned int       soutlen = sizeof(struct sockaddr_in);
  struct sockaddr_in sout;
  unsigned short     localPort = 0;
  unsigned long      localIp   = 0;
  unsigned short     peerPort = 0;
  unsigned long      peerIp   = 0;

  if(messageInNetworkByteOrder) {
    magic   = ntohl(*((uint32_t *)hdrbuf));
    reqtype = ntohl(*(((uint32_t *)hdrbuf) + 1));
    len     = ntohl(*(((uint32_t *)hdrbuf) + 2));
  } else {
    magic   = *((uint32_t *)hdrbuf);
    reqtype = *(((uint32_t *)hdrbuf) + 1);
    len     = *(((uint32_t *)hdrbuf) + 2);
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
    os << "Tx ";
    printIp(os, localIp);
    os << ":" << localPort;
    os << "->";
    printIp(os, peerIp);
    os << ":" << peerPort;
  } else {
    os << "Rx ";
    printIp(os, peerIp);
    os << ":" << peerPort;
    os << "->";
    printIp(os, localIp);
    os << ":" << localPort;
  }

  os << " " << castorMagicNb2Str(magic);
  os << " " << castorReqTypeToStr(magic, reqtype);

  // In the case of a VDQM_CLIENTINFO message the complete message
  // (header + body) is passed to printMessage, so the body can be printed
  if( messageWasSent && (magic == RTCOPY_MAGIC_OLD0) &&
    (reqtype == VDQM_CLIENTINFO) ) {

    char* ptr = ((char*)hdrbuf) + 12;

    uint32_t tapeReqId = ntohl(*((uint32_t *)ptr)); ptr += 4;
    uint32_t port      = ntohl(*((uint32_t *)ptr)); ptr += 4;
    uint32_t euid      = ntohl(*((uint32_t *)ptr)); ptr += 4;
    uint32_t egid      = ntohl(*((uint32_t *)ptr)); ptr += 4;
    char*    cHost     = ptr;                     ; ptr += strlen(cHost)+1;
    char*    dgn       = ptr;                     ; ptr += strlen(dgn)+1;
    char*    drive     = ptr;                     ; ptr += strlen(drive)+1;
    char*    cName     = ptr;

    os << std::endl << " ";
    os << " Len:"   << len;
    os << " ReqId:" << tapeReqId;
    os << " Port:"  << port;
    os << " Euid:"  << euid;
    os << " Egid:"  << egid;
    os << " CHost:" << cHost;
    os << " Dgn:"   << dgn;
    os << " Drive:" << drive;
    os << " CName:" << cName;
  }

  os << std::endl;
}


//------------------------------------------------------------------------------
// printTapeDriveStatusBitset
//------------------------------------------------------------------------------
void castor::vdqm::DevTools::printTapeDriveStatusBitset(std::ostream &os,
  int bitset) {

  bool first = true;

  // For each drive unit status mask
  for(int i=0; unitStatusTypes_[i].mask != 0; i++) {

    // If the status bit is set
    if(bitset & unitStatusTypes_[i].mask) {

      // Write a separator to the stream if needed
      if(first) {
        first = false;
      } else {
        os << "+";
      }

      // Write the textual representation to the stream
      os << unitStatusTypes_[i].str;

      // Remove the set bit.  This is done to see if there are any unknown set
      // bits left over at the end of the conversion
      bitset &= ~unitStatusTypes_[i].mask;
    }
  }

  // If the bitset contained one or more unknown set bits
  if(bitset != 0) {
    // Write a separator to the stream if needed
    if(first) {
      first = false;
    } else {
      os << "+";
    }

    // Write the unknown bit(s) to the stream
    os << "UNKNOWN_BITS_0x" << std::hex << bitset << std::dec;
  }
}


//------------------------------------------------------------------------------
// printTapeDriveStatus
//------------------------------------------------------------------------------
const char *castor::vdqm::DevTools::tapeDriveStatus2Str(
  const castor::vdqm::TapeDriveStatusCodes status) {

  switch(status) {
  case castor::vdqm::UNIT_UP         : return "UNIT_UP";
  case castor::vdqm::UNIT_STARTING   : return "UNIT_STARTING";
  case castor::vdqm::UNIT_ASSIGNED   : return "UNIT_ASSIGNED";
  case castor::vdqm::VOL_MOUNTED     : return "VOL_MOUNTED";
  case castor::vdqm::FORCED_UNMOUNT  : return "FORCED_UNMOUNT";
  case castor::vdqm::UNIT_DOWN       : return "UNIT_DOWN";
  case castor::vdqm::WAIT_FOR_UNMOUNT: return "WAIT_FOR_UNMOUNT";
  case castor::vdqm::STATUS_UNKNOWN  : return "STATUS_UNKNOWN";
  default                            : return "UNKNOWN";
  };
}


//------------------------------------------------------------------------------
// castorMagicNb2Str
//------------------------------------------------------------------------------
const char *castor::vdqm::DevTools::castorMagicNb2Str(const uint32_t magic) {
  return ::castorMagicNb2Str(magic); // CDevTools
}


//------------------------------------------------------------------------------
// rtcpReqTypeToStr
//------------------------------------------------------------------------------
const char *castor::vdqm::DevTools::rtcpReqTypeToStr(const uint32_t type) {
  return ::rtcpReqTypeToStr(type); // CDevTools
}


//------------------------------------------------------------------------------
// vdqmReqTypeToStr
//------------------------------------------------------------------------------
const char *castor::vdqm::DevTools::vdqmReqTypeToStr(const uint32_t type) {
  return ::vdqmReqTypeToStr(type); // CDevTools
}


//------------------------------------------------------------------------------
// castorReqTypeToStr
//------------------------------------------------------------------------------
const char *castor::vdqm::DevTools::castorReqTypeToStr(const uint32_t magic,
  const uint32_t type) {
  return ::castorReqTypeToStr(magic, type); //CDevTools
}
