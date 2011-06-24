#include "h/serrno.h"
#include "h/tapebridge_sendTapeBridgeFlushedToTape.h"

#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <netdb.h>
#include <string>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>

void writeUsage(std::ostream &os) {
  os <<
    "Usage:\n"
    "\tsendflushedtotape hostname callback_port\n"
    "Where:\n"
    "\thostname     is the name of the host where tapebridged is running.\n" <<
    "\tcallbackPort is the port on which tapebridged is listening for\n"
    "\t             callbacks from rtcpd." <<
    std::endl;
}

bool isValidInt(const char *const str) {
  for(const char *p = str; *p != '\0'; p++) {
    if(*p < '0' || *p > '9') {
      return false;
    }
  }

  return true;
}

int main(int argc, char **argv) {
  /* Check command-line arguments */
  if(argc != 3) {
    std::cerr <<
      "ERROR"
      ": Wrong number of command-line arguments"
      ": expected=2 actual=" << (argc-1) <<
      std::endl;
    std::cerr << std::endl;
    writeUsage(std::cerr);
    return 1;
  }
  if(!isValidInt(argv[2])) {
    std::cerr <<
      "ERROR"
      ": callbackPort is not a valid integer"
      ": value=\"" << argv[2] << "\"" <<
      std::endl;
    std::cerr << std::endl;
    writeUsage(std::cerr);
    return 1;
  }

  const std::string hostname(argv[1]);
  const unsigned short callbackPort = atoi(argv[2]);

  std::cout << "Using hostname=" << hostname <<
    " callbackPort=" << callbackPort << std::endl;

  /* Get the network-byte-ordered address of tapebridged */
  in_addr_t ipAddr = 0;
  {
    struct hostent *host;
    if(NULL == (host = gethostbyname(hostname.c_str()))) {
      std::cerr <<
        "ERROR"
        ": gethostbyname(\"" << hostname << "\") failed" << std::endl;
      return 1;
    }

    ipAddr = *(host->h_addr_list[0]);
  }

  /* Create a socket */
  int socketFd = 0;
  if(0 > (socketFd = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP))) {
    std::cerr <<
      "ERROR"
      ": socket(PF_INET, SOCK_STREAM, IPPROTO_TCP) failed" << std::endl;
    return 1;
  }

  /* Connect to the callback port of tapebridged */
  struct sockaddr_in sockAddr;
  memset(&sockAddr, '\0', sizeof(sockAddr));
  sockAddr.sin_family      = AF_INET;
  sockAddr.sin_addr.s_addr = ipAddr;
  sockAddr.sin_port        = htons(callbackPort);

  if(0 != connect(socketFd, (struct sockaddr *) &sockAddr, sizeof(sockAddr))) {
    const char *const errMsg = strerror(errno);
    std::cerr <<
      "ERROR"
      ": connect(socketFd, (struct sockaddr *) &sockAddr, sizeof(sockAddr))"
      " failed"
      ": " << errMsg << std::endl;
    close(socketFd);
    return 1;
  }

  const int netReadWriteTimeout = 60;
  tapeBridgeFlushedToTapeMsgBody_t msgBody;
  msgBody.volReqId = 1111;
  msgBody.tapeFseq = 2222;
  const int sendRc = tapebridge_sendTapeBridgeFlushedToTape(socketFd,
    netReadWriteTimeout, &msgBody);

  if(0 > sendRc) {
    const char *const errMsg = sstrerror(serrno);
    std::cerr <<
      "ERROR"
      ": tapebridge_sendTapeBridgeFlushedToTape() failed"
      ": " << errMsg;

    close(socketFd);
    return 1;
  }

  close(socketFd);
  close(0);
  close(1);
  close(2);

  return 0;
}
