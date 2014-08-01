#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <netdb.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sstream>
#include <exception>
#include <stdexcept>
#include "serrno.h"

extern "C" {
  int mover_close_file(const int port, const char* transferUuid, const u_signed64 filesize, const char* cksumtype, const char* cksumvalue,
                       int* errorcode, char** errormsg) {
    try {
      int sockfd = 0, n = 0;
      /* Prepare close message. Protocol:
         CLOSE <transferUuid> <fileSize> <cksumType> <cksumValue> <errorCode>[ <error message>]
       */
      std::ostringstream writeBuf;
      writeBuf << "CLOSE " << transferUuid << ' ' << filesize << ' ' << cksumtype
               << ' ' << cksumvalue << ' ' << *errorcode;
      if(*errorcode) {
        writeBuf << ' ' << *errormsg;
      }

      try {
        // connect and send data
        if((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
          throw std::runtime_error("Failed to create socket");
        }
        struct sockaddr_in serv_addr;
        serv_addr.sin_family = AF_INET;
        serv_addr.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr.s_addr);
        if(connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
          throw std::runtime_error("Failed to connect");
        } 
        n = write(sockfd, writeBuf.str().c_str(), writeBuf.str().length());

        // process result
        if (n != (int)writeBuf.str().length()) {
          throw std::runtime_error("Failed to send CLOSE message");
        }

        // synchronously read answer back until \n
        // Format is <returnCode> [<error message>]\n
        n = 0;
        char readBuf[1024];
        while(n < 1024 && 1 == read(sockfd, &readBuf[n], 1)) {
          if(n > 0 && readBuf[n] == ' ' && *errorcode == 0) {
            /* so far we have the return code */
            *errorcode = (int)strtol(readBuf, NULL, 10);
            n = 0;
          }
          else if(readBuf[n] == '\n') {
            /* this is the end of the data */
            readBuf[n] = '\0';
            break;
          }
          else {
            n++;
          }
        }
        // ok, we got the reply, return it to the caller
        if(*errorcode != 0) {
          *errormsg = strdup(readBuf);
        }
      }
      catch (std::exception& e) {
        // report any exception to the caller
        *errorcode = SEINTERNAL;
        *errormsg = strdup(e.what());
      }
    }
    catch (...) {
      // this is to avoid core dumps on standard exceptions
      *errorcode = SEINTERNAL;
      *errormsg = strdup("mover_close_file: caught general exception");
    }
    return *errorcode;
  }

} /* extern C */

