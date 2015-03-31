/******************************************************************************
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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

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

  /* internal function to connect to the diskmanager */
  int connectToDiskmanager(const int port) {
    // connect and send data
    int sockfd = 0;
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
    return sockfd;
  }


  /* internal function to parse the answer from the diskmanager */
  void parseAnswer(const int sockfd, int* rc, char** errormsg) {
    // synchronously read answer back from sockfd until \n
    // Format is <returnCode> [<error message>]\n
    int n = 0;
    char readBuf[1024];
    while(n < 1024 && 1 == read(sockfd, &readBuf[n], 1)) {
      if(n > 0 && readBuf[n] == ' ' && *rc == 0) {
        /* so far we have the return code */
        *rc = (int)strtol(readBuf, NULL, 10);
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
    readBuf[n] = '\0';
    // ok, we got the reply, return it to the caller
    if(*rc != 0) {
      *errormsg = strdup(readBuf);
    }
  }


  int mover_open_file(const int port, const char* transferMetaData, int* errorcode, char** errormsg) {
    *errormsg = NULL;
    try {
      /* Prepare open message. Protocol:
         OPEN <errorCode> <transferMetaData>
         where the latter is a tuple (cf. header for its full specification)
       */
      std::ostringstream writeBuf;
      writeBuf << "OPEN " << *errorcode << " " << transferMetaData;
      *errorcode = 0;

      try {
        int sockfd = 0, n = 0;
        // connect and send data
        sockfd = connectToDiskmanager(port);
        n = write(sockfd, writeBuf.str().c_str(), writeBuf.str().length());
        if (n != (int)writeBuf.str().length()) {
          std::ostringstream ss << "Failed to send OPEN message, expected length was "
                                << writeBuf.str().length() << ", real length was " << n;
          throw std::runtime_error(ss.str());
        }
        // synchronously read and parse answer
        parseAnswer(sockfd, errorcode, errormsg);
        // close the connection
        close(sockfd);
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
      *errormsg = strdup("mover_open_file: caught general exception");
    }
    return *errorcode;
  }


  int mover_close_file(const int port, const char* transferUuid, const u_signed64 filesize, const char* cksumtype, const char* cksumvalue,
                       int* errorcode, char** errormsg) {
    try {
      /* Prepare close message. Protocol:
         CLOSE <transferUuid> <fileSize> <cksumType> <cksumValue> <errorCode>[ <error message>]
       */
      std::ostringstream writeBuf;
      writeBuf << "CLOSE " << transferUuid << ' ' << filesize << ' ' << cksumtype
               << ' ' << cksumvalue << ' ' << *errorcode;
      if(*errorcode) {
        writeBuf << ' ' << *errormsg;
        free(*errormsg);
        *errormsg = NULL;
        *errorcode = 0;
      }

      try {
        int sockfd = 0, n = 0;
        // connect and send data
        sockfd = connectToDiskmanager(port);
        n = write(sockfd, writeBuf.str().c_str(), writeBuf.str().length());
        if (n != (int)writeBuf.str().length()) {
          std::ostringstream ss << "Failed to send CLOSE message, expected length was "
                                << writeBuf.str().length() << ", real length was " << n;
          throw std::runtime_error(ss.str());
        }
        // synchronously read and parse answer
        parseAnswer(sockfd, errorcode, errormsg);
        // close the connection
        close(sockfd);
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

