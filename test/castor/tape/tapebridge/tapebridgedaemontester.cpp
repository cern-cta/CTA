/******************************************************************************
 *                test/castor/tape/tapebridge/tapebridgedaemontester.cpp
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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#include "test_exception.hpp"
#include "castor/PortNumbers.hpp"
#include "castor/vdqm/RemoteCopyConnection.hpp"
#include "h/Cuuid.h"

#include <errno.h>
#include <exception>
#include <sstream>
#define _XOPEN_SOURCE 600
#include <string.h>
#include <string>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

bool pathExists(const char *const path) {
   struct stat fileStat;

   memset(&fileStat, '\0', sizeof(fileStat));

   return 0 == stat(path, &fileStat);
}

/**
 * Forks ann exec a tapebridged process and returns its process id.
 */
pid_t forkAndExecTapebridged() throw(std::exception) {
   const char *const executable  = "tapebridged";
   pid_t             forkRc      = 0;
   int               saved_errno = 0;

   if(!pathExists(executable)) {
     std::ostringstream oss;

     oss <<
       "File does not exist"
       ": path=" << executable;

     test_exception te(oss.str());

     throw te;
   }

   forkRc = fork();
   saved_errno = errno;

   switch(forkRc) {
   case -1:
     {
       char buf[1024];
       std::ostringstream oss;

       strerror_r(saved_errno, buf, sizeof(buf));
       oss <<
       "Failed to fork child process"
         ": Function=" << __FUNCTION__ <<
         " Line=" << __LINE__ <<
         ": " << buf;

       test_exception te(oss.str());

       throw te;
     }
     break;
   case 0: // Child
     {
       char *argv[1] = {NULL};
       execv(executable, argv);
     }
     exit(1); // Should never get here so exit with an error
   default: // Parent
     return forkRc;
   }
}

int main() {
  const unsigned short port = castor::TAPEBRIDGE_VDQMPORT;
  const std::string host("127.0.0.1");
  bool acknSucc = false;
  

  try {
    forkAndExecTapebridged();
  } catch(std::exception &ex) {
    std::cerr <<
      "Aborting"
      ": " << ex.what() << std::endl;
    return(1);
  }

  try {
    castor::vdqm::RemoteCopyConnection connection(port, host);

    std::cout <<
      "Connecting to rtcpd"
      ": host=" << host << " port=" << port << std::endl;
    connection.connect();

    std::cout << "Sending job" << std::endl;
    const Cuuid_t        cuuid          = nullCuuid;
    const char    *const remoteCopyType  = "RTCPD";
    const u_signed64     tapeRequestID   = 1111;
    const std::string    clientUserName  = "pippo";
    const std::string    clientMachine   = "localhost";
    const int            clientPort      = 2222;
    const int            clientEuid      = 3333;
    const int            clientEgid      = 4444;
    const std::string    deviceGroupName = "DGN";
    const std::string    tapeDriveName   = "drive";
    acknSucc = connection.sendJob(
      cuuid,
      remoteCopyType,
      tapeRequestID,
      clientUserName,
      clientMachine,
      clientPort,
      clientEuid,
      clientEgid,
      deviceGroupName,
      tapeDriveName);
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "Failed to send job" <<
      ": " << ex.getMessage().str() << std::endl;
    return(1);
  }

  if(!acknSucc) {
    std::cerr <<
      "Received a negative acknowledgement from tapebridged" << std::endl;
    return(1);
  }

  return 0;
}
