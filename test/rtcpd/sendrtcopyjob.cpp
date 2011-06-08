/******************************************************************************
 *                test/rtcpd/sendrtcopyjob.cpp
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

#include "h/rtcp_constants.h"
#include "castor/vdqm/RemoteCopyConnection.hpp"

#include <iostream>
#include <string>

void writeUsage(std::ostream &os) {
  os <<
    "Usage:\n"
    "\tsendrtcopyjob hostname\n"
    "Where:\n"
    "\thostname is the name of the host where rtcpd is running" <<
    std::endl;
}

int main(int argc, char **argv) {
  if(argc != 2) {
    std::cerr <<
      "Wrong number of command-line arguments"
      ": expected=1 actual=" << (argc-1) <<
      std::endl;
    std::cerr << std::endl;
    writeUsage(std::cerr);
    return 1;
  }

  const unsigned short port = RTCOPY_PORT;
  const std::string host(argv[1]);

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
    bool acknSucc = connection.sendJob(
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

    std::cout << "acknSucc=" << (acknSucc ? "TRUE" : "FALSE") << std::endl;
  } catch(castor::exception::Exception &ex) {
    std::cerr <<
      "Failed to send job" <<
      ": " << ex.getMessage().str() << std::endl;
    return(1);
  }

  return 0;
}
