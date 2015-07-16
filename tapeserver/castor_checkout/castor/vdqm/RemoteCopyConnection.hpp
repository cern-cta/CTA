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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/
#pragma once

#include "castor/io/AbstractTCPSocket.hpp"

#include <string>


namespace castor {

  namespace vdqm {
    
    // Forward declaration
    class TapeDrive;
    class TapeRequest;

    /**
     * A dedicated socket class, able to deal with socket manipulation
     * and to handle sending of messages to RTCPD and tape aggregator daemons.
     */
    class RemoteCopyConnection : public castor::io::AbstractTCPSocket {

    public:

      /**
       * Constructor building a Socket objet around a regular socket
       * @param socket the regular socket used
       */
      RemoteCopyConnection(int socket) throw();

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given by its name
       */
      RemoteCopyConnection(const unsigned short port,
        const std::string host)
        ;

      /**
       * Constructor building a socket on a given port of a given host
       * @param port the port on which the socket should be opened on
       * remote host
       * @param host the host to connect to, given as an ip address
       */
      RemoteCopyConnection(const unsigned short port,
        const unsigned long ip)
        ;


      /**
       * destructor
       */
      ~RemoteCopyConnection() throw();
      
      
      /**
       * connects the socket to the given address
       */
       virtual void connect()
         ;      
      
      
      /**
       * Asks the RTCPD or tape aggregator daemon to start a job.
       * 
       * @param tapeRequestID
       * @param remoteCopyType remote copy type to be used for exception and
       * log messages
       * @param clientID the client's identification information
       * @param tapeDrive the tape drive to be used
       * @exception In case of errors 
       */
      bool sendJob(
        const Cuuid_t     &cuuid,
        const char        *remoteCopyType,
        const u_signed64  tapeRequestID,
        const std::string &clientUserName,
        const std::string &clientMachine,
        const int          clientPort,
        const int          clientEuid,
        const int          clientEgid,
        const std::string &deviceGroupName,
        const std::string &tapeDriveName)
        ;    

      
    private:
      
      /**
       * This is a helper function for sendJob() to read the answer of the
       * RTCPD or tape aggregator daemon
       * 
       * @param cuuid the cuuid to be used for logging
       * @param remoteCopyType remote copy type to be used for exception and
       * log messages
       * @return false, in case of problems on RTCP side
       */
      bool readAnswer(const Cuuid_t &cuuid, const char *remoteCopyType)
        ;    
    };

  } // namespace vdqm

} // namespace castor      

