/******************************************************************************
 *                      RCPJobSubmitter.hpp
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
#ifndef _CASTOR_TAPE_AGGREGATOR_RCPJOBSUBMITTER_HPP_
#define _CASTOR_TAPE_AGGREGATOR_RCPJOBSUBMITTER_HPP_

#include "h/net.h"

#include <string>


namespace castor {

namespace io {
  class AbstractTCPSocket;
}

namespace tape {
namespace aggregator {
    
  /**
   * Remote copy job submitter.
   *
   * A helper class for submitting remote copy jobs to either RTCPD or tape
   * aggregator daemons.
   */
  class RCPJobSubmitter {

  public:

    /**
     * Submits a remote copy job to either a RTCPD or tape aggregator daemon.
     * 
     * @param host the hostname of the RTCPD or tape aggregator daemon
     * @param port the port number of the RTCPD or tape aggregator daemon
     * @param remoteCopyType remote copy type to be used for exception messages
     * @param tapeRequestID tape request ID
     * @param clientUserName client user name
     * @param clientHost client host
     * @param clientPort client port
     * @param clientEuid client user ID
     * @param clientEgid client group ID
     * @param deviceGroupName device group name
     * @param tapeDriveName tape drive name
     */
    static void submit(
      const std::string &host,
      const unsigned     port,
      const char        *remoteCopyType,
      const u_signed64   tapeRequestID,
      const std::string &clientUserName,
      const std::string &clientHost,
      const int          clientPort,
      const int          clientEuid,
      const int          clientEgid,
      const std::string &deviceGroupName,
      const std::string &tapeDriveName)
      throw (castor::exception::Exception);    

      
  private:
      
    /**
     * This is a helper function for sendJob() to read the reply of the RTCOPY
     * or tape aggregator daemon
     * 
     * @param remoteCopyType remote copy type to be used for exception messages
     */
    static void readReply(castor::io::AbstractTCPSocket &socket,
      const char *remoteCopyType) throw (castor::exception::Exception);    

  }; // class RCPJobSubmitter

} // namespace aggregator
} // namespace tape
} // namespace castor      

#endif // _CASTOR_TAPE_AGGREGATOR_RCPJOBSUBMITTER_HPP_
