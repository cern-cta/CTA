/******************************************************************************
 *                      castor/tape/tapebridge/RtcpJobSubmitter.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
#ifndef _CASTOR_TAPE_TAPEBRIDGE_RTCPJOBSUBMITTER_HPP_
#define _CASTOR_TAPE_TAPEBRIDGE_RTCPJOBSUBMITTER_HPP_

#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "h/net.h"

#include <string>


namespace castor {

namespace io {
  class AbstractTCPSocket;
}

namespace tape {
namespace tapebridge {
    
  /**
   * Remote copy job submitter.
   *
   * A helper class for submitting remote-copy jobs to either RTCPD or tape
   * tapebridge daemons.
   */
  class RtcpJobSubmitter {

  public:

    /**
     * Submits a remote-copy job to either a RTCPD or tape tapebridge daemon.
     * 
     * @param host The hostname of the RTCPD or tape tapebridge daemon.
     * @param port The port number of the RTCPD or tape tapebridge daemon.
     * @param netReadWriteTimeout The timeout to be used when performing
     * network reads and writes.
     * @param remoteCopyType The remote-copy type to be used for exception
     * messages
     * @param tapeRequestID  The tape request ID.
     * @param clientUserName The client user name.
     * @param clientHost     The client host.
     * @param clientPort     The client port.
     * @param clientEuid     The client user ID.
     * @param clientEgid     The client group ID.
     * @param dgn            The device group name.
     * @param driveUnit      The name of the tape drive unit.
     * @param reply          The reply from RTCPD which may be positive or
     *                       negative.
     */
    static void submit(
      const std::string              &host,
      const unsigned int             port,
      const int                      netReadWriteTimeout,
      const char                     *remoteCopyType,
      const u_signed64               tapeRequestID,
      const std::string              &clientUserName,
      const std::string              &clientHost,
      const int                      clientPort,
      const int                      clientEuid,
      const int                      clientEgid,
      const std::string              &dgn,
      const std::string              &driveUnit,
      legacymsg::RtcpJobReplyMsgBody &reply)
      throw(castor::exception::Exception);    

      
  private:
      
    /**
     * Reads the reply of the RTCOPY or tape tapebridge daemon.
     * 
     * @param sock The socket of the connection to the RTCOPY or tape-bridge
     * daemon.
     * @param netReadWriteTimeout The timeout to be used when performing
     * network reads and writes.
     * @param remoteCopyType The remote-copy type to be used for exception
     * messages.
     * @param reply The reply from RTCOPY or tape-bridge daemon.
     */
    static void readReply(
      castor::io::AbstractTCPSocket  &sock,
      const int                      netReadWriteTimeout,
      const char                     *remoteCopyType,
      legacymsg::RtcpJobReplyMsgBody &reply)
      throw(castor::exception::Exception);    

  }; // class RtcpJobSubmitter

} // namespace tapebridge
} // namespace tape
} // namespace castor      

#endif // _CASTOR_TAPE_TAPEBRIDGE_RTCPJOBSUBMITTER_HPP_
