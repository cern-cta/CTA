/******************************************************************************
 *                castor/tape/aggregator/RtcpdHandlerThread.hpp
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
 * @author Steven Murray Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_RTCPDREQUESTHANDLERTHREAD_HPP
#define CASTOR_TAPE_AGGREGATOR_RTCPDREQUESTHANDLERTHREAD_HPP 1

#include "castor/io/ServerSocket.hpp"
#include "castor/server/IThread.hpp"
#include "castor/server/Queue.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMessage.hpp"
#include "castor/tape/aggregator/RtcpTapeRequestMessage.hpp"
#include "castor/tape/aggregator/RtcpFileRequestMessage.hpp"
#include "h/rtcp_constants.h"

namespace castor {
namespace tape {
namespace aggregator {

  /**
   * Handles the RTCPD->VDQM protocol.
   */
  class RtcpdHandlerThread : public castor::server::IThread {

  public:

    /**
     * Constructor
     */
    RtcpdHandlerThread() throw();

    /**
     * Destructor
     */
    ~RtcpdHandlerThread() throw();

    /**
     * Initialization of the thread.
     */
    virtual void init() throw();

    /**
     * Main work for this thread.
     */
    virtual void run(void *param) throw();

    /**
     * Convenience method to stop the thread.
     */
    virtual void stop() throw();

  private:

    /**
     * Get the volume request ID from RTCPD.
     *
     * @param cuuid The ccuid to be used for logging.
     * @param socket The socket of the connection with RTCPD.
     * @param reply The request structure to be filled with the reply from
     * RTCPD.
     */
    void getVolumeRequestIdFromRtcpd(const Cuuid_t &cuuid,
      castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
      RtcpTapeRequestMessage &reply) throw(castor::exception::Exception);

    /**
     * Receives an RTCPD tape request message from RTCPD.
     *
     * @param cuuid The ccuid to be used for logging.
     * @param socket The socket of the connection with RTCPD.
     * @param request The request which will be filled with the contents of the
     * received message.
     */
    void receiveRtcpTapeRequest(const Cuuid_t &cuuid,
      castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
      RtcpTapeRequestMessage &request) throw(castor::exception::Exception);

    /**
     * Send volume to RTCPD.
     *
     * @param cuuid The ccuid to be used for logging.
     * @param socket The socket of the connection with RTCPD.
     * @param request The request to be sent to RTCPD.
     * @param reply The structure to be filled with the reply from
     * RTCPD.
     */
    void sendVolumeToRtcpd(const Cuuid_t &cuuid,
      castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
      RtcpTapeRequestMessage &request, RtcpTapeRequestMessage &reply)
      throw(castor::exception::Exception);

    /**
     * Receives an RTCPD file request message from RTCPD.
     *
     * @param cuuid The ccuid to be used for logging.
     * @param socket The socket of the connection with RTCPD.
     * @param request The request which will be filled with the contents of the
     * received message.
     */
    void receiveRtcpFileRequest(const Cuuid_t &cuuid,
      castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
      RtcpFileRequestMessage &request) throw(castor::exception::Exception);

    /**
     * Send file to RTCPD.
     *
     * @param cuuid The ccuid to be used for logging.
     * @param socket The socket of the connection with RTCPD.
     * @param request The request to be sent to RTCPD.
     * @param reply The structure to be filled with the reply from
     * RTCPD.
     */
    void sendFileToRtcpd(const Cuuid_t &cuuid,
      castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
      RtcpFileRequestMessage &request, RtcpFileRequestMessage &reply)
      throw(castor::exception::Exception);

    /**
     * Receives an acknowledge message from RTCPD and returns the status code
     * embedded within the message.
     *
     * @param cuuid The ccuid to be used for logging.
     * @param socket The socket of the connection with RTCPD.
     * @param message The message structure to be filled with the acknowledge
     * message.
     */
    void receiveRtcpAcknowledge(const Cuuid_t &cuuid,
      castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
      RtcpAcknowledgeMessage &message) throw(castor::exception::Exception);

    /**
     * Sends the specified RTCPD acknowledge message to RTCPD using the
     * specified socket.
     */
    void sendRtcpAcknowledge(const Cuuid_t &cuuid,
      castor::io::AbstractTCPSocket &socket, const int netReadWriteTimeout,
      const RtcpAcknowledgeMessage &message)
      throw(castor::exception::Exception);

  }; // class RtcpdHandlerThread

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_RTCPDREQUESTHANDLERTHREAD_HPP
