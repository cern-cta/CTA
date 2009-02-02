/******************************************************************************
 *                castor/tape/aggregator/Transceiver.hpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_AGGREGATOR_TRANSCEIVER_HPP
#define CASTOR_TAPE_AGGREGATOR_TRANSCEIVER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/aggregator/RtcpAcknowledgeMsgBody.hpp"
#include "castor/tape/aggregator/RtcpTapeRequestMsgBody.hpp"
#include "castor/tape/aggregator/RtcpFileRequestMsgBody.hpp"

namespace castor     {
namespace tape       {
namespace aggregator {

/**
 * Provides functions for sending and receiving the messages used by the tape
 * aggregator.
 */
class Transceiver {

public:

  /**
   * Gets the volume request ID from RTCPD by sending and receiving the
   * necessary messages.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param reply The request structure to be filled with the reply from
   * RTCPD.
   */
  static void getVolumeRequestIdFromRtcpd(const int socketFd,
    const int netReadWriteTimeout, RtcpTapeRequestMsgBody &reply)
    throw(castor::exception::Exception);

  /**
   * Gives a volume ID to RTCPD by sending and receiving the necessary
   * messages.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request to be sent to RTCPD.
   * @param reply The structure to be filled with the reply from
   * RTCPD.
   */
  static void giveVolumeIdToRtcpd(const int socketFd,
    const int netReadWriteTimeout, RtcpTapeRequestMsgBody &request,
    RtcpTapeRequestMsgBody &reply) throw(castor::exception::Exception);

  /**
   * Gives the description of a file to RTCPD by sending and receiving the
   * necessary messages.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request to be sent to RTCPD.
   * @param reply The structure to be filled with the reply from
   * RTCPD.
   */
  static void giveFileInfoToRtcpd(const int socketFd,
    const int netReadWriteTimeout, RtcpFileRequestMsgBody &request,
    RtcpFileRequestMsgBody &reply) throw(castor::exception::Exception);

  /**
   * Signals the end of the file list to RTCPD by sending and receiving the
   * necessary messages.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   */
  static void signalNoMoreRequestsToRtcpd(const int socketFd,
    const int netReadWriteTimeout) throw(castor::exception::Exception);

  /**
   * Receives an acknowledge message from RTCPD and returns the status code
   * embedded within the message.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param message The message structure to be filled with the acknowledge
   * message.
   */
  static void receiveRtcpAcknowledge(const int socketFd,
    const int netReadWriteTimeout, RtcpAcknowledgeMsgBody &message)
    throw(castor::exception::Exception);

  /**
   * Sends the specified RTCPD acknowledge message to RTCPD using the
   * specified socket.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param message The message to be sent.
   */
  static void sendRtcpAcknowledge(const int socketFd,
    const int netReadWriteTimeout, const RtcpAcknowledgeMsgBody &message)
    throw(castor::exception::Exception);

  /**
   * Receives an RTCP job submission message.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request which will be filled with the contents of the
   * received message.
   */
  static void receiveRcpJobRequest(const int socketFd,
    const int netReadWriteTimeout, RcpJobRequestMsgBody &request)
    throw(castor::exception::Exception);

  /**
   * Gives a request for more work to RTCPD by sending and receiving the
   * necessary messages.
   *
   * A request for more work is infact a file list with one special file
   * request which does not contain any file information, but instead contains
   * a flag indicating a request for more work.
   */ 
  static void giveRequestForMoreWorkToRtcpd(const int socketFd,
    const int netReadWriteTimeout, const uint32_t volReqId)
    throw(castor::exception::Exception);

  /**
   * Gives the specified file list of one and only one actual file description
   * to RTCPD by sending and receiving the necessary messages.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * @param filePath The file path.
   * @param umask The umask of the file.
   * @param requestMoreWork Set to true if RTCPD should be told that there is
   * a possibility of more work in the future.
   * network read and write operations.
   */
  static void giveFileListToRtcpd(const int socketFd,
    const int netReadWriteTimeout, const uint32_t volReqId,
    const char *const filePath, const uint32_t umask,
    const bool requestMoreWork) throw(castor::exception::Exception);

  /**
   * Receives a message header.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request which will be filled with the contents of the
   * received message.
   */
  static void receiveRtcpMessageHeader(const int socketFd,
    const int netReadWriteTimeout, MessageHeader &header)
    throw(castor::exception::Exception);

  /**
   * Receives the body of an RTCPD file request message from RTCPD.
   *
   * @param socketFd The socket file desscriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param header The message header which has already been received.
   * @param request The request which will be filled with the contents of the
   * received message.
   */
  static void receiveRtcpFileRequestBody(const int socketFd,
    const int netReadWriteTimeout, const MessageHeader &header,
    RtcpFileRequestMsgBody &request) throw(castor::exception::Exception);

  /**
   * Receives the body of an RTCPD tape request message from RTCPD.
   *
   * @param socketFd The socket file desscriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param header The message header which has already been received.
   * @param request The request which will be filled with the contents of the
   * received message.
   */
  static void receiveRtcpTapeRequestBody(const int socketFd,
    const int netReadWriteTimeout, const MessageHeader &header,
    RtcpTapeRequestMsgBody &request) throw(castor::exception::Exception);

private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  Transceiver() {}

}; // class Transceiver

} // namespace aggregator
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_AGGREGATOR_TRANSCEIVER_HPP
