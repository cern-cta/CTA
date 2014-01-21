/******************************************************************************
 *                castor/tape/tapebridge/RtcpTxRx.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_RTCPTXRX_HPP
#define CASTOR_TAPE_TAPEBRIDGE_RTCPTXRX_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/Internal.hpp"
#include "castor/io/ClientSocket.hpp"
#include "castor/tape/tapebridge/Constants.hpp"
#include "castor/tape/tapebridge/DlfMessageConstants.hpp"
#include "castor/tape/tapebridge/LogHelper.hpp"
#include "castor/tape/legacymsg/RtcpDumpTapeRqstMsgBody.hpp"
#include "castor/tape/legacymsg/CommonMarshal.hpp"
#include "castor/tape/legacymsg/RtcpMarshal.hpp"
#include "castor/tape/legacymsg/MessageHeader.hpp"
#include "castor/tape/net/net.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Cuuid.h"

#include <iostream>
#include <stdint.h>


namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Provides functions for sending and receiving the messages of the RTCOPY
 * protocol, in other words the protocol used between the VDQM and RTCPD and
 * between RTCPClientD and RTCPD.
 */
class RtcpTxRx {

public:

  /**
   * Gets the request information from RTCPD by sending and receiving the
   * necessary messages.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param reply The request structure to be filled with the reply from
   * RTCPD.
   */
  static void getRequestInfoFromRtcpd(const Cuuid_t &cuuid,
    const uint32_t volReqId, const int socketFd, const int netReadWriteTimeout,
    legacymsg::RtcpTapeRqstErrMsgBody &reply)
    throw(castor::exception::Exception);

  /**
   * Gives information about a volume to RTCPD by sending and receiving the
   * necessary messages.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request to be sent to RTCPD.
   * @param reply The structure to be filled with the reply from
   * RTCPD.
   */
  static void giveVolumeToRtcpd(const Cuuid_t &cuuid, const uint32_t volReqId,
    const int socketFd, const int netReadWriteTimeout,
    legacymsg::RtcpTapeRqstErrMsgBody &request)
    throw(castor::exception::Exception);

  /**
   * Pings RTCPD using the specified socket.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   */
  static void pingRtcpd(const Cuuid_t &cuuid, const uint32_t volReqId,
    const int socketFd, const int netReadWriteTimeout)
    throw(castor::exception::Exception);

  /**
   * Receives an RTCP job submission message.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The request which will be filled with the contents of the
   * received message.
   */
  static void receiveRtcpJobRqst(const Cuuid_t &cuuid, const int socketFd,
    const int netReadWriteTimeout, legacymsg::RtcpJobRqstMsgBody &request)
    throw(castor::exception::Exception);

  /**
   * Asks RTCPD to request more work in the future.
   *
   * A request for more work is in fact a file list with one special file
   * request which does not contain any file information, but instead contains
   * a flag indicating a request for more work.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param tapePath The tape path.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param mode The access mode.
   */ 
  static void askRtcpdToRequestMoreWork(const Cuuid_t &cuuid,
    const uint32_t volReqId, const char *const tapePath, const int socketFd,
    const int netReadWriteTimeout, const uint32_t mode)
    throw(castor::exception::Exception);

  /**
   * Gives the specified file list of one and only one actual file description
   * to RTCPD by sending and receiving the necessary messages.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param mode The access mode.
   * @param filePath The file path.
   * @param fileSize The size of the file in bytes.
   * @param tapePath The tape path.
   * @param recordFormat The record format.
   * @param tapeFileId The tape file ID.
   * @param umask The umask of the file.
   * @param positionMethod The position method.
   * @param tapeFseq The tape file sequence.
   * @param diskFseq The disk file sequence which has been overloaded to mean
   * the file transaction ID between the tapebridge and rtcpd.
   * @param nameServerHostName The host name of the name server.
   * @param castorFileId The CASTOR file ID.
   * @param blockId The block ID.
   */
  static void giveFileToRtcpd(const Cuuid_t &cuuid, const uint32_t volReqId,
    const int socketFd, const int netReadWriteTimeout, const uint32_t mode,
    const char *const filePath, const uint64_t fileSize,
    const char *const tapePath, const char *const recordFormat,
    const char *const tapeFileId, const uint32_t umask,
    const int32_t positionMethod, const int32_t tapeFseq,
    const int32_t diskFseq, char (&nameServerHostName)[CA_MAXHOSTNAMELEN+1],
    const uint64_t castorFileId, unsigned char (&blockId)[4]) 
    throw(castor::exception::Exception);


  /**
   * Tells RTCP to dump the tape.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param request The body of the RTCPD dump tape request.
   */
  static void tellRtcpdDumpTape(const Cuuid_t &cuuid, const uint32_t volReqId,
    const int socketFd, const int netReadWriteTimeout,
    legacymsg::RtcpDumpTapeRqstMsgBody &request)
    throw(castor::exception::Exception);

  /**
   * Receives a message body from RTCPD.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file desscriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param header The message header which has already been received.
   * @param body The body structure which will be filled with the contents of
   * the received message body.
   */
  template<class T> static void receiveMsgBody(
    const Cuuid_t &cuuid,
    const uint32_t volReqId,
    const int socketFd,
    const int,
    const legacymsg::MessageHeader &header,
    T &body) throw(castor::exception::Exception) {

    // Length of body buffer = Length of message buffer - length of header
    char bodyBuf[RTCPMSGBUFSIZE - 3 * sizeof(uint32_t)];

    // If the message body is too large
    if(header.lenOrStatus > sizeof(bodyBuf)) {
      TAPE_THROW_CODE(EMSGSIZE,
       ": Message body from RTCPD is too large"
       ": Maximum=" << sizeof(bodyBuf) << " Received=" << header.lenOrStatus);
    }

    {
      castor::dlf::Param params[] = {
        castor::dlf::Param("volReqId", volReqId),
        castor::dlf::Param("socketFd", socketFd)};

      castor::dlf::dlf_writep(cuuid, DLF_LVL_DEBUG,
        TAPEBRIDGE_RECEIVE_MSGBODY_FROM_RTCPD, params);
    }

    // Read the message body
    try {
      net::readBytes(socketFd, RTCPDNETRWTIMEOUT, header.lenOrStatus, bodyBuf);
    } catch (castor::exception::Exception &ex) {
      TAPE_THROW_CODE(EIO,
           ": Failed to read message body from RTCPD"
        << ": "<< ex.getMessage().str());
    }

    // Unmarshal the message body
    try {
      const char *p           = bodyBuf;
      size_t     remainingLen = header.lenOrStatus;
      legacymsg::unmarshal(p, remainingLen, body);
    } catch(castor::exception::Exception &ex) {
      TAPE_THROW_EX(castor::exception::Internal,
        ": Failed to unmarshal message body from RTCPD" <<
        ": "<< ex.getMessage().str());
    }

    LogHelper::logMsgBody(cuuid, DLF_LVL_DEBUG,
      TAPEBRIDGE_RECEIVED_MSGBODY_FROM_RTCPD, volReqId, socketFd, body);
  }

  /**
   * Inidcates the end of the current file list to RTCPD by sending and
   * receiving the necessary messages.
   *
   * @param cuuid The ccuid to be used for logging.
   * @param volReqId The volume request ID to be used for logging.
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   */
  static void tellRtcpdEndOfFileList(const Cuuid_t &cuuid,
    const uint32_t volReqId, const int socketFd, const int netReadWriteTimeout)
    throw(castor::exception::Exception);

private:

  /**
   * Private constructor to inhibit instances of this class from being
   * instantiated.
   */
  RtcpTxRx() {}

  /**
   * Gives the description of a file to RTCPD by sending and receiving the
   * necessary messages.
   *
   * @param socketFd The socket file descriptor of the connection with RTCPD.
   * @param netReadWriteTimeout The timeout to be applied when performing
   * network read and write operations.
   * @param mode The access mode.
   * @param request The request to be sent to RTCPD.
   * @param reply The structure to be filled with the reply from
   * RTCPD.
   */
  static void giveFileToRtcpd(const Cuuid_t &cuuid, const uint32_t volReqId,
    const int socketFd, const int netReadWriteTimeout, const uint32_t mode,
    legacymsg::RtcpFileRqstErrMsgBody &request)
    throw(castor::exception::Exception);

  /**
   * Throws an exception if the expected magic number is not equal to the
   * actual value.
   *
   * @param expected The expected magic number.
   * @param actual The actual magic number.
   * @param function The name of the caller function.
   */
  static void checkMagic(const uint32_t expected, const uint32_t actual,
    const char *function) throw(castor::exception::Exception);

  /**
   * Throws an exception if the expected RTCOPY_MAGIC request type is not equal
   * to the actual value.
   *
   * @param expected The expected request type.
   * @param received The actual request type.
   * @param function The name of the caller function.
   */
  static void checkRtcopyReqType(const uint32_t expected,
    const uint32_t actual, const char *function)
    throw(castor::exception::Exception);

  /**
   * Throws an exception if one of the expected RTCOPY_MAGIC request types is
   * not equal to the actual value.
   *
   * @param expected   Array of expected request types.
   * @param nbExpected The size of the array of expected request types.
   * @param received   The actual request type.
   * @param function   The name of the caller function.
   */
  static void checkRtcopyReqType(const uint32_t *expected,
    const size_t nbExpected, const uint32_t actual, const char *function)
    throw(castor::exception::Exception);

  /**
   * Throws an exception if one of the expected RTCOPY_MAGIC request types is
   * not equal to the actual value.
   *
   * @param expected   Array of expected request types.
   * @param received   The actual request type.
   * @param function   The name of the caller function.
   */
  template <size_t n> static void checkRtcopyReqType(
    const uint32_t (&expected)[n], const uint32_t actual, const char *function)
    throw(castor::exception::Exception) {
    checkRtcopyReqType(expected, n, actual, function);
  }

}; // class RtcpTxRx

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_RTCPTXRX_HPP
