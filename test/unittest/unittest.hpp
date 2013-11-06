/******************************************************************************
 *                test/unittest/unittest.hpp
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

#ifndef TEST_UNITTEST_UNITTEST_HPP
#define TEST_UNITTEST_UNITTEST_HPP 1

#include "castor/tape/legacymsg/RtcpSegmentAttributes.hpp"

/**
 * Namespace full of utility methods for helping to write unit tests of CASTOR.
 */
namespace unittest {

  /**
   * Creates a unix-domain / local-domain socket for listening for incoming
   * connection requests.
   *
   * @param listenSockPath The full path of the file to be used for the
   *                       local-domain socket.
   * @return               A descriptor referencing the newly created socket.
   */
  int createLocalListenSocket(const char *const listenSockPath)
    throw (std::exception);

  /**
   * Reads in, acknowledges and drops the specified number of RTCOPY messages
   * from the specified socket.
   *
   * @param sockFdAndNMsgsPtr A pointer to a std::pair<int, int> where first is
   *                          the file-descriptor of the socket from which the
   *                          messages are to be read, and second specifies the
   *                          number of messages to be read in, acknowledged and
   *                          dropped.
   * @return                  Always NULL.
   */
  void *readInAckAndDropNRtcopyMsgs(void *sockFdAndNMsgsPtr);

  /**
   * Reads in the header and body of an RTCOPY message from the specified
   * socket, writes back a positive acknowledgement and then returns the
   * message as a raw block of data.  The size of the block of data can be
   * determined by unmarshalling the RTCOPY message header at the start of the
   * block.  The block is guaranteed to be at least be the size of an RTCOPY
   * message header  which is:
   *
   * uint32_t magic + uint32_t reqType + uint32_t len = 12 bytes
   *
   * Please note the message header and body to be read from the socket must be
   * correct.
   *
   * @param sockFdPtr Pointer to the file-descriptor of the socket from which
   *                  the message hedaer and body are to be read.
   * @return          Pointer to the newly allocated and filled raw block of
   *                  message header and body.  If an error is detected then
   *                  NULL is returned.
   *
   */
  void *readInAndAckRtcopyMsgHeaderAndBody(void *sockFdPtr);

  /**
   * Writes the specified positive RTCOPY acknowledgement to the specified
   * socket.
   *
   * @param sockFd  The file-descriptor of the socket to be written to.
   * @param magic   The magic number of the acknowledgement.
   * @param reqType The reqType number of the acknowledgement.
   */
  void writeRtcopyAck(
    const int      sockFd,
    const uint32_t magic,
    const uint32_t reqType)
    throw(std::exception);

  /**
   * Returns true if the connection associated with the specified
   * socket-descriptor has been closed by the peer.
   *
   * Please note that this method will perform a read on the specified if there
   * is a pending read event.
   *
   * @param sockFd A file-descriptor referencing the socket of the connection
   *               to be tested.
   */
  bool connectionHasBeenClosedByPeer(const int sockFd) throw(std::exception);

  /**
   * Reads in an RTCOPY acknowledgement message from the connection associated
   * with specified socket-descriptor.
   *
   * @param sockFd A file-descriptor referencing the socket of the connection
   *               to be tested.
   * @param magic   The expected magic-number of the acknowledgement message.
   * @param reqType The expected request-type of the acknowledgement message.
   * @param status  The expected status of the acknowledgement message.
   */
  void readAck(
    const int      sockFd,
    const uint32_t magic,
    const uint32_t reqType,
    const uint32_t status)
    throw(std::exception);

  /**
   * Writes a RTCP_REQUEST_MORE_WORK message to the specified connection.
   *
   * @param sockFd   A file-descriptor referencing the socket of the connection
   *                 to be written to.
   * @param volReqId The volume-request id of the RTCP_REQUEST_MORE_WORK
   *                 message.
   * @param tapePath The tape path of the RTCP_REQUEST_MORE_WORK message.
   */
  void writeRTCP_REQUEST_MORE_WORK(
    const int         sockFd,
    const uint32_t    volReqId,
    const char *const tapePath)
    throw(std::exception);

  /**
   * Write an RTCP_ENDOF_REQ message to the specified connection.
   *
   * @param sockFd A file-descriptor referencing the socket of the connection
   *               to be written to.
   */
  void writeRTCP_ENDOF_REQ(const int sockFd)
    throw(std::exception);

  /**
   * Writes an RTCP_FINISHED message to the specified connection.
   *
   * @param sockFd         A file-descriptor referencing the socket of the
   *                       connection to be written to.
   * @param volReqId       The volume-request id of the RTCP_REQUEST_MORE_WORK
   *                       message.
   * @param tapePath       The tape path of the RTCP_REQUEST_MORE_WORK message.
   * @param positionMethod TPPOSIT_FSEQ, TPPOSIT_FID, TPPOSIT_EOI or
   *                       TPPOSIT_BLKID
   * @param tapeFseq       The tape-file sequence-number.
   * @param diskFseq       The disk-file sequence-number.
   * @param bytesIn        Bytes from disk (write) or bytes from tape (read).
   * @param bytesOut       Bytes to tape (write) or bytes to disk (read).
   * @param segAttr        The segment attributes of the file.
   */
  void writeRTCP_FINISHED(
    const int         sockFd,
    const uint32_t    volReqId,
    const char *const tapePath,
    const int32_t     positionMethod,
    const int32_t     tapeFseq,
    const int32_t     diskFseq,
    const uint64_t    bytesIn,
    const uint64_t    bytesOut,
    const struct castor::tape::legacymsg::RtcpSegmentAttributes &segAttr)
    throw(std::exception);

  /**
   * Wrapper around the castor::tape::net::acceptConnection() method that
   * converts anything thrown by the wrapped method into an std::exception.
   *
   * @param listenSockFd The file descriptor of the listener socket.
   * @param timeout      The timeout in seconds to be used when waiting for a
   *                     connection.
   */
  int netAcceptConnection(
    const int    listenSockFd,
    const time_t &timeout)
    throw(std::exception);
} // namespace unittest

#endif // TEST_UNITTEST_UNITTEST_HPP
