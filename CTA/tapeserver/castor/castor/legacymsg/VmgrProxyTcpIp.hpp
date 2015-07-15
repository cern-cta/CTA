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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/io/io.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/legacymsg/VmgrTapeInfoRqstMsgBody.hpp"
#include "castor/legacymsg/VmgrTapeMountedMsgBody.hpp"
#include "castor/legacymsg/VmgrMarshal.hpp"
#include "castor/log/Logger.hpp"

namespace castor {
namespace legacymsg {

/**
 * A concrete implementation of the interface to the vmgr daemon.
 */
class VmgrProxyTcpIp: public VmgrProxy {
public:
  /**
   * Constructor.
   *
   * @param vmgrHostName The name of the host on which the vmgrd daemon is
   * running.
   * @param vmgrPort The TCP/IP port on which the vmgrd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  VmgrProxyTcpIp(const std::string &vmgrHostName,
    const unsigned short vmgrPort, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~VmgrProxyTcpIp() throw();

  /**
   * Notifies the vmgrd daemon that the specified tape has been mounted for read.
   *
   * @param vid The volume identifier of the mounted tape.
   * @param jid The ID of the process that mounted the tape.
   */
  void tapeMountedForRead(const std::string &vid, const uint32_t jid);

  /**
   * Notifies the vmgrd daemon that the specified tape has been mounted for read.
   *
   * @param vid The volume identifier of the mounted tape.
   * @param jid The ID of the process that mounted the tape.
   */
  void tapeMountedForWrite(const std::string &vid, const uint32_t jid);

  /**
   * Gets information from vmgrd about the specified tape
   *
   * @param vid The volume identifier of the tape.
   * @return The reply from the vmgrd daemon.
   */
  legacymsg::VmgrTapeInfoMsgBody queryTape(const std::string &vid);

  /**
   * Gets information from the vmgrd daemon about the specified tape pool.
   *
   * @param poolName The name of teh tape pool.
   * @return The reply from the vmgrd daemon.
   */
  legacymsg::VmgrPoolInfoMsgBody queryPool(const std::string &poolName);

private:

  /**
   * The size of a vmgr request buffer in bytes.
   */
  static const size_t VMGR_REQUEST_BUFSIZE = 256;

  /**
   * The size of a vmr reply buffer in bytes.
   */
  static const size_t VMGR_REPLY_BUFSIZE = 4096;

  /**
   * The name of the host on which the vmgrd daemon is running.
   */
  const std::string m_vmgrHostName;

  /**
   * The TCP/IP port on which the vmgrd daemon is listening.
   */
  const unsigned short m_vmgrPort;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

  /**
   * Connects to the VMGR, writes a tape mount notification and receives the
   * reply.
   *
   * @param body Message body containing the notification.
   */
  void connectWriteNotificationAndReadReply(const VmgrTapeMountedMsgBody &body)
    const;

  /**
   * Connects to the vmgrd daemon.
   *
   * @return The socket-descriptor of the connection with the vmgrd daemon.
   */
  int connectToVmgr() const;

  /**
   * Writes a tape mount notification message with the specifed contents to the
   * specified connection.
   *
   * @param body Message body containing the notification.
   */
  void writeTapeMountNotificationMsg(const int fd,
    const VmgrTapeMountedMsgBody &body) const;

  /**
   * Reads a VMGR rc-type reply message from the specified connection.
   *
   * If the reply message represents an error then this method translates it
   * into an exception and then throws that exception.
   *
   * @param fd The file-descriptor of the connection.
   */
  void readVmgrRcReply(const int fd) const;

  /**
   * Reads a message header from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   * @return The message header.
   */
  MessageHeader readMsgHeader(const int fd) const;

  /**
   * Reads an ERR_MSG from the specified connection with the VMGR.
   *
   * @param fd          File descriptor of the connection.
   * @param replyHeader Reply header.
   * @return            The ERR_MSG in teh form of an exception object.
   */
  castor::exception::Exception readErrReply(const int fd,
    const MessageHeader &replyHeader) const;

  /**
   * Reads  DATA_MSG from the specified connection with the VMGR.
   *
   * @param fd          File descriptor of the connection
   * @param replyHeader Reply header
   * @param reply       Output parameter: Destination structure for the reply
   *                    coming from the VMGR
   */
  template <class MsgBody> MsgBody readDataReply(const int fd,
    MessageHeader &replyHeader) const {
    // Length of body buffer = Length of message buffer - length of header
    char bodyBuf[VMGR_REPLY_BUFSIZE - 3 * sizeof(uint32_t)];

    // If the message body is larger than the receive buffer
    if(replyHeader.lenOrStatus > sizeof(bodyBuf)) {
      castor::exception::Exception ex;
      ex.getMessage() <<
        "Message body from VMGR is larger than the receive buffer"
        ": reqType=MSG_DATA receiveBufferSize=" << sizeof(bodyBuf) << " bytes"
        " errorStringSizeIncludingNullTerminator=" << replyHeader.lenOrStatus <<
        " bytes";
      throw ex;
    }

    // Receive the message body
    try {
      io::readBytes(fd, m_netTimeout, replyHeader.lenOrStatus, bodyBuf);
    } catch (castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to receive message body from VMGR"
        ": reqType=MSG_DATA: " << ne.getMessage().str();
      throw ex;
    }

    // Unmarshal the message body
    MsgBody body;
    try {
      const char *bufPtr = bodyBuf;
      size_t len = replyHeader.lenOrStatus;
      legacymsg::unmarshal(bufPtr, len, body);
    } catch(castor::exception::Exception &ne) {
      castor::exception::Exception ex;
      ex.getMessage() << "Failed to unmarshal message body from VMGR"
        ": reqType=MSG_DATA: "<< ne.getMessage().str();
      throw ex;
    }

    return body;
  }

}; // class VmgrProxyTcpIp

} // namespace legacymsg
} // namespace castor
