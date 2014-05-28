/******************************************************************************
 *         castor/legacymsg/VmgrProxyTcpIp.hpp
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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/legacymsg/VmgrProxy.hpp"
#include "castor/legacymsg/VmgrTapeInfoRqstMsgBody.hpp"
#include "castor/legacymsg/VmgrTapeMountedMsgBody.hpp"
#include "castor/legacymsg/VmgrMarshal.hpp"
#include "castor/log/Logger.hpp"

const size_t VMGR_REQUEST_BUFSIZE = 256;
const size_t VMGR_REPLY_BUFSIZE = 4096;

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
   * @param log The object representing the API of the CASTOR logging system.
   * @param vmgrHostName The name of the host on which the vmgrd daemon is
   * running.
   * @param vmgrPort The TCP/IP port on which the vmgrd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  VmgrProxyTcpIp(log::Logger &log, const std::string &vmgrHostName,
    const unsigned short vmgrPort, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~VmgrProxyTcpIp() throw();

  /**
   * Notifies the vmgrd daemon that the specified tape has been mounted for read.
   *
   * @param vid The volume identifier of the mounted tape.
   * @param
   */
  void tapeMountedForRead(const std::string &vid);

  /**
   * Notifies the vmgrd daemon that the specified tape has been mounted for read.
   *
   * @param vid The volume identifier of the mounted tape.
   * @param
   */
  void tapeMountedForWrite(const std::string &vid);
  
  /**
   * Gets information from vmgrd about the specified tape
   * 
   * @param vid   The volume identifier of the tape.
   * @param reply The structure containing the reply from vmgrd
   */
  void queryTape(const std::string &vid, legacymsg::VmgrTapeInfoMsgBody &reply);

private:

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

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
   * Connects to the vmgrd daemon.
   *
   * @return The socket-descriptor of the connection with the vmgrd daemon.
   */
  int connectToVmgr() const ;
  
  /**
   * Writes a tape mount notification message with the specifed contents to the specified
   * connection.
   *
   * @param body The message body containing information of the tape mounted
   */
  void writeTapeMountNotificationMsg(const int fd, const legacymsg::VmgrTapeMountedMsgBody &body) ;
  
  /**
   * Send the tape mount notification to the VMGR and receives the reply
   * 
   * @param body The message body containing information of the tape mounted
   */
  void sendNotificationAndReceiveReply(const legacymsg::VmgrTapeMountedMsgBody &body);
  
  /**
   * Reads a VMGR rc-type reply message from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   * @return The message.
   */
  void readVmgrRcReply(const int fd) ;

  /**
   * Reads an ack message from the specified connection.
   *
   * @param fd The file-descriptor of the connection.
   * @return The message.
   */
  MessageHeader readRcReplyMsg(const int fd) ;
  
  /**
   * Marshals the Query Tape request
   * 
   * @param request  Source request
   * @param buf      Destination buffer
   * @param totalLen Total length of bytes marshaled
   */
  void marshalQueryTapeRequest(const std::string &vid, legacymsg::VmgrTapeInfoRqstMsgBody &request, char *buf, size_t bufLen, size_t &totalLen);
  
  /**
   * Send the request out to the VMGR
   * 
   * @param fd       File descriptor of the connection
   * @param buf      Source buffer
   * @param totalLen Buffer length
   */
  void sendQueryTapeRequest(const std::string &vid, const int fd, char *buf, size_t totalLen);
  
  /**
   * Receives the header of the reply coming from the VMGR
   * 
   * @param fd          File descriptor of the connection
   * @param replyHeader Destination structure for the header
   */
  void receiveQueryTapeReplyHeader(const std::string &vid, const int fd, legacymsg::MessageHeader &replyHeader);
  
  /**
   * Function that receives and handles the error string coming from the VMGR
   * 
   * @param fd          File descriptor of the connection
   * @param replyHeader Reply header
   */
  void handleErrorReply(const std::string &vid, const int fd, legacymsg::MessageHeader &replyHeader);
  
  /**
   * Function that receives and unmarshals the reply data coming from the VMGR
   * 
   * @param fd          File descriptor of the connection
   * @param replyHeader Reply header
   * @param reply       Destination structure for the reply coming from the VMGR
   */
  void handleDataReply(const std::string &vid, const int fd, legacymsg::MessageHeader &replyHeader, legacymsg::VmgrTapeInfoMsgBody &reply);

}; // class VmgrProxyTcpIp

} // namespace legacymsg
} // namespace castor

