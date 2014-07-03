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
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/exception/Exception.hpp"
#include "castor/legacymsg/CupvCheckMsgBody.hpp"
#include "castor/legacymsg/CupvProxy.hpp"
#include "castor/legacymsg/MessageHeader.hpp"
#include "castor/log/Logger.hpp"

#include <string>

namespace castor {
namespace legacymsg {

/**
 * Proxy class representing the CASTOR user-privilege validation-daemon.
 */
class CupvProxyTcpIp: public CupvProxy {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param cupvHostName The name of the host on which the cupvd daemon is
   * running.
   * @param vdqmPort The TCP/IP port on which the cupvd daemon is listening.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  CupvProxyTcpIp(log::Logger &log, const std::string &cupvHostName,
    const unsigned short cupvPort, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~CupvProxyTcpIp() throw();

  /**
   * Returns true if the specified authorization is granted else false.
   *
   * @param privUid The user ID of the privilege.
   * @param privGid The group ID of the privilege.
   * @param srcHost The source host.
   * @param tgtHost The target host.
   * @param priv The privilege which must be one of the following:
   * P_ADMIN, P_GRP_ADMIN, P_OPERATOR, P_TAPE_OPERATOR, P_TAPE_SYSTEM or
   * P_UPV_ADMIN.
   * @return True if the specified authorization is granted else false.
   */
  bool isGranted(
    const uid_t privUid,
    const gid_t privGid,
    const std::string &srcHost,
    const std::string &tgtHost,
    const int privilege) ;

private:

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The name of the host on which the cupvd daemon is running.
   */
  const std::string m_cupvHostName;

  /**
   * The TCP/IP port on which the cupvd daemon is listening.
   */
  const unsigned short m_cupvPort;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

  /**
   * Connects to the cupvd daemon.
   *
   * @return The socket-descriptor of the connection with the vdqmd daemon.
   */
  int connectToCupv() const ;

  /**
   * Writes a CUPV_CHECK message with the specifed contents to the specified
   * connection.
   *
   * @param body The message body.
   */
  void writeCupvCheckMsg(const int fd, const CupvCheckMsgBody &body) ;

  /**
   * Reads the header of an CUPV_MAGIC message from the specified connection.
   *
   * @param fd The file descriptor of the connection.
   * @return The message header.
   */
  MessageHeader readCupvMsgHeader(const int fd) ;

}; // class CupvProxyTcpIp

} // namespace legacymsg
} // namespace castor

