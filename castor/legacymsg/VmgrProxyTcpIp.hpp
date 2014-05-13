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

#include "castor/legacymsg/VmgrProxy.hpp"
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
  void tapeMountedForRead(const std::string &vid)
    throw (castor::exception::Exception);

  /**
   * Notifies the vmgrd daemon that the specified tape has been mounted for read.
   *
   * @param vid The volume identifier of the mounted tape.
   * @param
   */
  void tapeMountedForWrite(const std::string &vid)
    throw (castor::exception::Exception);

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
  int connectToVmgr() const throw(castor::exception::Exception);

}; // class VmgrProxyTcpIp

} // namespace legacymsg
} // namespace castor

