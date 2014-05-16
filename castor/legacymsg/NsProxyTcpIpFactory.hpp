/******************************************************************************
 *         castor/legacymsg/NsProxyTcpIpFactory.hpp
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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/legacymsg/NsProxyFactory.hpp"

namespace castor {
namespace legacymsg {

/**
 * Concrete factory for creating objects of type RmcProxyTcpIp.
 */
class NsProxyTcpIpFactory: public NsProxyFactory {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  NsProxyTcpIpFactory(log::Logger &log, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~NsProxyTcpIpFactory() throw();

  /**
   * Creates an object of type NsProxyTcpIp on the heap and returns a pointer
   * to it.
   *
   * Please note that it is the responsibility of the caller to deallocate the
   * proxy object from the heap.
   *
   * @return A pointer to the newly created object.
   */
  NsProxy *create();

private:

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

}; // class NsProxyTcpIpFactory

} // namespace legacymsg
} // namespace castor

