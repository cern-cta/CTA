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
 * @author dkruse@cern.ch
 *****************************************************************************/

#pragma once

#include "castor/messages/TapeserverProxyFactory.hpp"

namespace castor {
namespace messages {

/**
 * Concrete factory for creating objects of type TapeserverProxyZmq.
 */
class TapeserverProxyZmqFactory: public TapeserverProxyFactory {
public:

  /**
   * Constructor.
   *
   * @param log The object representing the API of the CASTOR logging system.
   * @param netTimeout The timeout in seconds to be applied when performing
   * network read and write operations.
   */
  TapeserverProxyZmqFactory(log::Logger &log, const unsigned short tapeserverPort, const int netTimeout) throw();

  /**
   * Destructor.
   */
  ~TapeserverProxyZmqFactory() throw();

  /**
   * Creates an object of type TapeserverProxyZmq on the heap and returns a pointer
   * to it.
   *
   * Please note that it is the responsibility of the caller to deallocate the
   * proxy object from the heap.
   *
   * @param zmqContext The ZMQ context.
   * @return A pointer to the newly created object.
   */
  TapeserverProxy *create(void *const zmqContext);

private:

  /**
   * The object representing the API of the CASTOR logging system.
   */
  log::Logger &m_log;
  
  /**
   * The TCP/IP port on which the vdqmd daemon is listening.
   */
  const unsigned short m_tapeserverPort;

  /**
   * The timeout in seconds to be applied when performing network read and
   * write operations.
   */
  const int m_netTimeout;

}; // class TapeserverProxyZmqFactory

} // namespace messages
} // namespace castor

