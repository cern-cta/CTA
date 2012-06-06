/******************************************************************************
 *                      castor/tape/net/IpAndPort.hpp
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
 *
 * @author Steven.Murray@cern.ch
 *****************************************************************************/

#ifndef CASTOR_TAPE_NET_IPANDPORT_HPP
#define CASTOR_TAPE_NET_IPANDPORT_HPP 1

#include <stdint.h>
#include <stdlib.h>


namespace castor {
namespace tape   {
namespace net    {

/**
 * Class used to store the IP number and the port number of a TCP/IP address.
 */
class IpAndPort {
public:

  /**
   * Constructor.
   *
   * @param ip   The IP number of the TCP/IP address.
   * @param port The port number of the TCP/IP address.
   */
  IpAndPort(const unsigned long ip, const unsigned short port) throw();

  /**
   * Sets the IP number of the TCP/IP address.
   */ 
  void setIp(const unsigned long ip) throw();

  /**
   * Sets the port number of the TCP/IP address.
   */
  void setPort(const unsigned short port) throw();

  /**
   * Gets the IP number of the TCP/IP address.
   */
  unsigned long getIp() const throw();

  /**
   * Gets the port number of the TCP/IP address.
   */
  unsigned short getPort() const throw();

private:

  /**
   * The IP number of the TCP/IP address.
   */
  unsigned long  m_ip;

  /**
   * The port number of the TCP/IP address.
   */
  unsigned short m_port;
}; // class IpAndPort
  	
} // namespace net
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_NET_IPANDPORT_HPP
