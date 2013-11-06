/******************************************************************************
 *                castor/tape/tapebridge/ClientAddressTcpIp.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_CLIENTADDRESSTCPIP_HPP
#define CASTOR_TAPE_TAPEBRIDGE_CLIENTADDRESSTCPIP_HPP 1

#include "castor/tape/tapebridge/ClientAddress.hpp"

#include <stdint.h>
#include <string>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Represents the TCP/IP address of a client of the tapebridged daemon.
 */
class ClientAddressTcpIp : public ClientAddress {
public:

  /**
   * Constructor.
   *
   * @param hostname The hostname of the TCP/IP address.
   * @param port     The port number of the TCP/IP address.
   */
  ClientAddressTcpIp(
    const std::string &hostname,
    const uint32_t    port);

  /**
   * Destructor
   */
  ~ClientAddressTcpIp() throw();

  /**
   * Throws a castor::exception::InvalidArgument exception if the client
   * address represented by this object is invalid.
   */
  void checkValidity() const throw(castor::exception::InvalidArgument);

  /**
   * Connects to the client at the address represented by this object.
   *
   * @param netTimeout      The timeout in seconds to be used when connecting
   *                        to the client.
   * @param connectDuration Out parameter: The time it took to connect to the
   *                        client.
   * @return                The socket-descriptor of the connection with the
   *                        client.
   */
  int connectToClient(
    const int netTimeout,
    timeval   &connectDuration)
    const throw(castor::exception::Exception);

  /**
   * Returns the hostname of the TCP/IP address.
   */
  const std::string &getHostname() const throw();

  /**
   * Returns the port number of the TCP/IP address.
   */
  uint32_t getPort() const throw();

private:

  /**
   * The hostname of the TCP/IP address.
   */
  const std::string m_hostname;

  /**
   * The port number of the TCP/IP address.
   */
  const uint32_t m_port;

  /**
   * Generates and returns a human-readable description of the specified
   * TCP/IP address.
   *
   * @param hostname The hostname of the TCP/IP address.
   * @param port     The port number of the TCP/IP address.
   */
  std::string generateDescription(
    const std::string &hostname,
    const uint32_t port)
    const;

}; // class ClientAddressTcpIp

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_CLIENTADDRESSTCPIP_HPP
