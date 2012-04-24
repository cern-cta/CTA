/******************************************************************************
 *                castor/tape/tapebridge/ClientAddress.hpp
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

#ifndef CASTOR_TAPE_TAPEBRIDGE_CLIENTADDRESS_HPP
#define CASTOR_TAPE_TAPEBRIDGE_CLIENTADDRESS_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/exception/InvalidArgument.hpp"

#include <string>
#include <sys/time.h>

namespace castor     {
namespace tape       {
namespace tapebridge {

/**
 * Abstract class representing the address of a client of the tapebridged
 * daemon.
 */
class ClientAddress {
public:

  /**
   * Constructor.
   *
   * @param description A human-readable description of the client address.
   */
  ClientAddress(const std::string &description);

  /**
   * Destructor.
   */
  virtual ~ClientAddress() throw();

  /**
   * Throws a castor::exception::InvalidArgument exception if the client
   * address represented by this object is invalid.
   */
  virtual void checkValidity()
    const throw(castor::exception::InvalidArgument) = 0;

  /**
   * Returns a human-readable description of the client address.
   */
  const std::string &getDescription() const throw ();

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
  virtual int connectToClient(
    const int netTimeout,
    timeval   &connectDuration)
    const throw(castor::exception::Exception) = 0;

private:

  /**
   * A human-readable description of the client address.
   */
  const std::string m_description;

}; // class ClientAddress

} // namespace tapebridge
} // namespace tape
} // namespace castor

#endif // CASTOR_TAPE_TAPEBRIDGE_CLIENTADDRESS_HPP
