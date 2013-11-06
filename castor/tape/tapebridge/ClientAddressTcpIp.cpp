/******************************************************************************
 *                castor/tape/tapebridge/ClientAddressTcpIp.cpp
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

#include "castor/io/ClientSocket.hpp"
#include "castor/tape/tapebridge/ClientAddressTcpIp.hpp"
#include "castor/tape/utils/utils.hpp"
#include "h/Castor_limits.h"

#include <sstream>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::ClientAddressTcpIp::ClientAddressTcpIp(
  const std::string &hostname,
  const uint32_t    port):
  ClientAddress(generateDescription(hostname, port)),
  m_hostname(hostname),
  m_port(port) {
  // Do nothing
}


//-----------------------------------------------------------------------------
// generateDescription
//-----------------------------------------------------------------------------
std::string castor::tape::tapebridge::ClientAddressTcpIp::generateDescription(
  const std::string &hostname, const uint32_t port) const {
  std::ostringstream oss;

  oss << "hostname=" << hostname << " port=" << port;
  return oss.str();
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::ClientAddressTcpIp::~ClientAddressTcpIp() throw() {
  // Do nothing
}


//-----------------------------------------------------------------------------
// checkValidity
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientAddressTcpIp::checkValidity()
  const throw(castor::exception::InvalidArgument) {
  if(getHostname().empty()) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Client address is invalid"
      ": hostname is an empty string");
  }
  if(CA_MAXHOSTNAMELEN < getHostname().length()) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
      ": Client address is invalid"
      ": hostname is too long"
      ": maximum=" << CA_MAXHOSTNAMELEN);
  }
}


//-----------------------------------------------------------------------------
// connectToClient
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientAddressTcpIp::connectToClient(
  const int netTimeout,
  timeval   &connectDuration)
  const throw(castor::exception::Exception) {
  castor::io::ClientSocket sock(getPort(), getHostname());
  sock.setTimeout(netTimeout);
  try {
    timeval connectStartTime = {0, 0};
    timeval connectEndTime   = {0, 0};
    utils::getTimeOfDay(&connectStartTime, NULL);
    sock.connect();
    utils::getTimeOfDay(&connectEndTime, NULL);
    connectDuration = utils::timevalAbsDiff(connectStartTime, connectEndTime);
  } catch(castor::exception::Exception &ex) {
    TAPE_THROW_CODE(ex.code(),
      ": Failed to send message"
      ": Failed to connect to client"
      ": hostname=" << getHostname() <<
      " port=" << getPort() <<
      " netTimeout=" << netTimeout <<
      ": " << ex.getMessage().str());
  }

  // Release the socket-descriptor from the castor::io::ClientSocket object so
  // that it is not closed by the destructor of the object
  const int socketDescriptor = sock.socket();
  sock.resetSocket();

  // Return the socket-descriptor
  return(socketDescriptor);
}


//-----------------------------------------------------------------------------
// getHostname
//-----------------------------------------------------------------------------
const std::string &castor::tape::tapebridge::ClientAddressTcpIp::getHostname()
  const throw() {
  return m_hostname;
}


//-----------------------------------------------------------------------------
// getPort
//-----------------------------------------------------------------------------
uint32_t castor::tape::tapebridge::ClientAddressTcpIp::getPort()
  const throw () {
  return m_port;
}
