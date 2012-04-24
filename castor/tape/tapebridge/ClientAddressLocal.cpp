/******************************************************************************
 *                castor/tape/tapebridge/ClientAddressLocal.cpp
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

#include "castor/tape/net/net.hpp"
#include "castor/tape/tapebridge/ClientAddressLocal.hpp"
#include "castor/tape/utils/SmartFd.hpp"
#include "castor/tape/utils/utils.hpp"

#include <errno.h>
#include <sstream>
#include <sys/un.h>


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::ClientAddressLocal::ClientAddressLocal(
  const std::string &filename):
  ClientAddress(generateDescription(filename)),
  m_filename(filename) {
  // Do nothing
}


//-----------------------------------------------------------------------------
// generateDescription
//-----------------------------------------------------------------------------
std::string castor::tape::tapebridge::ClientAddressLocal::generateDescription(
  const std::string &filename) const {
  std::ostringstream oss;

  oss << "filename=" << filename;
  return oss.str();
}


//-----------------------------------------------------------------------------
// destructor
//-----------------------------------------------------------------------------
castor::tape::tapebridge::ClientAddressLocal::~ClientAddressLocal() throw() {
  // Do nothing
}


//-----------------------------------------------------------------------------
// checkValidity
//-----------------------------------------------------------------------------
void castor::tape::tapebridge::ClientAddressLocal::checkValidity()
  const throw(castor::exception::InvalidArgument) {
  if(m_filename.empty()) {
    TAPE_THROW_EX(castor::exception::InvalidArgument,
    ": Client address is invalid"
    ": filename is empty");
  }
}


//-----------------------------------------------------------------------------
// connectToClient
//-----------------------------------------------------------------------------
int castor::tape::tapebridge::ClientAddressLocal::connectToClient(
  const int netTimeout,
  timeval   &connectDuration)
  const throw(castor::exception::Exception) {

  struct sockaddr_un address;
  const socklen_t    address_len  = sizeof(address);
  memset(&address, '\0', sizeof(address));
  address.sun_family = PF_LOCAL;
  strncpy(address.sun_path, m_filename.c_str(), sizeof(address.sun_path) - 1);

  timeval connectStartTime = {0, 0};
  timeval connectEndTime   = {0, 0};
  utils::getTimeOfDay(&connectStartTime, NULL);

  utils::SmartFd smartConnectSock;
  try {
    smartConnectSock.reset(net::connectWithTimeout(
      PF_LOCAL,
      SOCK_STREAM,
      0, // sockProtocol
      (const struct sockaddr *)&address,
      address_len,
      netTimeout));
  } catch(castor::exception::Exception &ex) {
    // Add context and re-throw
    TAPE_THROW_CODE(ex.code(),
      ": Failed to connect to client"
      ": filename=" << m_filename <<
      ": " << ex.getMessage().str());
  }

  utils::getTimeOfDay(&connectEndTime, NULL);
  connectDuration = utils::timevalAbsDiff(connectStartTime, connectEndTime);

  return smartConnectSock.release();
}


//-----------------------------------------------------------------------------
// getFilename
//-----------------------------------------------------------------------------
const std::string &castor::tape::tapebridge::ClientAddressLocal::getFilename()
  const throw() {
  return m_filename;
}
