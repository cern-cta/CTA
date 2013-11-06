/******************************************************************************
 *                      castor/tape/net/IpAndPort.cpp
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

#include "castor/tape/net/IpAndPort.hpp"


//-----------------------------------------------------------------------------
// constructor
//-----------------------------------------------------------------------------
castor::tape::net::IpAndPort::IpAndPort(
  const unsigned long  ip,
  const unsigned short port) throw():
  m_ip(ip),
  m_port(port) {
  // Do nothing
}


//-----------------------------------------------------------------------------
// setIp
//-----------------------------------------------------------------------------
void castor::tape::net::IpAndPort::setIp(const unsigned long ip) throw() {
  m_ip = ip;
}


//-----------------------------------------------------------------------------
// setPort
//-----------------------------------------------------------------------------
void castor::tape::net::IpAndPort::setPort(const unsigned short port) throw() {
  m_port = port;
}


//-----------------------------------------------------------------------------
// getIp
//-----------------------------------------------------------------------------
unsigned long castor::tape::net::IpAndPort::getIp() const throw() {
  return m_ip;
}


//-----------------------------------------------------------------------------
// getPort
//-----------------------------------------------------------------------------
unsigned short castor::tape::net::IpAndPort::getPort() const throw() {
  return m_port;
}
