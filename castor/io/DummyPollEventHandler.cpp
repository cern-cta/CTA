/******************************************************************************
 *                castor/io/DummyPollEventHandler.cpp
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

#include "castor/io/DummyPollEventHandler.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::io::DummyPollEventHandler::DummyPollEventHandler(const int fd) throw():
  m_fd(fd) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::io::DummyPollEventHandler::~DummyPollEventHandler() throw() {
}

//------------------------------------------------------------------------------
// getFd
//------------------------------------------------------------------------------
int castor::io::DummyPollEventHandler::getFd() throw() {
  return m_fd;
}

//------------------------------------------------------------------------------
// fillPollFd
//------------------------------------------------------------------------------
void castor::io::DummyPollEventHandler::fillPollFd(struct pollfd &fd) throw() {
  fd.fd = 0;
  fd.events = 0;
  fd.revents = 0;
}

//------------------------------------------------------------------------------
// handleEvent
//------------------------------------------------------------------------------
void castor::io::DummyPollEventHandler::handleEvent(const struct pollfd &fd)
  throw(castor::exception::Exception) {
}
