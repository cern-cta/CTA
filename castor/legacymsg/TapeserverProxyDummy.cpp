/******************************************************************************
 *         castor/legacymsg/TapeserverProxyDummy.cpp
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

#include "castor/legacymsg/TapeserverProxyDummy.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::legacymsg::TapeserverProxyDummy::TapeserverProxyDummy() throw() {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::legacymsg::TapeserverProxyDummy::~TapeserverProxyDummy() throw() {
}

//------------------------------------------------------------------------------
// gotReadMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyDummy::gotReadMountDetailsFromClient(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// gotWriteMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyDummy::gotWriteMountDetailsFromClient(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// gotDumpMountDetailsFromClient
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyDummy::gotDumpMountDetailsFromClient(
  const std::string &unitName,
  const std::string &vid)
  throw(castor::exception::Exception) {
}

//------------------------------------------------------------------------------
// tapeUnmounted
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyDummy::tapeUnmounted(
const std::string &vid) {

}
  
//------------------------------------------------------------------------------
// tapeMountedForRead
//------------------------------------------------------------------------------
void castor::legacymsg::TapeserverProxyDummy::tapeMountedForRead(
const std::string &vid) {
  
}

