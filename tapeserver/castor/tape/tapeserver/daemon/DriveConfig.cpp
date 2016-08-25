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
 *
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/mediachanger/LibrarySlotParser.hpp"
#include "castor/tape/tapeserver/daemon/DriveConfig.hpp"
#include "common/exception/Exception.cpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveConfig::DriveConfig() throw():
  m_librarySlot(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveConfig::DriveConfig(
  const std::string &unitName,
  const std::string &logicalLibrary,
  const std::string &devFilename,
  const std::string &librarySlot):
  m_unitName(unitName),
  m_logicalLibrary(logicalLibrary),
  m_devFilename(devFilename),
  m_librarySlot(mediachanger::LibrarySlotParser::parse(librarySlot)) {
}

//------------------------------------------------------------------------------
// copy constructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveConfig::DriveConfig(
  const DriveConfig &obj):
  m_unitName(obj.m_unitName),
  m_logicalLibrary(obj.m_logicalLibrary),
  m_devFilename(obj.m_devFilename),
  m_librarySlot(0 == obj.m_librarySlot ? 0 : obj.m_librarySlot->clone()) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveConfig::~DriveConfig() throw() {
  delete m_librarySlot;
}

//------------------------------------------------------------------------------
// assignment operator
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::DriveConfig &castor::tape::tapeserver::
  daemon::DriveConfig::operator=(const DriveConfig &rhs) {
  // If this is not a self assigment
  if(this != &rhs) {
    // Avoid a memory leak
    delete(m_librarySlot);

    m_unitName    = rhs.m_unitName;
    m_logicalLibrary         = rhs.m_logicalLibrary;
    m_devFilename = rhs.m_devFilename;
    m_librarySlot = 0 == rhs.m_librarySlot ? 0 : rhs.m_librarySlot->clone();
  }

  return *this;
}

//------------------------------------------------------------------------------
// getUnitName
//------------------------------------------------------------------------------
const std::string &castor::tape::tapeserver::daemon::DriveConfig::getUnitName()
  const throw() {
  return m_unitName;
}

//------------------------------------------------------------------------------
// getLogicalLibrary
//------------------------------------------------------------------------------
const std::string &castor::tape::tapeserver::daemon::DriveConfig::getLogicalLibrary()
  const throw() {
  return m_logicalLibrary;
}
  
//------------------------------------------------------------------------------
// getDevFilename
//------------------------------------------------------------------------------
const std::string &castor::tape::tapeserver::daemon::DriveConfig::
  getDevFilename() const throw() {
  return m_devFilename;
}

//------------------------------------------------------------------------------
// getLibrarySlot
//------------------------------------------------------------------------------
const castor::mediachanger::LibrarySlot &castor::tape::tapeserver::daemon::
  DriveConfig::getLibrarySlot() const {
  if(0 == m_librarySlot) {
    cta::exception::Exception ex;
    ex.getMessage() << "Failed to get library slot: Value not set";
    throw ex;
  }
  return *m_librarySlot;
}
