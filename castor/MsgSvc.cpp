/******************************************************************************
 *                      MsgSvc.cpp
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
 * @(#)$RCSfile: MsgSvc.cpp,v $ $Revision: 1.1.1.1 $ $Release$ $Date: 2004/05/12 12:13:34 $ $Author: sponcec3 $
 *
 * 
 *
 * @author Sebastien Ponce
 *****************************************************************************/

// Include Files
#include "castor/logstream.h"
#include "castor/Constants.hpp"
#include "castor/IService.hpp"
#include "castor/SvcFactory.hpp"
#include "common.h"

// Local Includes
#include "MsgSvc.hpp"

// -----------------------------------------------------------------------
// Instantiation of a static factory class
// -----------------------------------------------------------------------
static castor::SvcFactory<castor::MsgSvc> s_factoryMsgSvc;
const castor::IFactory<castor::IService>& MsgSvcFactory = s_factoryMsgSvc;
std::string castor::MsgSvc::s_defaultLogfilename = "log";

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
castor::MsgSvc::MsgSvc(const std::string name) :
  BaseSvc(name) {
  // Gather some configurations
  char* logFileRead = getconfent("LogFile", (char*)name.c_str(), 0);
  const char* logFile;
  if (0 == logFileRead) {
    logFile = s_defaultLogfilename.c_str();
  } else {
    logFile = strdup(logFileRead);
  }
  char* logLevel = getconfent((char*)logFile, "Level", 0);
  castor::logstream::Level l = castor::logstream::INFO;
  if (0 == logLevel) {
  } else if (0 == strncmp(logLevel, "VER", 3)) {
    l = castor::logstream::VERBOSE;
  } else if (0 == strncmp(logLevel, "DEB", 3)) {
    l = castor::logstream::DEBUG;
  } else if (0 == strncmp(logLevel, "WAR", 3)) {
    l = castor::logstream::WARNING;
  } else if (0 == strncmp(logLevel, "ERR", 3)) {
    l = castor::logstream::ERROR;
  } else if (0 == strncmp(logLevel, "FAT", 3)) {
    l = castor::logstream::FATAL;
  } else if (0 == strncmp(logLevel, "ALW", 3)) {
    l = castor::logstream::ALWAYS;
  }
  // create the inner stream
  m_stream = new castor::logstream(logFile, l);
}

// -----------------------------------------------------------------------
// Destructor
// -----------------------------------------------------------------------
castor::MsgSvc::~MsgSvc() {
  m_stream->close();
  delete m_stream;
}

// -----------------------------------------------------------------------
// id
// -----------------------------------------------------------------------
const unsigned int castor::MsgSvc::id() const {
  return ID();
}

// -----------------------------------------------------------------------
// ID
// -----------------------------------------------------------------------
const unsigned int castor::MsgSvc::ID() {
  return SVC_MSG;
}

// -----------------------------------------------------------------------
// stream
// -----------------------------------------------------------------------
castor::logstream& castor::MsgSvc::stream() {
  return *m_stream;
}


// -----------------------------------------------------------------------
//  Returns the logfilename
// -----------------------------------------------------------------------
std::string castor::MsgSvc::defaultLogfilename() {
  return s_defaultLogfilename;
}

// -----------------------------------------------------------------------
//  Sets the logfilename
// -----------------------------------------------------------------------
void castor::MsgSvc::setDefaultLogfilename(std::string lfn) {
  s_defaultLogfilename = lfn;
}
