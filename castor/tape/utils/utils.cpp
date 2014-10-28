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
 *
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#include "castor/exception/InvalidArgument.hpp"
#include "castor/mediachanger/ConfigLibrarySlot.hpp"
#include "castor/tape/Constants.hpp"
#include "castor/tape/utils/utils.hpp"
#include "castor/utils/SmartFILEPtr.hpp"
#include "castor/utils/utils.hpp"
#include "h/Castor_limits.h"
#include "h/getconfent.h"
#include "h/rtcp_constants.h"
#include "h/serrno.h"

#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <locale>
#include <sstream>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

//-----------------------------------------------------------------------------
// toHexString
//-----------------------------------------------------------------------------
std::string castor::tape::utils::toHexString( const void * mem, unsigned int n ){
  std::ostringstream out;
  const unsigned char * p = reinterpret_cast< const unsigned char *>( mem );
  for ( unsigned int i = 0; i < n; i++ ) {
     if (0 != i) { 
       out << " "; 
     }
     out << std::uppercase << std::hex << std::setw(2) << std::setfill( out.widen('0') ) << int(p[i]);
     
  }
  return out.str();
}

//-----------------------------------------------------------------------------
// volumeClientTypeToString
//-----------------------------------------------------------------------------
const char *castor::tape::utils::volumeClientTypeToString(
  const tapegateway::ClientType mode) throw() {

  switch(mode) {
  case tapegateway::TAPE_GATEWAY: return "TAPE_GATEWAY";
  case tapegateway::READ_TP     : return "READ_TP";
  case tapegateway::WRITE_TP    : return "WRITE_TP";
  case tapegateway::DUMP_TP     : return "DUMP_TP";
  default                       : return "UKNOWN";
  }
}

//-----------------------------------------------------------------------------
// volumeModeToString
//-----------------------------------------------------------------------------
const char *castor::tape::utils::volumeModeToString(
  const tapegateway::VolumeMode mode) throw() {

  switch(mode) {
  case tapegateway::READ : return "READ";
  case tapegateway::WRITE: return "WRITE";
  case tapegateway::DUMP : return "DUMP";
  default                : return "UKNOWN";
  }
}

//------------------------------------------------------------------------------
// readFileIntoList
//------------------------------------------------------------------------------
void castor::tape::utils::readFileIntoList(const char *const filename,
  std::list<std::string> &lines)  {

  std::ifstream file(filename);

  if(!file) {
    castor::exception::Exception ex(ECANCELED);

    ex.getMessage() << "Failed to open file: Filename='" << filename << "'";

    throw ex;
  } 

  std::string line;

  while(!file.eof()) {
    line.clear();

    std::getline(file, line, '\n');

    if(!line.empty() || !file.eof()) {
      lines.push_back(line);
    }
  }
}

//------------------------------------------------------------------------------
// parseFileList
//------------------------------------------------------------------------------
void castor::tape::utils::parseFileList(const char *filename,
  std::list<std::string> &list)  {

  readFileIntoList(filename, list);

  std::list<std::string>::iterator itor=list.begin();

  while(itor!=list.end()) {
    std::string &line = *itor;

    // Left and right trim the line
    line = castor::utils::trimString(line);

    // Remove the line if it is an empty string or if it starts with the shell
    // comment character '#'
    if(line.empty() || (line.size() > 0 && line[0] == '#')) {
      itor = list.erase(itor);
    } else {
      itor++;
    }
  }
}

//------------------------------------------------------------------------------
// getPortFromConfig
//------------------------------------------------------------------------------
unsigned short castor::tape::utils::getPortFromConfig(
  const char *const category, const char *const name,
  const unsigned short defaultPort)
  throw(exception::InvalidConfigEntry, castor::exception::Exception) {

  unsigned short    port  = defaultPort;
  const char *const value = getconfent(category, name, 0);

  if(value != NULL) {
    if(castor::utils::isValidUInt(value)) {
      port = atoi(value);
    } else {
      exception::InvalidConfigEntry ex(category, name, value);

      ex.getMessage() <<
        "Invalid '" << category << " " << name << "' configuration entry"
        ": Value should be an unsigned integer greater than 0"
        ": Value='" << value << "'";

      throw ex;
    }

    if(port == 0) {
      exception::InvalidConfigEntry ex(category, name, value);

      ex.getMessage() <<
        "Invalid '" << category << " " << name << "' configuration entry"
        ": Value should be an unsigned integer greater than 0"
        ": Value='" << value << "'";

      throw ex;
    }
  }

  return port;
}

//------------------------------------------------------------------------------
// tapeBlockIdToString
//------------------------------------------------------------------------------
std::string castor::tape::utils::tapeBlockIdToString(
  const unsigned char blockId0,
  const unsigned char blockId1,
  const unsigned char blockId2,
  const unsigned char blockId3) throw() {
  std::ostringstream oss;

  oss << std::hex << std::setfill('0') <<
    std::setw(2) << (int)blockId0 <<
    std::setw(2) << (int)blockId1 <<
    std::setw(2) << (int)blockId2 <<
    std::setw(2) << (int)blockId3;

  return oss.str();
}
