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
// singleSpaceString
//------------------------------------------------------------------------------
std::string castor::tape::utils::singleSpaceString(
  const std::string &str) throw() {
  bool inWhitespace = false;
  bool strContainsNonWhiteSpace = false;

  // Output string stream used to construct the result
  std::ostringstream result;

  // For each character in the original string
  for(std::string::const_iterator itor = str.begin(); itor != str.end();
    itor++) {

    // If the character is a space or a tab
    if(*itor == ' ' || *itor == '\t') {

      // Remember we are in whitespace
      inWhitespace = true;

    // Else the character is not a space or a tab
    } else {

      // If we are leaving whitespace
      if(inWhitespace) {

        // Remember we have left whitespace
        inWhitespace = false;

        // Remember str contains non-whitespace
        strContainsNonWhiteSpace = true;

        // Insert a single space into the output string stream
        result << " ";
      }

      // Insert the character into the output string stream
      result << *itor;

    }
  }

  // If str is not emtpy and does not contain any non-whitespace characters
  // then nothing has been written to the result stream, therefore write a
  // single space
  if(!str.empty() && !strContainsNonWhiteSpace) {
    result << " ";
  }

  return result.str();
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
// parseTpconfigFile
//------------------------------------------------------------------------------
void castor::tape::utils::parseTpconfigFile(const std::string &filename,
  TpconfigLines &lines)  {
  // Open the TPCONFIG file for reading
  castor::utils::SmartFILEPtr file(fopen(filename.c_str(), "r"));
  {
    const int savedErrno = errno;

    // Throw an exception if the file could not be opened
    if(file.get() == NULL) {
      castor::exception::Exception ex(savedErrno);

      ex.getMessage() <<
        "Failed to parse TPCONFIG file"
        ": Failed to open file"
        ": filename='" << filename << "'"
        ": " << sstrerror(savedErrno);

      throw ex;
    }
  }

  // Line buffer
  char lineBuf[1024];

  // The error number recorded immediately after fgets is called
  int fgetsErrno = 0;

  // For each line was read
  for(int lineNb = 1; fgets(lineBuf, sizeof(lineBuf), file.get()); lineNb++) {
    fgetsErrno = errno;

    // Create a std::string version of the line
    std::string line(lineBuf);

    // Remove the newline character if there is one
    {
      const std::string::size_type newlinePos = line.find("\n");
      if(newlinePos != std::string::npos) {
        line = line.substr(0, newlinePos);
      }
    }

    // If there is a comment, then remove it from the line
    {
      const std::string::size_type startOfComment = line.find("#");
      if(startOfComment != std::string::npos) {
        line = line.substr(0, startOfComment);
      }
    }

    // Left and right trim the line of whitespace
    line = castor::utils::trimString(std::string(line));

    // If the line is not empty
    if(line != "") {

      // Replace each occurance of whitespace with a single space
      line = singleSpaceString(line);

      // Split the line into its component data-columns
      std::vector<std::string> columns;
      castor::utils::splitString(line, ' ', columns);

      // The expected number of data-columns in a TPCONFIG data-line is 4:
      //   unitName dgn devFilename librarySlot
      const unsigned int expectedNbOfColumns = 4;

      // Throw an exception if the number of data-columns is invalid
      if(columns.size() != expectedNbOfColumns) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "Failed to parse TPCONFIG file"
          ": Invalid number of data columns in TPCONFIG line"
          ": filename='" << filename << "'"
          " lineNb=" << lineNb <<
          " expectedNbColumns=" << expectedNbOfColumns <<
          " actualNbColumns=" << columns.size() <<
          " expectedFormat='unitName dgn devFilename librarySlot'";
        throw ex;
      }

      const TpconfigLine configLine(
        columns[0], // unitName
        columns[1], // dgn
        columns[2], // devFilename
        columns[3]  // librarySlot
      );

      if(CA_MAXUNMLEN < configLine.unitName.length()) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "Failed to parse TPCONFIG file"
          ": Tape-drive unit-name is too long"
          ": filename='" << filename << "'"
          " lineNb=" << lineNb <<
          " unitName=" << configLine.unitName <<
          " maxUnitNameLen=" << CA_MAXUNMLEN <<
          " actualUnitNameLen=" << configLine.unitName.length();
        throw ex;
      }

      if(CA_MAXDGNLEN < configLine.dgn.length()) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() <<
          "Failed to parse TPCONFIG file"
          ": DGN is too long"
          ": filename='" << filename << "'"
          " lineNb=" << lineNb <<
          " dgn=" << configLine.dgn <<
          " maxDgnLen=" << CA_MAXDGNLEN <<
          " actualDgnLen=" << configLine.dgn.length();
        throw ex;
      }

      // Store the value of the data-columns in the output list parameter
      lines.push_back(TpconfigLine(configLine));
    }
  }

  // Throw an exception if there was error whilst reading the file
  if(ferror(file.get())) {
    castor::exception::Exception ex(fgetsErrno);

    ex.getMessage() <<
      "Failed to parse TPCONFIG file"
      ": Failed to read file"
      ": filename='" << filename << "'"
      ": " << sstrerror(fgetsErrno);

    throw ex;
  }
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
