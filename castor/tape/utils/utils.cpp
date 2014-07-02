/******************************************************************************
 *                      castor/tape/utils.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/

#include "castor/exception/InvalidArgument.hpp"
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
// writeStrings
//-----------------------------------------------------------------------------
void castor::tape::utils::writeStrings(std::ostream &os,
  const char **strings, const int stringsLen, const char *const separator) {
  // For each string
  for(int i=0; i<stringsLen; i++) {
    // Add a separator if this is not the first string
    if(i > 0) {
      os << separator;
    }

    // Write the string to the output stream
    os << strings[i];
  }
}

//-----------------------------------------------------------------------------
// magicToString
//-----------------------------------------------------------------------------
const char *castor::tape::utils::magicToString(const uint32_t magic)
  throw() {
  switch(magic) {
  case RTCOPY_MAGIC_VERYOLD: return "RTCOPY_MAGIC_VERYOLD";
  case RTCOPY_MAGIC_SHIFT  : return "RTCOPY_MAGIC_SHIFT";
  case RTCOPY_MAGIC_OLD0   : return "RTCOPY_MAGIC_OLD0";
  case RTCOPY_MAGIC        : return "RTCOPY_MAGIC";
  case RFIO2TPREAD_MAGIC   : return "RFIO2TPREAD_MAGIC";
  default                  : return "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// rtcopyReqTypeToString
//-----------------------------------------------------------------------------
const char *castor::tape::utils::rtcopyReqTypeToString(
  const uint32_t reqType) throw() {
  switch(reqType) {
  case GIVE_OUTP         : return "GIVE_OUTP";
  case RTCP_TAPE_REQ     : return "RTCP_TAPE_REQ";
  case RTCP_FILE_REQ     : return "RTCP_FILE_REQ";
  case RTCP_NOMORE_REQ   : return "RTCP_NOMORE_REQ";
  case RTCP_TAPEERR_REQ  : return "RTCP_TAPEERR_REQ";
  case RTCP_FILEERR_REQ  : return "RTCP_FILEERR_REQ";
  case RTCP_ENDOF_REQ    : return "RTCP_ENDOF_REQ";
  case RTCP_DUMP_REQ     : return "RTCP_DUMP_REQ";
  case RTCP_DUMPTAPE_REQ : return "RTCP_DUMPTAPE_REQ";
  case RTCP_PING_REQ     : return "RTCP_PING_REQ";
  case RTCP_HAS_MORE_WORK: return "RTCP_HAS_MORE_WORK";
  default                : return "UNKNOWN";
  }
}

//-----------------------------------------------------------------------------
// procStatusToString
//-----------------------------------------------------------------------------
const char *castor::tape::utils::procStatusToString(
  const uint32_t procStatus) throw() {
  switch(procStatus) {
  case RTCP_WAITING           : return "RTCP_WAITING";
  case RTCP_POSITIONED        : return "RTCP_POSITIONED";
  case RTCP_PARTIALLY_FINISHED: return "RTCP_PARTIALLY_FINISHED";
  case RTCP_FINISHED          : return "RTCP_FINISHED";
  case RTCP_EOV_HIT           : return "RTCP_EOV_HIT";
  case RTCP_UNREACHABLE       : return "RTCP_UNREACHABLE";
  case RTCP_REQUEST_MORE_WORK : return "RTCP_REQUEST_MORE_WORK";
  default                     : return "UNKNOWN";
  }
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
    line = trimString(line);

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
// trimString
//------------------------------------------------------------------------------
std::string castor::tape::utils::trimString(const std::string &str) throw() {
  const char *whitespace = " \t";

  std::string::size_type start = str.find_first_not_of(whitespace);
  std::string::size_type end   = str.find_last_not_of(whitespace);

  // If all the characters of the string are whitespace
  if(start == std::string::npos) {
    // The result is an empty string
    return std::string("");
  } else {
    // The result is what is in the middle
    return str.substr(start, end - start + 1);
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

//-----------------------------------------------------------------------------
// checkTpconfigLineDgn
//-----------------------------------------------------------------------------
static void checkTpconfigLineDgn(const std::string &catalogueDgn,
  const castor::tape::utils::TpconfigLine &line) {
  if(catalogueDgn != line.dgn) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only be asscoiated with one DGN"
      ": catalogueDgn=" << catalogueDgn << " lineDgn=" << line.dgn;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDevFilename
//-----------------------------------------------------------------------------
static void checkTpconfigLineDevFilename(const std::string &catalogueDevFilename,
  const castor::tape::utils::TpconfigLine &line)  {
  if(catalogueDevFilename != line.devFilename) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one device filename"
      ": catalogueDevFilename=" << catalogueDevFilename <<
      " lineDevFilename=" << line.devFilename;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineDensity
//-----------------------------------------------------------------------------
static void checkTpconfigLineDensity(const std::list<std::string> &catalogueDensities,
  const castor::tape::utils::TpconfigLine &line)  {
  for(std::list<std::string>::const_iterator itor = catalogueDensities.begin();
    itor != catalogueDensities.end(); itor++) {
    if((*itor) == line.density) {
      castor::exception::Exception ex;
      ex.getMessage() << "Invalid TPCONFIG line"
        ": A tape drive can only be associated with a tape density once"
        ": repeatedDensity=" << line.density;
      throw ex;
    }
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLineLibrarySlot
//-----------------------------------------------------------------------------
static void checkTpconfigLineLibrarySlot(const std::string &catalogueLibrarySlot,
  const  castor::tape::utils::TpconfigLine &line)  { 
  if(catalogueLibrarySlot != line.librarySlot) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one slot within its library"
      ": catalogueLibrarySlot=" << catalogueLibrarySlot <<
      " lineLibrarySlot=" << line.librarySlot;
    throw ex;
  } 
}   
    
//-----------------------------------------------------------------------------
// checkTpconfigLineDevType
//-----------------------------------------------------------------------------
static void checkTpconfigLineDevType(const std::string &catalogueDevType,
  const castor::tape::utils::TpconfigLine &line)  {
  if(catalogueDevType != line.devType) {
    castor::exception::Exception ex;
    ex.getMessage() << "Invalid TPCONFIG line"
      ": A tape drive can only have one device type"
      ": catalogueDevType=" << catalogueDevType <<
      " lineDevType=" << line.devType;
    throw ex;
  }
}

//-----------------------------------------------------------------------------
// checkTpconfigLine
//-----------------------------------------------------------------------------
static void checkTpconfigLine(const castor::tape::utils::DriveConfig &catalogueEntry,
  const castor::tape::utils::TpconfigLine &line) {
  checkTpconfigLineDgn(catalogueEntry.dgn, line);
  checkTpconfigLineDevFilename(catalogueEntry.devFilename, line);
  checkTpconfigLineDensity(catalogueEntry.densities, line);
  checkTpconfigLineLibrarySlot(catalogueEntry.librarySlot, line);
  checkTpconfigLineDevType(catalogueEntry.devType, line);
}

//------------------------------------------------------------------------------
// Static and therefore hidden function used by parseTpconfigFile()
//
// This function 
//------------------------------------------------------------------------------
static void enterTpconfigLine(castor::tape::utils::DriveConfigMap &drives, 
  const castor::tape::utils::TpconfigLine &line)
  throw(castor::exception::Exception) {
  using namespace castor::tape::utils;

  DriveConfigMap::iterator itor = drives.find(line.unitName);

  // If the drive is not in the catalogue
  if(drives.end() == itor) {
    // Insert it
    DriveConfig d;
    d.unitName = line.unitName;
    d.dgn = line.dgn;
    d.devFilename = line.devFilename;
    d.densities.push_back(line.density);
    d.librarySlot = line.librarySlot;
    d.devType = line.devType;
    drives[line.unitName] = d;
  // Else the drive is already in the catalogue
  } else {
    checkTpconfigLine(itor->second, line);

    // Each TPCONFIG line for a given drive specifies a new tape density
    //
    // Add the new density to the list of supported densities already stored
    // within the tape-drive catalogue
    itor->second.densities.push_back(line.density);
  }
}

//------------------------------------------------------------------------------
// parseTpconfigFile
//------------------------------------------------------------------------------
void castor::tape::utils::parseTpconfigFile(const std::string &filename,
  DriveConfigMap &drives) throw(castor::exception::Exception) {
  // Parse TPCONFIG in order to generate raw TpconfigLines
  TpconfigLines lines;
  parseTpconfigFile(filename, lines);

  // Construct the configuration of each tape drive from the previous parsed
  // lines of TPCONFIG
  for(TpconfigLines::const_iterator itor = lines.begin();
    itor != lines.end(); itor++) {
    enterTpconfigLine(drives, *itor);
  }
}

//------------------------------------------------------------------------------
// parseTpconfigFile
//------------------------------------------------------------------------------
void castor::tape::utils::parseTpconfigFile(const std::string &filename,
  TpconfigLines &lines)  {

  // The expected number of data-columns in a TPCONFIG data-line
  const unsigned int expectedNbOfColumns = 6;

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
    line = trimString(std::string(line));

    // If the line is not empty
    if(line != "") {

      // Replace each occurance of whitespace with a single space
      line = singleSpaceString(line);

      // Split the line into its component data-columns
      std::vector<std::string> columns;
      castor::utils::splitString(line, ' ', columns);

      // Throw an exception if the number of data-columns is invalid
      if(columns.size() != expectedNbOfColumns) {
        castor::exception::InvalidArgument ex;

        ex.getMessage() <<
          "Failed to parse TPCONFIG file"
          ": Invalid number of data columns in TPCONFIG line"
          ": filename='" << filename << "'"
          ": expectedNbColumns=" << expectedNbOfColumns <<
          ": actualNbColumns=" << columns.size() <<
          ": lineNb=" << lineNb;

        throw ex;
      }

      // Store the value of the data-coulmns in the output list parameter
      lines.push_back(TpconfigLine(
        columns[0], // unitName
        columns[1], // dgn
        columns[2], // devFilename
        columns[3], // density
        columns[4], // positionInLibrary
        columns[5]  // devType
      ));
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
// extractTpconfigDriveNames
//------------------------------------------------------------------------------
void castor::tape::utils::extractTpconfigDriveNames(
  const TpconfigLines &tpconfigLines, std::list<std::string> &driveNames)
  throw() {

  // Clear the list of drive unit names
  driveNames.clear();

  // For each parsed TPCONFIG data-line
  for(TpconfigLines::const_iterator itor = tpconfigLines.begin();
    itor != tpconfigLines.end(); itor++) {

    // If the drive unit name of the data-line is not already in the list of
    // drive-unit names, then add it
    if(std::find(driveNames.begin(), driveNames.end(), itor->unitName) ==
      driveNames.end()) {
      driveNames.push_back(itor->unitName);
    }
  }
}

//------------------------------------------------------------------------------
// getMandatoryValueFromConfiguration
//------------------------------------------------------------------------------
const char *castor::tape::utils::getMandatoryValueFromConfiguration(
  const char *const category, const char *const name)
   {

  const char *const tmp = getconfent(category, name, 0);

  if(tmp == NULL) {
    castor::exception::InvalidConfiguration ex;

    ex.getMessage() <<
      "Failed to get the value of the mandatory configuration parameter " <<
      category << "/" << name <<
      ": The parameter is not specified in castor.conf";

    throw ex;
  }

  if(isAnEmptyString(tmp)) {
    castor::exception::InvalidConfiguration ex;

    ex.getMessage() <<
      "Failed to get the value of the mandatory configuration parameter " <<
      category << "/" << name <<
      ": The value of the parameter is an empty string";

    throw ex;
  }

  return tmp;
}

//------------------------------------------------------------------------------
// isAnEmptyString
//------------------------------------------------------------------------------
bool castor::tape::utils::isAnEmptyString(const char *const str) throw() {
  return *str == '\0';
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

//---------------------------------------------------------------------------
// appendPathToEnvVar
//---------------------------------------------------------------------------
void castor::tape::utils::appendPathToEnvVar(const std::string &envVarName,
  const std::string &pathToBeAppended)  {

  // Get the current value of the enviornment variable (there may not be one)
  const char *const currentPath = getenv(envVarName.c_str());

  // Construct the new value of PYTHONPATH
  std::string newPath;
  if(currentPath == NULL || '\0' == *currentPath) {
    newPath = pathToBeAppended;
  } else {
    newPath  = currentPath;
    newPath += ":";
    newPath += pathToBeAppended;
  }

  // Set the value of the environment variable to the new value
  const int overwrite = 1;
  const int rc = setenv(envVarName.c_str(), newPath.c_str(), overwrite);
  if(rc == -1) {
    TAPE_THROW_EX(castor::exception::Exception,
      ": setenv() call failed" <<
      ": Insufficient space in the environment" <<
      ": name=" << envVarName <<
      " value=" << newPath <<
      " overwrite=" << overwrite);
  } else if(rc != 0) {
    TAPE_THROW_EX(castor::exception::Exception,
      ": setenv() call failed" <<
      ": Unknown error");
  }
}
