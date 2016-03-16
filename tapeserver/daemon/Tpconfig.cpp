/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "tapeserver/daemon/Tpconfig.hpp"
#include "common/utils/utils.hpp"
#include "common/exception/Errnum.hpp"

#include <errno.h>
#include <memory>
#include <fstream>
#include <algorithm>

//------------------------------------------------------------------------------
// parseTpconfigFile
//------------------------------------------------------------------------------
cta::tape::daemon::Tpconfig 
cta::tape::daemon::Tpconfig::parseFile(const std::string &filename) {
  Tpconfig ret;
  // Try to open the configuration file, throwing an exception if there is a
  // failure
  std::ifstream file(filename);
  if (file.fail()) {
    cta::exception::Errnum ex;
    ex.getMessage() << " In Tpconfig::parseFile()"
      ": Failed to open tpConfig file"
      ": filename=" << filename;
    throw ex;
  }
  
  std::string line;
  size_t lineNumber=0;
  while(++lineNumber, std::getline(file, line)) {
    // If there is a comment, then remove it from the line
    line = line.substr(0, line.find('#'));
    // get rid of potential tabs
    std::replace(line.begin(),line.end(),'\t',' ');
    std::istringstream sline(line);
    // This is the expected fields in the line. 
    std::string unitName, logicalLibarry, devFilePath, librarySlot;
    // If there is nothing on the line, ignore it.
    if (!(sline >> unitName)) continue;
    // But if there is anything, we expect all parameters.
    if (!( (sline >> logicalLibarry)
         &&(sline >> devFilePath)
         &&(sline >> librarySlot))) {
      cta::exception::Exception ex;
      ex.getMessage() << "In Tpconfig::parseFile(): " <<
        "missing parameter(s) from line=" << line <<
        " filename=" << filename;
      throw ex;
    }
    std::string extra;
    if ( sline >> extra) {
      cta::exception::Exception ex;
      ex.getMessage() << "In Tpconfig::parseFile(): " <<
        "extra parameter(s) from line=" << line <<
        " filename=" << filename;
      throw ex;
    }
    // The constructor implicitly validates the lengths 
    const TpconfigLine configLine(unitName, logicalLibarry, devFilePath, librarySlot);
    // Store the value of the data-columns in the output list parameter
    ret.push_back(TpconfigLine(configLine));
  }
  return ret;
}
