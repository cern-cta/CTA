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

#include "castor/tape/tapeserver/daemon/TpconfigLines.hpp"
#include "castor/utils/SmartFILEPtr.hpp"
#include "castor/utils/utils.hpp"
#include "serrno.h"

#include <errno.h>

//------------------------------------------------------------------------------
// parseTpconfigFile
//------------------------------------------------------------------------------
castor::tape::tapeserver::daemon::TpconfigLines castor::tape::tapeserver::
  daemon::TpconfigLines:: parseFile(const std::string &filename) {
  TpconfigLines lines;

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
        ": " << castor::utils::errnoToString(savedErrno);

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
      line = castor::utils::singleSpaceString(line);

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
      ": " << castor::utils::errnoToString(fgetsErrno);

    throw ex;
  }

  return lines;
}
