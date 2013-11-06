/******************************************************************************
 *                 castor/tape/tpcp/TapeFileSequenceParser.cpp
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
 * @author Nicola.Bessone@cern.ch Steven.Murray@cern.ch
 *****************************************************************************/
 
#include "castor/exception/InvalidArgument.hpp"
#include "castor/tape/tpcp/TapeFileSequenceParser.hpp"
#include "castor/tape/tpcp/TapeFseqRange.hpp"
#include "castor/tape/tpcp/TapeFseqRangeList.hpp"
#include "castor/tape/utils/utils.hpp"

#include <string>
#include <vector>


//------------------------------------------------------------------------------
// parseTapeFileSequence
//------------------------------------------------------------------------------
void castor::tape::tpcp::TapeFileSequenceParser::parse(char *const str,
  TapeFseqRangeList &tapeFseqRanges)
  throw (castor::exception::InvalidArgument) {

  std::vector<std::string> rangeStrs;
  int nbBoundaries = 0;

  // Range strings are separated by commas
  utils::splitString(str, ',', rangeStrs);

  // For each range string
  for(std::vector<std::string>::const_iterator itor=rangeStrs.begin();
    itor!=rangeStrs.end(); itor++) {

    std::vector<std::string> boundaryStrs;

    // Lower and upper boundary strings are separated by a dash ('-')
    utils::splitString(*itor, '-', boundaryStrs);

    nbBoundaries = boundaryStrs.size();

    switch(nbBoundaries) {
    case 1: // Range string = "n"
      if(!utils::isValidUInt(boundaryStrs[0].c_str())) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << boundaryStrs[0]
          << "': Expecting an unsigned integer";
        throw ex;
      }

      {
        const uint32_t upperLower = atoi(boundaryStrs[0].c_str());
        tapeFseqRanges.push_back(TapeFseqRange(upperLower, upperLower));
      }
      break;

    case 2: // Range string = "m-n" or "-n" or "m-" or "-"

      // If "-n" or "-" then the range string is invalid
      if(boundaryStrs[0] == "") {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << *itor
          << "': Strings of the form '-n' or '-' are invalid";
        throw ex;
      }

      // At this point the range string must be either "m-n" or "m-"

      // Parse the "m" of "m-n" or "m-"
      if(!utils::isValidUInt(boundaryStrs[0].c_str())) {
        castor::exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << *itor
          << "': The lower boundary should be an unsigned integer";
        throw ex;
      }

      {
        const uint32_t lower = atoi(boundaryStrs[0].c_str());

        if(lower == 0) {
          castor::exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << *itor
            << "': The lower boundary can not be '0'";
          throw ex;
        }

        // If "m-"
        if(boundaryStrs[1] == "") {

          // Infinity (or until the end of tape) is represented by 0
          tapeFseqRanges.push_back(TapeFseqRange(lower, 0));

        // Else "m-n"
        } else {

          // Parse the "n" of "m-n"
          if(!utils::isValidUInt(boundaryStrs[1].c_str())) {
            castor::exception::InvalidArgument ex;
            ex.getMessage() << "Invalid range string: '" << *itor
              << "': The upper boundary should be an unsigned integer";
            throw ex;
          }

          const uint32_t upper = atoi(boundaryStrs[1].c_str());

          if(upper == 0){
            castor::exception::InvalidArgument ex;
            ex.getMessage() << "Invalid range string: '" << *itor
              << "': The upper boundary can not be '0'";
            throw ex;
          }

          if(lower > upper){
            castor::exception::InvalidArgument ex;
            ex.getMessage() << "Invalid range string: '" << *itor
              << "': The lower boundary cannot be greater than the upper "
              "boundary";
            throw ex;
          }

          tapeFseqRanges.push_back(TapeFseqRange(lower, upper));
        } // Else "m-n"
      }


      break;

    default: // Range string is invalid
      castor::exception::InvalidArgument ex;
      ex.getMessage() << "Invalid range string: '" << *itor
        << "': A range string can only contain one or no dashes ('-')";
      throw ex;
    }
  }
}
