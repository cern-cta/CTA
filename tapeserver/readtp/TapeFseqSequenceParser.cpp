/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#include "common/exception/InvalidArgument.hpp"
#include "tapeserver/readtp/TapeFseqSequenceParser.hpp"
#include "common/utils/utils.hpp"

#include <list>
#include <string>
#include <vector>

namespace cta {
namespace tapeserver {
namespace readtp {

//------------------------------------------------------------------------------
// parseTapeFileSequence
//------------------------------------------------------------------------------
TapeFseqRangeList TapeFileSequenceParser::parse(char* const str) {
  TapeFseqRangeList tapeFseqRanges;
  std::vector<std::string> rangeStrs;

  // Range strings are separated by commas
  utils::splitString(str, ',', rangeStrs);

  // For each range string
  for (std::vector<std::string>::const_iterator itor = rangeStrs.begin(); itor != rangeStrs.end(); ++itor) {
    std::vector<std::string> boundaryStrs;

    // Lower and upper boundary strings are separated by a dash ('-')
    utils::splitString(*itor, '-', boundaryStrs);

    int nbBoundaries = boundaryStrs.size();

    switch (nbBoundaries) {
      case 1:  // Range string = "n"
        if (!utils::isValidUInt(boundaryStrs[0].c_str())) {
          exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << boundaryStrs[0] << "': Expecting an unsigned integer";
          throw ex;
        }

        {
          const uint32_t upperLower = atoi(boundaryStrs[0].c_str());
          tapeFseqRanges.push_back(TapeFseqRange(upperLower, upperLower));
        }
        break;

      case 2:  // Range string = "m-n" or "-n" or "m-" or "-"

        // If "-n" or "-" then the range string is invalid
        if (boundaryStrs[0] == "") {
          exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << *itor << "': Strings of the form '-n' or '-' are invalid";
          throw ex;
        }

        // At this point the range string must be either "m-n" or "m-"

        // Parse the "m" of "m-n" or "m-"
        if (!utils::isValidUInt(boundaryStrs[0].c_str())) {
          exception::InvalidArgument ex;
          ex.getMessage() << "Invalid range string: '" << *itor
                          << "': The lower boundary should be an unsigned integer";
          throw ex;
        }

        {
          const uint32_t lower = atoi(boundaryStrs[0].c_str());

          if (lower == 0) {
            exception::InvalidArgument ex;
            ex.getMessage() << "Invalid range string: '" << *itor << "': The lower boundary can not be '0'";
            throw ex;
          }

          // If "m-"
          if (boundaryStrs[1] == "") {
            // Infinity (or until the end of tape) is represented by 0
            tapeFseqRanges.push_back(TapeFseqRange(lower, 0));

            // Else "m-n"
          }
          else {
            // Parse the "n" of "m-n"
            if (!utils::isValidUInt(boundaryStrs[1].c_str())) {
              exception::InvalidArgument ex;
              ex.getMessage() << "Invalid range string: '" << *itor
                              << "': The upper boundary should be an unsigned integer";
              throw ex;
            }

            const uint32_t upper = atoi(boundaryStrs[1].c_str());

            if (upper == 0) {
              exception::InvalidArgument ex;
              ex.getMessage() << "Invalid range string: '" << *itor << "': The upper boundary can not be '0'";
              throw ex;
            }

            if (lower > upper) {
              exception::InvalidArgument ex;
              ex.getMessage() << "Invalid range string: '" << *itor
                              << "': The lower boundary cannot be greater than the upper "
                                 "boundary";
              throw ex;
            }

            tapeFseqRanges.push_back(TapeFseqRange(lower, upper));
          }  // Else "m-n"
        }

        break;

      default:  // Range string is invalid
        exception::InvalidArgument ex;
        ex.getMessage() << "Invalid range string: '" << *itor
                        << "': A range string can only contain one or no dashes ('-')";
        throw ex;
    }
  }
  return tapeFseqRanges;
}

}  // namespace readtp
}  // namespace tapeserver
}  // namespace cta