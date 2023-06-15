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

#pragma once

#include "common/exception/Exception.hpp"
#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"

#include <list>

namespace cta {
namespace tapeserver {
namespace readtp {

/**
 * Helper class to parse tape file sequence parameter strings.
 */
class TapeFileSequenceParser {
public:
  /**
   * Parse the specified tape file sequence parameter string and store the
   * resulting ranges into m_parsedCommandLine.tapeFseqRanges.
   *
   * The syntax rules for a tape file sequence specification are:
   * <ul>
   *  <li>  f1            File f1.
   *  <li>  f1-f2         Files f1 to f2 included.
   *  <li>  f1-           Files from f1 to the last file of the tape.
   *  <li>  f1-f2,f4,f6-  Series of ranges "," separated.
   * </ul>
   *
   * @param str The string received as an argument for the TapeFileSequence
   * option.
   * @return The resulting list of tape file sequence ranges.
   */
  static TapeFseqRangeList parse(char* const str);

};  // class TapeFileSequenceParser

}  // namespace readtp
}  // namespace tapeserver
}  // namespace cta