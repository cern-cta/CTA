/******************************************************************************
 *                 castor/tape/tpcp/TapeFileSequenceParser.hpp
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

#ifndef CASTOR_TAPE_TPCP_TAPEFILESEQUENCEPARSER_HPP
#define CASTOR_TAPE_TPCP_TAPEFILESEQUENCEPARSER_HPP 1

#include "castor/exception/Exception.hpp"
#include "castor/tape/tpcp/TapeFseqRangeList.hpp"

namespace castor {
namespace tape   {
namespace tpcp   {

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
   * @param tapeFseqRanges The resulting list of tape file sequence ranges.
   */
  static void parse(char *const str, TapeFseqRangeList &tapeFseqRanges)
    throw (castor::exception::Exception);

}; // class TapeFileSequenceParser

} // namespace tpcp
} // namespace tape
} // namespace castor


#endif // CASTOR_TAPE_TPCP_TAPEFILESEQUENCEPARSER_HPP
