/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"
#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"

#include <list>

namespace cta::tapeserver::readtp {

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

}  // namespace cta::tapeserver::readtp
