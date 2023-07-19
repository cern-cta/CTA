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

#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"

#include <string>

namespace cta {
namespace tapeserver {
namespace readtp {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-readtp.
 */
struct ReadtpCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help;
  
  /**
   * The tape VID to read.
   */
  std::string m_vid;

  /**
   * Sequence of file fSeqs to read.
   */
  TapeFseqRangeList m_fSeqRangeList;
    
  /**
   * The destination file list url.
   */  
  std::string m_destinationFileListURL;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  ReadtpCmdLineArgs(const int argc, char *const *const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream &os);
}; // class ReadtpCmdLineArgs

} // namespace readtp
} // namespace tapeserver
} // namespace cta
