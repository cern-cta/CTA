/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
   * Path to the xroot private key file.
   */
  std::string m_xrootPrivateKeyPath;

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
