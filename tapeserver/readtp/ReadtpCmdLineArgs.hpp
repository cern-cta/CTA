/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "tapeserver/readtp/TapeFseqRangeListSequence.hpp"

#include <optional>
#include <string>

namespace cta::tapeserver::readtp {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-readtp.
 */
struct ReadtpCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool help = false;

  /**
   * The tape VID to read.
   */
  std::string m_vid;

  /**
   * The unit name of the drive used to mount the tape.
   */
  std::optional<std::string> m_unitName;

  /**
   * Sequence of file fSeqs to read.
   */
  TapeFseqRangeList m_fSeqRangeList;

  /**
   * The destination file list url.
   */
  std::string m_destinationFileListURL;

  /**
   * Enable if we intend to find the files by blockId
   */
  bool m_searchByBlockID = false;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  ReadtpCmdLineArgs(const int argc, char* const* const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream& os);
};  // class ReadtpCmdLineArgs

}  // namespace cta::tapeserver::readtp
