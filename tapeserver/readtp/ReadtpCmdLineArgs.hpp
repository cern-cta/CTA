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
   * Search by block id, with no optimizations
   */
  bool m_enablePositionByBlockID0 = false;

  /**
   * Search by block id, with optimization 1
   */
  bool m_enablePositionByBlockID1 = false;

  /**
   * Search by block id, with optimization 2
   */
  bool m_enablePositionByBlockID2 = false;

  /**
   * Enable global read session, instead of 1 session per file
   */
  bool m_enableGlobalReadSession = false;

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
