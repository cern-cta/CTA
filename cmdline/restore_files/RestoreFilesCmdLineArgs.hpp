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

#include "version.h"
#include "common/optional.hpp"

#include <list>

namespace cta {
namespace admin {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-restore-deleted-archive.
 */
struct RestoreFilesCmdLineArgs {
  /**
   * True if the usage message should be printed.
   */
  bool m_help;

  /**
   * True if debug messages should be printed
   */
  bool m_debug;

  /**
   * Archive file id of the files to restore
   */
  optional<uint64_t> m_archiveFileId;

  /**
   * Disk instance of the files to restore
   */
  optional<std::string> m_diskInstance;

  /**
   * Fxids of the files to restore
   */
  optional<std::list<std::string>> m_eosFxids;

  /**
   * Vid of the tape of the files to restore
   */
  optional<std::string> m_vid;

  /**
   *Copy number of the files to restore
   */
  optional<uint64_t> m_copyNumber;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  RestoreFilesCmdLineArgs(const int argc, char *const *const argv);

   /**
   * Read a list of eos file ids from a file and write the options to a list
   *
   * @param filename The name of the file to read
   * @param optionList The list to append the options.
   */
   void readFidListFromFile(const std::string &filename, std::list<std::string> &optionList);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream &os);



}; // class RestoreFilesCmdLineArgs

} // namespace admin
} // namespace cta
