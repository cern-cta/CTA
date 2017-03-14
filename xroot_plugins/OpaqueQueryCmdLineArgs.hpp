/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <ostream>
#include <stdint.h>
#include <string>

namespace cta {
namespace xroot_plugins {

/**
 * Structure to store the command-line arguments of the command-line tool
 * named cta-xroot_plugins-fake-eos.
 */
struct OpaqueQueryCmdLineArgs {
  /**
   * The name of the host on which the CTA front end is running.
   */
  std::string ctaHost;

  /**
   * The TCP/IP port on which the CTA front end is listening.
   */
  uint16_t ctaPort;

  /**
   * The name of the file containing binary the query argument to be sent to the
   * CTA front end using:
   *
   *     XrdCl::FileSystem::Query(XrdCl::QueryCode::Opaque, arg, response)
   *
   * Where arg is the contents of the named file.
   */
  std::string queryFilename;

  /**
   * The name of the file to which the binary response from the CTA front end
   * will be written.
   */
  std::string responseFilename;

  /**
   * True if the usage message should be printed.
   */
  bool help;

  /**
   * Constructor that parses the specified command-line arguments.
   *
   * @param argc The number of command-line arguments including the name of the
   * executable.
   * @param argv The vector of command-line arguments.
   */
  OpaqueQueryCmdLineArgs(const int argc, char *const *const argv);

  /**
   * Prints the usage message of the command-line tool.
   *
   * @param os The output stream to which the usage message is to be printed.
   */
  static void printUsage(std::ostream &os);
};

} // namespace xroot_plugins
} // namespace cta
