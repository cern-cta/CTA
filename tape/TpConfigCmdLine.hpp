/******************************************************************************
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
 * @author Castor Dev team, castor-dev@cern.ch
 *****************************************************************************/

#pragma once

#include <string>

namespace tape {

/**
 * The result of parsing the command line of the tpconfig command-line tool.
 */ 
struct TpConfigCmdLine {
  /**
   * The unit name of the tape drive to be configured.
   */
  std::string unitName;

  /**
   * The status that the tape drive should be moved to.
   */
  int status;

  /**
   * Constructor.
   *
   * @param unitName The unit name of the tape drive to be configured.
   * @param status The status that the tape drive should be moved to.
   */
  TpConfigCmdLine(const std::string &unitName, const int status) throw();

  /**
   * Parses the specified command line.
   *
   * @param argc The number of command-line arguments including the program
   * name.
   * @param argv The array of command-line arguments including the program
   * name.
   * @return The parsed command-line.
   */
  static TpConfigCmdLine parse(const int argc, const char **argv);

private:

  /**
   * Parses the specified short version of the command-line.
   *
   * @param argc The number of command-line arguments including the program
   * name.
   * @param argv The array of command-line arguments including the program
   * name.
   * @return The parsed command-line.
   */
  static TpConfigCmdLine parseShortVersion(const int argc, const char **argv);

  /**
   * Parses the specified drive status string.
   *
   * @param str The string to be parsed.
   * @return The status.
   */
  static int parseStatus(const std::string &str);

  /**
   * Parses the specified long version of the command-line.
   *
   * @param argc The number of command-line arguments including the program
   * name.
   * @param argv The array of command-line arguments including the program
   * name.
   * @return The parsed command-line.
   */
  static TpConfigCmdLine parseLongVersion(const int argc, const char **argv);

}; // struct TpConfigCmdLine

} // namespace tape
