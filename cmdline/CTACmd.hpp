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

#include <string>

/**
 * Class implementing the business logic of the archive command-line tool.
 */
class CTACopyCmd {
public:
  /**
   * Constructor.
   */
  CTACopyCmd() throw();

  /**
   * The entry function of the command.
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments.
   */
  int main(const int argc, const char **argv) const throw();

protected:
  
  /**
   * Sends the command and waits for the reply
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments. 
   * @return the return code
   */
  int sendCommand(const int argc, const char **argv) const;
  
  /**
   * Formats the command path string
   *
   * @param argc The number of command-line arguments.
   * @param argv The command-line arguments. 
   * @return the command string
   */
  std::string formatCommandPath(const int argc, const char **argv) const;
  
  /**
   * Replaces all occurrences in a string "str" of a substring "from" with the string "to"
   * 
   * @param str  The original string
   * @param from The substring to replace
   * @param to   The replacement string
   */
  void replaceAll(std::string& str, const std::string& from, const std::string& to) const;
  
  /**
   * Encodes a string in base 64
   * 
   * @param msg string to encode
   * @return encoded string
   */
  std::string encode(const std::string msg) const;

}; // class CTACopyCmd
