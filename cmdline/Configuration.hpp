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

#include <istream>
#include <list>
#include <string>
#include <vector>

namespace cta {
namespace cmdline {

/**
 * A set of configuration details.
 */
class Configuration {
public:
  
  /**
   * Constructor.
   *
   * @param filename The configuration file name.
   */
  Configuration(const std::string &filename);
  
private:
  
  /**
   * The frontend hostname and port to connect.
   */
  std::string frontendHostAndPort;
  
  /**
   * The configuration file name.
   */
  const std::string filename;

  
  /**
   * Reads and parses the configuration information from the specified file.
   *
   * The input stream must contain one and only one configuration string.
   *
   * The format of the configuration string is:
   *
   *   <host>.cern.ch:<port>
   *
   * The file can contain multiple empty lines.
   *
   * The file can contain multiple comment lines where a comment
   * line starts with optional whitespace and a hash character '#'.
   *
   * @param filename The name of the file containing the configuration
   * information.
   * @return The configuration information.
   */
  std::string parseFile(const std::string &filename);

  /**
   * Reads and parses the configuration information from the specified file.
   *
   * The input stream must contain one and only one configuration string.
   *
   * The format of the configuration string is:
   *
   *   <host>.cern.ch:<port>
   *
   * The file can contain multiple empty lines.
   *
   * The file can contain multiple comment lines where a comment
   * line starts with optional whitespace and a hash character '#'.
   *
   * @param filename The name of the file containing the configuration
   * information.
   * @return The configuration information.
   */
  std::string parseStream(std::istream &inputStream);

  /**
   * Reads the entire contents of the specified stream and returns a list of the
   * non-empty lines.
   *
   * A line is considered not empty if it contains characters that are not white
   * space and are not part of a comment.
   *
   * @param is The input stream.
   * @return A list of the non-empty lines.
   */
   std::list<std::string> readNonEmptyLines(std::istream &inputStream);
   
  /**
   * Returns the result of trimming both left and right white-space from the
   * specified string.
   *
   * @param s The string to be trimmed.
   * @return The result of trimming the string.
   */
  std::string trimString(const std::string &s) throw();

  /**
   * Splits the specified string into a vector of strings using the specified
   * separator.
   *
   * Please note that the string to be split is NOT modified.
   *
   * @param str The string to be split.
   * @param separator The separator to be used to split the specified string.
   * @param result The vector when the result of spliting the string will be
   * stored.
   */
  void splitString(const std::string &str, const char separator, std::vector<std::string> &result);
  
  /**
   * Returns true if the specified string only contains numerals else false.
   *
   * @return True if the specified string only contains numerals else false.
   */
  static bool onlyContainsNumerals(const std::string &str) throw();
  
  /**
   * Human readable description of the format of the database
   * login/configuration file.
   */
  static const char *s_fileFormat;
  
public:
  /**
   * Returns the value of the configuration parameters.
   *
   * @return The frontend host and port from the configuration.
   */
  std::string getFrontendHostAndPort() throw();

}; // class Configuration

} // namespace cmdline
} // namespace cta
