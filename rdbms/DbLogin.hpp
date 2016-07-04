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

namespace cta {
namespace rdbms {

/**
 * A set of database login details.
 */
struct DbLogin {

  /**
   * Enumeration of the supported database types.
   */
  enum DbType {
    DBTYPE_IN_MEMORY,
    DBTYPE_ORACLE,
    DBTYPE_SQLITE,
    DBTYPE_NONE
  };

  /**
   * Constructor.
   *
   * @param dbType The type of the database.
   * @param username The username.
   * @param password The password.
   * @param database The database name.
   */
  DbLogin(
    const DbType dbType,
    const std::string &username,
    const std::string &password,
    const std::string &database);

  /**
   * The type of the database.
   */
  DbType dbType;

  /**
   * The user name.
   */
  std::string username;

  /**
   * The password.
   */
  std::string password;

  /**
   * The database name.
   */
  std::string database;

  /**
   * Reads and parses the database login information from the specified file.
   *
   * The input stream must contain one and only one connection string.
   *
   * The format of the connection string is:
   *
   *   in_memory or oracle:username/password@database
   *
   * The file can contain multiple empty lines.
   *
   * The file can contain multiple comment lines where a comment
   * line starts with optional whitespace and a hash character '#'.
   *
   * @param filename The name of the file containing the database login
   * information.
   * @return The database login information.
   */
  static DbLogin parseFile(const std::string &filename);

  /**
   * Reads and parses the database login information from the specified input
   * stream.
   *
   * The input stream must contain one and only one connection string.
   *
   * The format of the connection string is:
   *
   *   in_memory or oracle:username/password@database
   *
   * The input stream can contain multiple empty lines.
   *
   * The input stream can contain multiple comment lines where a comment
   * line starts with optional whitespace and a hash character '#'.
   *
   * @param inputStream The input stream to be read from.
   * @return The database login information.
   */
  static DbLogin parseStream(std::istream &inputStream);

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
  static std::list<std::string> readNonEmptyLines(std::istream &inputStream);

  /**
   * Parses the specified Oracle database connection details.
   *
   * @param userPassAndDb The Oracle database connection details in the form
   * username/password@database
   */
  static DbLogin parseOracleUserPassAndDb(const std::string &userPassAndDb);

  /**
   * Human readable description of the format of the database
   * login/configuration file.
   */
  static const char *s_fileFormat;

}; // class DbLogin

} // namespace rdbms
} // namespace cta
