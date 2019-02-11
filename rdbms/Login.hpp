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
struct Login {

  /**
   * Enumeration of the supported database types.
   */
  enum DbType {
    DBTYPE_IN_MEMORY,
    DBTYPE_ORACLE,
    DBTYPE_SQLITE,
    DBTYPE_MYSQL,
    DBTYPE_POSTGRESQL,
    DBTYPE_NONE
  };

  /**
   * Returns the string representation of the specified database type.
   *
   * @return The string representation of the specified database type.
   */
  static std::string dbTypeToString(const DbType &dbType) {
    switch(dbType) {
    case DBTYPE_IN_MEMORY: return "DBTYPE_IN_MEMORY";
    case DBTYPE_ORACLE: return "DBTYPE_ORACLE";
    case DBTYPE_SQLITE: return "DBTYPE_SQLITE";
    case DBTYPE_MYSQL: return "DBTYPE_MYSQL";
    case DBTYPE_POSTGRESQL: return "DBTYPE_POSTGRESQL";
    case DBTYPE_NONE: return "DBTYPE_NONE";
    default: return "UNKNOWN";
    }
  }

  /**
   * Constructor.
   */
  Login();

  /**
   * Constructor.
   *
   * @param type The type of the database.
   * @param user The username.
   * @param passwd The password.
   * @param db The database name.
   * @param host The hostname of the database server.
   * @param p The TCP/IP port on which the database server is listening.
   */
  Login(
    const DbType type,
    const std::string &user,
    const std::string &passwd,
    const std::string &db,
    const std::string &host,
    const uint16_t p);

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
   * The hostname of the database server.
   */
  std::string hostname;

  /**
   * The TCP/IP port on which the database server is listening.
   */
  uint16_t port;

  /**
   * Reads and parses the database login information from the specified file.
   *
   * The input stream must contain one and only one connection string.
   *
   * See the value of Login::s_fileFormat within Login.cpp for the format of the
   * connection string.
   *
   * The file can contain multiple comment lines where a comment
   * line starts with optional whitespace and a hash character '#'.
   *
   * @param filename The name of the file containing the database login
   * information.
   * @return The database login information.
   */
  static Login parseFile(const std::string &filename);

  /**
   * Reads and parses the database login information from the specified input
   * stream.
   *
   * The input stream must contain one and only one connection string.
   *
   * See parseString documentation for the format of the connection string.
   *
   * @param inputStream The input stream to be read from.
   * @return The database login information.
   */
  static Login parseStream(std::istream &inputStream);

  /**
   * Reads and parses the database login information from the specified string.
   *
   * The format of the connection string is one of the following:
   *
   *   in_memory
   *   oracle:username/password@database
   *   sqlite:path
   *   mysql://username:password@hostname:port/db_name
   *
   * @param connectionString The connection string.
   * @return The database login information.
   */
  static Login parseString(const std::string &connectionString);

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
   * Structure containing two strings: the database type string and the
   * connection details string for that database type.
   */
  struct DbTypeAndConnectionDetails {
    std::string dbTypeStr;
    std::string connectionDetails;
  };

  /**
   * Parses the specified connection string.
   *
   * @param connectionString The connection string.
   * @return The database type and connection details.
   */
  static DbTypeAndConnectionDetails parseDbTypeAndConnectionDetails(const std::string &connectionString);

  /**
   * Parses the specified database connection details.
   *
   * @param connectionDetails The database connection details.
   */
  static Login parseInMemory(const std::string &connectionDetails);

  /**
   * Parses the specified database connection details.
   *
   * @param connectionDetails The database connection details.
   */
  static Login parseOracle(const std::string &connectionDetails);

  /**
   * Parses the specified connection details.
   *
   * @param connectionDetails The database connection details.
   */
  static Login parseSqlite(const std::string &connectionDetails);

  /**
   * Parses the specified connection details.
   *
   * @param connectionDetails The database connection details.
   */
  static Login parseMySql(const std::string &connectionDetails);

  /**
   * Parses the specified connection details.
   *
   * @param connectionDetails The database connection details.
   */
  static Login parsePostgresql(const std::string &connectionDetails);

  /**
   * Human readable description of the format of the database
   * login/configuration file.
   */
  static const char *s_fileFormat;

}; // class Login

} // namespace rdbms
} // namespace cta
