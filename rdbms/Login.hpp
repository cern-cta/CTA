/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
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
    DBTYPE_POSTGRESQL,
    DBTYPE_NONE
  };

  /**
   * Returns the string representation of the specified database type.
   *
   * @return The string representation of the specified database type.
   */
  static std::string dbTypeToString(const DbType &dbType) {
    switch (dbType) {
      case DBTYPE_IN_MEMORY: return "DBTYPE_IN_MEMORY";
      case DBTYPE_ORACLE: return "DBTYPE_ORACLE";
      case DBTYPE_SQLITE: return "DBTYPE_SQLITE";
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
   * The connection string of the database (with hidden password)
   */
  std::string connectionString;

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

    static const std::string in_memory;
    static const std::string oracle;
    static const std::string sqlite;
    static const std::string postgresql;
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
   * Parses the specified connection details for the Postgres database.
   * The postgres selection type in the config file is "postgres".
   *
   * Possible database login configuration lines for postgres:
   *
   *     postgresql:[connectinfo]
   * or
   *     postgresql:[URI]
   *
   * connectinfo or URI are the connectionDetails and may be empty.
   * Otherwise connectinfo is a series of 1 or more keyword=value
   * separated by whitespace. See doucmentaion, e.g.
   * https://www.postgresql.org/docs/11/libpq-connect.html#LIBPQ-CONNSTRING
   *
   * an example configuraiton using the URI is
   * postgresql:postgresql://user:secret@localhost/mydb
   *
   * an example configuraiton using connect info
   * postgresql:user=user password=secret host=localhost dbname=mydb
   *
   * @param connectionDetails The database connection details.
   */
  static Login parsePostgresql(const std::string &connectionDetails);

  static const std::list<std::string> dbTypeStr;

  /**
   * Human readable description of the format of the database
   * login/configuration file.
   */
  static const char *s_fileFormat;

  /**
   * String displayed instead of the actual password.
   */
  static const std::string s_hiddenPassword;

 private:
  /**
   * Sets the connection string of an in_memory database
   */
  void setInMemoryConnectionString();

  /**
   * Sets the Oracle connection string of an oracle database
   * @param user
   * @param db
   */
  void setOracleConnectionString(const std::string &user, const std::string &db);

  /**
   * Sets the connection string of a Postgresql database
   * @param connectionDetails
   */
  void setPostgresqlConnectionString(const std::string &connectionDetails);

  /**
   * Sets the connection string of a sqlite database
   * @param filename
   */
  void setSqliteConnectionString(const std::string & filename);

  /**
   * Returns true if the Postgresql connectionDetails contains a password, false otherwise
   * @param connectionDetails the connectionDetails retrieved from the configuration file
   * @return  true if the Postgresql connectionDetails contains a password, false otherwise
   */
  bool postgresqlHasPassword(const std::string & connectionDetails);
};  // class Login

}  // namespace rdbms
}  // namespace cta
