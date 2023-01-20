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

#include <algorithm>
#include <fstream>
#include <iostream>
#include <regex>
#include <sstream>
#include <stdexcept>
#include <vector>

#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/Login.hpp"

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// s_fileFormat
//------------------------------------------------------------------------------
const char *Login::s_fileFormat = "one of "
  "in_memory, "
  "oracle:username/password@database, "
  "sqlite:filename, "
  "postgresql:[connectinfo | URI]";

const std::string Login::s_hiddenPassword = "******";

const std::string Login::DbTypeAndConnectionDetails::in_memory = "in_memory";
const std::string Login::DbTypeAndConnectionDetails::oracle = "oracle";
const std::string Login::DbTypeAndConnectionDetails::sqlite = "sqlite";
const std::string Login::DbTypeAndConnectionDetails::postgresql = "postgresql";

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Login::Login():
  dbType(DBTYPE_NONE),
  port(0) {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
Login::Login(
  const DbType type,
  const std::string &user,
  const std::string &passwd,
  const std::string &db,
  const std::string &host,
  const uint16_t p):
  dbType(type),
  username(user),
  password(passwd),
  database(db),
  hostname(host),
  port(p) {
}

//------------------------------------------------------------------------------
// parseFile
//------------------------------------------------------------------------------
Login Login::parseFile(const std::string &filename) {
  try {
    std::ifstream file(filename);
    if (!file) {
      throw exception::Exception("Failed to open file");
    }

    return parseStream(file);
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string("Failed to parse database login file " + filename + ": " +
      ex.getMessage().str()));
  }
}

//------------------------------------------------------------------------------
// parseStream
//------------------------------------------------------------------------------
Login Login::parseStream(std::istream &inputStream) {
  const std::list<std::string> lines = readNonEmptyLines(inputStream);

  if (1 != lines.size()) {
    throw exception::Exception("There should only be one and only one line containing a connection string");
  }

  const std::string connectionString = lines.front();

  return parseString(connectionString);
}


//------------------------------------------------------------------------------
// parseStream
//------------------------------------------------------------------------------
Login Login::parseString(const std::string &connectionString) {
  if (connectionString.empty()) {
    throw exception::Exception("Invalid connection string: Empty string");
  }

  const auto typeAndDetails = parseDbTypeAndConnectionDetails(connectionString);

  if (typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::in_memory) {
    return parseInMemory(typeAndDetails.connectionDetails);
  } else if (typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::oracle) {
    return parseOracle(typeAndDetails.connectionDetails);
  } else if (typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::sqlite) {
    return parseSqlite(typeAndDetails.connectionDetails);
  } else if (typeAndDetails.dbTypeStr == DbTypeAndConnectionDetails::postgresql) {
    return parsePostgresql(typeAndDetails.connectionDetails);
  }

  throw exception::Exception(std::string("Invalid connection string: Unknown database type ") +
    typeAndDetails.dbTypeStr);
}

//------------------------------------------------------------------------------
// parseDbTypeAndConnectionDetails
//------------------------------------------------------------------------------
Login::DbTypeAndConnectionDetails Login::parseDbTypeAndConnectionDetails(const std::string &connectionString) {
  DbTypeAndConnectionDetails dbTypeAndConnectionDetails;

  // Parsing "databaseType:connectionDetails"
  std::string::size_type firstColonPos = connectionString.find(':');
  dbTypeAndConnectionDetails.dbTypeStr = connectionString.substr(0, firstColonPos);
  if (std::string::npos != firstColonPos && (connectionString.length() - 1) > firstColonPos) {
    dbTypeAndConnectionDetails.connectionDetails = connectionString.substr(firstColonPos + 1);
  }

  return dbTypeAndConnectionDetails;
}

//------------------------------------------------------------------------------
// readNonEmptyLines
//------------------------------------------------------------------------------
std::list<std::string> Login::readNonEmptyLines(std::istream &inputStream) {

  std::list<std::string> lines;
  std::string line;

  while (std::getline(inputStream, line)) {
    // Remove the newline character if there is one
    {
      const std::string::size_type newlinePos = line.find("\n");
      if (newlinePos != std::string::npos) {
        line.resize(newlinePos);
      }
    }

    // Left and right trim the line of whitespace
    line = utils::trimString(std::string(line));

    // Please note:
    //
    // Comments at the end of lines are not supported.
    // Ignoring whitespace, lines starting with '#' are comments.

    // If the line is not empty and is not a comment
    if (!line.empty() && line.at(0) != '#') {
      lines.push_back(line);
    }
  }

  return lines;
}

//------------------------------------------------------------------------------
// parseInMemory
//------------------------------------------------------------------------------
Login Login::parseInMemory(const std::string &connectionDetails) {
  if (!connectionDetails.empty()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  Login login(DBTYPE_IN_MEMORY, "", "", "", "", 0);
  login.setInMemoryConnectionString();
  return login;
}

void Login::setInMemoryConnectionString() {
  connectionString = Login::DbTypeAndConnectionDetails::in_memory;
}

//------------------------------------------------------------------------------
// parseOracle
//------------------------------------------------------------------------------
Login Login::parseOracle(const std::string &connectionDetails) {
  std::vector<std::string> userPassAndDbTokens;
  utils::splitString(connectionDetails, '@', userPassAndDbTokens);
  if (2 != userPassAndDbTokens.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  const std::string &userAndPass = userPassAndDbTokens[0];
  const std::string &db = userPassAndDbTokens[1];

  std::vector<std::string> userAndPassTokens;
  utils::splitString(userAndPass, '/', userAndPassTokens);
  if (2 != userAndPassTokens.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  const std::string &user = userAndPassTokens[0];
  const std::string &pass = userAndPassTokens[1];

  Login login(DBTYPE_ORACLE, user, pass, db, "", 0);
  login.setOracleConnectionString(user, db);
  return login;
}

void Login::setOracleConnectionString(const std::string & user, const std::string & db) {
  connectionString = Login::DbTypeAndConnectionDetails::oracle+":"+user+"/"+s_hiddenPassword+"@"+db;
}

//------------------------------------------------------------------------------
// parseSqlite
//------------------------------------------------------------------------------
Login Login::parseSqlite(const std::string &connectionDetails) {
  const std::string &filename = connectionDetails;

  if (filename.empty()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }

  Login login(DBTYPE_SQLITE, "", "", filename, "", 0);
  login.setSqliteConnectionString(filename);
  return login;
}

void Login::setSqliteConnectionString(const std::string & filename) {
  connectionString = Login::DbTypeAndConnectionDetails::sqlite+":"+filename;
}

//------------------------------------------------------------------------------
// parsePostgresql
//------------------------------------------------------------------------------
Login Login::parsePostgresql(const std::string &connectionDetails) {
  Login login(DBTYPE_POSTGRESQL, "", "", connectionDetails, "", 0);
  login.setPostgresqlConnectionString(connectionDetails);
  return login;
}

void Login::setPostgresqlConnectionString(const std::string& connectionDetails) {
  connectionString = Login::DbTypeAndConnectionDetails::postgresql+":";
  if (!postgresqlHasPassword(connectionDetails)) {
    // No password displayed so no need to hide it
    connectionString += connectionDetails;
  } else {
    cta::utils::Regex regex2("(postgresql://.*:)(.*)(@.*)");
    std::vector<std::string> result2 = regex2.exec(connectionDetails);
    connectionString += result2[1] + s_hiddenPassword + result2[3];
  }
}

bool Login::postgresqlHasPassword(const std::string& connectionDetails) {
  // https://www.postgresql.org/docs/10/libpq-connect.html
  if (connectionDetails.find("@") == std::string::npos) {
    return false;
  }
  cta::utils::Regex regex("postgresql://(.*)@");
  std::vector<std::string> result = regex.exec(connectionDetails);
  std::string usernamePassword = result[1];
  if (usernamePassword.find(":") == std::string::npos) {
    // No password provided, no need to hide it
    return false;
  }
  return true;
}

}  // namespace rdbms
}  // namespace cta
