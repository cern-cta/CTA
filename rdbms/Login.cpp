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

#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"
#include "Login.hpp"

#include <fstream>

namespace cta {
namespace rdbms {

//------------------------------------------------------------------------------
// s_fileFormat
//------------------------------------------------------------------------------
const char *Login::s_fileFormat = "either in_memory or oracle:username/password@database or sqlite:filename";

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

  if(typeAndDetails.dbTypeStr == "in_memory") {
    return Login(DBTYPE_IN_MEMORY, "", "", "", "", 0);
  } else if(typeAndDetails.dbTypeStr == "oracle") {
    return parseOracle(typeAndDetails.connectionDetails);
  } else if(typeAndDetails.dbTypeStr == "sqlite") {
    return parseSqlite(typeAndDetails.connectionDetails);
  } else if(typeAndDetails.dbTypeStr == "mysql") {
    return parseMySql(typeAndDetails.connectionDetails);
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

  while(std::getline(inputStream, line)) {
    // Remove the newline character if there is one
    {
      const std::string::size_type newlinePos = line.find("\n");
      if(newlinePos != std::string::npos) {
        line = line.substr(0, newlinePos);
      }
    }

    // Left and right trim the line of whitespace
    line = utils::trimString(std::string(line));

    // Please note:
    //
    // Comments at the end of lines are not supported.
    // Ignoring whitespace, lines starting with '#' are comments.

    // If the line is not empty and is not a comment
    if(!line.empty() && line.at(0) != '#') {
      lines.push_back(line);
    }
  }

  return lines;
}

//------------------------------------------------------------------------------
// parseOracle
//------------------------------------------------------------------------------
Login Login::parseOracle(const std::string &connectionDetails) {
  std::vector<std::string> userPassAndDbTokens;
  utils::splitString(connectionDetails, '@', userPassAndDbTokens);
  if(2 != userPassAndDbTokens.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  const std::string &userAndPass = userPassAndDbTokens[0];
  const std::string &db = userPassAndDbTokens[1];

  std::vector<std::string> userAndPassTokens;
  utils::splitString(userAndPass, '/', userAndPassTokens);
  if(2 != userAndPassTokens.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  const std::string &user = userAndPassTokens[0];
  const std::string &pass = userAndPassTokens[1];

  return Login(DBTYPE_ORACLE, user, pass, db, "", 0);
}

//------------------------------------------------------------------------------
// parseSqlite
//------------------------------------------------------------------------------
Login Login::parseSqlite(const std::string &connectionDetails) {
  const std::string &filename = connectionDetails;
  return Login(DBTYPE_SQLITE, "", "", filename, "", 0);
}

//------------------------------------------------------------------------------
// parseMySql
//------------------------------------------------------------------------------
Login Login::parseMySql(const std::string &connectionDetails) {
  //return Login(DBTYPE_MYSQL, "", "", connectionDetails, "", 0);
  throw exception::Exception(std::string(__FUNCTION__) + " not implemented");
}

} // namespace catalogue
} // namespace cta
