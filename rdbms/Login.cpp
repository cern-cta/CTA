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

#include "Login.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

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
  const std::string &db):
  dbType(type),
  username(user),
  password(passwd),
  database(db) {
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

  if(1 != lines.size()) {
    throw exception::Exception("There should only be one and only one line containing a connection string");
  }

  const std::string connectionString = lines.front();

  if(connectionString == "in_memory") {
    return Login(DBTYPE_IN_MEMORY, "", "", "");
  }

  std::vector<std::string> dbTypeAndConnDetails;
  utils::splitString(connectionString, ':', dbTypeAndConnDetails);
  if(2 != dbTypeAndConnDetails.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
  }
  const std::string &dbType = dbTypeAndConnDetails[0];
  const std::string &connDetails = dbTypeAndConnDetails[1];

  if(dbType == "oracle") {
    return parseOracleUserPassAndDb(connDetails);
  }

  if(dbType == "sqlite") {
    return Login(DBTYPE_SQLITE, "", "", connDetails);
  }

  throw exception::Exception(std::string("Invalid connection string: Correct format is ") + s_fileFormat);
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
// parseOracleUserPassAndDb
//------------------------------------------------------------------------------
Login Login::parseOracleUserPassAndDb(const std::string &userPassAndDb) {
  std::vector<std::string> userPassAndDbTokens;
  utils::splitString(userPassAndDb, '@', userPassAndDbTokens);
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

  return Login(DBTYPE_ORACLE, user, pass, db);
}

} // namesapce catalogue
} // namespace cta
