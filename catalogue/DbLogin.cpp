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

#include "catalogue/DbLogin.hpp"
#include "common/exception/Exception.hpp"
#include "common/utils/utils.hpp"

#include <fstream>
#include <list>

namespace cta {
namespace catalogue {

namespace {

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
std::list<std::string> readNonEmptyLines(std::istream &inputStream) {

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

    // If there is a comment, then remove it from the line
    {
      const std::string::size_type startOfComment = line.find("#");
      if(startOfComment != std::string::npos) {
        line = line.substr(0, startOfComment);
      }
    }

    // Left and right trim the line of whitespace
    line = utils::trimString(std::string(line));

    // If the line is not empty
    if(!line.empty()) {
      lines.push_back(line);
    }
  }

  return lines;
}

} // anonymous namespace

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DbLogin::DbLogin(
  const DbType dbType,
  const std::string &username,
  const std::string &password,
  const std::string &database):
  dbType(dbType),
  username(username),
  password(password),
  database(database) {
}

//------------------------------------------------------------------------------
// parseFile
//------------------------------------------------------------------------------
DbLogin DbLogin::parseFile(const std::string &filename) {
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
DbLogin DbLogin::parseStream(std::istream &inputStream) {
  const std::string fileFormat = "either in_memory or oracle:username/password@database";
  const std::list<std::string> lines = readNonEmptyLines(inputStream);

  if(1 != lines.size()) {
    throw exception::Exception("There should only be one and only one line containing a connection string");
  }

  const std::string connectionString = lines.front();

  if(connectionString == "in_memory") {
    const std::string username = "";
    const std::string password = "";
    const std::string database = "";
    return DbLogin(DBTYPE_IN_MEMORY, username, password, database);
  }

  std::vector<std::string> typeAndDetails; // Where details are username, password and database
  utils::splitString(connectionString, ':', typeAndDetails);
  if(2 != typeAndDetails.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + fileFormat);
  }

  if(typeAndDetails[0] != "oracle") {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + fileFormat);
  }

  std::vector<std::string> userPassAndDb;
  utils::splitString(typeAndDetails[1], '@', userPassAndDb);
  if(2 != userPassAndDb.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + fileFormat);
  }

  std::vector<std::string> userAndPass;
  utils::splitString(userPassAndDb[0], '/', userAndPass);
  if(2 != userAndPass.size()) {
    throw exception::Exception(std::string("Invalid connection string: Correct format is ") + fileFormat);
  }

  return DbLogin(DBTYPE_ORACLE, userAndPass[0], userAndPass[1], userPassAndDb[1]);
}

} // namesapce catalogue
} // namespace cta
