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

#include <errno.h>
#include <fstream>
#include <iostream>
#include <list>
#include <memory>
#include <stdint.h>
#include <sstream>
#include <stdio.h>
#include <stdexcept>
#include <string.h>

namespace {

using namespace cta;

/**
 * Reads the entire contents of the specified files and returns a list of the
 * non-empty lines.
 *
 * A line is considered not empty if it contains characters that are not white
 * space and are not part of a comment.
 *
 * @return A list of the non-empty lines.
 */
std::list<std::string> readNonEmptyLines(
  const std::string &filename) {

  std::ifstream file(filename);
  if(!file) {
    exception::Exception ex;
    ex.getMessage() << "Failed to open " << filename;
    throw ex;
  }

  std::list<std::string> lines;
  std::string line;

  while(std::getline(file, line)) {
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

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
DbLogin::DbLogin(
  const std::string &username,
  const std::string &password,
  const std::string &database):
  username(username),
  password(password),
  database(database) {
}

//------------------------------------------------------------------------------
// readFromFile
//------------------------------------------------------------------------------
DbLogin DbLogin::readFromFile(const std::string &filename) {
  const std::string fileFormat = "username/password@database";
  const std::list<std::string> lines = readNonEmptyLines(filename);

  if(1 != lines.size()) {
    std::ostringstream msg;
    msg << filename << " should contain one and only one connection string";
    throw std::runtime_error(msg.str());
  }

  const std::string connectionString = lines.front();

  std::vector<std::string> userPassAndDb;
  utils::splitString(connectionString, '@', userPassAndDb);
  if(2 != userPassAndDb.size()) {
    throw std::runtime_error(std::string("Invalid connection string"
      ": Correct format is ") + fileFormat);
  }

  std::vector<std::string> userAndPass;
  utils::splitString(userPassAndDb[0], '/', userAndPass);
  if(2 != userAndPass.size()) {
    throw std::runtime_error(std::string("Invalid connection string"
      ": Correct format is ") + fileFormat);
  }

  return DbLogin(userAndPass[0], userAndPass[1], userPassAndDb[1]);
}

} // namesapce catalogue
} // namespace cta
