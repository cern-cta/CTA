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

namespace cta {
namespace catalogue {

/**
 * A set of database login details.
 */
struct DbLogin {

  /**
   * Constructor.
   *
   * @param username The username.
   * @param password The password.
   * @param database The database name.
   */
  DbLogin(
    const std::string &username, 
    const std::string &password,
    const std::string &database);

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
   * Reads the database login information from the specified file.  The format
   * of the file is:
   *
   * username/password@database
   *
   * @param The name of the file containing the database login information.
   * @return The database login information.
   */
  static DbLogin readFromFile(const std::string &filename);

}; // class DbLogin

} // namespace catalogue
} // namespace cta
