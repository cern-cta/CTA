/**
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

#include <exception>
#include <string>

namespace cta {

/**
 * Class representing an exception.
 */
class Exception: public std::exception {
public:

  /**
   * Constructor.
   */
  Exception(const std::string &message);

  /**
   * Destructor.
   */
  ~Exception() throw();

  /**
   * Returns a description of what went wrong.
   */
  const char *what() const throw();

private:

  std::string m_message;

}; // class Exception

} // namespace cta
