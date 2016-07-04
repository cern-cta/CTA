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

#include "DbLogin.hpp"

namespace cta {
namespace rdbms {

/**
 * Abstract class specifying the interface to a factory of DbLogin objects.
 */
class DbLoginFactory {
public:

  /**
   * Destructor.
   */
  virtual ~DbLoginFactory() = 0;

  /**
   * Returns a newly created DbLogin object.
   *
   * @return A newly created DbLogin object.
   */
  virtual DbLogin create() = 0;

}; // class DbLogin

} // namespace rdbms
} // namespace cta
