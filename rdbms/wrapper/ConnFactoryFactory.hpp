/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "rdbms/wrapper/ConnFactory.hpp"
#include "rdbms/Login.hpp"

#include <memory>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * Abstract class that specifies the interface to a factory of ConnFactory objects.
 */
class ConnFactoryFactory {
public:

  /**
   * Returns a newly created ConnFactory object.
   *
   * @param login The database login details to be used to create new
   * connections.
   * @return A newly created ConnFactory object.
   */
  static std::unique_ptr<ConnFactory> create(const Login &login);

}; // class ConnFactoryFactory

} // namespace wrapper
} // namespace rdbms
} // namespace cta
