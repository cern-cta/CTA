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

#include "common/threading/Mutex.hpp"
#include "rdbms/wrapper/OcciEnv.hpp"

#include <memory>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A singleton version of OcciEnv.
 */
class OcciEnvSingleton: public OcciEnv {
public:

  /**
   * Returns the single instance of this class.
   */
  static OcciEnvSingleton &instance();

private:

  /**
   * Mutex used to implement a critical region around the implementation of the
   * instance() method.
   */
  static threading::Mutex s_mutex;

  /**
   * The single instance of this class.
   */
  static std::unique_ptr<OcciEnvSingleton> s_instance;

  /**
   * Private constructor because this class is a singleton.
   */
  OcciEnvSingleton();

  /**
   * Prevent copying.
   */
  OcciEnvSingleton(const OcciEnvSingleton &) = delete;

  /**
   * Prevent assignment.
   */
  void operator=(const OcciEnvSingleton &) = delete;

}; // class OcciEnvSingleton

} // namespace wrapper
} // namespace rdbms
} // namespace cta
