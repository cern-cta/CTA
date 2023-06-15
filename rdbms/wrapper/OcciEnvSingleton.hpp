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
class OcciEnvSingleton : public OcciEnv {
public:
  /**
   * Returns the single instance of this class.
   */
  static OcciEnvSingleton& instance();

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
  OcciEnvSingleton(const OcciEnvSingleton&) = delete;

  /**
   * Prevent assignment.
   */
  void operator=(const OcciEnvSingleton&) = delete;

};  // class OcciEnvSingleton

}  // namespace wrapper
}  // namespace rdbms
}  // namespace cta
