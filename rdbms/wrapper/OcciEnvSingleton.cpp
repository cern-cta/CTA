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

#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"
#include "rdbms/wrapper/OcciEnvSingleton.hpp"

namespace cta {
namespace rdbms {
namespace wrapper {

//------------------------------------------------------------------------------
// s_mutex
//------------------------------------------------------------------------------
threading::Mutex OcciEnvSingleton::s_mutex;

//------------------------------------------------------------------------------
// s_instance
//------------------------------------------------------------------------------
std::unique_ptr<OcciEnvSingleton> OcciEnvSingleton::s_instance;

//------------------------------------------------------------------------------
// instance
//------------------------------------------------------------------------------
OcciEnvSingleton &OcciEnvSingleton::instance() {
  try {
    threading::MutexLocker locker(s_mutex);

    if(nullptr == s_instance.get()) {
      s_instance.reset(new OcciEnvSingleton());
    }
    return *s_instance;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  } catch(std::exception &se) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + se.what());
  }
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
OcciEnvSingleton::OcciEnvSingleton() {
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta
