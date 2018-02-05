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
