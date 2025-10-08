/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/exception/Exception.hpp"
#include "common/threading/MutexLocker.hpp"
#include "rdbms/wrapper/OcciEnvSingleton.hpp"

namespace cta::rdbms::wrapper {

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

} // namespace cta::rdbms::wrapper
