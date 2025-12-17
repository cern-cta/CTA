/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/wrapper/OcciEnvSingleton.hpp"

#include "common/exception/Exception.hpp"
#include "common/process/threading/MutexLocker.hpp"

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
OcciEnvSingleton& OcciEnvSingleton::instance() {
  threading::MutexLocker locker(s_mutex);

  if (nullptr == s_instance.get()) {
    s_instance.reset(new OcciEnvSingleton());
  }
  return *s_instance;
}

}  // namespace cta::rdbms::wrapper
