/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/threading/Mutex.hpp"
#include "rdbms/wrapper/OcciEnv.hpp"

#include <memory>

namespace cta::rdbms::wrapper {

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
   * Private constructor because this class is a singleton
   */
  OcciEnvSingleton() = default;

  /**
   * Prevent copying.
   */
  OcciEnvSingleton(const OcciEnvSingleton &) = delete;

  /**
   * Prevent assignment
   */
  OcciEnvSingleton& operator=(const OcciEnvSingleton&) = delete;

}; // class OcciEnvSingleton

} // namespace cta::rdbms::wrapper
