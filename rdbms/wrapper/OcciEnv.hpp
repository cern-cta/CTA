/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/wrapper/ConnWrapper.hpp"
#include "rdbms/Login.hpp"

#include <memory>
#include <occi.h>

namespace cta::rdbms::wrapper {

/**
 * A convenience wrapper around an OCCI environment.
 */
class OcciEnv {
public:

  /**
   * Constructor.
   *
   * Creates an OCCI environment with a THREADED_MUTEXED mode.
   */
  OcciEnv();

  /**
   * Destructor.
   *
   * Terminates the underlying OCCI environment.
   */
  ~OcciEnv();

  /**
   * Creates an OCCI connection.
   *
   * This method will throw an exception if either the username, password ori
   * database parameters are nullptr pointers.
   *
   * @param login The database login information.
   * @return The newly created OCCI connection.
   */
  std::unique_ptr<ConnWrapper> createConn(const rdbms::Login& login);

private:

  /**
   * The OCCI environment.
   */
  oracle::occi::Environment *m_env;

}; // class OcciEnv

} // namespace cta::rdbms::wrapper
