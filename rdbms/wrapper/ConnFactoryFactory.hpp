/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/wrapper/ConnFactory.hpp"
#include "rdbms/Login.hpp"

#include <memory>

namespace cta::rdbms::wrapper {

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

} // namespace cta::rdbms::wrapper
