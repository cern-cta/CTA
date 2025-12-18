/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/Login.hpp"

namespace cta::rdbms {

/**
 * Abstract class specifying the interface to a factory of Login objects.
 */
class LoginFactory {
public:
  /**
   * Destructor.
   */
  virtual ~LoginFactory() = 0;

  /**
   * Returns a newly created Login object.
   *
   * @return A newly created Login object.
   */
  virtual Login create() = 0;

};  // class Login

}  // namespace cta::rdbms
