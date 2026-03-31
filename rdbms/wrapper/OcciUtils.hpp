/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <exception>

namespace cta::rdbms::wrapper {

/**
 * A convenience class containing some utilities used by the Oracle rdbms wrapper classes.
 */
class OcciUtils {
public:
  /**
   * A method to check if an exception was due to the connection to the DB backend being long
   */
  static bool isLostConnection(const std::exception& se);
};

}  // namespace cta::rdbms::wrapper
