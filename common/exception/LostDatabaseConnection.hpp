/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

#include <string>

namespace cta::exception {

/**
 * The database connection has been lost.
 */
class LostDatabaseConnection : public Exception {
  using Exception::Exception;
};

}  // namespace cta::exception
