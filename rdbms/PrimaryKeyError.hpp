/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/ConstraintError.hpp"

#include <string>

namespace cta::rdbms {

/**
 * A database primary key constraint has been violated
 */
class PrimaryKeyError : public ConstraintError {
  using ConstraintError::ConstraintError;
};

}  // namespace cta::rdbms
