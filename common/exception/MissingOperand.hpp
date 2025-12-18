/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * Missing operand.
 */
class MissingOperand : public cta::exception::Exception {
public:
  /**
   * Constructor
   */
  MissingOperand() : cta::exception::Exception() {}

};  // class MissingOperand

}  // namespace cta::exception
