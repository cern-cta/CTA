/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * Invalid argument exception
 */
struct InvalidArgument : public Exception {
  /**
   * constructor
   */
  explicit InvalidArgument(const std::string& what = "") noexcept : Exception(what) { }
};

} // namespace cta::exception
