/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::catalogue {

/**
 * Exception representing an unexpected mismatch between file sizes.
 */
class FileSizeMismatch : public exception::Exception {
public:
  /**
   * Constructor
   *
   * @param context optional context string added to the message at initialisation time
   * @param embedBacktrace whether to embed a backtrace of where the exception was thrown in the message
   */
  explicit FileSizeMismatch(const std::string& context = "", const bool embedBacktrace = true)
      : cta::exception::Exception(context, embedBacktrace) {}

  ~FileSizeMismatch() override = default;
};

}  // namespace cta::catalogue
