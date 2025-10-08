/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * Exception representing an unexpected mismatch between checksum types.
 */
class ChecksumTypeMismatch: public exception::Exception {
public:
  /**
   * Constructor.
   *
   * @param context optional context string added to the message
   * at initialisation time.
   * @param embedBacktrace whether to embed a backtrace of where the
   * exception was throw in the message
   */
  ChecksumTypeMismatch(const std::string &context = "", const bool embedBacktrace = true) :
    Exception(context, embedBacktrace) {}

  /**
   * Destructor.
   */
  ~ChecksumTypeMismatch() override = default;
};

} // namespace cta::exception
