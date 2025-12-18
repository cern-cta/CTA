/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * Failed to dismount volume
 */
class EncryptionException : public cta::exception::Exception {
public:
  explicit EncryptionException(const std::string& context, const bool embedBacktrace = false)
      : cta::exception::Exception(context, embedBacktrace) {}
};

}  // namespace cta::exception
