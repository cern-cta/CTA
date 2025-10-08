/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "common/exception/Exception.hpp"
#include <string>

namespace cta::exception {

class EndOfFile: public cta::exception::Exception {
public:
  explicit EndOfFile(const std::string& w) : cta::exception::Exception(w) {}
  virtual ~EndOfFile() = default;
};

} // namespace cta::exception
