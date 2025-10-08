/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once
#include "common/exception/Exception.hpp"
#include <string>

namespace cta::exception {

/**
 * A generic exception thrown when there is something wrong with the memory
 */
class MemException : public Exception {
public:
  explicit MemException(const std::string& what) : Exception(what) {}
  virtual ~MemException() = default;
};

} // namespace cta::exception
