/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * No Such Object exception
 */
class NoSuchObject : public cta::exception::Exception {
 public:
  /**
   * default constructor
   */
  explicit NoSuchObject(const std::string& what = "") : cta::exception::Exception(what) {}
};

} // namespace cta::exception
