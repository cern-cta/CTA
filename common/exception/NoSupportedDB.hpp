/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "common/exception/Exception.hpp"

namespace cta::exception {

/**
 * Failed to dismount volume.
 */
class NoSupportedDB : public cta::exception::Exception {
public:
  /**
   * Constructor
   */
  explicit NoSupportedDB(const std::string& what = "");
};  // class NoSupportedDB

} // namespace cta::exception

