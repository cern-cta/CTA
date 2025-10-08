/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>
#include <string>

#include "Backend.hpp"
#include "common/log/Logger.hpp"

namespace cta::objectstore {

class BackendFactory {
 public:
  static std::unique_ptr<Backend> createBackend(const std::string & URL, log::Logger & logger);
};
} // namespace cta::objectstore
