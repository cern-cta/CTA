/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>

#include "JsonStatisticsService.hpp"

namespace cta::statistics {

class JsonStatisticsServiceFactory {
 public:
  static std::unique_ptr<JsonStatisticsService> create(JsonStatisticsService::OutputStream *output,
    JsonStatisticsService::InputStream *input = nullptr) {
    return std::make_unique<JsonStatisticsService>(output, input);
  }
};

} // namespace cta::statistics
