/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "TapedConfig.hpp"
#include "common/log/LogContext.hpp"

#include <vector>

namespace cta::taped {

class TapedApp final {
public:
  TapedApp() = default;

  ~TapedApp() = default;

  void stop();

  int run(const TapedConfig& config, cta::log::Logger& log);

  bool isLive() const;

  bool isReady() const;
};

}  // namespace cta::taped
