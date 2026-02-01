/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MaintdConfig.hpp"
#include "common/log/LogContext.hpp"

#include <vector>

namespace cta::maintd {

class MaintdApp {
public:
  MaintdApp() = default;

  ~MaintdApp() = default;

  void stop();

  int run(const MaintdConfig& config, cta::log::Logger& log);

  bool isLive() const;

  bool isReady() const;

private:
};

}  // namespace cta::maintd
