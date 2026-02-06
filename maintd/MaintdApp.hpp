/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"
#include "common/log/LogContext.hpp"

#include <vector>

namespace cta::maintd {

class MaintdApp final {
public:
  MaintdApp() = default;

  ~MaintdApp() = default;

  void stop();

  int run(const MaintdConfig& config, cta::log::Logger& log);

  bool isLive() const;

  bool isReady() const;

private:
  std::unique_ptr<RoutineRunner> m_routineRunner;
};

}  // namespace cta::maintd
