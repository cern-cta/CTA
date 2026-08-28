/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"
#include "common/log/LogContext.hpp"

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
  // TODO: Synchronize runner publication: run() assigns this pointer while health and signal threads may read it.
  std::unique_ptr<RoutineRunner> m_routineRunner;
};

}  // namespace cta::maintd
