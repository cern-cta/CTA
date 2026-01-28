/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"

#include <atomic>
#include <memory>

namespace cta::maintd {

/**
 * This class is essentially a wrapper around a RoutineRunner.
 * It allows for reloading of the config.
 */
class MaintenanceDaemon {
public:
  MaintenanceDaemon(const MaintdConfig& config, cta::log::LogContext& lc);

  ~MaintenanceDaemon() = default;

  void run();

  void stop();

  bool isLive();

  bool isReady();

private:
  const MaintdConfig& m_config;
  cta::log::LogContext& m_lc;
  std::unique_ptr<RoutineRunner> m_routineRunner;

  std::atomic<bool> m_stopRequested;
};

}  // namespace cta::maintd
