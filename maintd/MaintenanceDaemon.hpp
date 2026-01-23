/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RoutineRunner.hpp"
#include "common/config/Config.hpp"

#include <atomic>
#include <memory>

namespace cta::maintd {

/**
 * This class is essentially a wrapper around a RoutineRunner.
 * It allows for reloading of the config.
 */
class MaintenanceDaemon {
public:
  MaintenanceDaemon(cta::common::Config& config, cta::log::LogContext& lc);

  ~MaintenanceDaemon() = default;

  void run();

  void stop();

  void reload();

  bool isLive();

  bool isReady();

private:
  cta::common::Config& m_config;
  cta::log::LogContext& m_lc;
  std::unique_ptr<RoutineRunner> m_routineRunner;

  std::atomic<bool> m_stopRequested;
};

}  // namespace cta::maintd
