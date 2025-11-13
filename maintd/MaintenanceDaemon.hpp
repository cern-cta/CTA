/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <atomic>
#include <memory>
#include "RoutineRunner.hpp"
#include "common/config/Config.hpp"

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

private:
  cta::common::Config& m_config;
  cta::log::LogContext& m_lc;
  std::unique_ptr<RoutineRunner> m_routineRunner;

  std::atomic<bool> m_stopRequested;
};

}  // namespace cta::maintd
