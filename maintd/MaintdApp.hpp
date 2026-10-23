/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "MaintdConfig.hpp"
#include "RoutineRunner.hpp"
#include "common/log/LogContext.hpp"
#include "service-resources/ServiceResources.hpp"

#include <memory>

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
  // Declared before m_routineRunner: the routines hold references to the resources
  std::unique_ptr<service_resources::ServiceResources> m_resources;
  std::unique_ptr<RoutineRunner> m_routineRunner;
};

}  // namespace cta::maintd
