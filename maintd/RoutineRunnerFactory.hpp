/*
 * SPDX-FileCopyrightText: 2025 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "RoutineRunner.hpp"
#include "common/log/LogContext.hpp"
#include "service-resources/ServiceResources.hpp"

namespace cta::maintd {

/**
 * Responsible for create a RoutineRunner with a specific set of registered routines based on the provided config.
 */
class RoutineRunnerFactory final {
public:
  RoutineRunnerFactory(const MaintdConfig& config,
                       service_resources::ServiceResources& resources,
                       cta::log::LogContext& lc);
  std::unique_ptr<RoutineRunner> create();

private:
  const MaintdConfig& m_config;
  cta::log::LogContext& m_lc;

  service_resources::ServiceResources& m_resources;
};

}  // namespace cta::maintd
