/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "maintd/MaintdConfig.hpp"
#include "mountdecision/MountDecision.hpp"

#include <string>

namespace cta::maintd {

class MountDecisionRoutine final : public IRoutine {
public:
  MountDecisionRoutine(cta::log::LogContext& lc, const cta::runtime::MountDecisionConfig& mountDecisionConfig);

  void execute() final;
  std::string getName() const final;

private:
  cta::log::LogContext& m_lc;
  std::string m_counterKey;
  cta::mountdecision::MountDecision m_mountDecision;
};

}  // namespace cta::maintd
