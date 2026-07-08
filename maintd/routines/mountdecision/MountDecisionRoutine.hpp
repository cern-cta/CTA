/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "maintd/MaintdConfig.hpp"
#include "mountdecision/MountDecisionDB.hpp"

#include <memory>
#include <string>

namespace cta::maintd {

class MountDecisionRoutine final : public IRoutine {
public:
  MountDecisionRoutine(cta::log::LogContext& lc, const cta::runtime::MountDecisionConfig& mountDecisionConfig);

  void execute() final;
  std::string getName() const final;

private:
  std::unique_ptr<cta::mountdecision::MountDecisionDB>
  createMountDecisionDb(const cta::runtime::MountDecisionConfig& mountDecisionConfig) const;

  cta::log::LogContext& m_lc;
  std::unique_ptr<cta::mountdecision::MountDecisionDB> m_mountDecisionDb;
  std::string m_counterKey;
};

}  // namespace cta::maintd
