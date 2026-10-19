/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "commands/stream/CtaAdminResponseStream.hpp"

#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_admin.pb.h"

namespace cta::admin_api::commands::stream {

class RepackLsResponseStream final : public CtaAdminResponseStream {
public:
  RepackLsResponseStream(cta::catalogue::Catalogue& catalogue,
                         cta::Scheduler& scheduler,
                         const std::string& instanceName,
                         const admin::AdminCmd& adminCmd,
                         cta::admin::HeaderType headerType);

  bool isDone() override;

  cta::xrd::Data next() override;

private:
  cta::catalogue::Catalogue& m_catalogue;
  cta::Scheduler& m_scheduler;
  std::vector<cta::xrd::Data> m_repackItems;
  std::size_t m_repackItemsIdx = 0;
  std::string m_schedulerBackendName;

  void collectRepacks(const std::optional<std::string>& vid);
};

}  // namespace cta::admin_api::commands::stream
