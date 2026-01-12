/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/TapeDrive.hpp"

#include <unordered_map>

#include "cta_admin.pb.h"

namespace cta::frontend {

class DriveLsResponseStream final : public CtaAdminResponseStream {
public:
  DriveLsResponseStream(cta::catalogue::Catalogue& catalogue,
                        cta::Scheduler& scheduler,
                        const std::string& instanceName,
                        const admin::AdminCmd& adminCmd,
                        cta::log::LogContext& lc);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  cta::log::LogContext& m_lc;

  std::vector<common::dataStructures::TapeDrive> m_tapeDrives;
  std::size_t m_tapeDrivesIdx = 0;
  std::unordered_map<std::string, std::list<cta::catalogue::DriveConfigCatalogue::DriveConfig>>
    m_tapeDriveNameConfigMap;
  bool m_listAllDrives = false;
  std::optional<std::string> m_schedulerBackendName;
};

}  // namespace cta::frontend
