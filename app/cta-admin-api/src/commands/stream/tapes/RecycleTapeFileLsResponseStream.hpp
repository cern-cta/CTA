/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/CatalogueItor.hpp"
#include "commands/stream/CtaAdminResponseStream.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"

#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_admin.pb.h"

namespace cta::admin_api::commands::stream {

class RecycleTapeFileLsResponseStream final : public CtaAdminResponseStream {
public:
  RecycleTapeFileLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                  const std::string& instanceName,
                                  const admin::AdminCmd& adminCmd,
                                  cta::admin::HeaderType headerType);
  bool isDone() override;
  cta::xrd::Data next() override;

private:
  cta::catalogue::Catalogue& m_catalogue;
  catalogue::FileRecycleLogItor m_fileRecycleLogItor;
};

}  // namespace cta::admin_api::commands::stream
