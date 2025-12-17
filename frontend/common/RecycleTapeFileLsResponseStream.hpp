/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"

#include "cta_admin.pb.h"

namespace cta::frontend {

class RecycleTapeFileLsResponseStream final : public CtaAdminResponseStream {
public:
  RecycleTapeFileLsResponseStream(cta::catalogue::Catalogue& catalogue,
                                  cta::Scheduler& scheduler,
                                  const std::string& instanceName,
                                  const admin::AdminCmd& adminCmd);
  bool isDone() override;
  cta::xrd::Data next() override;

private:
  catalogue::FileRecycleLogItor m_fileRecycleLogItor;
};

}  // namespace cta::frontend
