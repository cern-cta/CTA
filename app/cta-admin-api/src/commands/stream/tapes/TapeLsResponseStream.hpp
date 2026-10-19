/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/CatalogueItor.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "commands/stream/CtaAdminResponseStream.hpp"

#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

#include "cta_admin.pb.h"

namespace cta::admin_api::commands::stream {

class TapeLsResponseStream final : public CtaAdminResponseStream {
public:
  TapeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                       const std::string& instanceName,
                       const admin::AdminCmd& adminCmd,
                       uint64_t missingFileCopiesMinAgeSecs,
                       cta::admin::HeaderType headerType);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  cta::catalogue::Catalogue& m_catalogue;
  catalogue::TapeItor m_tapesItor;
  cta::catalogue::TapeSearchCriteria m_searchCriteria;
  void validateSearchCriteria(bool hasAllFlag, bool hasAnySearchOption) const;
};

}  // namespace cta::admin_api::commands::stream
