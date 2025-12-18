/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/Tape.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class TapeLsResponseStream final : public CtaAdminResponseStream {
public:
  TapeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                       cta::Scheduler& scheduler,
                       const std::string& instanceName,
                       const admin::AdminCmd& adminCmd,
                       uint64_t missingFileCopiesMinAgeSecs);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<common::dataStructures::Tape> m_tapes;
  uint64_t m_missingFileCopiesMinAgeSecs;
  cta::catalogue::TapeSearchCriteria m_searchCriteria;
  void validateSearchCriteria(bool hasAllFlag, bool hasAnySearchOption) const;
};

}  // namespace cta::frontend
