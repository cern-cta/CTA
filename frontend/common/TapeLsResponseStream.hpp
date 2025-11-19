/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2025 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <list>
#include "cta_admin.pb.h"

#include "CtaAdminResponseStream.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/Tape.hpp"

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