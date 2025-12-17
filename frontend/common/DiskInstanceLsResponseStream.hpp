/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/DiskInstance.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class DiskInstanceLsResponseStream final : public CtaAdminResponseStream {
public:
  DiskInstanceLsResponseStream(cta::catalogue::Catalogue& catalogue,
                               cta::Scheduler& scheduler,
                               const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::common::dataStructures::DiskInstance> m_diskInstances;  //!< List of disk instances from the catalogue
};

}  // namespace cta::frontend
