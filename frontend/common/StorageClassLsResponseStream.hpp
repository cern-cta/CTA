/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "common/dataStructures/StorageClass.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class StorageClassLsResponseStream final : public CtaAdminResponseStream {
public:
  StorageClassLsResponseStream(cta::catalogue::Catalogue& catalogue,
                               cta::Scheduler& scheduler,
                               const std::string& instanceName,
                               const admin::AdminCmd& adminCmd);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::list<cta::common::dataStructures::StorageClass> m_storageClasses;
};

}  // namespace cta::frontend
