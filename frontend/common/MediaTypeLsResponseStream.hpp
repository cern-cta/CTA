/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CtaAdminResponseStream.hpp"
#include "catalogue/MediaTypeWithLogs.hpp"

#include <list>

#include "cta_admin.pb.h"

namespace cta::frontend {

class MediaTypeLsResponseStream final : public CtaAdminResponseStream {
public:
  MediaTypeLsResponseStream(cta::catalogue::Catalogue& catalogue,
                            cta::Scheduler& scheduler,
                            const std::string& instanceName);

  bool isDone() override;
  cta::xrd::Data next() override;

private:
  std::vector<cta::catalogue::MediaTypeWithLogs> m_mediaTypes;
  std::size_t m_mediaTypesIdx = 0;
};

}  // namespace cta::frontend
