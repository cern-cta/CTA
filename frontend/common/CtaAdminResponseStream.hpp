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

#include "cta_frontend.pb.h"
#include <catalogue/Catalogue.hpp>
#include <scheduler/Scheduler.hpp>

namespace cta::frontend {

class CtaAdminResponseStream {
public:
  CtaAdminResponseStream(cta::catalogue::Catalogue& catalogue,
                         cta::Scheduler& scheduler,
                         const std::string& instanceName)
      : m_catalogue(catalogue),
        m_scheduler(scheduler),
        m_instanceName(instanceName) {}

  virtual ~CtaAdminResponseStream() = default;
  virtual bool isDone() = 0;
  virtual cta::xrd::Data next() = 0;

protected:
  cta::catalogue::Catalogue& m_catalogue;
  cta::Scheduler& m_scheduler;
  const std::string m_instanceName;
};

}  // namespace cta::frontend