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

#include "catalogue/Catalogue.hpp"
#include "common/log/LogContext.hpp"
#include "maintd/IRoutine.hpp"
#include "objectstore/GarbageCollector.hpp"

/**
 * This class is just a simple wrapper around the objectstore GarbageCollector.
 */
namespace cta::maintd {

class GarbageCollectRoutine : public IRoutine {
public:
  GarbageCollectRoutine(cta::log::LogContext& lc,
                        cta::objectstore::Backend& os,
                        cta::objectstore::AgentReference& agentReference,
                        cta::catalogue::Catalogue& catalogue);
  void execute() final;

  std::string getName() const override final;

private:
  cta::log::LogContext& m_lc;
  cta::objectstore::GarbageCollector m_garbageCollector;
};

}  // namespace cta::maintd
