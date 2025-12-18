/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

  std::string getName() const final;

private:
  cta::log::LogContext& m_lc;
  cta::objectstore::GarbageCollector m_garbageCollector;
};

}  // namespace cta::maintd
