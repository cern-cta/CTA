/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "GarbageCollectRoutine.hpp"

namespace cta::maintd {

GarbageCollectRoutine::GarbageCollectRoutine(cta::log::LogContext& lc,
                                             objectstore::Backend& os,
                                             objectstore::AgentReference& agentReference,
                                             catalogue::Catalogue& catalogue)
    : m_lc(lc),
      m_garbageCollector(lc, os, agentReference, catalogue) {
  m_lc.log(cta::log::INFO, "In GarbageCollectRoutine: Created GarbageCollectRoutine");
}

void GarbageCollectRoutine::execute() {
  m_garbageCollector.runOnePass();
}

std::string GarbageCollectRoutine::getName() const {
  return "GarbageCollectRoutine";
}

}  // namespace cta::maintd
