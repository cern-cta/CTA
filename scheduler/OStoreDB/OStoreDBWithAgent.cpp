/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "scheduler/OStoreDB/OStoreDBWithAgent.hpp"

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
cta::OStoreDBWithAgent::OStoreDBWithAgent(cta::objectstore::Backend& be,
                                          cta::objectstore::AgentReference& ar,
                                          catalogue::Catalogue& catalogue,
                                          log::Logger& logger)
    : cta::OStoreDB(be, catalogue, logger) {
  cta::OStoreDB::setAgentReference(&ar);
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
cta::OStoreDBWithAgent::~OStoreDBWithAgent() noexcept {
  cta::OStoreDB::setAgentReference(nullptr);
}
