/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "objectstore/Agent.hpp"
#include "objectstore/Backend.hpp"
#include "scheduler/OStoreDB/OStoreDB.hpp"

namespace cta {

class OStoreDBWithAgent : public cta::OStoreDB {
public:
  /**
   * Constructor
   *
   * @param be The objectstore backend
   * @param ag The agent
   */
  OStoreDBWithAgent(cta::objectstore::Backend& be,
                    cta::objectstore::AgentReference& ar,
                    catalogue::Catalogue& catalogue,
                    log::Logger& logger);

  /**
   * Destructor
   */
  ~OStoreDBWithAgent() override;
};

}  // namespace cta
