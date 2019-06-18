/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "ObjectOps.hpp"
#include "common/dataStructures/DriveState.hpp"
#include "common/dataStructures/DriveNextState.hpp"

namespace cta { namespace objectstore {

class GenericObject;

class DriveState: public ObjectOps<serializers::DriveState, serializers::DriveState_t> {
public:
  // Undefined object constructor
  DriveState(Backend & os);
  
  // Garbage collection interface
  DriveState(GenericObject & go);
  void garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, 
    log::LogContext& lc, cta::catalogue::Catalogue& catalogue) override;

  // Initialization
  void initialize(const std::string & driveName);
  
  // Standard access constructor
  DriveState(const std::string & address, Backend & os);
  
  // Data access
  cta::common::dataStructures::DriveState getState();
  void setState(cta::common::dataStructures::DriveState & state);
  
  /**
   * JSON dump of the drive state
   * @return 
   */
  std::string dump();
};

}} // namespace cta::objectstore