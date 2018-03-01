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
#include "objectstore/cta.pb.h"
#include "common/dataStructures/RepackType.hpp"

namespace cta { namespace objectstore {

class Agent;
class GenericObject;

class RepackRequest: public ObjectOps<serializers::RepackRequest, serializers::RepackRequest_t> {
public:
  RepackRequest(const std::string & address, Backend & os);
  RepackRequest(Backend & os);
  RepackRequest(GenericObject & go);
  void initialize();
  
  // Parameters interface
  void setVid(const std::string & vid);
  void setRepackType(common::dataStructures::RepackType repackType);
  
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
};

}} // namespace cta::objectstore