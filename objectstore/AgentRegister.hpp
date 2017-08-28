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
#include <list>

namespace cta { namespace objectstore {
  
class Backend;
class Agent;
class GenericObject;

class AgentRegister: public ObjectOps<serializers::AgentRegister, serializers::AgentRegister_t> {
public:
  AgentRegister(Backend & os);
  AgentRegister(GenericObject & go);
  AgentRegister(const std::string & name, Backend & os);
  void initialize();
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  bool isEmpty();
  void addAgent (std::string name);
  void removeAgent (const std::string  & name);
  void trackAgent (std::string name);
  void untrackAgent(std::string name);
  std::list<std::string> getAgents();
  std::list<std::string> getUntrackedAgents();
  std::string dump();
};

}}