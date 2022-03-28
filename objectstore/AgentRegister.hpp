/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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