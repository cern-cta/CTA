/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ObjectOps.hpp"
#include <list>

namespace cta::objectstore {

class Backend;
class Agent;
class GenericObject;

class AgentRegister: public ObjectOps<serializers::AgentRegister, serializers::AgentRegister_t> {
public:
  explicit AgentRegister(Backend& os);
  explicit AgentRegister(GenericObject& go);
  AgentRegister(const std::string& name, Backend& os);
  void initialize() override;
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void garbageCollect(const std::string &presumedOwner, AgentReference & agentReference, log::LogContext & lc,
    cta::catalogue::Catalogue & catalogue) override;
  bool isEmpty();
  void addAgent (std::string name);
  void removeAgent (const std::string  &name);
  void trackAgent (const std::string &name);
  void untrackAgent(std::string name);
  std::list<std::string> getAgents();
  std::list<std::string> getUntrackedAgents();
  std::string dump();
};

} // namespace cta::objectstore
