/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"

namespace cta::objectstore {

class Backend;
class Agent;
class GenericObject;

class SchedulerGlobalLock : public ObjectOps<serializers::SchedulerGlobalLock, serializers::SchedulerGlobalLock_t> {
public:
  SchedulerGlobalLock(const std::string& address, Backend& os);
  explicit SchedulerGlobalLock(GenericObject& go);
  void initialize() override;
  CTA_GENERATE_EXCEPTION_CLASS(NotEmpty);
  void garbageCollect(const std::string& presumedOwner,
                      AgentReference& agentReference,
                      log::LogContext& lc,
                      cta::catalogue::Catalogue& catalogue) override;
  bool isEmpty();

  // Mount id management =======================================================
  void setNextMountId(uint64_t nextId);
  uint64_t getIncreaseCommitMountId();
  std::string dump();
};

}  // namespace cta::objectstore
