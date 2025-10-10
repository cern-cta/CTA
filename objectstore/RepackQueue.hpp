/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <list>
#include <set>
#include <string>

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "RetrieveQueue.hpp"

namespace cta::objectstore {

class GenericObject;

class RepackQueue: public ObjectOps<serializers::RepackQueue, serializers::RepackQueue_t> {
 public:

  // Constructor
  RepackQueue(const std::string & address, Backend & os);

  // Undefined object constructor
  explicit RepackQueue(Backend & os);

  // Upgrader form generic object
  explicit RepackQueue(GenericObject & go);

  // In memory initialiser
  void initialize() override;

  struct RequestsSummary {
    uint64_t requests;
  };
  RequestsSummary getRequestsSummary();

  void addRequestsAndCommit(const std::list<std::string> & requestAddresses, log::LogContext & lc);

  void addRequestsIfNecessaryAndCommit(const std::list<std::string> & requestAddresses, log::LogContext & lc);

  struct RequestDump {
    std::string address;
  };

  struct CandidateJobList {
    uint64_t remainingRequestsAfterCandidates = 0;
    uint64_t candidateRequests = 0;
    std::list<RequestDump> candidates;
  };

  // The set of repack requests to skip are requests previously identified by the caller as bad,
  // which still should be removed from the queue. They will be disregarded from  listing.
  CandidateJobList getCandidateList(uint64_t maxRequests, const std::set<std::string>& repackRequestsToSkip);

  void removeRequestsAndCommit(const std::list<std::string> & requestsAddresses);

  bool isEmpty();

  void garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, log::LogContext& lc,
    cta::catalogue::Catalogue& catalogue) override;

  std::string dump();
};

class RepackQueuePending: public RepackQueue {
  using RepackQueue::RepackQueue;
};

class RepackQueueToExpand: public RepackQueue {
  using RepackQueue::RepackQueue;
};

} // namespace cta::objectstore
