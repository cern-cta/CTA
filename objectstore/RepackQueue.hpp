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

#include <list>
#include <set>
#include <string>

#include "ObjectOps.hpp"
#include "objectstore/cta.pb.h"
#include "RetrieveQueue.hpp"

namespace cta { namespace objectstore {

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

}  // namespace objectstore
}  // namespace cta
