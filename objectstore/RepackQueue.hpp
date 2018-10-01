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

namespace cta { namespace objectstore {

class GenericObject;

class RepackQueue: public ObjectOps<serializers::RepackQueue, serializers::RepackQueue_t> {
public:

  // Constructor
  RepackQueue(const std::string & address, Backend & os);
  
  // Undefined object constructor
  RepackQueue(Backend & os);
  
  // Upgrader form generic object
  RepackQueue(GenericObject & go);

  // In memory initialiser
  void initialize();
  
  struct RequestsSummary {
    uint64_t requests;
  };
  RequestsSummary getRequestsSummary();
  
  void addRequestsAndCommit(const std::list<std::string> & requestAddresses, log::LogContext & lc);
  
  void addRequestsIfNecessaryAndCommit(const std::list<std::string> & requestAddresses, log::LogContext & lc);
  
  void removeRequestsAndCommit(const std::list<std::string> & requestsAddresses);
  
  bool isEmpty();
  
  void garbageCollect(const std::string& presumedOwner, AgentReference& agentReference, log::LogContext& lc, cta::catalogue::Catalogue& catalogue) override;
  
};

class RepackQueuePending: public RepackQueue {
public:
  template<typename...Ts> RepackQueuePending(Ts&...args): RepackQueue(args...) {}
};

class RepackQueueToExpand: public RepackQueue {
public:
  template<typename...Ts> RepackQueueToExpand(Ts&...args): RepackQueue(args...) {}
};

}} // namespace cta::objectstore
