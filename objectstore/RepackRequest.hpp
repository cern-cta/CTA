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
#include "common/log/TimingList.hpp"
#include "common/Timer.hpp"

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
  
  // An asynchronous request ownership updating class.
  class AsyncOwnerUpdater {
    friend class RepackRequest;
  public:
    void wait();
  private:
    std::function<std::string(const std::string &)> m_updaterCallback;
    std::unique_ptr<Backend::AsyncUpdater> m_backendUpdater;
    log::TimingList m_timingReport;
    utils::Timer m_timer;
  };
  // An owner updater factory. The owner MUST be previousOwner for the update to be executed.
  AsyncOwnerUpdater *asyncUpdateOwner(const std::string &owner, const std::string &previousOwner,
      cta::optional<serializers::RepackRequestStatus> newStatus);
};

}} // namespace cta::objectstore