/**
 * The CERN Tape Archive (CTA) project
 * Copyright © 2018 CERN
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

#include "RetrieveQueueAlgorithms.hpp"

namespace cta { namespace objectstore {

template<>
void ContainerTraits<RetrieveQueue>::
getLockedAndFetched(Container &cont, ScopedExclusiveLock &aqL, AgentReference &agRef,
  const ContainerIdentifier &contId, log::LogContext &lc)
{
  Helpers::getLockedAndFetchedQueue<Container>(cont, aqL, agRef, contId, QueueType::LiveJobs, lc);
}

template<>
void ContainerTraits<RetrieveQueue>::
addReferencesAndCommit(Container &cont, InsertedElement::list &elemMemCont, AgentReference &agentRef,
  log::LogContext &lc)
{
  std::list<RetrieveQueue::JobToAdd> jobsToAdd;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    jobsToAdd.push_back({e.copyNb, e.fSeq, rr.getAddressIfSet(), e.filesize, e.policy, ::time(nullptr)});
  }
  cont.addJobsAndCommit(jobsToAdd, agentRef, lc);
}

template<>
void ContainerTraits<RetrieveQueue>::
removeReferencesAndCommit(Container &cont, OpFailure<InsertedElement>::list &elementsOpFailures)
{
  std::list<std::string> elementsToRemove;
  for (auto &eof : elementsOpFailures) {
    elementsToRemove.emplace_back(eof.element->retrieveRequest->getAddressIfSet());
  }
  cont.removeJobsAndCommit(elementsToRemove);
}

template<>
auto ContainerTraits<RetrieveQueue>::
switchElementsOwnership(InsertedElement::list &elemMemCont, const ContainerAddress &contAddress,
  const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t,
  log::LogContext &lc) -> OpFailure<InsertedElement>::list
{
  std::list<std::unique_ptr<RetrieveRequest::AsyncOwnerUpdater>> updaters;
  for (auto &e : elemMemCont) {
    RetrieveRequest &rr = *e.retrieveRequest;
    auto copyNb = e.copyNb;
    updaters.emplace_back(rr.asyncUpdateOwner(copyNb, contAddress, previousOwnerAddress));
  }
  timingList.insertAndReset("asyncUpdateLaunchTime", t);
  auto u = updaters.begin();
  auto e = elemMemCont.begin();
  OpFailure<InsertedElement>::list ret;
  while (e != elemMemCont.end()) {
    try {
      u->get()->wait();
    } catch (...) {
      ret.push_back(OpFailure<InsertedElement>());
      ret.back().element = &(*e);
      ret.back().failure = std::current_exception();
    }
    u++;
    e++;
  }
  timingList.insertAndReset("asyncUpdateCompletionTime", t);
  return ret;
}


}} // namespace cta::objectstore
