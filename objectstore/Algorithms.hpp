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

/**
 * This file defines the containers, their traits and algorithms to add/remove
 * to/from them.
 */

#include <string>

#include "common/exception/Exception.hpp"
#include "common/exception/NoSuchObject.hpp"
#include "common/log/LogContext.hpp"
#include "common/log/TimingList.hpp"
#include "Helpers.hpp"

namespace cta {
namespace objectstore {

template<typename Q, typename C>
class ContainerAlgorithms {
public:
  ContainerAlgorithms(Backend& backend, AgentReference& agentReference) :
  m_backend(backend),
  m_agentReference(agentReference) {}

  typedef typename ContainerTraits<Q, C>::InsertedElement InsertedElement;
  typedef typename ContainerTraits<Q, C>::PopCriteria PopCriteria;
  typedef typename ContainerTraits<Q, C>::OwnershipSwitchFailure OwnershipSwitchFailure;
  typedef typename ContainerTraits<Q, C>::PoppedElementsBatch PoppedElementsBatch;
  typedef typename ContainerTraits<Q, C>::QueueType JobQueueType;

  /**
   * Reference objects in the container and then switch their ownership. Objects
   * are provided existing and owned by algorithm's agent.
   */
  void referenceAndSwitchOwnership(const typename ContainerTraits<Q, C>::ContainerIdentifier& contId,
                                   const typename ContainerTraits<Q, C>::ContainerAddress& prevContAddress,
                                   typename ContainerTraits<Q, C>::InsertedElement::list& elements,
                                   log::LogContext& lc) {
    C cont(m_backend);
    ScopedExclusiveLock contLock;
    log::TimingList timingList;
    utils::Timer t;
    ContainerTraits<Q, C>::getLockedAndFetched(cont, contLock, m_agentReference, contId, lc);
    timingList.insertAndReset("queueLockFetchTime", t);
    auto contSummaryBefore = ContainerTraits<Q, C>::getContainerSummary(cont);
    ContainerTraits<Q, C>::addReferencesAndCommit(cont, elements, m_agentReference, lc);
    timingList.insertAndReset("queueProcessAndCommitTime", t);
    auto failedOwnershipSwitchElements = ContainerTraits<Q, C>::switchElementsOwnership(
      elements, cont.getAddressIfSet(), prevContAddress, timingList, t, lc);
    timingList.insertAndReset("requestsUpdatingTime", t);
    // If ownership switching failed, remove failed object from queue to not leave stale pointers.
    if (failedOwnershipSwitchElements.size()) {
      ContainerTraits<Q, C>::removeReferencesAndCommit(cont, failedOwnershipSwitchElements);
      timingList.insertAndReset("queueRecommitTime", t);
    }
    auto contSummaryAfter = ContainerTraits<Q, C>::getContainerSummary(cont);
    // We are now done with the container.
    contLock.release();
    timingList.insertAndReset("queueUnlockTime", t);
    log::ScopedParamContainer params(lc);
    params.add("C", ContainerTraits<Q, C>::c_containerTypeName)
      .add(ContainerTraits<Q, C>::c_identifierType, contId)
      .add("containerAddress", cont.getAddressIfSet());
    contSummaryAfter.addDeltaToLog(contSummaryBefore, params);
    timingList.addToLog(params);
    if (failedOwnershipSwitchElements.empty()) {
      // The good case: all elements went through.
      std::list<std::string> transferedElements;
      for (const auto& e : elements) {
        transferedElements.emplace_back(ContainerTraits<Q, C>::getElementAddress(e));
      }
      m_agentReference.removeBatchFromOwnership(transferedElements, m_backend);
      // That's it, we're done.
      lc.log(log::INFO, "In ContainerAlgorithms::referenceAndSwitchOwnership(): Requeued a batch of elements.");
      return;
    }
    else {
      // Bad case: we have to filter the elements and remove ownership only for the successful ones.
      std::set<std::string> failedElementsSet;
      for (const auto& feos : failedOwnershipSwitchElements) {
        failedElementsSet.insert(ContainerTraits<Q, C>::getElementAddress(*feos.element));
      }
      std::list<std::string> transferedElements;
      typename ContainerTraits<Q, C>::OwnershipSwitchFailure failureEx(
        "In ContainerAlgorithms<>::referenceAndSwitchOwnership(): failed to switch ownership of some elements");
      for (const auto& e : elements) {
        if (!failedElementsSet.count(ContainerTraits<Q, C>::getElementAddress(e))) {
          transferedElements.emplace_back(ContainerTraits<Q, C>::getElementAddress(e));
        }
      }
      if (transferedElements.size()) {
        m_agentReference.removeBatchFromOwnership(transferedElements, m_backend);
      }
      failureEx.failedElements = failedOwnershipSwitchElements;
      params.add("errorCount", failedOwnershipSwitchElements.size());
      std::string failedElementsAddresses = "";
      for (auto& failedElement : failedElementsSet) {
        failedElementsAddresses += failedElement + " ";
      }
      params.add("failedElementsAddresses", failedElementsAddresses);
      lc.log(log::WARNING, "In ContainerAlgorithms::referenceAndSwitchOwnership(): "
                           "Encountered problems while requeuing a batch of elements");
      throw failureEx;
    }
  }

  /**
   * Addition of jobs to container. Convenience overload for cases when current agent is the previous owner
   * (most cases except garbage collection).
   */
  void referenceAndSwitchOwnership(const typename ContainerTraits<Q, C>::ContainerIdentifier& contId,
                                   typename ContainerTraits<Q, C>::InsertedElement::list& elements,
                                   log::LogContext& lc) {
    referenceAndSwitchOwnership(contId, m_agentReference.getAgentAddress(), elements, lc);
  }

  /**
   * Reference objects in the container and then switch their ownership (if needed).
   *
   * Objects are expected to be owned by an agent and not listed in the container, but situations
   * might vary. This function is typically used by the garbage collector. We do not take care of
   * dereferencing the object from the caller.
   */
  void referenceAndSwitchOwnershipIfNecessary(const typename ContainerTraits<Q, C>::ContainerIdentifier& contId,
                                              typename ContainerTraits<Q, C>::ContainerAddress& previousOwnerAddress,
                                              typename ContainerTraits<Q, C>::ContainerAddress& contAddress,
                                              typename ContainerTraits<Q, C>::InsertedElement::list& elements,
                                              log::LogContext& lc) {
    C cont(m_backend);
    ScopedExclusiveLock contLock;
    log::TimingList timingList;
    utils::Timer t;
    ContainerTraits<Q, C>::getLockedAndFetched(cont, contLock, m_agentReference, contId, lc);
    contAddress = cont.getAddressIfSet();  //TODO : It would be better to return this value
    auto contSummaryBefore = ContainerTraits<Q, C>::getContainerSummary(cont);
    timingList.insertAndReset("queueLockFetchTime", t);
    ContainerTraits<Q, C>::addReferencesIfNecessaryAndCommit(cont, elements, m_agentReference, lc);
    timingList.insertAndReset("queueProcessAndCommitTime", t);
    auto failedOwnershipSwitchElements = ContainerTraits<Q, C>::switchElementsOwnership(
      elements, cont.getAddressIfSet(), previousOwnerAddress, timingList, t, lc);
    timingList.insertAndReset("requestsUpdatingTime", t);
    // If ownership switching failed, remove failed object from queue to not leave stale pointers.
    if (failedOwnershipSwitchElements.size()) {
      ContainerTraits<Q, C>::removeReferencesAndCommit(cont, failedOwnershipSwitchElements);
      timingList.insertAndReset("queueRecommitTime", t);
    }
    // We are now done with the container.
    auto contSummaryAfter = ContainerTraits<Q, C>::getContainerSummary(cont);
    contLock.release();
    timingList.insertAndReset("queueUnlockTime", t);
    log::ScopedParamContainer params(lc);
    params.add("C", ContainerTraits<Q, C>::c_containerTypeName)
      .add(ContainerTraits<Q, C>::c_identifierType, contId)
      .add("containerAddress", cont.getAddressIfSet());
    contSummaryAfter.addDeltaToLog(contSummaryBefore, params);
    timingList.addToLog(params);
    if (failedOwnershipSwitchElements.empty()) {
      // That's it, we're done.
      lc.log(log::INFO,
             "In ContainerAlgorithms::referenceAndSwitchOwnershipIfNecessary(): Requeued a batch of elements.");
      return;
    }
    else {
      // Bad case: just return the failure set to the caller.
      typename ContainerTraits<Q, C>::OwnershipSwitchFailure failureEx(
        "In ContainerAlgorithms<>::referenceAndSwitchOwnershipIfNecessary(): failed to switch ownership of some "
        "elements");
      failureEx.failedElements = failedOwnershipSwitchElements;
      params.add("errorCount", failedOwnershipSwitchElements.size());
      lc.log(log::WARNING, "In ContainerAlgorithms::referenceAndSwitchOwnershipIfNecessary(): "
                           "Encountered problems while requeuing a batch of elements");
      throw failureEx;
    }
  }

  /**
   * Do a single round of popping from the pre-locked container.
   * @param cont
   * @param contLock
   * @param popCriteria
   * @param newStatus: optional new status, for pop-and-switch-status combined operation.
   * @param lc
   * @return
   */
  PoppedElementsBatch
    popNextBatchFromContainerAndSwitchStatus(C& cont,
                                             ScopedExclusiveLock& contLock,
                                             PopCriteria& popCriteria,
                                             std::optional<typename ContainerTraits<Q, C>::ElementStatus>& newStatus,
                                             log::LogContext& lc) {
    PoppedElementsBatch ret;
    auto previousSummary = ret.summary;
    log::TimingList timingList;
    utils::Timer t, totalTime;
    typename ContainerTraits<Q, C>::ContainerSummary contSummaryBefore, contSummaryAfter;
    try {
      cont.fetch();
    }
    catch (exception::Exception&) {
      timingList.insertAndReset("fetchQueueTime", t);
      // Failing to access the container will be logged
      goto logAndReturn;
    }
    timingList.insertAndReset("fetchQueueTime", t);
    contSummaryBefore = ContainerTraits<Q, C>::getContainerSummary(cont);
    {
      // We have a container. Get candidate element list from it.
      typename ContainerTraits<Q, C>::ElementsToSkipSet emptyElementsToSkip;
      PoppedElementsBatch candidateElements =
        ContainerTraits<Q, C>::getPoppingElementsCandidates(cont, popCriteria, emptyElementsToSkip, lc);
      timingList.insertAndReset("jobSelectionTime", t);
      // Reference the candidates to our agent
      std::list<typename ContainerTraits<Q, C>::ElementAddress> candidateElementsAddresses;
      for (auto& e : candidateElements.elements) {
        candidateElementsAddresses.emplace_back(ContainerTraits<Q, C>::getElementAddress(e));
      }
      timingList.insertAndReset("ownershipAdditionTime", t);
      m_agentReference.addBatchToOwnership(candidateElementsAddresses, m_backend);
      // We can now attempt to switch ownership of elements
      auto failedOwnershipSwitchElements = ContainerTraits<Q, C>::switchElementsOwnershipAndStatus(
        candidateElements, m_agentReference.getAgentAddress(), cont.getAddressIfSet(), timingList, t, lc, newStatus);
      if (failedOwnershipSwitchElements.empty()) {
        timingList.insertAndReset("updateResultProcessingTime", t);
        // This is the easy case (and most common case). Everything went through fine.
        ContainerTraits<Q, C>::removeReferencesAndCommit(cont, candidateElementsAddresses);
        timingList.insertAndReset("containerUpdateTime", t);
        contSummaryAfter = ContainerTraits<Q, C>::getContainerSummary(cont);
        // We skip the container trimming as we do not have the contId.
        // trimming might release the lock
        if (contLock.isLocked()) {
          contLock.release();
        }
        timingList.insertAndReset("containerUnlockTime", t);
        // All jobs are validated
        ret.summary += candidateElements.summary;
        ret.elements.insertBack(std::move(candidateElements.elements));
        timingList.insertAndReset("structureProcessingTime", t);
      }
      else {
        // For the failed files, we have to differentiate the not owned or not existing ones from other error cases.
        // For the not owned, not existing and those successfully switched, we have to de reference them form the container.
        // For other cases, we will leave the elements referenced in the container, as we cannot ensure de-referencing is safe.
        std::set<typename ContainerTraits<Q, C>::ElementAddress> elementsNotToDereferenceFromContainer;
        std::set<typename ContainerTraits<Q, C>::ElementAddress> elementsNotToReport;
        std::list<typename ContainerTraits<Q, C>::ElementAddress> elementsToDereferenceFromAgent;
        for (auto& e : failedOwnershipSwitchElements) {
          try {
            std::rethrow_exception(e.failure);
          }
          catch (cta::exception::NoSuchObject&) {
            elementsToDereferenceFromAgent.push_back(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
          }
          catch (Backend::WrongPreviousOwner&) {
            elementsToDereferenceFromAgent.push_back(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
          }
          catch (Backend::CouldNotUnlock&) {
            // Do nothing, this element was indeed OK.
          }
          catch (...) {
            // This is a different error, so we will leave the reference to the element in the container
            elementsNotToDereferenceFromContainer.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsToDereferenceFromAgent.push_back(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
          }
        }
        // We are done with the sorting. Apply the decisions...
        std::list<typename ContainerTraits<Q, C>::ElementAddress> elementsToDereferenceFromContainer;
        for (auto& e : candidateElements.elements) {
          if (!elementsNotToDereferenceFromContainer.count(ContainerTraits<Q, C>::getElementAddress(e))) {
            elementsToDereferenceFromContainer.push_back(ContainerTraits<Q, C>::getElementAddress(e));
          }
        }
        timingList.insertAndReset("updateResultProcessingTime", t);
        ContainerTraits<Q, C>::removeReferencesAndCommit(cont, elementsToDereferenceFromContainer);
        timingList.insertAndReset("containerUpdateTime", t);
        contSummaryAfter = ContainerTraits<Q, C>::getContainerSummary(cont);
        if (contLock.isLocked()) {
          contLock.release();
        }
        timingList.insertAndReset("containerUnlockTime", t);
        m_agentReference.removeBatchFromOwnership(elementsToDereferenceFromAgent, m_backend);
        for (auto& e : candidateElements.elements) {
          if (!elementsNotToReport.count(ContainerTraits<Q, C>::getElementAddress(e))) {
            ret.summary += ContainerTraits<Q, C>::getElementSummary(e);
            ret.elements.insertBack(std::move(e));
          }
        }
        timingList.insertAndReset("structureProcessingTime", t);
      }
    }
    {
      log::ScopedParamContainer params(lc);
      params.add("C", ContainerTraits<Q, C>::c_containerTypeName).add("containerAddress", cont.getAddressIfSet());
      ret.summary.addDeltaToLog(previousSummary, params);
      contSummaryAfter.addDeltaToLog(contSummaryBefore, params);
      timingList.addToLog(params);
      lc.log(log::INFO, "In ContainerTraits<Q,C>::popNextBatchFromContainer(): did one round of elements retrieval.");
    }
logAndReturn : {
  log::ScopedParamContainer params(lc);
  params.add("C", ContainerTraits<Q, C>::c_containerTypeName);
  ret.addToLog(params);
  timingList.addToLog(params);
  params.add("schedulerDbTime", totalTime.secs());
  lc.log(log::INFO, "In ContainerTraits<Q,C>::popNextBatchFromContainer(): elements retrieval complete.");
}
    return ret;
  }

  /**
   * Loop popping from a container until coming empty handed or fulfilling the criteria.
   * The popping can take several iterations. The container is re-found on each round.
   * @param contId container identifier (typically a string like vid, tape pool...)
   * @param queueType container type (usually represents the steps in the requests lifecycle (ToTranfer, FailedToReport, Failed...)
   * @param popCriteria
   * @param lc
   * @return
   */
  PoppedElementsBatch popNextBatch(const typename ContainerTraits<Q, C>::ContainerIdentifier& contId,
                                   typename ContainerTraits<Q, C>::PopCriteria& popCriteria,
                                   log::LogContext& lc) {
    // Prepare the return value
    PoppedElementsBatch ret;
    typename ContainerTraits<Q, C>::PopCriteria unfulfilledCriteria = popCriteria;
    size_t iterationCount = 0;
    typename ContainerTraits<Q, C>::ElementsToSkipSet elementsToSkip;
    log::TimingList timingList;
    utils::Timer t, totalTime;
    bool unexpectedException = false;
    bool didTrim = false;
    while (!unexpectedException && ret.summary < popCriteria && !didTrim) {
      typename ContainerTraits<Q, C>::PoppedElementsSummary previousSummary = ret.summary;
      log::TimingList localTimingList;
      // Get a container if it exists
      C cont(m_backend);
      iterationCount++;
      ScopedExclusiveLock contLock;
      try {
        ContainerTraits<Q, C>::getLockedAndFetchedNoCreate(cont, contLock, contId, lc);
      }
      catch (typename ContainerTraits<Q, C>::NoSuchContainer&) {
        localTimingList.insertAndReset("findLockFetchQueueTime", t);
        timingList += localTimingList;
        // We could not find a container to pop from: return what we have.
        goto logAndReturn;
      }
      localTimingList.insertAndReset("findLockFetchQueueTime", t);
      typename ContainerTraits<Q, C>::ContainerSummary contSummaryBefore, contSummaryAfter;
      contSummaryBefore = ContainerTraits<Q, C>::getContainerSummary(cont);
      // We have a container. Get candidate element list from it.
      PoppedElementsBatch candidateElements =
        ContainerTraits<Q, C>::getPoppingElementsCandidates(cont, unfulfilledCriteria, elementsToSkip, lc);
      localTimingList.insertAndReset("jobSelectionTime", t);
      // Reference the candidates to our agent
      std::list<typename ContainerTraits<Q, C>::ElementAddress> candidateElementsAddresses;
      for (auto& e : candidateElements.elements) {
        candidateElementsAddresses.emplace_back(ContainerTraits<Q, C>::getElementAddress(e));
      }
      localTimingList.insertAndReset("ownershipAdditionTime", t);
      m_agentReference.addBatchToOwnership(candidateElementsAddresses, m_backend);
      // We can now attempt to switch ownership of elements
      auto failedOwnershipSwitchElements = ContainerTraits<Q, C>::switchElementsOwnership(
        candidateElements, m_agentReference.getAgentAddress(), cont.getAddressIfSet(), localTimingList, t, lc);
      if (failedOwnershipSwitchElements.empty()) {
        localTimingList.insertAndReset("updateResultProcessingTime", t);
        // This is the easy case (and most common case). Everything went through fine.
        ContainerTraits<Q, C>::removeReferencesAndCommit(cont, candidateElementsAddresses);
        localTimingList.insertAndReset("containerUpdateTime", t);
        contSummaryAfter = ContainerTraits<Q, C>::getContainerSummary(cont);
        // If we emptied the container, we have to trim it.
        didTrim = ContainerTraits<Q, C>::trimContainerIfNeeded(cont, contLock, contId, lc);
        localTimingList.insertAndReset("containerTrimmingTime", t);
        // trimming might release the lock
        if (contLock.isLocked()) {
          contLock.release();
        }
        localTimingList.insertAndReset("containerUnlockTime", t);
        // All jobs are validated
        ret.summary += candidateElements.summary;
        unfulfilledCriteria -= candidateElements.summary;
        ret.elements.insertBack(std::move(candidateElements.elements));
        localTimingList.insertAndReset("structureProcessingTime", t);
      }
      else {
        // For the failed files, we have to differentiate the not owned or not existing ones from other error cases.
        // For the not owned, not existing and those successfully switched, we have to de reference them form the container.
        // For other cases, we will leave the elements referenced in the container, as we cannot ensure de-referencing is safe.
        std::set<typename ContainerTraits<Q, C>::ElementAddress> elementsNotToDereferenceFromContainer;
        std::set<typename ContainerTraits<Q, C>::ElementAddress> elementsNotToReport;
        std::list<typename ContainerTraits<Q, C>::ElementAddress> elementsToDereferenceFromAgent;
        for (auto& e : failedOwnershipSwitchElements) {
          try {
            std::rethrow_exception(e.failure);
          }
          catch (cta::exception::NoSuchObject&) {
            elementsToDereferenceFromAgent.push_back(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
          }
          catch (Backend::WrongPreviousOwner&) {
            elementsToDereferenceFromAgent.push_back(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
          }
          catch (Backend::CouldNotUnlock&) {
            // Do nothing, this element was indeed OK.
          }
          catch (...) {
            // This is a different error, so we will leave the reference to the element in the container
            elementsNotToDereferenceFromContainer.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsToDereferenceFromAgent.push_back(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
            elementsToSkip.insert(ContainerTraits<Q, C>::getElementAddress(*e.element));
            // If we get this kind of situation, we do not try to carry on, as it becomes too complex.
            unexpectedException = true;
          }
        }
        // We are done with the sorting. Apply the decisions...
        std::list<typename ContainerTraits<Q, C>::ElementAddress> elementsToDereferenceFromContainer;
        for (auto& e : candidateElements.elements) {
          if (!elementsNotToDereferenceFromContainer.count(ContainerTraits<Q, C>::getElementAddress(e))) {
            elementsToDereferenceFromContainer.push_back(ContainerTraits<Q, C>::getElementAddress(e));
          }
        }
        localTimingList.insertAndReset("updateResultProcessingTime", t);
        ContainerTraits<Q, C>::removeReferencesAndCommit(cont, elementsToDereferenceFromContainer);
        localTimingList.insertAndReset("containerUpdateTime", t);
        contSummaryAfter = ContainerTraits<Q, C>::getContainerSummary(cont);
        // If we emptied the container, we have to trim it.
        ContainerTraits<Q, C>::trimContainerIfNeeded(cont, contLock, contId, lc);
        localTimingList.insertAndReset("containerTrimmingTime", t);
        // trimming might release the lock
        if (contLock.isLocked()) {
          contLock.release();
        }
        localTimingList.insertAndReset("containerUnlockTime", t);
        m_agentReference.removeBatchFromOwnership(elementsToDereferenceFromAgent, m_backend);
        typename ContainerTraits<Q, C>::PoppedElementsSummary batchSummary = candidateElements.summary;
        for (auto& e : candidateElements.elements) {
          if (!elementsNotToReport.count(ContainerTraits<Q, C>::getElementAddress(e))) {
            ret.elements.insertBack(std::move(e));
          }
          else {
            batchSummary -= ContainerTraits<Q, C>::getElementSummary(e);
          }
        }
        ret.summary += batchSummary;
        unfulfilledCriteria -= batchSummary;
        localTimingList.insertAndReset("structureProcessingTime", t);
      }
      log::ScopedParamContainer params(lc);
      params.add("C", ContainerTraits<Q, C>::c_containerTypeName)
        .add(ContainerTraits<Q, C>::c_identifierType, contId)
        .add("containerAddress", cont.getAddressIfSet());
      ret.summary.addDeltaToLog(previousSummary, params);
      contSummaryAfter.addDeltaToLog(contSummaryBefore, params);
      localTimingList.addToLog(params);
      if (ret.elements.size()) {
        lc.log(log::INFO, "In Algorithms::popNextBatch(): did one round of elements retrieval.");
      }
      timingList += localTimingList;
    }
logAndReturn : {
  log::ScopedParamContainer params(lc);
  params.add("C", ContainerTraits<Q, C>::c_containerTypeName).add(ContainerTraits<Q, C>::c_identifierType, contId);
  ret.addToLog(params);
  timingList.addToLog(params);
  params.add("schedulerDbTime", totalTime.secs());
  params.add("iterationCount", iterationCount);
  if (ret.elements.size()) {
    lc.log(log::INFO, "In Algorithms::popNextBatch(): elements retrieval complete.");
  }
}
    return ret;
  }

private:
  Backend& m_backend;
  AgentReference& m_agentReference;
};

}  // namespace objectstore
}  // namespace cta
