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

#pragma once

/**
 * This file defines the containers, their traits and algorithms to add/remove
 * to/from them.
 */

#include <string>
#include "Helpers.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/TimingList.hpp"

namespace cta { namespace objectstore {

/**
 * Container traits type definitions
 */
template<typename C>
struct ContainerTraitsTypes
{
  struct ContainerSummary {
    void addDeltaToLog(const ContainerSummary&, log::ScopedParamContainer&);
  };

  struct InsertedElement {
    typedef std::list<InsertedElement> list;
  };

  struct ElementDescriptor {};

  struct PoppedElement {};
  struct PoppedElementsSummary;
  struct PopCriteria {
    PopCriteria();
    PopCriteria& operator-= (const PoppedElementsSummary &);
  };
  struct PoppedElementsSummary {
    bool operator<(const PopCriteria&);
    PoppedElementsSummary& operator+=(const PoppedElementsSummary&);
    PoppedElementsSummary(const PoppedElementsSummary&);
    void addDeltaToLog(const PoppedElementsSummary&, log::ScopedParamContainer&);
  };
  struct PoppedElementsList {
    PoppedElementsList();
    void insertBack(PoppedElementsList&&);
  };
  struct PoppedElementsBatch {
    PoppedElementsList elements;
    PoppedElementsSummary summary;
    void addToLog(log::ScopedParamContainer&);
  };
};


/**
 * Container traits definition
 */
template<typename C> 
struct ContainerTraits
{
  using ContainerSummary = typename ContainerTraitsTypes<C>::ContainerSummary;
  using InsertedElement = typename ContainerTraitsTypes<C>::InsertedElement;
  using ElementDescriptor = typename ContainerTraitsTypes<C>::ElementDescriptor;
  using PoppedElement = typename ContainerTraitsTypes<C>::PoppedElement;
  using PopCriteria = typename ContainerTraitsTypes<C>::PopCriteria;
  using PoppedElementsSummary = typename ContainerTraitsTypes<C>::PoppedElementsSummary;
  using PoppedElementsList = typename ContainerTraitsTypes<C>::PoppedElementsList;
  using PoppedElementsBatch = typename ContainerTraitsTypes<C>::PoppedElementsBatch;

  typedef C                                           Container;
  typedef std::string                                 ContainerAddress;
  typedef std::string                                 ElementAddress;
  typedef std::string                                 ContainerIdentifier;
  typedef std::list<std::unique_ptr<InsertedElement>> ElementMemoryContainer;
  typedef std::list<ElementDescriptor>                ElementDescriptorContainer;
  typedef std::set<ElementAddress>                    ElementsToSkipSet;

  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

  template<typename Element>
  struct OpFailure {
    Element *element;
    std::exception_ptr failure;
    typedef std::list<OpFailure> list;
  };

  class OwnershipSwitchFailure: public cta::exception::Exception {
  public:
    OwnershipSwitchFailure(const std::string &message): cta::exception::Exception(message) {};
    typename OpFailure<InsertedElement>::list failedElements;
  };

  template<typename Element>
  static ElementAddress getElementAddress(const Element &e);

  static ContainerSummary getContainerSummary(Container &cont);
  static void trimContainerIfNeeded(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void getLockedAndFetched(Container &cont, ScopedExclusiveLock &contLock, AgentReference &agRef,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void getLockedAndFetchedNoCreate(Container &cont, ScopedExclusiveLock &contLock,
    const ContainerIdentifier &cId, log::LogContext &lc);
  static void addReferencesAndCommit(Container &cont, typename InsertedElement::list &elemMemCont,
    AgentReference &agentRef, log::LogContext &lc);
  static void addReferencesIfNecessaryAndCommit(Container &cont, typename InsertedElement::list &elemMemCont,
    AgentReference &agentRef, log::LogContext &lc);
  static void removeReferencesAndCommit(Container &cont, typename OpFailure<InsertedElement>::list &elementsOpFailures);
  static void removeReferencesAndCommit(Container &cont, std::list<ElementAddress> &elementAddressList);

  static typename OpFailure<InsertedElement>::list
  switchElementsOwnership(typename InsertedElement::list &elemMemCont, const ContainerAddress &contAddress,
    const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t, log::LogContext &lc);

  static typename OpFailure<PoppedElement>::list
  switchElementsOwnership(PoppedElementsBatch &poppedElementBatch, const ContainerAddress &contAddress,
    const ContainerAddress &previousOwnerAddress, log::TimingList &timingList, utils::Timer &t, log::LogContext &lc);

  static PoppedElementsSummary getElementSummary(const PoppedElement &);
  static PoppedElementsBatch getPoppingElementsCandidates(Container &cont, PopCriteria &unfulfilledCriteria,
    ElementsToSkipSet &elemtsToSkip, log::LogContext &lc);

  static const std::string c_containerTypeName; // = "genericContainer"
  static const std::string c_identifierType;    // = "genericId"
};


/**
 * Container algorithms
 */
template<typename C>
class ContainerAlgorithms {
public:
  ContainerAlgorithms(Backend & backend, AgentReference & agentReference):
    m_backend(backend), m_agentReference(agentReference) {}
    
  typedef typename ContainerTraits<C>::InsertedElement InsertedElement;
  typedef typename ContainerTraits<C>::PopCriteria PopCriteria;
  typedef typename ContainerTraits<C>::OwnershipSwitchFailure OwnershipSwitchFailure;
    
  /** Reference objects in the container and then switch their ownership. Objects 
   * are provided existing and owned by algorithm's agent.
   */
  void referenceAndSwitchOwnership(const typename ContainerTraits<C>::ContainerIdentifier & contId,
      const typename ContainerTraits<C>::ContainerIdentifier & prevContId,
      typename ContainerTraits<C>::InsertedElement::list & elements, log::LogContext & lc) {
    C cont(m_backend);
    ScopedExclusiveLock contLock;
    log::TimingList timingList;
    utils::Timer t;
    ContainerTraits<C>::getLockedAndFetched(cont, contLock, m_agentReference, contId, lc);
    ContainerTraits<C>::addReferencesAndCommit(cont, elements, m_agentReference, lc);
    auto failedOwnershipSwitchElements = ContainerTraits<C>::switchElementsOwnership(elements, cont.getAddressIfSet(), prevContId, timingList, t, lc);
    // If ownership switching failed, remove failed object from queue to not leave stale pointers.
    if (failedOwnershipSwitchElements.size()) {
      ContainerTraits<C>::removeReferencesAndCommit(cont, failedOwnershipSwitchElements);
    }
    // We are now done with the container.
    contLock.release();
    if (failedOwnershipSwitchElements.empty()) {
      // The good case: all elements went through.
      std::list<std::string> transferedElements;
      for (const auto & e: elements) transferedElements.emplace_back(ContainerTraits<C>::getElementAddress(e));
      m_agentReference.removeBatchFromOwnership(transferedElements, m_backend);
      // That's it, we're done.
      return;
    } else {
      // Bad case: we have to filter the elements and remove ownership only for the successful ones.
      std::set<std::string> failedElementsSet;
      for (const auto & feos: failedOwnershipSwitchElements) failedElementsSet.insert(ContainerTraits<C>::getElementAddress(*feos.element));
      std::list<std::string> transferedElements;
      typename ContainerTraits<C>::OwnershipSwitchFailure failureEx(
          "In ContainerAlgorithms<>::referenceAndSwitchOwnership(): failed to switch ownership of some elements");
      for (const auto & e: elements) {
        if (!failedElementsSet.count(ContainerTraits<C>::getElementAddress(e))) {
          transferedElements.emplace_back(ContainerTraits<C>::getElementAddress(e));
        }
      }
      if (transferedElements.size()) m_agentReference.removeBatchFromOwnership(transferedElements, m_backend);
      failureEx.failedElements = failedOwnershipSwitchElements;
      throw failureEx;
    }
  }
  
  /** Reference objects in the container if needed and then switch their ownership (if needed). Objects 
   * are expected to be owned by agent, and not listed in the container but situations might vary.
   * This function is typically used by the garbage collector. We do noe take care of dereferencing
   * the object from the caller.
   */
  void referenceAndSwitchOwnershipIfNecessary(const typename ContainerTraits<C>::ContainerIdentifier & contId,
      typename ContainerTraits<C>::ContainerAddress & previousOwnerAddress,
      typename ContainerTraits<C>::ContainerAddress & contAddress,
      typename ContainerTraits<C>::InsertedElement::list & elements, log::LogContext & lc) {
    C cont(m_backend);
    ScopedExclusiveLock contLock;
    log::TimingList timingList;
    utils::Timer t;
    ContainerTraits<C>::getLockedAndFetched(cont, contLock, m_agentReference, contId, lc);
    contAddress = cont.getAddressIfSet();
    auto contSummaryBefore = ContainerTraits<C>::getContainerSummary(cont);
    timingList.insertAndReset("queueLockFetchTime", t);
    ContainerTraits<C>::addReferencesIfNecessaryAndCommit(cont, elements, m_agentReference, lc);
    timingList.insertAndReset("queueProcessAndCommitTime", t);
    auto failedOwnershipSwitchElements = ContainerTraits<C>::switchElementsOwnership(elements, cont.getAddressIfSet(), previousOwnerAddress, timingList, t, lc);
    timingList.insertAndReset("requestsUpdatingTime", t);
    // If ownership switching failed, remove failed object from queue to not leave stale pointers.
    if (failedOwnershipSwitchElements.size()) {
      ContainerTraits<C>::removeReferencesAndCommit(cont, failedOwnershipSwitchElements);
      timingList.insertAndReset("queueRecommitTime", t);
    }
    // We are now done with the container.
    auto contSummaryAfter = ContainerTraits<C>::getContainerSummary(cont);
    contLock.release();
    timingList.insertAndReset("queueUnlockTime", t);
    log::ScopedParamContainer params(lc);
    params.add("C", ContainerTraits<C>::c_containerTypeName)
          .add(ContainerTraits<C>::c_identifierType, contId)
          .add("containerAddress", cont.getAddressIfSet());
    contSummaryAfter.addDeltaToLog(contSummaryBefore, params);
    timingList.addToLog(params);
    if (failedOwnershipSwitchElements.empty()) {
      // That's it, we're done.
      lc.log(log::INFO, "In ContainerAlgorithms::referenceAndSwitchOwnershipIfNecessary(): Requeued a batch of elements.");
      return;
    } else {
      // Bad case: just return the failure set to the caller.
      typename ContainerTraits<C>::OwnershipSwitchFailure failureEx(
          "In ContainerAlgorithms<>::referenceAndSwitchOwnershipIfNecessar(): failed to switch ownership of some elements");
      failureEx.failedElements = failedOwnershipSwitchElements;
      params.add("errorCount", failedOwnershipSwitchElements.size());
      lc.log(log::WARNING, "In ContainerAlgorithms::referenceAndSwitchOwnershipIfNecessary(): "
          "Encountered problems while requeuing a batch of elements");
      throw failureEx;
    }
  }
  
  /**
   * Addition of jobs to container. Convenience overload for cases when current agent is the previous owner 
   * (most cases except garbage collection).
   */
  void referenceAndSwitchOwnership(const typename ContainerTraits<C>::ContainerIdentifier & contId,
      typename ContainerTraits<C>::InsertedElement::list & elements, log::LogContext & lc) {
    referenceAndSwitchOwnership(contId, m_agentReference.getAgentAddress(), elements, lc);
  }
  
  
  typename ContainerTraits<C>::PoppedElementsBatch popNextBatch(const typename ContainerTraits<C>::ContainerIdentifier & contId,
      typename ContainerTraits<C>::PopCriteria & popCriteria, log::LogContext & lc) {
    // Prepare the return value
    typename ContainerTraits<C>::PoppedElementsBatch ret;
    typename ContainerTraits<C>::PopCriteria unfulfilledCriteria = popCriteria;
    size_t iterationCount=0;
    typename ContainerTraits<C>::ElementsToSkipSet elementsToSkip;
    log::TimingList timingList;
    utils::Timer t, totalTime;
    bool unexpectedException = false;
    while (!unexpectedException && ret.summary < popCriteria) {
      typename ContainerTraits<C>::PoppedElementsSummary previousSummary = ret.summary;
      log::TimingList localTimingList;
      // Get a container if it exists
      C cont(m_backend);
      iterationCount++;
      ScopedExclusiveLock contLock;
      try {
        ContainerTraits<C>::getLockedAndFetchedNoCreate(cont, contLock, contId, lc);
      } catch (typename ContainerTraits<C>::NoSuchContainer &) {
        localTimingList.insertAndReset("findLockFetchQueueTime", t);
        timingList+=localTimingList;
        // We could not find a container to pop from: return what we have.
        goto logAndReturn;
      }
      localTimingList.insertAndReset("findLockFetchQueueTime", t);
      typename ContainerTraits<C>::ContainerSummary contSummaryBefore, contSummaryAfter;
      contSummaryBefore = ContainerTraits<C>::getContainerSummary(cont);
      // We have a container. Get candidate element list from it.
      typename ContainerTraits<C>::PoppedElementsBatch candidateElements = 
          ContainerTraits<C>::getPoppingElementsCandidates(cont, unfulfilledCriteria, elementsToSkip, lc);
      localTimingList.insertAndReset("jobSelectionTime", t);
      // Reference the candidates to our agent
      std::list<typename ContainerTraits<C>::ElementAddress> candidateElementsAddresses;
      for (auto & e: candidateElements.elements) {
        candidateElementsAddresses.emplace_back(ContainerTraits<C>::getElementAddress(e));
      }
      localTimingList.insertAndReset("ownershipAdditionTime", t);
      m_agentReference.addBatchToOwnership(candidateElementsAddresses, m_backend);
      // We can now attempt to switch ownership of elements
      auto failedOwnershipSwitchElements = ContainerTraits<C>::switchElementsOwnership(candidateElements, m_agentReference.getAgentAddress(), cont.getAddressIfSet(), localTimingList, t, lc);
      if (failedOwnershipSwitchElements.empty()) {
        localTimingList.insertAndReset("updateResultProcessingTime", t);
        // This is the easy case (and most common case). Everything went through fine.
        ContainerTraits<C>::removeReferencesAndCommit(cont, candidateElementsAddresses);
        localTimingList.insertAndReset("containerUpdateTime", t);
        contSummaryAfter = ContainerTraits<C>::getContainerSummary(cont);
        // If we emptied the container, we have to trim it.
        ContainerTraits<C>::trimContainerIfNeeded(cont, contLock, contId, lc);
        localTimingList.insertAndReset("containerTrimmingTime", t);
        // trimming might release the lock
        if (contLock.isLocked()) contLock.release();
        localTimingList.insertAndReset("containerUnlockTime", t);
        // All jobs are validated
        ret.summary += candidateElements.summary;
        unfulfilledCriteria -= candidateElements.summary;
        ret.elements.insertBack(std::move(candidateElements.elements));
        localTimingList.insertAndReset("structureProcessingTime", t);
      } else {
        // For the failed files, we have to differentiate the not owned or not existing ones from other error cases.
        // For the not owned, not existing and those successfully switched, we have to de reference them form the container.
        // For other cases, we will leave the elements referenced in the container, as we cannot ensure de-referencing is safe.
        std::set<typename ContainerTraits<C>::ElementAddress> elementsNotToDereferenceFromContainer;
        std::set<typename ContainerTraits<C>::ElementAddress> elementsNotToReport;
        std::list<typename ContainerTraits<C>::ElementAddress> elementsToDereferenceFromAgent;
        for (auto &e: failedOwnershipSwitchElements) {
          try {
            std::rethrow_exception(e.failure);
          } catch (Backend::NoSuchObject &) {
            elementsToDereferenceFromAgent.push_back(ContainerTraits<C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<C>::getElementAddress(*e.element));
          } catch (Backend::WrongPreviousOwner &) {
            elementsToDereferenceFromAgent.push_back(ContainerTraits<C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<C>::getElementAddress(*e.element));
          } catch (Backend::CouldNotUnlock&) {
            // Do nothing, this element was indeed OK.
          }
          catch (...) {
            // This is a different error, so we will leave the reference to the element in the container
            elementsNotToDereferenceFromContainer.insert(ContainerTraits<C>::getElementAddress(*e.element));
            elementsToDereferenceFromAgent.push_back(ContainerTraits<C>::getElementAddress(*e.element));
            elementsNotToReport.insert(ContainerTraits<C>::getElementAddress(*e.element));
            elementsToSkip.insert(ContainerTraits<C>::getElementAddress(*e.element));
            // If we get this kind of situation, we do not try to carry on, as it becomes too complex.
            unexpectedException = true;
          }
        }
        // We are done with the sorting. Apply the decisions...
        std::list<typename ContainerTraits<C>::ElementAddress> elementsToDereferenceFromContainer;
        for (auto & e: candidateElements.elements) {
          if (!elementsNotToDereferenceFromContainer.count(ContainerTraits<C>::getElementAddress(e))) {
            elementsToDereferenceFromContainer.push_back(ContainerTraits<C>::getElementAddress(e));
          }
        }
        localTimingList.insertAndReset("updateResultProcessingTime", t);
        ContainerTraits<C>::removeReferencesAndCommit(cont, elementsToDereferenceFromContainer);
        localTimingList.insertAndReset("containerUpdateTime", t);
        contSummaryAfter = ContainerTraits<C>::getContainerSummary(cont);
        // If we emptied the container, we have to trim it.
        ContainerTraits<C>::trimContainerIfNeeded(cont, contLock, contId, lc);
        localTimingList.insertAndReset("containerTrimmingTime", t);
        // trimming might release the lock
        if (contLock.isLocked()) contLock.release();
        localTimingList.insertAndReset("containerUnlockTime", t);
        m_agentReference.removeBatchFromOwnership(elementsToDereferenceFromAgent, m_backend);
        for (auto & e: candidateElements.elements) {
          if (!elementsNotToReport.count(ContainerTraits<C>::getElementAddress(e))) {
            ret.summary += ContainerTraits<C>::getElementSummary(e);
            unfulfilledCriteria -= ContainerTraits<C>::getElementSummary(e);
            ret.elements.insertBack(std::move(e));
          }
        }
        localTimingList.insertAndReset("structureProcessingTime", t);
      }
      log::ScopedParamContainer params(lc);
      params.add("C", ContainerTraits<C>::c_containerTypeName)
            .add(ContainerTraits<C>::c_identifierType, contId)
            .add("containerAddress", cont.getAddressIfSet());
      ret.summary.addDeltaToLog(previousSummary, params);
      contSummaryAfter.addDeltaToLog(contSummaryBefore, params);
      localTimingList.addToLog(params);
      lc.log(log::INFO, "In ContainerTraits<C>::PoppedElementsBatch(): did one round of elements retrieval.");
      timingList+=localTimingList;
    }
  logAndReturn:;
    {
      log::ScopedParamContainer params(lc);
      params.add("C", ContainerTraits<C>::c_containerTypeName)
            .add(ContainerTraits<C>::c_identifierType, contId);
      ret.addToLog(params);
      timingList.addToLog(params);
      params.add("schedulerDbTime", totalTime.secs());
      params.add("iterationsCounnt", iterationCount);
      lc.log(log::INFO, "In ContainerTraits<C>::PoppedElementsBatch(): elements retrieval complete.");
    }
    return ret;
  }
private:
  Backend & m_backend;
  AgentReference & m_agentReference;
};

}} // namespace cta::objectstore
