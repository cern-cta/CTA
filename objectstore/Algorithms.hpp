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

/**
 * This file defines the containers, their traits and algorithms to add/remove
 * to/from them.
 */

#include <string>
#include "ArchiveRequest.hpp"
#include "Helpers.hpp"
#include "common/log/LogContext.hpp"
#include "common/exception/Exception.hpp"
#include "common/log/TimingList.hpp"

namespace cta { namespace objectstore {

/**
 * Container traits definition. To be specialized class by class.
 * This is mostly a model.
 */
template <class C> 
class ContainerTraits{
public:
  typedef C                                   Container;
  typedef std::string                         ContainerAddress;
  typedef std::string                         ElementAddress;
  typedef std::string                         ContainerIdentifyer;
  static const std::string                    c_containerTypeName; //= "genericContainer";
  static const std::string                    c_identifyerType; // = "genericId";
  struct InsertedElement {
    typedef std::list<InsertedElement> list;
  };
  typedef std::list<std::unique_ptr<InsertedElement>> ElementMemoryContainer;
  typedef std::list <InsertedElement *>       ElementPointerContainer;
  class                                       ElementDescriptor {};
  typedef std::list<ElementDescriptor>        ElementDescriptorContainer;
  
  template <class Element>
  struct OpFailure {
    Element * element;
    std::exception_ptr failure;
    typedef std::list<OpFailure> list;
  };
  
  class PoppedElementsSummary;
  class PopCriteria {
  public:
    PopCriteria();
    PopCriteria& operator-= (const PoppedElementsSummary &);
  };
  class PoppedElementsList {
  public:
    PoppedElementsList();
    void insertBack(PoppedElementsList &&);
  };
  class PoppedElementsSummary {
  public:
    PoppedElementsSummary();
    bool operator< (const PopCriteria &);
    PoppedElementsSummary& operator+= (const PoppedElementsSummary &);
  };
  class PoppedElementsBatch {
  public:
    PoppedElementsBatch();
    PoppedElementsList elements;
    PoppedElementsSummary summary;
    void addToLog(log::ScopedParamContainer &);
  };
  typedef std::set<ElementAddress> ElementsToSkipSet;
  
  static void trimContainerIfNeeded(Container & cont);
  
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);
  
  template <class Element>
  static ElementAddress getElementAddress(const Element & e);
  
  static void getLockedAndFetched(Container & cont, ScopedExclusiveLock & contLock, AgentReference & agRef, const ContainerIdentifyer & cId,
    log::LogContext & lc);
  static void getLockedAndFetchedNoCreate(Container & cont, ScopedExclusiveLock & contLock, const ContainerIdentifyer & cId,
    log::LogContext & lc);
  static void addReferencesAndCommit(Container & cont, ElementMemoryContainer & elemMemCont);
  void removeReferencesAndCommit(Container & cont, typename OpFailure<InsertedElement>::list & elementsOpFailures);
  void removeReferencesAndCommit(Container & cont, std::list<ElementAddress>& elementAddressList);
  static ElementPointerContainer switchElementsOwnership(ElementMemoryContainer & elemMemCont, const ContainerAddress & contAddress,
      const ContainerAddress & previousOwnerAddress, log::LogContext & lc);
  template <class Element>
  static PoppedElementsSummary getElementSummary(const Element &);
  static PoppedElementsBatch getPoppingElementsCandidates(Container & cont, PopCriteria & unfulfilledCriteria,
      ElementsToSkipSet & elemtsToSkip, log::LogContext & lc);
};

template <class C>
class ContainerAlgorithms {
public:
  ContainerAlgorithms(Backend & backend, AgentReference & agentReference):
    m_backend(backend), m_agentReference(agentReference) {}
    
  typedef typename ContainerTraits<C>::InsertedElement InsertedElement;
    
  /** Reference objects in the container and then switch their ownership them. Objects 
   * are provided existing and owned by algorithm's agent. Returns a list of 
   * @returns list of elements for which the addition or ownership switch failed.
   * @throws */
  void referenceAndSwitchOwnership(const typename ContainerTraits<C>::ContainerIdentifyer & contId,
      typename ContainerTraits<C>::InsertedElement::list & elements, log::LogContext & lc) {
    C cont(m_backend);
    ScopedExclusiveLock contLock;
    ContainerTraits<C>::getLockedAndFetched(cont, contLock, m_agentReference, contId, lc);
    ContainerTraits<C>::addReferencesAndCommit(cont, elements, m_agentReference, lc);
    auto failedOwnershipSwitchElements = ContainerTraits<C>::switchElementsOwnership(elements, cont.getAddressIfSet(),
        m_agentReference.getAgentAddress(), lc);
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
  
  typename ContainerTraits<C>::PoppedElementsBatch popNextBatch(const typename ContainerTraits<C>::ContainerIdentifyer & contId,
      typename ContainerTraits<C>::PopCriteria & popCriteria, log::LogContext & lc) {
    // Prepare the return value
    typename ContainerTraits<C>::PoppedElementsBatch ret;
    typename ContainerTraits<C>::PopCriteria unfulfilledCriteria = popCriteria;
    size_t iterationCount=0;
    typename ContainerTraits<C>::ElementsToSkipSet elementsToSkip;
    log::TimingList timingList;
    utils::Timer t;
    while (ret.summary < popCriteria) {
      log::TimingList localTimingList;
      // Get a container if it exists
      C cont(m_backend);
      iterationCount++;
      ScopedExclusiveLock contLock;
      try {
        ContainerTraits<C>::getLockedAndFetchedNoCreate(cont, contLock, contId, lc);
      } catch (typename ContainerTraits<C>::NoSuchContainer &) {
        localTimingList.insertAndReset("findQueueTime", t);
        timingList+=localTimingList;
        // We could not find a container to pop from: return what we have.
        goto logAndReturn;
      }
      localTimingList.insertAndReset("findQueueTime", t);
      // We have a container. Get candidate element list from it.
      typename ContainerTraits<C>::PoppedElementsBatch candidateElements = 
          ContainerTraits<C>::getPoppingElementsCandidates(cont, unfulfilledCriteria, elementsToSkip, lc);
      
      // Reference the candidates to our agent
      std::list<typename ContainerTraits<C>::ElementAddress> candidateElementsAddresses;
      for (auto & e: candidateElements.elements) {
        candidateElementsAddresses.emplace_back(ContainerTraits<C>::getElementAddress(e));
      }
      m_agentReference.addBatchToOwnership(candidateElementsAddresses, m_backend);
      // We can now attempt to switch ownership of elements
      auto failedOwnershipSwitchElements = ContainerTraits<C>::switchElementsOwnership(candidateElements,
          m_agentReference.getAgentAddress(), cont.getAddressIfSet(), lc);
      if (failedOwnershipSwitchElements.empty()) {
        // This is the easy case (and most common case). Everything went through fine.
        ContainerTraits<C>::removeReferencesAndCommit(cont, candidateElementsAddresses);
        // If we emptied the container, we have to trim it.
        ContainerTraits<C>::trimContainerIfNeeded(cont, contLock, contId, lc);
        // trimming might release the lock
        if (contLock.isLocked()) contLock.release();
        // All jobs are validated
        ret.summary += candidateElements.summary;
        unfulfilledCriteria -= candidateElements.summary;
        ret.elements.insertBack(std::move(candidateElements.elements));
      } else {
        // For the failed files, we have to differentiate the not owned or not existing ones from other error cases.
        // For the not owned, not existing and those successfully switched, we have to de reference them form the container.
        // For other cases, we will leave the elements referenced in the container, as we cannot ensure de-referencing is safe.
        std::set<typename ContainerTraits<C>::ElementAddress> elementsNotToDereferenceFromContainer;
        std::set<typename ContainerTraits<C>::ElementAddress> elementsNotToReport;
        std::list<typename ContainerTraits<C>::ElementAddress> elementsToDereferenceFromAgent;
        for (auto &e: failedOwnershipSwitchElements) {
          try {
            throw e.failure;
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
          }
        }
        // We are done with the sorting. Apply the decisions...
        std::list<typename ContainerTraits<C>::ElementAddress> elementsToDereferenceFromContainer;
        for (auto & e: candidateElements.elements) {
          if (!elementsNotToDereferenceFromContainer.count(ContainerTraits<C>::getElementAddress(e))) {
            elementsToDereferenceFromContainer.push_back(ContainerTraits<C>::getElementAddress(e));
          }
        }
        ContainerTraits<C>::removeReferencesAndCommit(cont, elementsToDereferenceFromContainer);
        // If we emptied the container, we have to trim it.
        ContainerTraits<C>::trimContainerIfNeeded(cont, contLock, contId, lc);
        // trimming might release the lock
        if (contLock.isLocked()) contLock.release();
        m_agentReference.removeBatchFromOwnership(elementsToDereferenceFromAgent, m_backend);
        for (auto & e: candidateElements.elements) {
          if (!elementsNotToReport.count(ContainerTraits<C>::getElementAddress(e))) {
            ret.summary += ContainerTraits<C>::getElementSummary(e);
            unfulfilledCriteria -= ContainerTraits<C>::getElementSummary(e);
            ret.elements.insertBack(std::move(e));
          }
        }
      }
    }
  logAndReturn:;
    {
      log::ScopedParamContainer params(lc);
      params.add("C", ContainerTraits<C>::c_containerTypeName);
      params.add(ContainerTraits<C>::c_identifyerType, contId);
      ret.addToLog(params);
      timingList.addToLog(params);
      lc.log(log::INFO, "In ContainerTraits<C>::PoppedElementsBatch(): elements retrieval complete.");
    } 
    return ret;
  }
private:
  Backend & m_backend;
  AgentReference & m_agentReference;
};

}} // namespace cta::objectstore
