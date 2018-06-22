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
  class                                       Element {};
  typedef std::list<std::unique_ptr<Element>> ElementMemoryContainer;
  typedef std::list <Element *>               ElementPointerContainer;
  class                                       ElementDescriptor {};
  typedef std::list<ElementDescriptor>        ElementDescriptorContainer;
  struct ElementOpFailure {
    Element * element;
    std::exception_ptr failure;
  };
  typedef std::list<ElementOpFailure>         ElementOpFailureContainer;
  
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
  };
  
  CTA_GENERATE_EXCEPTION_CLASS(NoSuchContainer);

  static ElementAddress getElementAddress(const Element & e);
  static void getLockedAndFetched(Container & cont, ScopedExclusiveLock & contLock, AgentReference & agRef, const ContainerIdentifyer & cId,
    log::LogContext & lc);
  static void getLockedAndFetchedNoCreate(Container & cont, ScopedExclusiveLock & contLock, const ContainerIdentifyer & cId,
    log::LogContext & lc);
  static void addReferencesAndCommit(Container & cont, ElementMemoryContainer & elemMemCont);
  void removeReferencesAndCommit(Container & cont, ElementOpFailureContainer & elementsOpFailures);
  void removeReferencesAndCommit(Container & cont, std::list<ElementAddress>& elementAddressList);
  static ElementPointerContainer switchElementsOwnership(ElementMemoryContainer & elemMemCont, const ContainerAddress & contAddress,
      const ContainerAddress & previousOwnerAddress, log::LogContext & lc);
};

template <class C>
class ContainerAlgorithms {
public:
  ContainerAlgorithms(Backend & backend, AgentReference & agentReference):
    m_backend(backend), m_agentReference(agentReference) {}
    
  typedef typename ContainerTraits<C>::Element Element;
  typedef typename ContainerTraits<C>::ElementMemoryContainer ElementMemoryContainer;
    
  /** Reference objects in the container and then switch their ownership them. Objects 
   * are provided existing and owned by algorithm's agent. Returns a list of 
   * @returns list of elements for which the addition or ownership switch failed.
   * @throws */
  void referenceAndSwitchOwnership(const typename ContainerTraits<C>::ContainerIdentifyer & contId,
      typename ContainerTraits<C>::ElementMemoryContainer & elements, log::LogContext & lc) {
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
  
  typename ContainerTraits<C>::PoppedElementsBatch popNextBatch(typename ContainerTraits<C>::ContainerIdentifyer & contId,
      typename ContainerTraits<C>::PopCriteria & popCriteria, log::LogContext & lc) {
    // Prepare the return value
    typename ContainerTraits<C>::PoppedElementsBatch ret;
    typename ContainerTraits<C>::PopCriteria unfulfilledCriteria = popCriteria;
    size_t iterationCount=0;
    while (ret.summary < popCriteria) {
      // Get a container if it exists
      C cont(m_backend);
      iterationCount++;
      try {
        typename ContainerTraits<C>::getLockedAndFetchedNoCreate(cont, contId, lc);
      } catch (typename ContainerTraits<C>::NoSuchContainer &) {
        // We could not find a container to pop from: return what we have.
        return ret;
      }
      // We have a container. Get candidate element list from it.
      typename ContainerTraits<C>::PoppingElementsCandidateList candidateElements = 
          ContainerTraits<C>::getPoppingElementsCandidates(cont, unfulfilledCriteria, lc);
      // Reference the candidates to our agent
      std::list<typename ContainerTraits<C>::ElementAddress> candidateElementsAddresses;
      for (auto & e: candidateElements) {
        candidateElementsAddresses.emplace_back(ContainerTraits<C>::getElementAddress(e));
      }
      m_agentReference.addBatchToOwnership(candidateElementsAddresses, m_backend);
      // We can now attempt to switch ownership of elements
      auto failedOwnershipSwitchElements = ContainerTraits<C>::switchElementsOwnership(candidateElements,
          m_agentReference.getAgentAddress(), cont.getAddressIfSet());
      if (failedOwnershipSwitchElements.empty()) {
        // This is the easy (and most common case). Everything went through fine.
        ContainerTraits<C>::removeReferencesAndCommit(candidateElementsAddresses);
      } else {
        // For the failed files, we have to differentiate the not owned or not existing ones from other error cases.
        // For the not owned, not existing and those successfully switched, we have to de reference them form the container.
        // For other cases, we will leave the elements referenced in the container, as we cannot ensure de-referencing is safe.
        std::set<typename ContainerTraits<C>::ElementAddress> elementsNotToDereferenceFromContainer;
        for (auto &e: failedOwnershipSwitchElements) {
          try {
            throw e.failure;
          } catch (Backend::NoSuchObject &) {}
          catch (Backend::WrongPreviousOwner&) {}
          catch (...) {
            // This is a different error, so we will leave the reference to the element in the container
            elementsNotToDereferenceFromContainer.insert(ContainerTraits<C>::getElementAddress(*e.element));
          }
        }   
      }
    }
    return ret;
  }
private:
  Backend & m_backend;
  AgentReference & m_agentReference;
};

}} // namespace cta::objectstore
