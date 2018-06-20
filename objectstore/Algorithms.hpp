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

  static ElementAddress getElementAddress(const Element & e);
  static void getLockedAndFetched(Container & cont, ScopedExclusiveLock & contLock, AgentReference & agRef, const ContainerIdentifyer & cId,
    log::LogContext & lc);
  static void addReferencesAndCommit(Container & cont, ElementMemoryContainer & elemMemCont);
  static ElementPointerContainer switchElementsOwnership(Container & cont, ElementMemoryContainer & elements, log::LogContext & lc);
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
    auto failedOwnershipSwitchElements = ContainerTraits<C>::switchElementsOwnership(elements, cont, m_agentReference.getAgentAddress(), lc);
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
private:
  Backend & m_backend;
  AgentReference & m_agentReference;
};

}} // namespace cta::objectstore
