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

#include <memory>

#include "catalogue/CatalogueItorImpl.hpp"
#include "common/exception/Exception.hpp"

namespace cta::catalogue {

/**
 * A wrapper around an object that iterators over a list of items retrieved
 * from the CTA catalogue.
 *
 * This wrapper permits the user of the Catalogue API to use different
 * iterator implementations whilst only using a single iterator type.
 */
template <typename Item>
class CatalogueItor {
public:

  using Impl = CatalogueItorImpl<Item>;

  /**
   * Constructor.
   */
  CatalogueItor() = default;

  /**
   * Constructor.
   *
   * @param impl The object actually implementing this iterator.
   */
  explicit CatalogueItor(Impl *const impl) : m_impl(impl) {
    if (nullptr == impl) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: Pointer to implementation object is null");
    }
  }

  /**
   * Destructor.
   */
  ~CatalogueItor() = default;

  // copy constructor
  CatalogueItor(const CatalogueItor &rhs) = delete;

  // move constructor
  CatalogueItor(CatalogueItor &&rhs) noexcept : m_impl(std::move(rhs.m_impl)) {}

  // move assignment
  CatalogueItor &operator=(CatalogueItor &&rhs) noexcept {
    if (this != &rhs) {
      m_impl = std::move(rhs.m_impl);
    }
    return *this;
  }

  /**
   * Returns true if a call to next would return another item.
   */
  bool hasMore() const {
    if(nullptr == m_impl) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: This iterator is invalid");
    }
    return m_impl->hasMore();
  }

  /**
   * Returns the next item or throws an exception if there isn't one.
   */
  Item next() {
    if(nullptr == m_impl) {
      throw exception::Exception(std::string(__FUNCTION__) + " failed: This iterator is invalid");
    }
    return m_impl->next();
  }

private:

  /**
   * The object actually implementing this iterator.
   */
  std::unique_ptr<Impl> m_impl;

}; // class CatalogueItor

} // namespace cta::catalogue
