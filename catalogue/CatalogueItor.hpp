/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
