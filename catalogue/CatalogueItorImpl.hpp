/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

namespace cta::catalogue {

/**
 * Abstract class defining the interface to an iterator over a list of items
 * retrieved from the CTA catalogue.
 */
template<typename Item>
class CatalogueItorImpl {
public:
  /**
   * Destructor.
   */
  virtual ~CatalogueItorImpl() = default;

  /**
   * Returns true if a call to next would return another archive file.
   */
  virtual bool hasMore() = 0;

  /**
   * Returns the next item or throws an exception if there isn't one.
   */
  virtual Item next() = 0;

};  // class CatalogueItorImpl

}  // namespace cta::catalogue
