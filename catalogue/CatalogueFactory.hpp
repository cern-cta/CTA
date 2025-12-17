/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <memory>

namespace cta::catalogue {

class Catalogue;

/**
 * Specifies the interface to a factory for Catalogue objects
 */
class CatalogueFactory {
public:
  /**
   * Destructor
   */
  virtual ~CatalogueFactory() = default;

  /**
   * Returns a newly created CTA catalogue object
   */
  virtual std::unique_ptr<Catalogue> create() = 0;
};

}  // namespace cta::catalogue
