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

#include "catalogue/Catalogue.hpp"

#include <memory>

namespace cta {
namespace catalogue {

/**
 * Specifies the interface to a factory Catalogue objects.
 */
class CatalogueFactory {
public:

  /**
   * Destructor.
   */
  virtual ~CatalogueFactory();

  /**
   * Returns a newly created CTA catalogue object.
   */
  virtual std::unique_ptr<Catalogue> create() = 0;

}; // class CatalogueFactory

} // namespace catalogue
} // namespace cta
