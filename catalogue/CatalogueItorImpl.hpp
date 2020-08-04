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

namespace cta {
namespace catalogue {

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

}; // class CatalogueItorImpl

} // namespace catalogue
} // namespace cta
