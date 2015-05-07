/**
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

#include "cta/DirEntry.hpp"

#include <list>

namespace cta {

/**
 * Instances of ths class should be used in the following way to iterate
 * through the contents of a directory.
 *
 * \code
 * DirIterator &itor = ...
 *
 * while(itor.hasMore())) {
 *   const DirEntry &entry = itor.next();
 *
 *   // Do something with entry
 * }
 * \endcode
 */
class DirIterator {
public:

  /**
   * Constructor.
   */
  DirIterator();

  /**
   * Constructor.
   *
   * @param entries The directory entries.
   */
  DirIterator(const std::list<DirEntry> &entries);

  /**
   * Destructor.
   */
  ~DirIterator() throw();

  /**
   * Returns true if there are more directory entries.
   *
   * @return True if there are more directory entries.
   */
  bool hasMore();

  /**
   * Returns a reference to the next directory entry or throws an exception if
   * out of bounds.
   *
   * @return The next directory entry.
   */
  const DirEntry next();

private:

  /**
   * The directory entries.
   */
  std::list<DirEntry> m_entries;

}; // DirIterator

} // namespace cta
