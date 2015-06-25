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

#include "common/ArchiveDirEntry.hpp"

#include <list>

namespace cta {

/**
 * Instances of ths class should be used in the following way to iterate
 * through the contents of a directory.
 *
 * \code
 * ArchiveDirIterator &itor = ...
 *
 * while(itor.hasMore())) {
 *   const ArchiveDirEntry &entry = itor.next();
 *
 *   // Do something with entry
 * }
 * \endcode
 */
class ArchiveDirIterator {
public:

  /**
   * Constructor.
   */
  ArchiveDirIterator();

  /**
   * Constructor.
   *
   * @param entries The directory entries.
   */
  ArchiveDirIterator(const std::list<ArchiveDirEntry> &entries);

  /**
   * Destructor.
   */
  ~ArchiveDirIterator() throw();

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
  const ArchiveDirEntry next();

private:

  /**
   * The directory entries.
   */
  std::list<ArchiveDirEntry> m_entries;

}; // ArchiveDirIterator

} // namespace cta
