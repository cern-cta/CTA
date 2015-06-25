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

#include "common/DirIterator.hpp"
#include "common/exception/Exception.hpp"
using cta::exception::Exception;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirIterator::DirIterator() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::DirIterator::DirIterator(
  const std::list<ArchiveDirEntry> &entries): m_entries(entries) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::DirIterator::~DirIterator() throw() {
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool cta::DirIterator::hasMore() {
  return !m_entries.empty();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
const cta::ArchiveDirEntry cta::DirIterator::next() {
  if(m_entries.empty()) {
    throw Exception("Out of bounds: There are no more directory entries");
  }

  ArchiveDirEntry entry = m_entries.front();
  m_entries.pop_front();
  return entry;
}
