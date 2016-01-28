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

#include "common/archiveNS/ArchiveDirIterator.hpp"
#include "common/exception/Exception.hpp"
using cta::exception::Exception;

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::archiveNS::ArchiveDirIterator::ArchiveDirIterator() {
}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::common::archiveNS::ArchiveDirIterator::ArchiveDirIterator(
  const std::list<common::archiveNS::ArchiveDirEntry> &entries): m_entries(entries) {
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::common::archiveNS::ArchiveDirIterator::~ArchiveDirIterator() throw() {
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool cta::common::archiveNS::ArchiveDirIterator::hasMore() {
  return !m_entries.empty();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
const cta::common::archiveNS::ArchiveDirEntry cta::common::archiveNS::ArchiveDirIterator::next() {
  if(m_entries.empty()) {
    throw Exception("Out of bounds: There are no more directory entries");
  }

  common::archiveNS::ArchiveDirEntry entry = m_entries.front();
  m_entries.pop_front();
  return entry;
}
