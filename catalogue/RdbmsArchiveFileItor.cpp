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

#include "catalogue/RdbmsArchiveFileItor.hpp"

namespace cta {
namespace catalogue {

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
RdbmsArchiveFileItor::RdbmsArchiveFileItor(
  const RdbmsCatalogue &catalogue,
  const uint64_t nbArchiveFilesToPrefetch,
  const TapeFileSearchCriteria &searchCriteria):
  m_catalogue(catalogue),
  m_nbArchiveFilesToPrefetch(nbArchiveFilesToPrefetch),
  m_searchCriteria(searchCriteria),
  m_nextArchiveFileId(1) {
  try {
    if(1 > m_nbArchiveFilesToPrefetch) {
      exception::Exception ex;
      ex.getMessage() << "nbArchiveFilesToPrefetch must equal to or greater than 1: actual=" <<
        m_nbArchiveFilesToPrefetch;
      throw ex;
    }
    m_prefechedArchiveFiles = m_catalogue.getArchiveFilesForItor(m_nextArchiveFileId, m_nbArchiveFilesToPrefetch,
      m_searchCriteria);
    if(!m_prefechedArchiveFiles.empty()) {
      m_nextArchiveFileId = m_prefechedArchiveFiles.back().archiveFileID + 1;
    }
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " +ex.getMessage().str());
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
RdbmsArchiveFileItor::~RdbmsArchiveFileItor() {
}

//------------------------------------------------------------------------------
// hasMore
//------------------------------------------------------------------------------
bool RdbmsArchiveFileItor::hasMore() const {
  return !m_prefechedArchiveFiles.empty();
}

//------------------------------------------------------------------------------
// next
//------------------------------------------------------------------------------
common::dataStructures::ArchiveFile RdbmsArchiveFileItor::next() {
  try {
    if(m_prefechedArchiveFiles.empty()) {
      throw exception::Exception("No more archive files to iterate over");
    }

    common::dataStructures::ArchiveFile archiveFile = m_prefechedArchiveFiles.front();
    m_prefechedArchiveFiles.pop_front();

    if(m_prefechedArchiveFiles.empty()) {
      m_prefechedArchiveFiles = m_catalogue.getArchiveFilesForItor(m_nextArchiveFileId, m_nbArchiveFilesToPrefetch,
        m_searchCriteria);
      if(!m_prefechedArchiveFiles.empty()) {
        m_nextArchiveFileId = m_prefechedArchiveFiles.back().archiveFileID + 1;
      }
    }

    return archiveFile;
  } catch(exception::Exception &ex) {
    throw exception::Exception(std::string(__FUNCTION__) + " failed: " + ex.getMessage().str());
  }
}

} // namespace catalogue
} // namespace cta
