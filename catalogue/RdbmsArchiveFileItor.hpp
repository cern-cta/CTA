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

#include "catalogue/ArchiveFileItor.hpp"
#include "catalogue/RdbmsCatalogue.hpp"

namespace cta {
namespace catalogue {

/**
 * Rdbms implementation of ArchiveFileItor.
 */
class RdbmsArchiveFileItor: public ArchiveFileItor {
public:

  /**
   * Constructor.
   *
   * @param catalogue The RdbmsCatalogue.
   * @param nbArchiveFilesToPrefetch The number of archive files to prefetch.
   * @param searchCriteria The search criteria.
   */
  RdbmsArchiveFileItor(
    const RdbmsCatalogue &catalogue,
    const uint64_t nbArchiveFilesToPrefetch,
    const TapeFileSearchCriteria &searchCriteria);

  /**
   * Destructor.
   */
  ~RdbmsArchiveFileItor() override;

  /**
   * Returns true if a call to next would return another archive file.
   */
  bool hasMore() const override;

  /**
   * Returns the next archive or throws an exception if there isn't one.
   */
  common::dataStructures::ArchiveFile next() override;

private:

  /**
   * The RdbmsCatalogue.
   */
  const RdbmsCatalogue &m_catalogue;

  /**
   * The number of archive files to prefetch.
   */
  const uint64_t m_nbArchiveFilesToPrefetch;

  /**
   * The search criteria.
   */
  TapeFileSearchCriteria m_searchCriteria;

  /**
   * The current offset into the list of archive files in the form of an
   * archive file ID.
   */
  uint64_t m_nextArchiveFileId;

  /**
   * The current list of prefetched archive files.
   */
  std::list<common::dataStructures::ArchiveFile> m_prefechedArchiveFiles;
}; // class RdbmsArchiveFileItor

} // namespace catalogue
} // namespace cta
