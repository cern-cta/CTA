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

// The header file for atomic was is actually called cstdatomic in gcc 4.4
#if __GNUC__ == 4 && (__GNUC_MINOR__ == 4)
    #include <cstdatomic>
#else
  #include <atomic>
#endif

#include <string>

namespace cta {

/**
 * Mock CTA catalogue to facilitate unit testing.
 */
class MockCatalogue: public Catalogue {
public:

  /**
   * Constructor.
   */
  MockCatalogue();

  /**
   * Destructor.
   */
  virtual ~MockCatalogue();

  /**
   * Returns the next identifier to be used for a new archive file.
   *
   * @return The next identifier to be used for a new archive file.
   */
  virtual uint64_t getNextArchiveFileId();

  /**
   * Notifies the catalogue that a file has been written to tape.
   *
   * @param archiveRequest The identifier of the archive file.
   *
   */
  virtual void fileWrittenToTape(
    const ArchiveRequest &archiveRequest,
    const std::string vid,
    const uint64_t fSeq,
    const uint64_t blockId);

private:

  /**
   * The next identifier to be used for a new archive file.
   */
  std::atomic<uint64_t> m_nextArchiveFileId;

}; // class MockCatalogue

} // namespace cta
