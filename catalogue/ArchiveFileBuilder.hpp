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

#include "common/dataStructures/ArchiveFile.hpp"

#include <memory>

namespace cta {
namespace catalogue {

/**
 * Builds ArchiveFile objects from a stream of tape files ordered by archive ID
 * and then copy number.
 */
class ArchiveFileBuilder {
public:

  /**
   * Appends the specified tape file to the ArchiveFile object currently
   * construction.
   *
   * If this append method is called with the tape file of the next ArchiveFile
   * to be constructed then this means the current ArchiveFile under
   * construction is complete and this method will therefore return the current
   * and complete ArchiveFile object.  The appened tape file will be remembered
   * by this builder object and used to start the construction of the next
   * ArchiveFile object.
   *
   * If this append method is called with an ArchiveFile with no tape files at
   * all then this means the current ArchiveFile under
   * construction is complete and this method will therefore return the current
   * and complete ArchiveFile object.  The appened tape file will be remembered
   * by this builder object and used to start the construction of the next
   * ArchiveFile object.
   *
   * If the call to this append does not complete the ArchiveFile object
   * currently under construction then this method will returns an empty unique
   * pointer.
   *
   * @param tapeFile The tape file to be appended or an archive file with no
   * tape files at all.
   */
  std::unique_ptr<common::dataStructures::ArchiveFile> append(const common::dataStructures::ArchiveFile &tapeFile);

  /**
   * Returns a pointer to the ArchiveFile object currently under construction.
   * A return value of nullptr means there there is no ArchiveFile object
   * currently under construction.
   *
   * @return The ArchiveFile object currently under construction or nullptr
   * if there isn't one.
   */
  common::dataStructures::ArchiveFile *getArchiveFile();

  /**
   * If there is an ArchiveFile under construction then it is forgotten.
   */
  void clear();

private:

  /**
   * The Archivefile object currently under construction.
   */
  std::unique_ptr<common::dataStructures::ArchiveFile> m_archiveFile;

}; // class ArchiveFileBuilder

} // namespace catalogue
} // namespace cta
