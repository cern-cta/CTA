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

#include <string>

namespace cta {
namespace catalogue {

/**
 * The collection of criteria used to select a set of archive files.
 *
 * An archive file is selected if it means all of the specified criteria.
 *
 * A criterion is only considered specified if it has been set.
 *
 * Please note that no wild cards, for example '*' or '%', are supported.
 */
struct ArchiveFileSearchCriteria {

  /**
   * The unique identifier of an archive file.
   */
  std::string archiveFileId;

  /**
   * The name of a disk instance.
   */
  std::string diskInstance;

  /**
   * The unique identifier of a disk file within its disk instance.
   *
   * The combination of diskInstance and diskFileId is unique across all disk
   * instances.
   */
  std::string diskFileId;

  /**
   * The absolute path of a file within its disk instance.
   */
  std::string diskFilePath;

  /**
   * The owner of a file within its disk instance.
   */
  std::string diskFileUser;

  /**
   * The group of a file within its disk instance.
   */
  std::string diskFileGroup;

  /**
   * The storage class name of the file.
   */
  std::string storageClass;

  /**
   * The volume identifier of a tape.
   */
  std::string vid;

  /**
   * The copy number of a tape file.
   */
  std::string tapeFileCopyNb;

  /**
   * The name of a tape pool.
   */
  std::string tapePool;

}; // struct ArchiveFileSearchCriteria

} // namespace catalogue
} // namespace cta
