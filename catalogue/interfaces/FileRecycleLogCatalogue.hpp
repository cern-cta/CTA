/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include <string>

#include "catalogue/RecyleTapeFileSearchCriteria.hpp"

namespace cta {

namespace common::dataStructures {
struct FileRecycleLog;
}

namespace log {
struct LogContext;
}

namespace catalogue {

template <typename Item>
class CatalogueItor;

using FileRecycleLogItor = CatalogueItor<common::dataStructures::FileRecycleLog>;

/**
 * Specifies the interface to a factory Catalogue objects.
 */
class FileRecycleLogCatalogue {
public:
  virtual ~FileRecycleLogCatalogue() = default;

  /**
   * Returns all the currently deleted files by looking at the FILE_RECYCLE_LOG table
   *
   * @return The deleted archive files ordered by archive file ID.
   */
  virtual FileRecycleLogItor getFileRecycleLogItor(
    const RecycleTapeFileSearchCriteria & searchCriteria = RecycleTapeFileSearchCriteria()) const = 0;

  /**
   * Restores the deleted file in the Recycle log that match the criteria passed
   *
   * @param searchCriteria The search criteria
   * @param newFid the new Fid of the archive file (if the archive file must be restored)
   */
  virtual void restoreFileInRecycleLog(const RecycleTapeFileSearchCriteria & searchCriteria,
    const std::string &newFid) = 0;

  /**
   * Deletes all the log entries corresponding to the vid passed in parameter.
   *
   * Please note that this method is idempotent.  If there are no recycle log
   * entries associated to the vid passed in parameter, the method will return
   * without any error.
   *
   * @param vid, the vid of the files to be deleted
   * @param lc, the logContext
   */
  virtual void deleteFilesFromRecycleLog(const std::string& vid, log::LogContext& lc) = 0;
};  // class FileRecyleLogCatalogue

}} // namespace cta::catalogue
