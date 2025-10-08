/*
 * SPDX-FileCopyrightText: 2022 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

#include "catalogue/RecyleTapeFileSearchCriteria.hpp"

namespace cta {

namespace common::dataStructures {
struct FileRecycleLog;
}

namespace log {
class LogContext;
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
