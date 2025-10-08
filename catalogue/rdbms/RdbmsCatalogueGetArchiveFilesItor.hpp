/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/ArchiveFileBuilder.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/TapeFileSearchCriteria.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Stmt.hpp"
#include "rdbms/Rset.hpp"

namespace cta::catalogue {

/**
 * RdbmsCatalogue::getArchiveFiles() implementation of ArchiveFileItorImpl.
 */
class RdbmsCatalogueGetArchiveFilesItor: public catalogue::ArchiveFileItor::Impl {
public:

  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param conn The database connection.
   * @param searchCriteria The search criteria to be used when listing archive
   * files.
   */
  RdbmsCatalogueGetArchiveFilesItor(
    log::Logger &log,
    rdbms::Conn &&conn,
    const TapeFileSearchCriteria &searchCriteria,
    const std::string &tempDiskFxidsTableName);

  /**
   * Destructor.
   */
  ~RdbmsCatalogueGetArchiveFilesItor() override;

  /**
   * Returns true if a call to next would return another archive file.
   */
  bool hasMore() override;

  /**
   * Returns the next archive or throws an exception if there isn't one.
   */
  common::dataStructures::ArchiveFile next() override;

private:

  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger &m_log;

  /**
   * The search criteria to be used when listing archive files.
   */
  TapeFileSearchCriteria m_searchCriteria;

  /**
   * True if the result set is empty.
   */
  bool m_rsetIsEmpty = true;

  /**
   * True if hasMore() has been called and the corresponding call to next() has
   * not.
   *
   * This member-variable is used to prevent next() being called before
   * hasMore().
   */
  bool m_hasMoreHasBeenCalled = false;

  /**
   * The database connection.
   */
  rdbms::Conn m_conn;

  /**
   * The database statement.
   */
  rdbms::Stmt m_stmt;

  /**
   * The result set of archive files that is to be iterated over.
   */
  rdbms::Rset m_rset;

  /**
   * Releases the database resources.
   *
   * This method is idempotent.
   */
  void releaseDbResources() noexcept;

  /**
   * Builds ArchiveFile objects from a stream of tape files ordered by archive
   * ID and then copy number.
   */
  ArchiveFileBuilder<cta::common::dataStructures::ArchiveFile> m_archiveFileBuilder;
}; // class RdbmsCatalogueGetArchiveFilesItor

} // namespace cta::catalogue
