/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/ArchiveFileBuilder.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "common/dataStructures/ArchiveFile.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Rset.hpp"
#include "rdbms/Stmt.hpp"

namespace cta::catalogue {

/**
 * RdbmsCatalogue::getArchiveFilesForRepack() implementation of
 * ArchiveFileItorImpl.
 */
class RdbmsCatalogueGetArchiveFilesForRepackItor : public catalogue::ArchiveFileItor::Impl {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param connPool The database connection pool.
   * @param vid The volume identifier of the tape.
   * @param startFSeq The file sequence number of the first file.  Please note
   * that there might not be a file with this exact file sequence number.
   * @return The specified files in tape file sequence order.
   */
  RdbmsCatalogueGetArchiveFilesForRepackItor(log::Logger& log,
                                             rdbms::ConnPool& connPool,
                                             const std::string& vid,
                                             const uint64_t startFSeq);

  /**
   * Destructor.
   */
  ~RdbmsCatalogueGetArchiveFilesForRepackItor() override;

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
  log::Logger& m_log;

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
};  // class RdbmsCatalogueGetArchiveFilesForRepackItor

}  // namespace cta::catalogue
