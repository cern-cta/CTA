/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/ArchiveFileBuilder.hpp"
#include "catalogue/Catalogue.hpp"
#include "catalogue/CatalogueItor.hpp"
#include "catalogue/TapeSearchCriteria.hpp"
#include "common/dataStructures/Tape.hpp"
#include "common/log/Logger.hpp"
#include "rdbms/ConnPool.hpp"
#include "rdbms/Rset.hpp"
#include "rdbms/Stmt.hpp"

namespace cta::catalogue {

/**
 * RdbmsCatalogue::getArchiveFiles() implementation of ArchiveFileItorImpl.
 */
class RdbmsCatalogueGetTapesItor : public catalogue::TapeItor::Impl {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param conn The database connection.
   * @param searchCriteria The search criteria to be used when listing archive
   * files.
   */
  RdbmsCatalogueGetTapesItor(log::Logger& log, rdbms::Conn&& conn, const TapeSearchCriteria& searchCriteria);

  /**
   * Destructor.
   */
  ~RdbmsCatalogueGetTapesItor() override;

  /**
   * Returns true if a call to next would return another archive file.
   */
  bool hasMore() override;

  /**
   * Returns the next archive or throws an exception if there isn't one.
   */
  common::dataStructures::Tape next() override;

private:
  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger& m_log;

  /**
   * The search criteria to be used when listing archive files.
   */
  TapeSearchCriteria m_searchCriteria;

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
   * When selecting by disk file ID we may be forced to split the query into several parts
   */
  const bool m_splitByDiskFileId;
  static constexpr std::size_t MAX_DISK_FILE_ID_IN_QUERY = 100;
  std::size_t m_diskFileIdIdx = 0;

  /**
   * As we may be splitting the query in several parts, we want to ensure that each VID is only returned once
   */
  std::set<std::string, std::less<>> m_vidsInList;

  /**
   * Releases the database resources.
   *
   * This method is idempotent.
   */
  void releaseDbResources() noexcept;

  /**
   * Finds the next rset with a VID that has not been seen yet
   *
   * @return true if the rset was found or false if not
   */
  bool nextValidRset();

};  // class RdbmsCatalogueGetTapesItor

}  // namespace cta::catalogue
