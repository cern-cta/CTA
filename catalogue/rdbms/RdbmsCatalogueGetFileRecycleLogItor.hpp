/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <string>

#include "catalogue/Catalogue.hpp"
#include "common/dataStructures/FileRecycleLog.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta::catalogue {

class RdbmsCatalogueGetFileRecycleLogItor : public FileRecycleLogItor::Impl {
public:
/**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param conn A database connection.
   */
  RdbmsCatalogueGetFileRecycleLogItor(
    log::Logger &log,
    rdbms::Conn &&conn,
    const RecycleTapeFileSearchCriteria & searchCriteria,
    const std::string &tempDiskFxidsTableName);

  /**
   * Destructor.
   */
  ~RdbmsCatalogueGetFileRecycleLogItor() override;

  /**
   * Returns true if a call to next would return another archive file.
   */
  bool hasMore() override;

  /**
   * Returns the next archive or throws an exception if there isn't one.
   */
  common::dataStructures::FileRecycleLog next() override;

private:
  /**
   * Object representing the API to the CTA logging system.
   */
  log::Logger &m_log;


  /**
   * The database connection.
   */
  rdbms::Conn m_conn;

  /**
   * The search criteria to be used when listing recycled tape files.
   */
  RecycleTapeFileSearchCriteria m_searchCriteria;


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

  common::dataStructures::FileRecycleLog populateFileRecycleLog() const;
};

} // namespace cta::catalogue
