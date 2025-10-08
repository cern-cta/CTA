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
 * Iteratess across the tape files that make up the contents of a given tape.
 */
class RdbmsCatalogueTapeContentsItor: public ArchiveFileItor::Impl {
public:
  /**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param connPool The database connection pool.
   * @param vid The volume identifier of the tape.
   */
  RdbmsCatalogueTapeContentsItor(
    log::Logger &log,
    rdbms::ConnPool &connPool,
    const std::string &vid);

  /**
   * Destructor.
   */
  ~RdbmsCatalogueTapeContentsItor() override;

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
   * The volume identifier of the tape.
   */
  std::string m_vid;

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

}; // class RdbmsCatalogueTapeContentsItor

} // namespace cta::catalogue
