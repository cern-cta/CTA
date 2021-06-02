/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "Catalogue.hpp"
#include "rdbms/ConnPool.hpp"

namespace cta {
namespace catalogue {

class RdbmsCatalogueGetFileRecycleLogItor : public Catalogue::FileRecycleLogItor::Impl {
public:
/**
   * Constructor.
   *
   * @param log Object representing the API to the CTA logging system.
   * @param connPool The database connection pool.
   */
  RdbmsCatalogueGetFileRecycleLogItor(log::Logger &log, rdbms::ConnPool &connPool, const RecycleTapeFileSearchCriteria & searchCriteria);

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
   * The database connection pool.
   */
  rdbms::ConnPool &m_connPool;
  
  /**
   * The search criteria to be used when listing recycled tape files.
   */
  RecycleTapeFileSearchCriteria m_searchCriteria;


  /**
   * True if the result set is empty.
   */
  bool m_rsetIsEmpty;

  /**
   * True if hasMore() has been called and the corresponding call to next() has
   * not.
   *
   * This member-variable is used to prevent next() being called before
   * hasMore().
   */
  bool m_hasMoreHasBeenCalled;

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
  
  common::dataStructures::FileRecycleLog populateFileRecycleLog();

};

}}