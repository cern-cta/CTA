/*!
 * @project        The CERN Tape Archive (CTA)
 * @brief          Access Oracle DB for migration operations
 * @copyright      Copyright 2019 CERN
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

#include <common/make_unique.hpp>
#include <rdbms/Login.hpp>
#include <rdbms/ConnPool.hpp>


namespace cta {
namespace migration {

/*!
 * Manage a single database query over a single database connection
 */
class OracleDbConn
{
public:
  OracleDbConn() : m_queryIsEmpty(true) {}

  void connect(const std::string &dbconn = "", unsigned int max_num_conns = 1) {
    // Initialise the connection pool
    if(!dbconn.empty()) {
      m_connPool.reset(new rdbms::ConnPool(rdbms::Login::parseString(dbconn), max_num_conns));
    }

    // Initialise this connection
    m_conn = m_connPool->getConn();
  }

  void execute(const std::string &sqlString) {
    auto sql = m_conn.createStmt(sqlString);
    sql.executeNonQuery();
  }

  void query(const std::string &sqlString) {
    auto sql = m_conn.createStmt(sqlString);
    m_rSet = cta::make_unique<rdbms::Rset>(sql.executeQuery());
    m_queryIsEmpty = !m_rSet->next();
  }

  void reset() {
    m_rSet.reset();
    m_queryIsEmpty = true;
  }

  bool nextRow() {
    if(m_rSet->next()) return true;
    reset();
    return false;
  }

  std::string getResultColumnString(const std::string &col) const {
    return m_rSet->columnString(col);
  }

  uint64_t getResultColumnUint64(const std::string &col) const {
    return m_rSet->columnUint64(col);
  }

  std::string getResultColumnBlob(const std::string &col) const {
    return m_rSet->columnBlob(col);
  }

  bool isQueryEmpty() const {
    return m_queryIsEmpty;
  }

private:
  static std::unique_ptr<rdbms::ConnPool> m_connPool;    //!< The pool of connections to the database
  rdbms::Conn m_conn;                                    //!< The connection we are using
  std::unique_ptr<rdbms::Rset> m_rSet;                   //!< Result set for the last query executed
  bool m_queryIsEmpty;                                   //!< Track whether the last query had an empty result set
};

}} // namespace cta::migration
