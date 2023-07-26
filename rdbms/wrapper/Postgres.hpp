/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
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

#include "common/exception/Exception.hpp"
#include "common/exception/LostDatabaseConnection.hpp"
#include "common/utils/utils.hpp"
#include "rdbms/CheckConstraintError.hpp"
#include "rdbms/IntegrityConstraintError.hpp"
#include "rdbms/UniqueConstraintError.hpp"

#include <algorithm>
#include <cstring>
#include <iostream>
#include <libpq-fe.h>
#include <string>
#include <regex>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A convenience class containing some utilities used by the postgres rdbms wrapper classes.
 */
class Postgres {
public:

  /**
   * A method to throw a DB related error with an informative string obtained from the connection and/or
   * request status. The exception will be a LostDatabaseConnection() if the postgres connection structure
   * indicates that the connection is no longer valid.
   */
  [[noreturn]] static void ThrowInfo(const PGconn *conn, const PGresult *res, const std::string &prefix) {
    const char *const pgcstr = PQerrorMessage(conn);
    std::string pgstr;
    if (nullptr != pgcstr) {
      pgstr = pgcstr;
      pgstr.erase(std::remove(pgstr.begin(), pgstr.end(), '\n'), pgstr.end());
    }
    std::string resstr;
    bool uniqueViolation = false;
    bool checkViolation = false;
    bool integrityViolation = false;
    bool foreignKeyViolation = false;
    std::string violatedConstraint;
    if (nullptr != res) {
      resstr = "DB Result Status:" + std::to_string(PQresultStatus(res));
      const char *const e = PQresultErrorField(res, PG_DIAG_SQLSTATE);
      const char *const m = PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY);
      std::regex rgx("constraint\\s*\"(.*)\"");
      std::smatch match;
      std::string whatStr = (nullptr != m ? m : "");
      violatedConstraint = std::regex_search(whatStr, match, rgx) ? std::string(match[1]) : "";
      if (nullptr != e && '\0' != *e) {
        uniqueViolation = 0 == std::strcmp("23505", e);
        checkViolation = 0 == std::strcmp("23514", e);
        integrityViolation = 0 == std::strcmp("23000", e);
        foreignKeyViolation = 0 == std::strcmp("23503", e);
        resstr += " SQLState:" + std::string(e);
      }
    }
    std::string dbmsg;
    if (!pgstr.empty()) {
      dbmsg = pgstr;
      if (!resstr.empty()) {
        dbmsg += " ("+resstr+")";
      }
    } else {
      dbmsg = resstr;
    }

    if (!dbmsg.empty()) {
      dbmsg = "Database library reported: " + dbmsg;
    }
    if (!prefix.empty()) {
      if (!dbmsg.empty()) {
        dbmsg = prefix + ": " + dbmsg;
      } else {
        dbmsg = prefix;
      }
    }

    bool badconn = false;
    if (nullptr != conn) {
      if (CONNECTION_OK != PQstatus(conn)) {
        badconn = true;
      }
    }

    cta::utils::toUpper(violatedConstraint);
    if (badconn) {
      throw exception::LostDatabaseConnection(dbmsg);
    }
    if (uniqueViolation) {
      throw UniqueConstraintError(dbmsg, pgstr, PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY));
    }
    if (checkViolation) {
      throw CheckConstraintError(dbmsg, pgstr, PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY));
    }
    if (integrityViolation) {
      throw IntegrityConstraintError(dbmsg, pgstr, PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY));
    }
    if (foreignKeyViolation) {
      throw IntegrityConstraintError(dbmsg, pgstr, PQresultErrorField(res, PG_DIAG_MESSAGE_PRIMARY));
    }
    throw exception::Exception(dbmsg);
  }

  /**
   * A class to help managing the lifetime of a postgres result.
   */
  class Result {
  public:

    Result( const Result& ) = delete; // non construction-copyable
    Result& operator=( const Result& ) = delete; // non copyable

    /**
     * Constructor.
     * The Result object will manage a single result.
     * A single result may contain 0 or more rows.
     *
     * @param res A libpq result.
     */
    Result(PGresult *res) : m_res(res), m_rcode(PQresultStatus(res)) {
    }

   /**
    * Destructor.
    */
    ~Result() {
      clear();
    }

    /**
     * Relases the single result or ensures the conneciton has no pending results.
     */
    void clear() {
      PQclear(m_res);
      m_res = nullptr;
    }

    /**
     * Gets the current result.
     *
     * @return The libpq result.
     */
    PGresult *get() const {
      return m_res;
    }

    /**
     * Gets the current result code.
     *
     * @return The libpq result rcode.
     */
    ExecStatusType rcode() const {
      return m_rcode;
    }

  private:
    /**
     * The result as a libpq result.
     */
    PGresult *m_res;

    /**
     * rcode of current result.
     */
    ExecStatusType m_rcode;

  }; // class Result

  /**
   * A class to help managing a sequence of results from a postgres connection.
   * Each result in the sequence may in tern contain 0 or more rows.
   */
  class ResultItr {
  public:

    ResultItr( const ResultItr& ) = delete; // non construction-copyable
    ResultItr& operator=( const ResultItr& ) = delete; // non copyable

    /**
     * Constructor.
     * The ResultItr object will manage a sequence of results, the first of which
     * is only available after calling next().
     *
     * @param conn A libpq connection.
     */
    ResultItr(PGconn *conn) :
      m_conn(conn), m_res(nullptr), m_finished(false), m_rcode(PGRES_FATAL_ERROR) {
    }

   /**
    * Destructor.
    */
    ~ResultItr() {
      clear();
    }

    /**
     * Relases the single result or ensures the conneciton has no pending results.
     */
    void clear() {
      while(nullptr != next());
    }

    /**
     * Gets the current result.
     *
     * @return The libpq result.
     */
    PGresult *get() const {
      return m_res;
    }

    /**
     * Moves to the next result available.
     *
     * @return  The libpq result of the next result or nullptr if none is available.
     */
    PGresult *next() {
      if (m_finished) {
        return nullptr;
      }
      PQclear(m_res);
      m_res = PQgetResult(m_conn);
      m_rcode = PQresultStatus(m_res);
      if (nullptr == m_res) {
        m_finished = true;
      }
      return m_res;
    }

    /**
     * Gets the current result code.
     *
     * @return The libpq result rcode.
     */
    ExecStatusType rcode() const {
      return m_rcode;
    }

  private:
    /**
     * The libpq postgres connection from which to fetch results
     */
    PGconn *m_conn;

    /**
     * The current result as a libpq result.
     */
    PGresult *m_res;

    /**
     * Indicates that the end of result series has been detected on the connection.
     */
    bool m_finished;

    /**
     * rcode of	current	result.
     */
    ExecStatusType m_rcode;

  }; // class ResultItr

}; // class Postgres

} // namespace wrapper
} // namespace rdbms
} // namespace cta
