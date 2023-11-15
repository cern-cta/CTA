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

#include "rdbms/wrapper/Sqlite.hpp"

#include <sstream>

namespace cta::rdbms::wrapper {

//------------------------------------------------------------------------------
// rcToStr
//------------------------------------------------------------------------------
std::string Sqlite::rcToStr(const int rc) {
  switch (rc) {
    // Support some extended SQLite error code.
    case SQLITE_CONSTRAINT_PRIMARYKEY:
      return "Primary key error";
    case SQLITE_CONSTRAINT_UNIQUE:
      return "Unique constraint error";
    default: 
      // By default, just use the basic error code.
      switch(rc & 0xFF) {
        case SQLITE_ABORT:
          return "Abort requested";
        case SQLITE_AUTH:
          return "Authorization denied";
        case SQLITE_BUSY:
          return "Busy";
        case SQLITE_CANTOPEN:
          return "Cannot open database file";
        case SQLITE_CONSTRAINT:
          return "Constraint violation";
        case SQLITE_CORRUPT:
          return "Database file corrupted";
        case SQLITE_DONE:
          return "Statement finished executing successfully";
        case SQLITE_EMPTY:
          return "Database file empty";
        case SQLITE_FORMAT:
          return "Database format error";
        case SQLITE_FULL:
          return "Database full";
        case SQLITE_INTERNAL:
          return "Internal SQLite library error";
        case SQLITE_INTERRUPT:
          return "Interrupted";
        case SQLITE_IOERR:
          return "I/O error";
        case SQLITE_LOCKED:
          return "A table is locked";
        case SQLITE_MISMATCH:
          return "Datatype mismatch";
        case SQLITE_MISUSE:
          return "Misuse";
        case SQLITE_NOLFS:
          return "OS does not provide large file support";
        case SQLITE_NOMEM:
          return "Memory allocation error";
        case SQLITE_NOTADB:
          return "Not a database file";
        case SQLITE_OK:
          return "Operation successful";
        case SQLITE_PERM:
          return "Permission denied";
        case SQLITE_RANGE:
          return "Invalid bind parameter index";
        case SQLITE_READONLY:
          return "Failed to write to read-only database";
        case SQLITE_ROW:
          return "A new row of data is ready for reading";
        case SQLITE_SCHEMA:
          return "Database schema changed";
        case SQLITE_TOOBIG:
          return "TEXT or BLOCK too big";
        case SQLITE_ERROR:
          return "Generic error";
      default: {
          std::ostringstream oss;
          oss << "Unknown SQLite return code " << rc;
          return oss.str();
        }
      }

  }
}

} // namespace cta::rdbms::wrapper
