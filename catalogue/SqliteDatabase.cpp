/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "catalogue/SqliteDatabase.hpp"
#include "common/exception/Exception.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::catalogue::SqliteDatabase::SqliteDatabase(const std::string &filename) {
  m_dbHandle = NULL;
  if(sqlite3_open(filename.c_str(), &m_dbHandle)) {
    sqlite3_close(m_dbHandle);
    exception::Exception ex;
    ex.getMessage() << "Failed to construct SqliteDatabase: sqlite3_open failed"
      ": " << sqlite3_errmsg(m_dbHandle);
    throw(ex);
  }
}

//------------------------------------------------------------------------------
// destructor
//------------------------------------------------------------------------------
cta::catalogue::SqliteDatabase::~SqliteDatabase() {
  sqlite3_close(m_dbHandle);
}

//------------------------------------------------------------------------------
// getHandle
//------------------------------------------------------------------------------
sqlite3 *cta::catalogue::SqliteDatabase::getHandle() {
  return m_dbHandle;
}
