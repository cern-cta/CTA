/*
 * The CERN Tape Archive(CTA) project
 * Copyright(C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 *(at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "catalogue/RdbmsCatalogue.hpp"

namespace cta {
namespace catalogue {

class CatalogueFactory;

/**
 * An SQLite implementation of the CTA catalogue.
 */
class SqliteCatalogue: public RdbmsCatalogue {

  /**
   * The CatalogueFactory is a friend so that it can call the private
   * constructor of this class.
   */
  friend CatalogueFactory;

private:

  /**
   * Private constructor only to be called by the CatalogueFactory class (a
   * friend).
   *
   * @param filename The filename to be passed to the sqlite3_open() function.
   */
  SqliteCatalogue(const std::string &filename);

protected:

  /**
   * Protected constructor only to be called by sub-classes.
   *
   * Please note that it is the responsibility of the sub-class to set
   * RdbmsCatalogue::m_conn.
   */
  SqliteCatalogue();

public:

  /**
   * Destructor.
   */
  virtual ~SqliteCatalogue();

protected:

  /**
   * Returns a unique archive ID that can be used by a new archive file within
   * the catalogue.
   *
   * This method must be implemented by the sub-classes of RdbmsCatalogue
   * because different database technologies propose different solution to the
   * problem of generating ever increasing numeric identifiers.
   */
  virtual uint64_t getNextArchiveFileId();

}; // class SqliteCatalogue

} // namespace catalogue
} // namespace cta
