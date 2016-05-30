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

/**
 * Class used to facilitate unit testing by making public one or more of the
 * protected members of its super class.
 */
class TestingRdbmsCatalogue: public RdbmsCatalogue {
public:
  /**
   * Constructor.
   *
   * @param conn The connection to the underlying relational database.  Please
   * note that the TestingRdbmsCatalogue will own and therefore delete the
   * specified database connection.
   */
  TestingRdbmsCatalogue(DbConn *const conn): RdbmsCatalogue(conn) {
  }

  /**
   * Destructor.
   */
  virtual ~TestingRdbmsCatalogue()  {
  }

  using RdbmsCatalogue::insertArchiveFile;
  using RdbmsCatalogue::createTapeFile;
  using RdbmsCatalogue::getArchiveFile;
  using RdbmsCatalogue::getTapeLastFSeq;
  using RdbmsCatalogue::setTapeLastFSeq;

}; // class TestingRdbmsCatalogue

} // namespace catalogue
} // namespace cta
