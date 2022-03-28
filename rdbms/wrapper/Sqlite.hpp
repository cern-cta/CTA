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

#include <sqlite3.h>
#include <string>

namespace cta {
namespace rdbms {
namespace wrapper {

/**
 * A helper class for working with SQLite.
 */
class Sqlite {
public:

  /**
   * Returns the string representation of the specified SQLite return code.
   *
   * @param rc The SQLite return code.
   * @return The string representation of the SQLite return code.
   */
  static std::string rcToStr(const int rc);

}; // class SqlLiteStmt

} // namespace wrapper
} // namespace rdbms
} // namespace cta
