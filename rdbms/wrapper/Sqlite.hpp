/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <sqlite3.h>
#include <string>

namespace cta::rdbms::wrapper {

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

} // namespace cta::rdbms::wrapper
