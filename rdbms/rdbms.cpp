/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "rdbms/rdbms.hpp"

namespace cta::rdbms {

//------------------------------------------------------------------------------
// getSqlForException
//------------------------------------------------------------------------------
std::string getSqlForException(const std::string &sql, const std::string::size_type maxSqlLenInExceptions) {
  if(sql.length() <= maxSqlLenInExceptions) {
    return sql;
  } else {
    if(maxSqlLenInExceptions >= 3) {
      return sql.substr(0, maxSqlLenInExceptions - 3) + "...";
    } else {
      return std::string("...").substr(0, maxSqlLenInExceptions);
    }
  }
}

} // namespace cta::rdbms
