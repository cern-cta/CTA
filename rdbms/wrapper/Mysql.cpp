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
#include "common/exception/Exception.hpp"

#include "rdbms/wrapper/Mysql.hpp"

#include <my_global.h>
#include <mysql.h>

#include <iostream>

namespace cta {
namespace rdbms {
namespace wrapper {

static 
std::string replace_string(const std::string& sql, 
                           const std::string& from, const std::string& to) {
  std::string real_sql = sql;
  const size_t tok_sz = from.size();

  while (true) {
    auto idx = real_sql.find(from);
    if (idx == std::string::npos) {
      break;
    }
    real_sql.replace(idx, tok_sz, to);
  }

  return real_sql;
}

std::string Mysql::translate_it(const std::string& sql) {
  std::string real_sql = sql;

  // if found :name, replace it with '?'
  const std::string named_tok = ":";
  const std::string replaced_tok = "?";
  while (true) {
    auto idx = real_sql.find(named_tok);
    if (idx == std::string::npos) {
      break;
    }
    // find the next token
    size_t len = real_sql.length();
    size_t l = 1; // next to ":"
    for (; idx+l < len; ++l) {
      const char& c = real_sql[idx+l];
      // check token
      if (('0' <= c && c <= '9') ||
          ('A' <= c && c <= 'Z') ||
          ('a' <= c && c <= 'z') ||
          c == '_') {

      } else {
        break;
      }
    }

    real_sql.replace(idx, l, replaced_tok);

  }

  // replace CONSTRAINT name from sql 
  const std::string constraint_tok = "CONSTRAINT";
  const size_t constraint_tok_sz = constraint_tok.size();
  const std::string constraint_replaced_tok = "";
  while (true) {
    auto idx = real_sql.find(constraint_tok);
    if (idx == std::string::npos) {
      break;
    }

    size_t len = real_sql.length();
    size_t l = constraint_tok_sz;

    // space
    for (; idx+l < len; ++l) {
      const char& c = real_sql[idx+l];
      if (c == ' ') {

      } else {
        break;
      }
    }
    // token
    for (; idx+l < len; ++l) {
      const char& c = real_sql[idx+l];
      if (c != ' ') {

      } else {
        break;
      }
    }

    real_sql.replace(idx, l, constraint_replaced_tok);
  }

  // replace VARCHAR2 to VARCHAR
  real_sql = replace_string(real_sql, "VARCHAR2", "VARCHAR");

  // https://dev.mysql.com/doc/refman/8.0/en/numeric-type-overview.html
  real_sql = replace_string(real_sql, "INTEGER", "BIGINT UNSIGNED");
  return real_sql;
}

Mysql::FieldsInfo::FieldsInfo(MYSQL_RES* result_metadata)
  : column_count(0) {

  if (!result_metadata) {
    throw exception::Exception(std::string(__FUNCTION__) + " empty metadata!");
    return;
  }

  MYSQL_FIELD *fields = NULL;
  fields = mysql_fetch_fields(result_metadata);

  if (!fields) {
    throw exception::Exception(std::string(__FUNCTION__) + " can not fetch fields.");
  }

  column_count = mysql_num_fields(result_metadata);
  // std::string debug_str;

  for (int i = 0; i < column_count; ++i) {
    const std::string name = fields[i].name;
    const enum_field_types type = fields[i].type;
    column2idx[name] = i;

    names.push_back(name);
    types.push_back(type);

    maxsizes.push_back(fields[i].length);
  }

  // throw exception::Exception(debug_str);
}

bool Mysql::FieldsInfo::exists(const std::string& column) {
  return column2idx.find(column) != column2idx.end();
}

} // namespace wrapper
} // namespace rdbms
} // namespace cta




