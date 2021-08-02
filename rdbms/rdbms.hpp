/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#pragma once

#include "rdbms/Constants.hpp"

#include <string>

namespace cta {
namespace rdbms {

/**
 * Returns a clipped version of the specified SQL string that has a length more
 * appropriate SQL strings within exception messages.
 *
 * The string will be clipped at a maxmum of maxSqlLenInExceptions characters.
 * If the string is actually clipped then the three last characters will be
 * replaced by an ellipsis of three dots, in other word "...".  These 3
 * characters will indicate to the reader of the exception message that the
 * SQL statement has been clipped.
 *
 * @param sql The original SQL string that may need to be clipped.
 * @param maxSqlLenInExceptions The maximum length an SQL statement can have in
 * an exception error message.
 * @return The SQL string to be used in an exception message.
 */
 std::string getSqlForException(const std::string &sql,
   const std::string::size_type maxSqlLenInExceptions = MAX_SQL_LEN_IN_EXCEPTIONS);

} // namespace rdbms
} // namespace cta
