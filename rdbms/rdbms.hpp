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

#include "rdbms/Constants.hpp"

#include <string>

namespace cta::rdbms {

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

} // namespace cta::rdbms
