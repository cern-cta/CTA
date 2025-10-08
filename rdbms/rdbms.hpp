/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
