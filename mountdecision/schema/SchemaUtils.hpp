/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "rdbms/Conn.hpp"

#include <string>
#include <string_view>

namespace cta::mountdecision {

bool tableExists(const std::string& tableName, rdbms::Conn& conn);

void executeNonQueries(rdbms::Conn& conn, std::string_view sqlStmts);

}  // namespace cta::mountdecision
