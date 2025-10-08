/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>

namespace cta::rdbms {

/**
 * The maximum length an SQL statement can have in exception error message.
 */
const std::string::size_type MAX_SQL_LEN_IN_EXCEPTIONS = 80;

} // namespace cta::rdbms
