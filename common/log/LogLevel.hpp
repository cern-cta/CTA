/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "common/log/Constants.hpp"

#include <string>

namespace cta::log {

/**
 * Returns the numeric value of the specified log level.
 */
int toLogLevel(std::string_view s);

}  // namespace cta::log
