/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <sys/capability.h>

namespace cta::server::ProcessCap {

/**
   * C++ wrapper around the C functions cap_get_proc() and cap_to_text().
   *
   * @return The string representation the capabilities of the current
   * process.
   */
std::string getProcText();

bool hasRawIoCap();

}  // namespace cta::server::ProcessCap
