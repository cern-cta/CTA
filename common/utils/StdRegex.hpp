/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

// GCC 14 sanitizer builds report uninitialized std::function members in libstdc++ regex automaton states.
// Include this before libraries that use <regex> too; restore diagnostics before compiling CTA code.
#if defined(CTA_GCC14_SANITIZER_REGEX_WORKAROUND)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
#endif

#include <regex>

#if defined(CTA_GCC14_SANITIZER_REGEX_WORKAROUND)
#pragma GCC diagnostic pop
#endif
