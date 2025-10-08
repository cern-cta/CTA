/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <errno.h>
#include "common/exception/NoEntry.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
cta::exception::NoEntry::NoEntry() :
  // No backtrace for this exception
  cta::exception::Exception("", false) {}
