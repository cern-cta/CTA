/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <errno.h>
#include "common/exception/OutOfMemory.hpp"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
cta::exception::OutOfMemory::OutOfMemory() :
  cta::exception::Exception() {}
