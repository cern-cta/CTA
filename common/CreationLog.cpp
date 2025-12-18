/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "common/CreationLog.hpp"

#include <limits>

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::CreationLog::CreationLog() : time(std::numeric_limits<decltype(time)>::max()) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------

cta::CreationLog::CreationLog(const cta::common::dataStructures::OwnerIdentity& user,
                              const std::string& host,
                              const time_t time,
                              const std::string& comment)
    : user(user),
      host(host),
      time(time),
      comment(comment) {}
