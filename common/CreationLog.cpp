/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <limits>

#include "common/CreationLog.hpp"

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------
cta::CreationLog::CreationLog(): 
  time(std::numeric_limits<decltype(time)>::max()) {}

//------------------------------------------------------------------------------
// constructor
//------------------------------------------------------------------------------

cta::CreationLog::CreationLog(const cta::common::dataStructures::OwnerIdentity& user, 
    const std::string& host, const time_t time, const std::string& comment):
  user(user), host(host), time(time), comment(comment) {}
