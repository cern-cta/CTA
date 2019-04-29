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

#pragma once

#include <string>

#include "common/dataStructures/OwnerIdentity.hpp"

namespace cta {

/**
 * Class containing the security information necessary to authorise a user
 * submitting a requests from a specific host.
 */
struct CreationLog {

  /**
   * Constructor.
   */
  CreationLog();

  /**
   * Constructor.
   */
  CreationLog(const common::dataStructures::OwnerIdentity &user, const std::string &host,
    const time_t time,  const std::string & comment = "");

  /**
   * The identity of the creator.
   */
  common::dataStructures::OwnerIdentity user;

  /**
   * The network name of the host from which they are submitting a request.
   */
  std::string host;
  
  /**
   * The time of creation
   */
  time_t time;
  
  /**
   * The comment at creation time
   */
  std::string comment;

}; // struct CreationLog

} // namespace cta
