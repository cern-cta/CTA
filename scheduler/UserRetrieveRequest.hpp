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

#include "scheduler/UserRequest.hpp"

#include <stdint.h>

namespace cta {

/**
 * Abstract class representing a user request to retrieve some data.
 */
class UserRetrieveRequest: public UserRequest {
public:

  /**
   * Constructor.
   */
  UserRetrieveRequest();

  /**
   * Destructor.
   */
  virtual ~UserRetrieveRequest() throw() = 0;

  /**
   * Constructor.
   *
   * @param priority The priority of the request.
   * @param user The identity of the user who made the request.
   * @param creationTime Optionally the absolute time at which the user request
   * was created.  If no value is given then the current time is used.
   */
  UserRetrieveRequest(
    const uint64_t priority,
    const common::dataStructures::EntryLog &creationLog);

}; // class RetrieveRequest

} // namespace cta
