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

#include "nameserver/NameServerFactory.hpp"

namespace cta {

/**
 * Concrete implementation of a name server factory that creates mock name
 * server objects.
 */
class MockNameServerFactory: public NameServerFactory {
public:

  /**
   * Destructor.
   */
  ~MockNameServerFactory() throw();

  /**
   * Returns a newly created name server object.
   *
   * @return A newly created name server object.
   */
  std::unique_ptr<NameServer> create();

}; // class MockNameServerFactory

} // namespace cta
