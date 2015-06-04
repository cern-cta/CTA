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

#include "SchedulerDatabaseFactory.hpp"

namespace cta {

/**
 * A conncret implementation of a scheduler database factory that creates mock
 * scheduler database objects.
 */
class MockSchedulerDatabaseFactory: public SchedulerDatabaseFactory {
public:

  /**
   * Destructor.
   */
  ~MockSchedulerDatabaseFactory() throw();

  /**
   * Returns a newly created scheduler database object.
   *
   * @return A newly created scheduler database object.
   */
  std::unique_ptr<SchedulerDatabase> create() const;

}; // class MockSchedulerDatabaseFactory

} // namespace cta
