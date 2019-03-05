/*
 * The CERN Tape Retrieve (CTA) project
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

#include "common/log/LogContext.hpp"
#include "common/dataStructures/RepackInfo.hpp"
#include "scheduler/SchedulerDatabase.hpp"

namespace cta {

/**
 * Control structure for the RepackRequest.
 */
class RepackRequest {
  friend class Scheduler;
public:
  void expand();
  const cta::common::dataStructures::RepackInfo getRepackInfo() const;
  void fail();
protected:
  std::unique_ptr<cta::SchedulerDatabase::RepackRequest> m_dbReq;
}; // class RepackRequest

} // namespace cta