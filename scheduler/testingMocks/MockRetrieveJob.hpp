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

#include "scheduler/RetrieveMount.hpp"
#include "scheduler/RetrieveJob.hpp"
#include <memory>

namespace cta {
  class MockRetrieveJob: public cta::RetrieveJob {
  public:
    int completes;
    int failures;
    MockRetrieveJob(RetrieveMount & rm): cta::RetrieveJob(rm,
    cta::common::dataStructures::RetrieveRequest(), 
    cta::common::dataStructures::ArchiveFile(), 1,
    cta::PositioningMethod::ByBlock), completes(0), failures(0) {
      archiveFile.tapeFiles[1];
    } 
    virtual void asyncComplete() override { completes++;  }
    virtual void checkComplete() override {}
    virtual void failed(const std::string & errorReport, cta::log::LogContext &) override { failures++; };
    
    ~MockRetrieveJob() throw() {} 
  };
}
