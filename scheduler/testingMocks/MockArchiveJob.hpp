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
  class MockArchiveJob: public cta::ArchiveJob {
  public:
    int completes;
    int failures;
    MockArchiveJob(cta::ArchiveMount & am, cta::NameServer &ns): cta::ArchiveJob(am, 
        ns, cta::common::archiveNS::ArchiveFile(), 
        cta::RemotePathAndStatus(), cta::NameServerTapeFile()), completes(0), failures(0) {} 
      
    ~MockArchiveJob() throw() {} 

    virtual void complete() {
      completes++;
    }
    
    virtual void failed(const cta::exception::Exception& ex) {
      failures++;
    }

    virtual void retry() {}
  };
}
