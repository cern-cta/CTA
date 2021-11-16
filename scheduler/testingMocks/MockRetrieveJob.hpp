/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
    MockRetrieveJob(RetrieveMount & rm): cta::RetrieveJob(&rm,
    cta::common::dataStructures::RetrieveRequest(), 
    cta::common::dataStructures::ArchiveFile(), 1,
    cta::PositioningMethod::ByBlock), completes(0), failures(0) {
      common::dataStructures::TapeFile tf;
      tf.copyNb = 1;
      archiveFile.tapeFiles.push_back(tf);
    } 
    virtual void asyncSetSuccessful() override { completes++;  }
    void transferFailed(const std::string &failureReason, cta::log::LogContext&) override { failures++; };

    cta::optional<std::string> diskSystemName() override {
      return m_diskSystemName;
    }

    void setDiskSystemName(std::string diskSystemName) { //allow tests to optionally set a disk system name
      m_diskSystemName = diskSystemName;
    }
    
    ~MockRetrieveJob() throw() {} 
  
  private:
    cta::optional<std::string> m_diskSystemName;
  };
}
