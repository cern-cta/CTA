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
#include "scheduler/testingMocks/MockRetrieveJob.hpp"
#include "catalogue/DummyCatalogue.hpp"
#include <memory>

namespace cta {
  class MockRetrieveMount: public cta::RetrieveMount {
  public:
    int getJobs;
    int completes;
    MockRetrieveMount(cta::catalogue::Catalogue &catalogue): RetrieveMount(catalogue), getJobs(0), completes(0) {}

    ~MockRetrieveMount() throw() {
    }

    std::list<std::unique_ptr<cta::RetrieveJob> > getNextJobBatch(uint64_t filesRequested,
      uint64_t bytesRequested, log::LogContext & logContext) override {
      std::list<std::unique_ptr<cta::RetrieveJob> > ret;
      // Count the attempt to get a file (even if not successful).
      getJobs++;
      while (m_jobs.size()) {
        ret.emplace_back(m_jobs.front().release());
        m_jobs.pop_front();
        // Count the next attempt to get the file"
        if (filesRequested <= 1 || bytesRequested <= ret.back()->archiveFile.fileSize)
          break;
        else
          getJobs++;
        bytesRequested -= ret.back()->archiveFile.fileSize;
        filesRequested--;
      }
      return ret;
    }

    virtual std::string getMountTransactionId() const override {
      return "1234567890";
    }

    void abort(const std::string&) override { completes ++; }

    void diskComplete() override { completes ++;}

    void tapeComplete() override {};

    bool bothSidesComplete() override { return false; }

    void setDriveStatus(cta::common::dataStructures::DriveStatus status, const cta::optional<std::string> & reason) override {};

    void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override {};

    void setTapeMounted(log::LogContext &logContext) const override {};

    void flushAsyncSuccessReports(std::queue<std::unique_ptr<cta::RetrieveJob> >& successfulRetrieveJobs, cta::log::LogContext& logContext) override {};

  private:

    std::list<std::unique_ptr<cta::RetrieveJob>> m_jobs;
  public:
    void createRetrieveJobs(const unsigned int nbJobs) {
      for(unsigned int i = 0; i < nbJobs; i++) {
        m_jobs.push_back(std::unique_ptr<cta::RetrieveJob>(
          new MockRetrieveJob(*this)));
      }
    }
  }; // class MockRetrieveMount
}
