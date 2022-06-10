/*
 * @project      The CERN Tape Archive (CTA)
 * @copyright    Copyright © 2021-2022 CERN
 * @license      This program is free software, distributed under the terms of the GNU General Public
 *               Licence version 3 (GPL Version 3), copied verbatim in the file "COPYING". You can
 *               redistribute it and/or modify it under the terms of the GPL Version 3, or (at your
 *               option) any later version.
 *
 *               This program is distributed in the hope that it will be useful, but WITHOUT ANY
 *               WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 *               PARTICULAR PURPOSE. See the GNU General Public License for more details.
 *
 *               In applying this licence, CERN does not waive the privileges and immunities
 *               granted to it by virtue of its status as an Intergovernmental Organization or
 *               submit itself to any jurisdiction.
 */

#pragma once

#include "catalogue/DummyCatalogue.hpp"
#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/testingMocks/MockRetrieveJob.hpp"
#include <memory>

namespace cta {
class MockRetrieveMount : public cta::RetrieveMount {
public:
  int getJobs;
  int completes;
  explicit MockRetrieveMount(cta::catalogue::Catalogue &catalogue) : RetrieveMount(catalogue), getJobs(0), completes(0) {}

  ~MockRetrieveMount() noexcept override = default;

  std::list<std::unique_ptr<cta::RetrieveJob>> getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested,
                                                               log::LogContext &logContext) override {
    std::list<std::unique_ptr<cta::RetrieveJob>> ret;
    // Count the attempt to get a file (even if not successful).
    getJobs++;
    while (!m_jobs.empty()) {
      ret.emplace_back(m_jobs.front().release());
      m_jobs.pop_front();
      // Count the next attempt to get the file
      if (filesRequested <= 1 || bytesRequested <= ret.back()->archiveFile.fileSize)
        break;
      else
        getJobs++;
      bytesRequested -= ret.back()->archiveFile.fileSize;
      filesRequested--;
    }
    return ret;
  }

  std::string getMountTransactionId() const override { return "1234567890"; }

  std::string getDrive() const override { return "VDSTK11"; }

  void complete() override { completes++; }

  void diskComplete() override { completes++; }

  void tapeComplete() override{};

  bool bothSidesComplete() override { return false; }

  void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                      const std::optional<std::string> &reason) override{};

  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats &stats) override{};

  void setTapeMounted(log::LogContext &logContext) const override{};

  void flushAsyncSuccessReports(std::queue<std::unique_ptr<cta::RetrieveJob>> &successfulRetrieveJobs,
                                cta::log::LogContext &logContext) override{};

  void requeueJobBatch(std::vector<std::unique_ptr<cta::RetrieveJob>> &jobs, log::LogContext &logContext) override{};

  bool reserveDiskSpace(const cta::DiskSpaceReservationRequest &request, log::LogContext &logContext) override {
    return true;
  }

  void putQueueToSleep(const std::string &diskSystemName, const uint64_t sleepTime,
                       log::LogContext &logContext) override {
    m_sleepingQueues.push_back(std::make_pair(diskSystemName, sleepTime));
  };

  void addDiskSystemToSkip(const cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip &diskSystem) override {
    m_diskSystemsToSkip.insert(diskSystem);
  }

private:
  std::list<std::unique_ptr<cta::RetrieveJob>> m_jobs;
  std::set<cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip> m_diskSystemsToSkip;
  std::list<std::pair<std::string, uint64_t>> m_sleepingQueues;

public:
  void createRetrieveJobs(const unsigned int nbJobs) {
    for (unsigned int i = 0; i < nbJobs; i++) {
      m_jobs.push_back(std::unique_ptr<cta::RetrieveJob>(new MockRetrieveJob(*this)));
    }
  }

  std::list<std::pair<std::string, uint64_t>> getSleepingQueues() { return m_sleepingQueues; }

};  // class MockRetrieveMount
}  // namespace cta
