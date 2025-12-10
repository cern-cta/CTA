/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "scheduler/RetrieveJob.hpp"
#include "scheduler/RetrieveMount.hpp"
#include "scheduler/testingMocks/MockRetrieveJob.hpp"

#include <list>
#include <memory>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace cta {

namespace catalogue {
class Catalogue;
}

class MockRetrieveMount : public cta::RetrieveMount {
public:
  int getJobs;
  int completes;

  explicit MockRetrieveMount(cta::catalogue::Catalogue& catalogue)
      : RetrieveMount(catalogue),
        getJobs(0),
        completes(0) {}

  ~MockRetrieveMount() noexcept override = default;

  std::list<std::unique_ptr<cta::RetrieveJob>>
  getNextJobBatch(uint64_t filesRequested, uint64_t bytesRequested, log::LogContext& logContext) override {
    std::list<std::unique_ptr<cta::RetrieveJob>> ret;
    // Count the attempt to get a file (even if not successful).
    getJobs++;
    while (!m_jobs.empty()) {
      ret.emplace_back(std::move(m_jobs.front()));
      m_jobs.pop_front();
      // Count the next attempt to get the file
      if (filesRequested <= 1 || bytesRequested <= ret.back()->archiveFile.fileSize) {
        break;
      } else {
        getJobs++;
      }
      bytesRequested -= ret.back()->archiveFile.fileSize;
      filesRequested--;
    }
    return ret;
  }

  std::string getMountTransactionId() const override { return "1234567890"; }

  std::string getDrive() const override { return "VDSTK11"; }

  void complete() override { completes++; }

  void diskComplete() override { completes++; }

  void tapeComplete() override {};

  bool bothSidesComplete() override { return false; }

  void setDriveStatus(cta::common::dataStructures::DriveStatus status,
                      const std::optional<std::string>& reason) override {};

  void setTapeSessionStats(const castor::tape::tapeserver::daemon::TapeSessionStats& stats) override {};

  void setTapeMounted(log::LogContext& logContext) const override {};

  void setJobBatchTransferred(std::queue<std::shared_ptr<cta::RetrieveJob>>& successfulRetrieveJobs,
                              cta::log::LogContext& logContext) override {};

  uint64_t requeueJobBatch(const std::list<std::string>& jobIDsList, cta::log::LogContext& logContext) const override {
    return 0;
  };

  void requeueJobBatch(std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs, log::LogContext& logContext) override {};

  bool reserveDiskSpace(const cta::DiskSpaceReservationRequest& request, log::LogContext& logContext) override {
    return true;
  }

  void
  putQueueToSleep(const std::string& diskSystemName, const uint64_t sleepTime, log::LogContext& logContext) override {
    m_sleepingQueues.emplace_back(diskSystemName, sleepTime);
  };

  void addDiskSystemToSkip(const cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip& diskSystem) override {
    m_diskSystemsToSkip.insert(diskSystem);
  }

private:
  std::list<std::unique_ptr<cta::RetrieveJob>> m_jobs;
  std::set<cta::SchedulerDatabase::RetrieveMount::DiskSystemToSkip> m_diskSystemsToSkip;
  std::list<std::pair<std::string, uint64_t>> m_sleepingQueues;

public:
  void createRetrieveJobs(const unsigned int nbJobs) {
    for (unsigned int i = 0; i < nbJobs; i++) {
      m_jobs.emplace_back(new MockRetrieveJob(*this));
    }
  }

  std::list<std::pair<std::string, uint64_t>> getSleepingQueues() { return m_sleepingQueues; }

};  // class MockRetrieveMount
}  // namespace cta
