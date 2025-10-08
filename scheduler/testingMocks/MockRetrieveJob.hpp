/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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

    std::optional<std::string> diskSystemName() override {
      return m_diskSystemName;
    }

    void setDiskSystemName(std::string diskSystemName) { //allow tests to optionally set a disk system name
      m_diskSystemName = diskSystemName;
    }

    ~MockRetrieveJob() noexcept {}

  private:
    std::optional<std::string> m_diskSystemName;
  };
}
