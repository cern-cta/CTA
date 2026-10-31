/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "catalogue/dummy/DummyCatalogue.hpp"
#include "scheduler/ArchiveJob.hpp"

namespace unitTests {

class TestingArchiveJob : public cta::ArchiveJob {
public:
  TestingArchiveJob() : cta::ArchiveJob(nullptr, catalogue(), {}, "", {}) {}

private:
  static cta::catalogue::Catalogue& catalogue() {
    // ArchiveJob stores a reference, so the dummy must outlive every test job.
    static cta::catalogue::DummyCatalogue instance;
    return instance;
  }
};

}  // namespace unitTests
