/*
 * SPDX-FileCopyrightText: 2026 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <vector>

namespace cta::common::dataStructures {

struct DeadMountCandidateIDs {
  std::vector<uint64_t> archivePending;
  std::vector<uint64_t> archiveActive;
  std::vector<uint64_t> retrievePending;
  std::vector<uint64_t> retrieveActive;
  std::vector<uint64_t> archiveRepackPending;
  std::vector<uint64_t> archiveRepackActive;
  std::vector<uint64_t> retrieveRepackPending;
  std::vector<uint64_t> retrieveRepackActive;
};

}  // namespace cta::common::dataStructures
