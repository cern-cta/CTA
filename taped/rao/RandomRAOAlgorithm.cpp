/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RandomRAOAlgorithm.hpp"

#include "common/utils/Timer.hpp"
#include "taped/scsi/Structures.hpp"

#include <algorithm>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <vector>

namespace cta::tape::rao {

std::vector<uint64_t> RandomRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs) {
  std::vector<uint64_t> raoIndices(jobs.size());
  cta::utils::Timer totalTimer;
  std::iota(raoIndices.begin(), raoIndices.end(), 0);
  std::mt19937 generator(std::random_device {}());
  std::shuffle(raoIndices.begin(), raoIndices.end(), generator);
  m_raoTimings.insertAndReset("RAOAlgorithmTime", totalTimer);
  return raoIndices;
}

}  // namespace cta::tape::rao
