/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/Timer.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/RandomRAOAlgorithm.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"


namespace castor::tape::tapeserver::rao {

std::vector<uint64_t> RandomRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) {
  std::vector<uint64_t> raoIndices(jobs.size());
  cta::utils::Timer totalTimer;
  std::iota(raoIndices.begin(), raoIndices.end(), 0);
  std::random_shuffle(raoIndices.begin(), raoIndices.end());
  m_raoTimings.insertAndReset("RAOAlgorithmTime", totalTimer);
  return raoIndices;
}

} // namespace castor::tape::tapeserver::rao
