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

#include <algorithm>
#include <numeric>

#include "LinearRAOAlgorithm.hpp"
#include "common/Timer.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace rao {

LinearRAOAlgorithm::LinearRAOAlgorithm() {}

LinearRAOAlgorithm::~LinearRAOAlgorithm() {}

std::vector<uint64_t> LinearRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs) {
  std::vector<uint64_t> raoIndices(jobs.size());
  //Initialize the vector of indices
  cta::utils::Timer t;
  cta::utils::Timer totalTimer;
  std::iota(raoIndices.begin(), raoIndices.end(), 0);
  m_raoTimings.insertAndReset("vectorInitializationTime", t);
  //Sort the indices regarding the fseq of the jobs located in the vector passed in parameter
  std::stable_sort(raoIndices.begin(), raoIndices.end(), [&jobs](const uint64_t index1, const uint64_t index2) {
    return jobs[index1]->selectedTapeFile().fSeq < jobs[index2]->selectedTapeFile().fSeq;
  });
  m_raoTimings.insertAndReset("vectorSortingTime", t);
  m_raoTimings.insertAndReset("RAOAlgorithmTime", totalTimer);
  return raoIndices;
}

std::string LinearRAOAlgorithm::getName() const {
  return "linear";
}
}  // namespace rao
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor