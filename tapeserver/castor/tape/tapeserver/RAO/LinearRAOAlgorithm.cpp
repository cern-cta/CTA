/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include <algorithm>
#include <numeric>

#include "LinearRAOAlgorithm.hpp"
#include "common/Timer.hpp"

namespace castor::tape::tapeserver::rao {

std::vector<uint64_t> LinearRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) {
  std::vector<uint64_t> raoIndices(jobs.size());
  //Initialize the vector of indices
  cta::utils::Timer t;
  cta::utils::Timer totalTimer;
  std::iota(raoIndices.begin(),raoIndices.end(),0);
  m_raoTimings.insertAndReset("vectorInitializationTime",t);
  //Sort the indices regarding the fseq of the jobs located in the vector passed in parameter
  std::stable_sort(raoIndices.begin(),raoIndices.end(),[&jobs](const uint64_t index1, const uint64_t index2){
    return jobs[index1]->selectedTapeFile().fSeq < jobs[index2]->selectedTapeFile().fSeq;
  });
  m_raoTimings.insertAndReset("vectorSortingTime",t);
  m_raoTimings.insertAndReset("RAOAlgorithmTime",totalTimer);
  return raoIndices;
}

std::string LinearRAOAlgorithm::getName() const {
  return "linear";
}

} // namespace castor::tape::tapeserver::rao
