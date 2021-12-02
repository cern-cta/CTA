/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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

#include <numeric>

#include "RandomRAOAlgorithm.hpp"
#include "common/Timer.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

RandomRAOAlgorithm::RandomRAOAlgorithm() {
}

std::vector<uint64_t> RandomRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) {
  std::vector<uint64_t> raoIndices(jobs.size());
  cta::utils::Timer totalTimer;
  std::iota(raoIndices.begin(),raoIndices.end(),0);
  std::random_shuffle(raoIndices.begin(), raoIndices.end());
  m_raoTimings.insertAndReset("RAOAlgorithmTime",totalTimer);
  return raoIndices;
}

RandomRAOAlgorithm::~RandomRAOAlgorithm() {
}

std::string RandomRAOAlgorithm::getName() const {
  return "random";
}

}}}}
