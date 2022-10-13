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

#include <memory>
#include <numeric>
#include <string>
#include <vector>

#include "common/Timer.hpp"
#include "tapeserver/castor/tape/tapeserver/RAO/RandomRAOAlgorithm.hpp"
#include "tapeserver/castor/tape/tapeserver/SCSI/Structures.hpp"


namespace castor { namespace tape { namespace tapeserver { namespace rao {

RandomRAOAlgorithm::RandomRAOAlgorithm() {
}

std::vector<uint64_t> RandomRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) {
  std::vector<uint64_t> raoIndices(jobs.size());
  cta::utils::Timer totalTimer;
  std::iota(raoIndices.begin(), raoIndices.end(), 0);
  std::random_shuffle(raoIndices.begin(), raoIndices.end());
  m_raoTimings.insertAndReset("RAOAlgorithmTime", totalTimer);
  return raoIndices;
}

RandomRAOAlgorithm::~RandomRAOAlgorithm() {
}

std::string RandomRAOAlgorithm::getName() const {
  return "random";
}

}}}}
