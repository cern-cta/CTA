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

#include "CostHeuristicFactory.hpp"
#include "CTACostHeuristic.hpp"
#include "common/exception/Exception.hpp"

namespace castor {
namespace tape {
namespace tapeserver {
namespace rao {

CostHeuristicFactory::CostHeuristicFactory() {}

std::unique_ptr<CostHeuristic>
  CostHeuristicFactory::createCostHeuristic(const RAOOptions::CostHeuristicType& costHeuristicType) {
  std::unique_ptr<CostHeuristic> ret;
  switch (costHeuristicType) {
    case RAOOptions::CostHeuristicType::cta: {
      ret.reset(new CTACostHeuristic());
      break;
    }
    default:
      std::string errorMsg = "In CostHeuristicFactory::createCostHeuristic() unable to instanciate a cost heuristic"
                             " because the cost heuristic type given in parameter (" +
                             std::to_string(costHeuristicType) + ") is unknown.";
      throw cta::exception::Exception(errorMsg);
  }
  return ret;
}

CostHeuristicFactory::~CostHeuristicFactory() {}

}  // namespace rao
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor