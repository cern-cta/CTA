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

#include "CostHeuristicFactory.hpp"
#include "CTACostHeuristic.hpp"
#include "common/exception/Exception.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

CostHeuristicFactory::CostHeuristicFactory() {
}

std::unique_ptr<CostHeuristic> CostHeuristicFactory::createCostHeuristic(const RAOOptions::CostHeuristicType & costHeuristicType) {
  std::unique_ptr<CostHeuristic> ret;
  switch(costHeuristicType){
    case RAOOptions::CostHeuristicType::cta: {
      ret.reset(new CTACostHeuristic());
      break;
    }
    default:
      std::string errorMsg = "In CostHeuristicFactory::createCostHeuristic() unable to instanciate a cost heuristic"
        " because the cost heuristic type given in parameter (" + std::to_string(costHeuristicType) + ") is unknown.";
      throw cta::exception::Exception(errorMsg);
  }
  return ret;
}

CostHeuristicFactory::~CostHeuristicFactory() {
}

}}}}