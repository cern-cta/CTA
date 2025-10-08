/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "CostHeuristicFactory.hpp"
#include "CTACostHeuristic.hpp"
#include "common/exception/Exception.hpp"

namespace castor::tape::tapeserver::rao {

std::unique_ptr<CostHeuristic> CostHeuristicFactory::createCostHeuristic(const RAOOptions::CostHeuristicType& costHeuristicType) {
  std::unique_ptr<CostHeuristic> ret;
  switch(costHeuristicType) {
    case RAOOptions::CostHeuristicType::cta: {
      ret.reset(new CTACostHeuristic());
      break;
    }
  }
  return ret;
}

} // namespace castor::tape::tapeserver::rao
