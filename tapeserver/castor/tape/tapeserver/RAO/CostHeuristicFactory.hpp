/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once
#include <memory>

#include "CostHeuristic.hpp"
#include "RAOOptions.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * Factory of concrete CostHeuristic objects
 */
class CostHeuristicFactory {
public:
  CostHeuristicFactory() = default;
  virtual ~CostHeuristicFactory() = default;

  /**
   * Returns the unique_ptr to the instance of the CostHeuristic instance according to the type given in parameter
   * @param costHeuristicType the type of CostHeuristic to instanciate
   * @return the unique_ptr to the instance of the CostHeuristic instance according to the type given in parameter
   */
  std::unique_ptr<CostHeuristic> createCostHeuristic(const RAOOptions::CostHeuristicType& costHeuristicType);
};

} // namespace castor::tape::tapeserver::rao
