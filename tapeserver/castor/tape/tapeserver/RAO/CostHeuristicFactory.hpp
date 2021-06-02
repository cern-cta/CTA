/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#pragma once
#include <memory>

#include "CostHeuristic.hpp"
#include "RAOOptions.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * Factory of concrete CostHeuristic objects
 */
class CostHeuristicFactory {
public:
  CostHeuristicFactory();
  /**
   * Returns the unique_ptr to the instance of the CostHeuristic instance according to the type given in parameter
   * @param costHeuristicType the type of CostHeuristic to instanciate
   * @return the unique_ptr to the instance of the CostHeuristic instance according to the type given in parameter
   */
  std::unique_ptr<CostHeuristic> createCostHeuristic(const RAOOptions::CostHeuristicType & costHeuristicType);
  virtual ~CostHeuristicFactory();
private:

};

}}}}
