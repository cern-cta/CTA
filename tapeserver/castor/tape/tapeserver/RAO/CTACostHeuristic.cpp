/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#include "CTACostHeuristic.hpp"
#include "RAOHelpers.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

CTACostHeuristic::CTACostHeuristic() {
}

CTACostHeuristic::~CTACostHeuristic() {
}

double CTACostHeuristic::getCost(const FilePositionInfos & file1, const FilePositionInfos & file2) const {
  
  double cost = 0.0;
  
  uint64_t distance = RAOHelpers::computeLongitudinalDistance(file1,file2);

  int wrapChange = RAOHelpers::doesWrapChange(file1,file2);
  int bandChange = RAOHelpers::doesBandChange(file1,file2);
  int landingZoneChange = RAOHelpers::doesLandingZoneChange(file1,file2);
  int directionChange = RAOHelpers::doesDirectionChange(file1,file2);
  
  int stepBack = RAOHelpers::doesStepBack(file1,file2);
  
  cost = 4.29 + wrapChange * 6.69 + bandChange * 3.2 + landingZoneChange * (-6.04) + directionChange * 5.22 + stepBack * 11.32 + distance * 0.0006192;
  return cost;
}

}}}}
