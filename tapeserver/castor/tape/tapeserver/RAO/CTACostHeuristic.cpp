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

#include "CTACostHeuristic.hpp"
#include "RAOHelpers.hpp"

namespace castor::tape::tapeserver::rao {

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

} // namespace castor::tape::tapeserver::rao
