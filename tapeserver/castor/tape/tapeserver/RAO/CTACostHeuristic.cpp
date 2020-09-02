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
#include <cmath>

namespace castor { namespace tape { namespace tapeserver { namespace rao {

CTACostHeuristic::CTACostHeuristic() {
}

CTACostHeuristic::~CTACostHeuristic() {
}

double CTACostHeuristic::getCost(const FilePositionInfos& file1, const FilePositionInfos& file2) const {
  
  double cost = 0.0;
  
  Position endFile1Position = file1.getEndPosition();
  Position startFile2Position = file2.getStartPosition();
  uint64_t endFile1LPos = endFile1Position.getLPos();
  uint64_t startFile2LPos = startFile2Position.getLPos();
  uint32_t endFile1Wrap = endFile1Position.getWrap();
  uint32_t startFile2Wrap = startFile2Position.getWrap();
  uint8_t endFile1Band = file1.getEndBand();
  uint8_t startFile2Band = file2.getStartBand();
  uint8_t endFile1LandingZone = file1.getEndLandingZone();
  uint8_t startFile2LandingZone = file2.getStartLandingZone();
  
  uint64_t distance = std::abs(endFile1Position.getLPos() - startFile2Position.getLPos());
  //TODO : Refactor with the RAOHelpers
  int wrapChange = (endFile1Wrap != startFile2Wrap);
  int bandChange = (endFile1Band != startFile2Band);
  int landingZoneChange = (endFile1LandingZone != startFile2LandingZone);
  int directionChange = ((endFile1Wrap % 2) != (startFile2Wrap % 2));
  
  int stepBack = 0;
  
  if(endFile1Wrap == startFile2Wrap){
    if((endFile1Wrap % 2) == 0 && (startFile2LPos < endFile1LPos)){
      stepBack = 1;
    } else if ((endFile1Wrap % 2) == 1 && (startFile2LPos > endFile1LPos)){
      stepBack = 1;
    }
  }
  
  cost = 4.29 + wrapChange * 6.69 + bandChange * 3.2 + landingZoneChange * (-6.04) + directionChange * 5.22 + stepBack * 11.32 + distance * 0.0006192;
  return cost;
}

}}}}
