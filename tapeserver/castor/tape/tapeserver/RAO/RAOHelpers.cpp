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

#include "RAOHelpers.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

  void RAOHelpers::improveEndOfLastWrapPositionIfPossible(std::vector<drive::endOfWrapPosition>& endOfWrapPositions) {
    uint64_t nbBlocksPerWrap = 0;
    auto nbEndOfWrapPositions = endOfWrapPositions.size();
    if(nbEndOfWrapPositions < 2){
      //No improvement possible
      return;
    }
    for(uint64_t i = 1; i < nbEndOfWrapPositions; ++i){
      nbBlocksPerWrap += (endOfWrapPositions.at(i).blockId - endOfWrapPositions.at(i-1).blockId);
    }
    uint64_t meanNbBlocksPerWrap = nbBlocksPerWrap / nbEndOfWrapPositions;
    endOfWrapPositions[nbEndOfWrapPositions-1].blockId = endOfWrapPositions.at(nbEndOfWrapPositions-2).blockId + meanNbBlocksPerWrap;
  }
  
  uint8_t RAOHelpers::determineBand(uint32_t nbWrapsOnTape, uint32_t wrapNumber){
    //As a tape has always 4 bands the following formula will give the band number to which the wrapNumber
    //belongs to
    return (wrapNumber / (nbWrapsOnTape / 4));
  }
  
  uint8_t RAOHelpers::determineLandingZone(uint64_t minTapeLPos, uint64_t maxTapeLpos, uint64_t fileLPos){
    uint64_t mid = (maxTapeLpos - minTapeLPos) / 2;
    return fileLPos < mid ? 0 : 1;
  }

}}}}