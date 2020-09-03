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
    unsigned int nbBandsTape = 4;
    if(wrapNumber > nbWrapsOnTape - 1){
      std::string errorMsg = "In RAOHelpers::determineBand(), the wrapNumber (" + std::to_string(wrapNumber) + ") of the file is greater than the number of wraps the tape contains (" + std::to_string(nbWrapsOnTape) + ").";
      throw cta::exception::Exception(errorMsg);
    }
    return (wrapNumber / (nbWrapsOnTape / nbBandsTape));
  }
  
  uint8_t RAOHelpers::determineLandingZone(uint64_t minTapeLpos, uint64_t maxTapeLpos, uint64_t fileLpos){
    uint64_t mid = (maxTapeLpos - minTapeLpos) / 2;
    return fileLpos < mid ? 0 : 1;
  }
  
  bool RAOHelpers::doesWrapChange(uint32_t wrap1, uint32_t wrap2){
    return (wrap1 != wrap2);
  }
  
  bool RAOHelpers::doesBandChange(uint8_t band1, uint8_t band2){
    return (band1 != band2);
  }
  
  bool RAOHelpers::doesLandingZoneChange(uint8_t landingZone1, uint8_t landingZone2){
    return (landingZone1 != landingZone2);
  }
  
  bool RAOHelpers::doesDirectionChange(uint32_t wrap1, uint32_t wrap2){
    return ((wrap1 % 2) != (wrap2 % 2));
  }

  bool RAOHelpers::doesStepBack(uint32_t wrap1, uint64_t lpos1, uint32_t wrap2, uint64_t lpos2){
    bool stepBack = false;
    if(wrap1 == wrap2){
      if((wrap1 % 2 == 0) && (lpos1 > lpos2)){
        stepBack = true;
      } else if((wrap1 % 2 == 1) && (lpos1 < lpos2)){
        stepBack = true;
      }
    }
    return stepBack;
  }
  
}}}}