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

#include "RAOHelpers.hpp"
#include "tapeserver/castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor::tape::tapeserver::rao {

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
  
  uint8_t RAOHelpers::determineLandingZone(uint64_t minTapeLpos, uint64_t maxTapeLpos, uint64_t blockLpos){
    uint64_t mid = (maxTapeLpos - minTapeLpos) / 2;
    return blockLpos < mid ? 0 : 1;
  }
  
  bool RAOHelpers::doesWrapChange(const FilePositionInfos & file1PositionInfos, const FilePositionInfos & file2PositionInfos){
    return (file1PositionInfos.getEndPosition().getWrap() != file2PositionInfos.getBeginningPosition().getWrap());
  }
  
  bool RAOHelpers::doesBandChange(const FilePositionInfos & file1PositionInfos, const FilePositionInfos & file2PositionInfos){
    return (file1PositionInfos.getEndBand() != file2PositionInfos.getBeginningBand());
  }
  
  bool RAOHelpers::doesLandingZoneChange(const FilePositionInfos & file1PositionInfos, const FilePositionInfos & file2PositionInfos){
    return (file1PositionInfos.getEndLandingZone() != file2PositionInfos.getBeginningLandingZone());
  }
  
  bool RAOHelpers::doesDirectionChange(const FilePositionInfos & file1PositionInfos, const FilePositionInfos & file2PositionInfos){
    return ((file1PositionInfos.getEndPosition().getWrap() % 2) != (file2PositionInfos.getBeginningPosition().getWrap() % 2));
  }

  bool RAOHelpers::doesStepBack(const FilePositionInfos & file1PositionInfos, const FilePositionInfos & file2PositionInfos){
    Position endOfFile1Position = file1PositionInfos.getEndPosition();
    Position beginningOfFile2Position = file2PositionInfos.getBeginningPosition();
    uint32_t endOfFile1Wrap = endOfFile1Position.getWrap();
    uint64_t endOfFile1LPos = endOfFile1Position.getLPos();
    uint32_t beginningOfFile2Wrap = beginningOfFile2Position.getWrap();
    uint64_t beginningOfFile2LPos = beginningOfFile2Position.getLPos();
    bool stepBack = false;
    if(endOfFile1Wrap == beginningOfFile2Wrap){
      if((endOfFile1Wrap % 2 == 0) && (endOfFile1LPos > beginningOfFile2LPos)){
        stepBack = true;
      } else if((endOfFile1Wrap % 2 == 1) && (endOfFile1LPos < beginningOfFile2LPos)){
        stepBack = true;
      }
    }
    return stepBack;
  }
  
  uint64_t RAOHelpers::computeLongitudinalDistance(const FilePositionInfos & file1PositionInfos, const FilePositionInfos & file2PositionInfos) {
    uint64_t endOfFile1Lpos = file1PositionInfos.getEndPosition().getLPos();
    uint64_t beginningOfFile2Lpos = file2PositionInfos.getBeginningPosition().getLPos();
    return endOfFile1Lpos > beginningOfFile2Lpos ? endOfFile1Lpos - beginningOfFile2Lpos : beginningOfFile2Lpos - endOfFile1Lpos;
  }
  
} // namespace castor::tape::tapeserver::rao
