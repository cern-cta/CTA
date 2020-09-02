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

#include "InterpolationFilePositionEstimator.hpp"
#include "RAOHelpers.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

InterpolationFilePositionEstimator::InterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition> & endOfWrapPositions, 
  const cta::catalogue::MediaType & mediaType): m_endOfWrapPositions(endOfWrapPositions), m_mediaType(mediaType) {
  checkMediaTypeConsistency();
}

InterpolationFilePositionEstimator::~InterpolationFilePositionEstimator() {
}

FilePositionInfos InterpolationFilePositionEstimator::getFilePosition(const cta::RetrieveJob& job) const {
  FilePositionInfos ret;
  cta::common::dataStructures::TapeFile tapeFile = job.selectedTapeFile();
  uint64_t startBlock = tapeFile.blockId;
  Position startPosition = getPhysicalPosition(startBlock);
  ret.setStartPosition(startPosition);
  uint64_t endBlock = determineEndBlock(tapeFile);
  Position endPosition = getPhysicalPosition(endBlock);
  ret.setEndPosition(endPosition);
  if(!m_mediaType.nbWraps){
    std::string errorMsg = "In InterpolationFilePositionEstimator::getFilePosition(), the media type " + m_mediaType.name+" does not contain any information about the number of wraps the media has.";
    throw cta::exception::Exception(errorMsg);
  }
  ret.setStartBand(RAOHelpers::determineBand(m_mediaType.nbWraps.value(),startPosition.getWrap()));
  ret.setEndBand(RAOHelpers::determineBand(m_mediaType.nbWraps.value(),endPosition.getWrap()));
  ret.setStartLandingZone(RAOHelpers::determineLandingZone(m_mediaType.minLPos.value(),m_mediaType.maxLPos.value(),startPosition.getLPos()));
  ret.setEndLandingZone(RAOHelpers::determineLandingZone(m_mediaType.minLPos.value(),m_mediaType.maxLPos.value(),endPosition.getLPos()));
  return ret;
}

Position InterpolationFilePositionEstimator::getPhysicalPosition(const uint64_t blockId) const {
  Position ret;
  ret.setWrap(determineWrapNb(blockId));
  ret.setLPos(determineLPos(blockId,ret.getWrap()));
  return ret;
}

uint64_t InterpolationFilePositionEstimator::determineWrapNb(const uint64_t blockId) const {
  if(m_endOfWrapPositions.empty()){
    std::string errorMsg = "In InterpolationFilePositionEstimator::determineWrapNb(), unable to find the wrap number of the blockId " + std::to_string(blockId) + " because no EOWP informations have been found.";
    throw cta::exception::Exception(errorMsg);
  }
  auto eowpItor = m_endOfWrapPositions.begin();
  while(eowpItor != m_endOfWrapPositions.end() && blockId > eowpItor->blockId){
    eowpItor++;
  }
  if(eowpItor == m_endOfWrapPositions.end()){
    eowpItor--;
    std::string errorMsg = "In InterpolationFilePositionEstimator::determineWrapNb(), the blockId " + std::to_string(blockId) + " is greater than the last wrap EOWP blockId ("+std::to_string(eowpItor->blockId)+")"; 
    throw cta::exception::Exception(errorMsg);
  }
  return eowpItor->wrapNumber;
}

uint64_t InterpolationFilePositionEstimator::determineLPos(const uint64_t blockId, const uint64_t wrapNumber) const {
  uint64_t retLpos;
  uint64_t fileBlockId = blockId;
  uint64_t minLpos = m_mediaType.minLPos.value();
  uint64_t maxLpos = m_mediaType.maxLPos.value();
  uint64_t b_max = m_endOfWrapPositions.at(wrapNumber).blockId;
  if(wrapNumber > 0){
    drive::endOfWrapPosition previousWrapPositionInfos = m_endOfWrapPositions.at(wrapNumber-1); 
    b_max -= previousWrapPositionInfos.blockId;
    fileBlockId -= previousWrapPositionInfos.blockId;
  }
  if(wrapNumber % 2 == 0){
    retLpos = minLpos + fileBlockId * (maxLpos - minLpos) / (double)b_max;
  } else {
    retLpos = maxLpos - fileBlockId * (maxLpos - minLpos) / (double)b_max;
  }
  return retLpos;
}

uint64_t InterpolationFilePositionEstimator::determineEndBlock(const cta::common::dataStructures::TapeFile& file) const{
  return file.blockId + (file.fileSize / c_blockSize) + 1; 
}


void InterpolationFilePositionEstimator::checkMediaTypeConsistency(){
  if(!m_mediaType.minLPos || !m_mediaType.maxLPos){
    std::string errorMsg = "In InterpolationFilePositionEstimator::checkMediaTypeConsistency(), the media type (" + m_mediaType.name + ") associated to the tape does not give informations about the minLPos and maxLPos.";
    throw cta::exception::Exception(errorMsg);
  }
  if(!m_mediaType.nbWraps){
    std::string errorMsg = "In InterpolationFilePositionEstimator::checkMediaTypeConsistency(), the media type (" + m_mediaType.name + ") associated to the tape does not give informations about number of wraps the media contains.";
    throw cta::exception::Exception(errorMsg);
  }
}

}}}}