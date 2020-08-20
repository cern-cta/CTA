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

namespace castor { namespace tape { namespace tapeserver { namespace rao {

InterpolationFilePositionEstimator::InterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition> & endOfWrapPositions, 
  const cta::catalogue::MediaType & mediaType): m_endOfWrapPositions(endOfWrapPositions), m_mediaType(mediaType) {
}

InterpolationFilePositionEstimator::~InterpolationFilePositionEstimator() {
}

FilePosition InterpolationFilePositionEstimator::getFilePosition(const cta::RetrieveJob& job) const {
  FilePosition ret;
  cta::common::dataStructures::TapeFile tapeFile = job.selectedTapeFile();
  uint64_t startBlock = tapeFile.blockId;
  Position startPosition = getPhysicalPosition(startBlock);
  ret.setStartPosition(startPosition);
  
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
  uint64_t fileLpos;
  cta::optional<uint64_t> minTapeLpos = m_mediaType.minLPos;
  cta::optional<uint64_t> maxTapeLpos = m_mediaType.maxLPos;
  uint64_t fileBlockId = blockId;
  if(!minTapeLpos || !maxTapeLpos){
    std::string errorMsg = "In InterpolationFilePositionEstimator::determineLPos(), the media type (" + m_mediaType.name + ") associated to the tape tape does not give informations about the minLPos and maxLPos.";
    throw cta::exception::Exception(errorMsg);
  }
  uint64_t minLpos = minTapeLpos.value();
  uint64_t maxLpos = maxTapeLpos.value();
  uint64_t b_max = m_endOfWrapPositions.at(wrapNumber).blockId;
  if(wrapNumber > 0){
    drive::endOfWrapPosition previousWrapPositionInfos = m_endOfWrapPositions.at(wrapNumber-1); 
    b_max -= previousWrapPositionInfos.blockId;
    fileBlockId -= previousWrapPositionInfos.blockId;
  }
  if(wrapNumber % 2 == 0){
    fileLpos = minLpos + fileBlockId * (maxLpos - minLpos) / b_max;
  } else {
    fileLpos = maxLpos - fileBlockId * (maxLpos - minLpos) / b_max;
  }
  return fileLpos;
}


}}}}