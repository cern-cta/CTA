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

#include "InterpolationFilePositionEstimator.hpp"
#include "RAOHelpers.hpp"

namespace castor::tape::tapeserver::rao {

InterpolationFilePositionEstimator::InterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition> & endOfWrapPositions, 
  const cta::catalogue::MediaType & mediaType): m_endOfWrapPositions(endOfWrapPositions), m_mediaType(mediaType) {
  checkMediaTypeConsistency();
}

FilePositionInfos InterpolationFilePositionEstimator::getFilePosition(const cta::RetrieveJob& job) const {
  FilePositionInfos ret;
  //Get the tape file informations
  cta::common::dataStructures::TapeFile tapeFile = job.selectedTapeFile();
  uint64_t beginningBlockId = tapeFile.blockId;
  //Set physical positions
  Position beginningPosition = getPhysicalPosition(beginningBlockId);
  ret.setBeginningPosition(beginningPosition);
  uint64_t endBlockId = determineEndBlockId(tapeFile);
  Position endPosition = getPhysicalPosition(endBlockId);
  ret.setEndPosition(endPosition);
  //Set band informations
  ret.setBeginningBand(RAOHelpers::determineBand(m_mediaType.nbWraps.value(),beginningPosition.getWrap()));
  ret.setEndBand(RAOHelpers::determineBand(m_mediaType.nbWraps.value(),endPosition.getWrap()));
  //Set landing zone informations
  ret.setBeginningLandingZone(RAOHelpers::determineLandingZone(m_mediaType.minLPos.value(),m_mediaType.maxLPos.value(),beginningPosition.getLPos()));
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
  if(m_endOfWrapPositions.size() == 1){
    //We are sure that if we have only 1 wrap, the file will be on the wrap 0
    return 0;
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

uint64_t InterpolationFilePositionEstimator::determineEndBlockId(const cta::common::dataStructures::TapeFile& file) const {
  return file.blockId + (file.fileSize / c_blockSize) + 1; 
}


void InterpolationFilePositionEstimator::checkMediaTypeConsistency(){
  if(!m_mediaType.minLPos || !m_mediaType.maxLPos){
    std::string errorMsg = "In InterpolationFilePositionEstimator::checkMediaTypeConsistency(), the media type (" + m_mediaType.name + ") associated to the tape does not give informations about the minLPos and maxLPos.";
    throw cta::exception::Exception(errorMsg);
  }
  if(!m_mediaType.nbWraps){
    std::string errorMsg = "In InterpolationFilePositionEstimator::checkMediaTypeConsistency(), the media type (" + m_mediaType.name + ") associated to the tape mounted does not give informations about the number of wraps the media contains.";
    throw cta::exception::Exception(errorMsg);
  }
}

} // namespace castor::tape::tapeserver::rao
