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

#include "SLTFRAOAlgorithm.hpp"
#include "InterpolationFilePositionEstimator.hpp"
#include "RAOHelpers.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

SLTFRAOAlgorithm::SLTFRAOAlgorithm() {}

std::vector<uint64_t> SLTFRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) {
  std::vector<uint64_t> ret;
  return ret;
}


SLTFRAOAlgorithm::~SLTFRAOAlgorithm() {
}


SLTFRAOAlgorithm::Builder::Builder(const RAOParams& data, drive::DriveInterface * drive, cta::catalogue::Catalogue * catalogue):m_data(data),m_drive(drive),m_catalogue(catalogue){
  m_algorithm.reset(new SLTFRAOAlgorithm());
}

std::unique_ptr<SLTFRAOAlgorithm> SLTFRAOAlgorithm::Builder::build() {
  initializeFilePositionEstimator();
  initializeCostHeuristic();
  return std::move(m_algorithm);
}

void SLTFRAOAlgorithm::Builder::initializeFilePositionEstimator() {
  switch(m_data.getRAOAlgorithmOptions().getFilePositionEstimatorType()){
    case RAOOptions::FilePositionEstimatorType::interpolation: {
      std::string vid = m_data.getMountedVid();
      cta::catalogue::MediaType tapeMediaType = m_catalogue->getMediaTypeByVid(vid);
      std::vector<drive::endOfWrapPosition> endOfWrapPositions = m_drive->getEndOfWrapPositions();
      RAOHelpers::improveEndOfLastWrapPositionIfPossible(endOfWrapPositions);
      m_algorithm->m_filePositionEstimator.reset(new InterpolationFilePositionEstimator(endOfWrapPositions,tapeMediaType));
      break;
    }
    default:
      throw cta::exception::Exception("In SLTFRAOAlgorithm::Builder::initializeFilePositionEstimator() unable to instanciate an estimator to estimate the position of the files on tape.");
      break;
  }
}




void SLTFRAOAlgorithm::Builder::initializeCostHeuristic() {
  /*switch(m_data.getRAOAlgorithmOptions().getCostHeuristicType()){
    case RAOOptions::CostHeuristicType::cta:
    {
      m_algorithm->m_costHeuristic.reset(new )
    }
  }*/
}



}}}}