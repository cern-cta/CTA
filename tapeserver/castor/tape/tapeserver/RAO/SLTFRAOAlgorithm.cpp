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
#include "CTACostHeuristic.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

SLTFRAOAlgorithm::SLTFRAOAlgorithm() {}

std::vector<uint64_t> SLTFRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) {
  std::vector<uint64_t> ret;
  //Determine all the files position
  std::vector<RAOFile> files = computeAllFilesPosition(jobs);
  computeCostBetweenAllFiles(files);
  
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
  switch(m_data.getRAOAlgorithmOptions().getCostHeuristicType()){
    case RAOOptions::CostHeuristicType::cta:
    {
      m_algorithm->m_costHeuristic.reset(new CTACostHeuristic());
      break;
    }
    default:
      throw cta::exception::Exception("In SLTFRAOAlgorithm::Builder::initializeCostHeuristic() unknown type of cost heuristic.");
  }
}

std::vector<RAOFile> SLTFRAOAlgorithm::computeAllFilesPosition(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) const {
  std::vector<RAOFile> files;
  for(uint64_t i = 0; i < jobs.size(); ++i){
    files.push_back(RAOFile(i,m_filePositionEstimator->getFilePosition(*(jobs.at(i)))));
  }
  //Create a dummy file that starts at the beginning of the tape (the SLTF algorithm will start from this file)
  std::unique_ptr<cta::RetrieveJob> dummyRetrieveJob = createFakeRetrieveJobForFileAtBeginningOfTape();
  files.push_back(RAOFile(jobs.size(),m_filePositionEstimator->getFilePosition(*dummyRetrieveJob)));
  return files;
}

void SLTFRAOAlgorithm::computeCostBetweenAllFiles(std::vector<RAOFile> & files) const {
  for(unsigned int i = 0; i < files.size(); ++i){
    auto & sourceFile = files.at(i);
    for(unsigned int j = 0; j < files.size(); ++j){
      //We don't want the distance between the file and itself
      if(i != j){
        auto & destinationFile = files.at(j);
        double distanceToFileJ = m_costHeuristic->getCost(sourceFile.getFilePositionInfos(),destinationFile.getFilePositionInfos());
        files.at(i).addDistanceToFile(distanceToFileJ,destinationFile);
      }
    }
  }
}

std::vector<uint64_t> SLTFRAOAlgorithm::performSLTF(const std::vector<RAOFile>& files) const {
  //TODO
  std::vector<uint64_t> solution;
  //Start from the fake file that is at the beginning of the tape
  auto firstFile = files.back();
  solution.push_back(firstFile.getClosestFileIndex());
  for(unsigned int i = 0; i < files.size() - 1; ++i){
    solution.push_back(files.at(i).getClosestFileIndex());
  }
  return solution;
}


std::unique_ptr<cta::RetrieveJob> SLTFRAOAlgorithm::createFakeRetrieveJobForFileAtBeginningOfTape() const {
  std::unique_ptr<cta::RetrieveJob> ret;
  cta::common::dataStructures::ArchiveFile archiveFile;
  cta::common::dataStructures::TapeFile tapeFile;
  tapeFile.blockId = 0;
  tapeFile.copyNb = 1;
  tapeFile.fSeq = 0;
  tapeFile.fileSize = 0;
  archiveFile.tapeFiles.push_back(tapeFile);
  cta::common::dataStructures::RetrieveRequest retrieveRequest;
  ret.reset(new cta::RetrieveJob(nullptr,retrieveRequest,archiveFile,1,cta::PositioningMethod::ByBlock));
  return ret;
}
}}}}