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
#include "common/Timer.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

SLTFRAOAlgorithm::SLTFRAOAlgorithm() {}

SLTFRAOAlgorithm::SLTFRAOAlgorithm(std::unique_ptr<FilePositionEstimator> & filePositionEstimator, std::unique_ptr<CostHeuristic> & costHeuristic):m_filePositionEstimator(std::move(filePositionEstimator)),m_costHeuristic(std::move(costHeuristic)) {}

std::vector<uint64_t> SLTFRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) {
  std::vector<uint64_t> ret;
  //Determine all the files position
  cta::utils::Timer t;
  cta::utils::Timer totalTimer;
  SLTFRAOAlgorithm::RAOFilesContainer files = computeAllFilesPosition(jobs);
  m_raoTimings.insertAndReset("computeAllFilesPositionTime",t);
  //Perform a Short Locate Time First algorithm on the files
  ret = performSLTF(files);
  m_raoTimings.insertAndReset("performSLTFTime",t);
  m_raoTimings.insertAndReset("totalTime",totalTimer);
  return ret;
}


SLTFRAOAlgorithm::~SLTFRAOAlgorithm() {
}


SLTFRAOAlgorithm::Builder::Builder(const RAOParams& data, drive::DriveInterface * drive, cta::catalogue::Catalogue * catalogue):m_raoParams(data),m_drive(drive),m_catalogue(catalogue){
  m_algorithm.reset(new SLTFRAOAlgorithm());
}

std::unique_ptr<SLTFRAOAlgorithm> SLTFRAOAlgorithm::Builder::build() {
  initializeFilePositionEstimator();
  initializeCostHeuristic();
  return std::move(m_algorithm);
}

void SLTFRAOAlgorithm::Builder::initializeFilePositionEstimator() {
  switch(m_raoParams.getRAOAlgorithmOptions().getFilePositionEstimatorType()){
    case RAOOptions::FilePositionEstimatorType::interpolation: {
      std::string vid = m_raoParams.getMountedVid();
      cta::utils::Timer t;
      cta::catalogue::MediaType tapeMediaType = m_catalogue->getMediaTypeByVid(vid);
      m_algorithm->m_raoTimings.insertAndReset("catalogueGetMediaTypeByVidTime",t);
      std::vector<drive::endOfWrapPosition> endOfWrapPositions = m_drive->getEndOfWrapPositions();
      m_algorithm->m_raoTimings.insertAndReset("getEndOfWrapPositionsTime",t);
      RAOHelpers::improveEndOfLastWrapPositionIfPossible(endOfWrapPositions);
      m_algorithm->m_raoTimings.insertAndReset("improveEndOfWrapPositionsIfPossibleTime",t);
      m_algorithm->m_filePositionEstimator.reset(new InterpolationFilePositionEstimator(endOfWrapPositions,tapeMediaType));
      break;
    }
    default:
      throw cta::exception::Exception("In SLTFRAOAlgorithm::Builder::initializeFilePositionEstimator() unable to instanciate an estimator to estimate the position of the files on tape.");
      break;
  }
}

void SLTFRAOAlgorithm::Builder::initializeCostHeuristic() {
  switch(m_raoParams.getRAOAlgorithmOptions().getCostHeuristicType()){
    case RAOOptions::CostHeuristicType::cta:
    {
      m_algorithm->m_costHeuristic.reset(new CTACostHeuristic());
      break;
    }
    default:
      throw cta::exception::Exception("In SLTFRAOAlgorithm::Builder::initializeCostHeuristic() unknown type of cost heuristic.");
  }
}

SLTFRAOAlgorithm::RAOFilesContainer SLTFRAOAlgorithm::computeAllFilesPosition(const std::vector<std::unique_ptr<cta::RetrieveJob> >& jobs) const {
  SLTFRAOAlgorithm::RAOFilesContainer files;
  for(uint64_t i = 0; i < jobs.size(); ++i){
    files.insert({i,RAOFile(i,m_filePositionEstimator->getFilePosition(*(jobs.at(i))))});
  }
  //Create a dummy file that starts at the beginning of the tape (blockId = 0) (the SLTF algorithm will start from this file)
  std::unique_ptr<cta::RetrieveJob> dummyRetrieveJob = createFakeRetrieveJobForFileAtBeginningOfTape();
  files.insert({jobs.size(),RAOFile(jobs.size(),m_filePositionEstimator->getFilePosition(*dummyRetrieveJob))});
  return files;
}

void SLTFRAOAlgorithm::computeCostBetweenFileAndOthers(RAOFile & file, const SLTFRAOAlgorithm::RAOFilesContainer & otherFiles) const {
  FilePositionInfos filePositionInfos = file.getFilePositionInfos();
  for(auto &otherFile: otherFiles) {
    double distance = m_costHeuristic->getCost(filePositionInfos,otherFile.second.getFilePositionInfos());
    file.addDistanceToFile(distance,otherFile.second);
  }
}

std::vector<uint64_t> SLTFRAOAlgorithm::performSLTF(SLTFRAOAlgorithm::RAOFilesContainer & files) const {
  std::vector<uint64_t> solution;
  //Start from the fake file that is at the beginning of the tape (end of the files container)
  RAOFile firstFile = files.rbegin()->second;
  files.erase(firstFile.getIndex());
  computeCostBetweenFileAndOthers(firstFile,files);
  uint64_t closestFileIndex = firstFile.getClosestFileIndex();
  solution.push_back(closestFileIndex);
  while(!files.empty()){
    RAOFile currentFile = files.at(closestFileIndex);
    files.erase(currentFile.getIndex());
    if(files.size()){
      computeCostBetweenFileAndOthers(currentFile,files);
      closestFileIndex = currentFile.getClosestFileIndex();
      solution.push_back(closestFileIndex);
    }
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

std::string SLTFRAOAlgorithm::getName() const {
  return "sltf";
}

}}}}