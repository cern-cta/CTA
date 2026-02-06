/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "SLTFRAOAlgorithm.hpp"

#include "CTACostHeuristic.hpp"
#include "CostHeuristicFactory.hpp"
#include "FilePositionEstimatorFactory.hpp"
#include "InterpolationFilePositionEstimator.hpp"
#include "RAOHelpers.hpp"
#include "common/utils/Timer.hpp"

namespace castor::tape::tapeserver::rao {

std::vector<uint64_t> SLTFRAOAlgorithm::performRAO(const std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs) {
  std::vector<uint64_t> ret;
  //Determine all the files position
  cta::utils::Timer t;
  cta::utils::Timer totalTimer;
  SLTFRAOAlgorithm::RAOFilesContainer files = computeAllFilesPosition(jobs);
  m_raoTimings.insertAndReset("computeAllFilesPositionTime", t);
  //Perform a Short Locate Time First algorithm on the files
  ret = performSLTF(files);
  m_raoTimings.insertAndReset("performSLTFTime", t);
  m_raoTimings.insertAndReset("RAOAlgorithmTime", totalTimer);
  return ret;
}

SLTFRAOAlgorithm::Builder::Builder(const RAOParams& data) : m_raoParams(data) {
  m_algorithm.reset(new SLTFRAOAlgorithm());
}

std::unique_ptr<SLTFRAOAlgorithm> SLTFRAOAlgorithm::Builder::build() {
  initializeFilePositionEstimator();
  initializeCostHeuristic();
  return std::move(m_algorithm);
}

void SLTFRAOAlgorithm::Builder::initializeFilePositionEstimator() {
  RAOOptions::FilePositionEstimatorType filePositionType =
    m_raoParams.getRAOAlgorithmOptions().getFilePositionEstimatorType();
  switch (filePositionType) {
    case RAOOptions::FilePositionEstimatorType::interpolation: {
      if (m_catalogue != nullptr && m_drive != nullptr) {
        m_algorithm->m_filePositionEstimator =
          FilePositionEstimatorFactory::createInterpolationFilePositionEstimator(m_raoParams.getMountedVid(),
                                                                                 m_catalogue,
                                                                                 m_drive,
                                                                                 m_algorithm->m_raoTimings);
      } else {
        //If the catalogue or the drive is not set, we don't have any way to give the InterpolationFilePositionEstimator the end of wrap position
        //infos and the media type. Throw an exception
        throw cta::exception::Exception(
          "In SLTFRAOAlgorithm::Builder::initializeFilePositionEstimator(), the drive and the catalogue are needed to "
          "build the InterpolationFilePositionEstimator.");
      }
      break;
    }
  }
}

void SLTFRAOAlgorithm::Builder::initializeCostHeuristic() {
  CostHeuristicFactory factory;
  m_algorithm->m_costHeuristic =
    factory.createCostHeuristic(m_raoParams.getRAOAlgorithmOptions().getCostHeuristicType());
}

SLTFRAOAlgorithm::RAOFilesContainer
SLTFRAOAlgorithm::computeAllFilesPosition(const std::vector<std::unique_ptr<cta::RetrieveJob>>& jobs) const {
  SLTFRAOAlgorithm::RAOFilesContainer files;
  for (uint64_t i = 0; i < jobs.size(); ++i) {
    files.insert({i, RAOFile(i, m_filePositionEstimator->getFilePosition(*(jobs.at(i))))});
  }
  //Create a dummy file that starts at the beginning of the tape (blockId = 0) (the SLTF algorithm will start from this file)
  std::unique_ptr<cta::RetrieveJob> dummyRetrieveJob = createFakeRetrieveJobForFileAtBeginningOfTape();
  files.insert({jobs.size(), RAOFile(jobs.size(), m_filePositionEstimator->getFilePosition(*dummyRetrieveJob))});
  return files;
}

void SLTFRAOAlgorithm::computeCostBetweenFileAndOthers(RAOFile& file,
                                                       const SLTFRAOAlgorithm::RAOFilesContainer& otherFiles) const {
  FilePositionInfos filePositionInfos = file.getFilePositionInfos();
  for (auto& otherFile : otherFiles) {
    double distance = m_costHeuristic->getCost(filePositionInfos, otherFile.second.getFilePositionInfos());
    file.addDistanceToFile(distance, otherFile.second);
  }
}

std::vector<uint64_t> SLTFRAOAlgorithm::performSLTF(SLTFRAOAlgorithm::RAOFilesContainer& files) const {
  std::vector<uint64_t> solution;
  //Start from the fake file that is at the beginning of the tape (end of the files container)
  RAOFile firstFile = files.rbegin()->second;
  files.erase(firstFile.getIndex());
  computeCostBetweenFileAndOthers(firstFile, files);
  uint64_t closestFileIndex = firstFile.getClosestFileIndex();
  solution.push_back(closestFileIndex);
  while (!files.empty()) {
    RAOFile currentFile = files.at(closestFileIndex);
    files.erase(currentFile.getIndex());
    if (files.size()) {
      computeCostBetweenFileAndOthers(currentFile, files);
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
  ret.reset(new cta::RetrieveJob(nullptr, retrieveRequest, archiveFile, 1, cta::PositioningMethod::ByBlock));
  return ret;
}

}  // namespace castor::tape::tapeserver::rao
