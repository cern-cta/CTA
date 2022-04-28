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
#include <gtest/gtest.h>

#include <vector>
#include <map>

#include "InterpolationFilePositionEstimator.hpp"
#include "RAOHelpers.hpp"
#include "SLTFRAOAlgorithm.hpp"
#include "CTACostHeuristic.hpp"

namespace unitTests {
  
  using namespace castor::tape::tapeserver;
  
  class RAOTestEnvironment : public ::testing::Environment {
  public:
    static std::vector<drive::endOfWrapPosition> getLTO7MEndOfWrapPositions() {
      std::vector<drive::endOfWrapPosition> ret;
      ret.push_back({0,208310,0});
      ret.push_back({1,416271,0});
      ret.push_back({2,624562,0});
      ret.push_back({3,633521,0});
      return ret;
    }
    
    static cta::catalogue::MediaType getLTO7MMediaType() {
      cta::catalogue::MediaType mediaType;
      mediaType.name = "LTO7M";
      mediaType.minLPos = 2696;
      mediaType.maxLPos = 171097;
      mediaType.nbWraps = 112;
      return mediaType;
    }
    
    static std::unique_ptr<cta::RetrieveJob> createRetrieveJobForRAOTests(const uint64_t blockId, const uint8_t copyNb, const uint64_t fseq, const uint64_t fileSize){
      std::unique_ptr<cta::RetrieveJob> ret;
      cta::common::dataStructures::ArchiveFile archiveFile;
      cta::common::dataStructures::TapeFile tapeFile;
      tapeFile.blockId = blockId;
      tapeFile.copyNb = copyNb;
      tapeFile.fSeq = fseq;
      tapeFile.fileSize = fileSize;
      archiveFile.tapeFiles.push_back(tapeFile);
      cta::common::dataStructures::RetrieveRequest retrieveRequest;
      ret.reset(new cta::RetrieveJob(nullptr,retrieveRequest,archiveFile,1,cta::PositioningMethod::ByBlock));
      return ret;
    }
    
    static std::vector<std::unique_ptr<cta::RetrieveJob>> generateRetrieveJobs() {
      std::vector<std::unique_ptr<cta::RetrieveJob>> ret;
      ret.emplace_back(createRetrieveJobForRAOTests(0,1,1,10));
      return ret;
    }
    
    static std::vector<std::unique_ptr<cta::RetrieveJob>> generateRetrieveJobsForSLTF(){
      std::vector<std::unique_ptr<cta::RetrieveJob>> ret;
      ret.emplace_back(createRetrieveJobForRAOTests(420000,1,11,1000000000)); //0
      ret.emplace_back(createRetrieveJobForRAOTests(300000,1,12,1000000000)); //1
      ret.emplace_back(createRetrieveJobForRAOTests(30528,1,9,1000000000)); //2
      ret.emplace_back(createRetrieveJobForRAOTests(26712,1,8,1000000000)); //3
      ret.emplace_back(createRetrieveJobForRAOTests(0,1,1,1000000000)); //4
      ret.emplace_back(createRetrieveJobForRAOTests(19080,1,6,1000000000)); //5
      ret.emplace_back(createRetrieveJobForRAOTests(15264,1,5,1000000000)); //6
      ret.emplace_back(createRetrieveJobForRAOTests(34344,1,10,1000000000)); //7
      return ret;
    }
    
  };
  
  class RAOTest: public ::testing::Test {
  protected:
    
    void SetUp() {
      
    }

    void TearDown() {
    }

  }; 

  TEST_F(RAOTest,InterpolationFilePositionEstimatorWrap0Test){
    /**
     * This test tests the InterpolationFilePositionEstimator::getFilePosition() method
     */
    
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getLTO7MEndOfWrapPositions();
    cta::catalogue::MediaType mediaType = RAOTestEnvironment::getLTO7MMediaType();
    
    rao::InterpolationFilePositionEstimator estimator(eowPositions,mediaType);
    {
      std::unique_ptr<cta::RetrieveJob> retrieveJob = RAOTestEnvironment::createRetrieveJobForRAOTests(0,1,1,100000000);
      rao::FilePositionInfos positionFile = estimator.getFilePosition(*retrieveJob);
      //The LPOS start position of the file should be equal to the minLPos of the LTO7 media type
      rao::Position startPositionFile = positionFile.getBeginningPosition();
      ASSERT_EQ(0,startPositionFile.getWrap());
      ASSERT_EQ(mediaType.minLPos.value(), startPositionFile.getLPos());
      
      rao::Position endPositionFile = positionFile.getEndPosition();
      ASSERT_EQ(0,endPositionFile.getWrap());
      
      auto jobTapeFile = retrieveJob->selectedTapeFile();
      uint64_t endPositionBlockId = jobTapeFile.blockId + (jobTapeFile.fileSize / rao::InterpolationFilePositionEstimator::c_blockSize) + 1;
      uint64_t expectedEndPositionLPos = mediaType.minLPos.value() + endPositionBlockId * (mediaType.maxLPos.value() - mediaType.minLPos.value()) / eowPositions.at(0).blockId;
      ASSERT_EQ(expectedEndPositionLPos,endPositionFile.getLPos());
      
      ASSERT_EQ(0,positionFile.getBeginningBand());
      ASSERT_EQ(0,positionFile.getEndBand());
      ASSERT_EQ(0,positionFile.getBeginningLandingZone());
      ASSERT_EQ(0,positionFile.getEndLandingZone());
    }
  }
  
  TEST_F(RAOTest,InterpolationFilePositionEstimatorWrap1Test){
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getLTO7MEndOfWrapPositions();
    cta::catalogue::MediaType mediaType = RAOTestEnvironment::getLTO7MMediaType();
    
    rao::InterpolationFilePositionEstimator estimator(eowPositions,mediaType);
    
    {
      //Now create a retrieve job that has a blockId greater than the first wrap end of wrap position
      std::unique_ptr<cta::RetrieveJob> retrieveJob = RAOTestEnvironment::createRetrieveJobForRAOTests(210000,1,1,100000000);
      rao::FilePositionInfos positionFile = estimator.getFilePosition(*retrieveJob);
      double b_max = (double) eowPositions.at(1).blockId - (double) eowPositions.at(0).blockId; 
      auto jobTapeFile = retrieveJob->selectedTapeFile();
      uint64_t fileBlockId = jobTapeFile.blockId - eowPositions.at(0).blockId;
      uint64_t expectedLPos = mediaType.maxLPos.value() - fileBlockId * (mediaType.maxLPos.value() - mediaType.minLPos.value()) / b_max;
      ASSERT_EQ(1,positionFile.getBeginningPosition().getWrap());
      ASSERT_EQ(expectedLPos,positionFile.getBeginningPosition().getLPos());
      
      rao::Position endPositionFile = positionFile.getEndPosition();
      ASSERT_EQ(1,endPositionFile.getWrap());
      
      uint64_t endPositionBlockId = jobTapeFile.blockId + (jobTapeFile.fileSize / rao::InterpolationFilePositionEstimator::c_blockSize) + 1 - eowPositions.at(0).blockId;
      uint64_t expectedEndPositionLPos = mediaType.maxLPos.value() - endPositionBlockId * (mediaType.maxLPos.value() - mediaType.minLPos.value()) / b_max;
      ASSERT_EQ(expectedEndPositionLPos,endPositionFile.getLPos());
      
      ASSERT_EQ(0,positionFile.getBeginningBand());
      ASSERT_EQ(0,positionFile.getEndBand());
      ASSERT_EQ(1,positionFile.getBeginningLandingZone());
      ASSERT_EQ(1,positionFile.getEndLandingZone());
    }
  }
  
  TEST_F(RAOTest,InterpolationFilePositionEstimatorThrowsExceptionIfBlockIdGreaterThanLastEOWP){
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getLTO7MEndOfWrapPositions();
    cta::catalogue::MediaType mediaType = RAOTestEnvironment::getLTO7MMediaType();
    
    rao::InterpolationFilePositionEstimator estimator(eowPositions,mediaType);
    
    {
      std::unique_ptr<cta::RetrieveJob> retrieveJob = RAOTestEnvironment::createRetrieveJobForRAOTests(100000000,1,3,30);
      ASSERT_THROW(estimator.getFilePosition(*retrieveJob),cta::exception::Exception);
    }
  }
  
  TEST_F(RAOTest,RAOHelpersImproveLastEOWPIfPossible){
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getLTO7MEndOfWrapPositions();
    drive::endOfWrapPosition eowpBeforeImprovement = eowPositions.at(eowPositions.size()-1);
    rao::RAOHelpers::improveEndOfLastWrapPositionIfPossible(eowPositions);
    drive::endOfWrapPosition eowpAfterImprovement = eowPositions.at(eowPositions.size()-1);
    ASSERT_LT(eowpBeforeImprovement.blockId,eowpAfterImprovement.blockId);
  }
  
  TEST_F(RAOTest,RAOHelpersDetermineBand){
    uint32_t nbWrapsLTO7 = 112;
    ASSERT_EQ(0,rao::RAOHelpers::determineBand(nbWrapsLTO7,0));
    ASSERT_EQ(0,rao::RAOHelpers::determineBand(nbWrapsLTO7,27));
    ASSERT_EQ(1,rao::RAOHelpers::determineBand(nbWrapsLTO7,28));
    ASSERT_EQ(3,rao::RAOHelpers::determineBand(nbWrapsLTO7,111));
    ASSERT_THROW(rao::RAOHelpers::determineBand(nbWrapsLTO7,112),cta::exception::Exception);
  }
  
  TEST_F(RAOTest,RAOHelpersDetermineLandingZone){
    auto LTO7MediaType = RAOTestEnvironment::getLTO7MMediaType();
    uint64_t minTapeLpos = LTO7MediaType.minLPos.value();
    uint64_t maxTapeLpos = LTO7MediaType.maxLPos.value();
    
    uint64_t file1Lpos = minTapeLpos + 50;
    uint64_t file2Lpos = maxTapeLpos - 50;
    uint64_t file3Lpos = (maxTapeLpos - minTapeLpos) / 2;
    
    ASSERT_EQ(0,rao::RAOHelpers::determineLandingZone(minTapeLpos,maxTapeLpos,file1Lpos));
    ASSERT_EQ(1,rao::RAOHelpers::determineLandingZone(minTapeLpos,maxTapeLpos,file2Lpos));
    ASSERT_EQ(1,rao::RAOHelpers::determineLandingZone(minTapeLpos,maxTapeLpos,file3Lpos));
  }
  
  TEST_F(RAOTest,RAOHelpersDoesWrapChange){
    rao::Position pos1;
    pos1.setWrap(0);
    rao::FilePositionInfos file1;
    file1.setEndPosition(pos1);
    rao::Position pos2;
    pos2.setWrap(1);
    rao::FilePositionInfos file2;
    file2.setBeginningPosition(pos2);
    
    ASSERT_TRUE(rao::RAOHelpers::doesWrapChange(file1,file2));
    pos1.setWrap(5);
    file1.setEndPosition(pos1);
    pos2.setWrap(5);
    file2.setBeginningPosition(pos2);
    ASSERT_FALSE(rao::RAOHelpers::doesWrapChange(file1,file2));
  }
  
  TEST_F(RAOTest,RAOHelpersDoesBandChange){
    rao::FilePositionInfos file1;
    file1.setEndBand(0);
    rao::FilePositionInfos file2;
    file2.setBeginningBand(1);
    ASSERT_TRUE(rao::RAOHelpers::doesBandChange(file1,file2));
    file1.setEndBand(4);
    file2.setBeginningBand(4);
    ASSERT_FALSE(rao::RAOHelpers::doesBandChange(file1,file2));
  }
  
  TEST_F(RAOTest,RAOHelpersDoesLandingZoneChange){
    rao::FilePositionInfos file1;
    file1.setEndLandingZone(0);
    rao::FilePositionInfos file2;
    file2.setBeginningLandingZone(1);
    ASSERT_TRUE(rao::RAOHelpers::doesLandingZoneChange(file1,file2));
    file1.setEndLandingZone(0);
    file2.setBeginningLandingZone(0);
    ASSERT_FALSE(rao::RAOHelpers::doesLandingZoneChange(file1,file2));
  }
  
  TEST_F(RAOTest,RAOHelpersDoesDirectionChange){
    rao::Position pos1;
    pos1.setWrap(0);
    
    rao::FilePositionInfos file1;
    file1.setEndPosition(pos1);
    
    rao::Position pos2;
    pos2.setWrap(1);
    
    rao::FilePositionInfos file2;
    file2.setBeginningPosition(pos2);
    //end of file 1 wrap = 0, beginning of file 2 wrap = 1, direction change = true
    ASSERT_TRUE(rao::RAOHelpers::doesDirectionChange(file1,file2));
    pos2.setWrap(0);
    file2.setBeginningPosition(pos2);
    //end of file 1 wrap = 0, beginning of file2 wrap = 0, direction change = false
    ASSERT_FALSE(rao::RAOHelpers::doesDirectionChange(file1,file2));
    pos2.setWrap(2);
    file2.setBeginningPosition(pos2);
    //end of file 1 wrap = 0, beginning of file2 wrap = 2, direction change = false
    ASSERT_FALSE(rao::RAOHelpers::doesDirectionChange(file1,file2));
    pos1.setWrap(1);
    file1.setEndPosition(pos1);
    pos2.setWrap(3);
    file2.setBeginningPosition(pos2);
    //end of file 1 wrap = 1, beginning of file2 wrap = 3, direction change = false
    ASSERT_FALSE(rao::RAOHelpers::doesDirectionChange(file1,file2));
  }
  
  TEST_F(RAOTest,RAOHelpersDoesStepBack){
    rao::FilePositionInfos file1;
    rao::FilePositionInfos file2;
    
    rao::Position endFile1Pos;
    
    rao::Position beginningFile2Pos;
    
    endFile1Pos.setWrap(0);
    endFile1Pos.setLPos(1);
    file1.setEndPosition(endFile1Pos);
    beginningFile2Pos.setWrap(0);
    beginningFile2Pos.setLPos(0);
    file2.setBeginningPosition(beginningFile2Pos);
    //endOfFileLpos > beginningFile2Lpos, file1 and file 2 are on the same wrap = 0: stepback = true
    ASSERT_TRUE(rao::RAOHelpers::doesStepBack(file1,file2));
    endFile1Pos.setWrap(1);
    endFile1Pos.setLPos(0);
    file1.setEndPosition(endFile1Pos);
    beginningFile2Pos.setWrap(1);
    beginningFile2Pos.setLPos(1);
    file2.setBeginningPosition(beginningFile2Pos);
    //endOfFile1 and beginningOfFile2 Wrap = 1, endOfFile1Lpos < beginningOfFile2Lpos: steback = true
    ASSERT_TRUE(rao::RAOHelpers::doesStepBack(file1,file2));
    endFile1Pos.setWrap(0);
    endFile1Pos.setLPos(0);
    file1.setEndPosition(endFile1Pos);
    beginningFile2Pos.setWrap(0);
    beginningFile2Pos.setLPos(1);
    file2.setBeginningPosition(beginningFile2Pos);
    //endOfFile1 and beginningOfFile2 Wrap = 0, endOfFile1Lpos < beginningOfFile2Lpos: stepback = false
    ASSERT_FALSE(rao::RAOHelpers::doesStepBack(file1,file2));
    endFile1Pos.setWrap(1);
    endFile1Pos.setLPos(1);
    file1.setEndPosition(endFile1Pos);
    beginningFile2Pos.setWrap(1);
    beginningFile2Pos.setLPos(0);
    file2.setBeginningPosition(beginningFile2Pos);
    //endOfFile1 and beginningOfFile2 wrap = 1, endOfFile1Lpos > beginningOfFile2Lpos: stepback = false
    ASSERT_FALSE(rao::RAOHelpers::doesStepBack(file1,file2));
  }
  
  TEST_F(RAOTest,RAOHelpersComputeDistance){
    rao::FilePositionInfos file1;
    rao::FilePositionInfos file2;
    
    rao::Position endFile1Position;
    endFile1Position.setLPos(2);
    rao::Position beginningFile2Position;
    beginningFile2Position.setLPos(2);
    file1.setEndPosition(endFile1Position);
    file2.setBeginningPosition(beginningFile2Position);
    ASSERT_EQ(0,rao::RAOHelpers::computeLongitudinalDistance(file1,file2));
    
    endFile1Position.setLPos(2);
    beginningFile2Position.setLPos(1);
    file1.setEndPosition(endFile1Position);
    file2.setBeginningPosition(beginningFile2Position);
    ASSERT_EQ(1,rao::RAOHelpers::computeLongitudinalDistance(file1,file2));
    
    endFile1Position.setLPos(1);
    beginningFile2Position.setLPos(2);
    file1.setEndPosition(endFile1Position);
    file2.setBeginningPosition(beginningFile2Position);
    ASSERT_EQ(1,rao::RAOHelpers::computeLongitudinalDistance(file1,file2));
  }
  
  TEST_F(RAOTest, RAOSLTFAlgorithm){
    auto jobs = RAOTestEnvironment::generateRetrieveJobsForSLTF();
    std::unique_ptr<rao::FilePositionEstimator> filePositionEstimator;
    std::unique_ptr<rao::CostHeuristic> costHeuristic;
    filePositionEstimator.reset(new rao::InterpolationFilePositionEstimator(RAOTestEnvironment::getLTO7MEndOfWrapPositions(),RAOTestEnvironment::getLTO7MMediaType()));
    costHeuristic.reset(new rao::CTACostHeuristic());
    std::unique_ptr<rao::SLTFRAOAlgorithm> sltfRAOAlgorithm = std::make_unique<rao::SLTFRAOAlgorithm>(filePositionEstimator,costHeuristic);
    std::vector<uint64_t> raoOrder = sltfRAOAlgorithm->performRAO(jobs);
    std::vector<uint64_t> expectedRAOOrder = {4,6,5,3,2,7,0,1};
    ASSERT_EQ(expectedRAOOrder,raoOrder);
  }
}
