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
#include <gtest/gtest.h>

#include <vector>
#include <map>

#include "InterpolationFilePositionEstimator.hpp"
#include "common/make_unique.hpp"
#include "RAOHelpers.hpp"

namespace unitTests {
  
  using namespace castor::tape::tapeserver;
  
  class RAOTestEnvironment : public ::testing::Environment {
  public:
    static std::vector<drive::endOfWrapPosition> getEndOfWrapPositions() {
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
    
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getEndOfWrapPositions();
    cta::catalogue::MediaType mediaType = RAOTestEnvironment::getLTO7MMediaType();
    
    rao::InterpolationFilePositionEstimator estimator(eowPositions,mediaType);
    {
      std::unique_ptr<cta::RetrieveJob> retrieveJob = RAOTestEnvironment::createRetrieveJobForRAOTests(0,1,1,10);
      rao::FilePosition positionFile = estimator.getFilePosition(*retrieveJob);
      //The LPOS start position of the file should be equal to the minLPos of the LTO7 media type
      rao::Position startPositionFile = positionFile.getStartPosition();
      ASSERT_EQ(0,startPositionFile.getWrap());
      ASSERT_EQ(mediaType.minLPos.value(), startPositionFile.getLPos());
      
      rao::Position endPositionFile = positionFile.getEndPosition();
      ASSERT_EQ(0,endPositionFile.getWrap());
      
      /*TODO : TO BE CONTINUED
       * auto jobTapeFile = retrieveJob->selectedTapeFile();
      uint64_t endPositionBlockId = mediaType.minLPos.value() + (jobTapeFile.blockId / rao::InterpolationFilePositionEstimator::c_blockSize) + 1;
      ASSERT_EQ(mediaType.minLPos.value() + endPositionBlockId,endPositionFile.getLPos());*/
    }
    
    {
      std::unique_ptr<cta::RetrieveJob> retrieveJob = RAOTestEnvironment::createRetrieveJobForRAOTests(2,1,2,25);
      rao::FilePosition positionFile = estimator.getFilePosition(*retrieveJob);
      rao::Position startPositionFile = positionFile.getStartPosition();
      ASSERT_EQ(0,startPositionFile.getWrap());
      double b_max = (double) eowPositions.at(0).blockId;
      uint64_t expectedLPos = mediaType.minLPos.value() + retrieveJob->selectedTapeFile().blockId * (mediaType.maxLPos.value() - mediaType.minLPos.value()) / b_max;
      ASSERT_EQ(expectedLPos,positionFile.getStartPosition().getLPos());
    }
  }
  
  TEST_F(RAOTest,InterpolationFilePositionEstimatorWrap1Test){
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getEndOfWrapPositions();
    cta::catalogue::MediaType mediaType = RAOTestEnvironment::getLTO7MMediaType();
    
    rao::InterpolationFilePositionEstimator estimator(eowPositions,mediaType);
    
    {
      //Now create a retrieve job that has a blockId greater than the first wrap end of wrap position
     std::unique_ptr<cta::RetrieveJob> retrieveJob = RAOTestEnvironment::createRetrieveJobForRAOTests(210000,1,1,10);
      rao::FilePosition positionFile = estimator.getFilePosition(*retrieveJob);
      double b_max = (double) eowPositions.at(1).blockId - (double) eowPositions.at(0).blockId; 
      uint64_t fileBlockId = retrieveJob->selectedTapeFile().blockId - eowPositions.at(0).blockId;
      uint64_t expectedLPos = mediaType.maxLPos.value() - fileBlockId * (mediaType.maxLPos.value() - mediaType.minLPos.value()) / b_max;
      ASSERT_EQ(1,positionFile.getStartPosition().getWrap());
      ASSERT_EQ(expectedLPos,positionFile.getStartPosition().getLPos());
    }
  }
  
  TEST_F(RAOTest,InterpolationFilePositionEstimatorThrowsExceptionIfBlockIdGreaterThanLastEOWP){
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getEndOfWrapPositions();
    cta::catalogue::MediaType mediaType = RAOTestEnvironment::getLTO7MMediaType();
    
    rao::InterpolationFilePositionEstimator estimator(eowPositions,mediaType);
    
    {
      std::unique_ptr<cta::RetrieveJob> retrieveJob = RAOTestEnvironment::createRetrieveJobForRAOTests(100000000,1,3,30);
      ASSERT_THROW(estimator.getFilePosition(*retrieveJob),cta::exception::Exception);
    }
  }
  
  TEST_F(RAOTest,ImproveLastEOWPIfPossible){
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getEndOfWrapPositions();
    drive::endOfWrapPosition eowpBeforeImprovement = eowPositions.at(eowPositions.size()-1);
    rao::RAOHelpers::improveEndOfLastWrapPositionIfPossible(eowPositions);
    drive::endOfWrapPosition eowpAfterImprovement = eowPositions.at(eowPositions.size()-1);
    ASSERT_LT(eowpBeforeImprovement.blockId,eowpAfterImprovement.blockId);
  }
  
}
