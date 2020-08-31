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

namespace unitTests {
  
  using namespace castor::tape::tapeserver;
  
  class RAOTestEnvironment : public ::testing::Environment {
  public:
    static std::vector<drive::endOfWrapPosition> getEndOfWrapPositions() {
      std::vector<drive::endOfWrapPosition> ret;
      ret.push_back({0,208310,0});
      ret.push_back({1,416271,0});
      ret.push_back({2,624562,0});
      return ret;
    }
    
    static cta::catalogue::MediaType getLTO7MMediaType() {
      cta::catalogue::MediaType mediaType;
      mediaType.name = "LTO7M";
      mediaType.minLPos = 2696;
      mediaType.maxLPos = 171097;
      return mediaType;
    }
  };
  
  class RAOTest: public ::testing::Test {
  protected:
    
    void SetUp() {
      
    }

    void TearDown() {
    }

  }; 

  TEST_F(RAOTest,InterpolationFilePositionEstimatorTest){
    
    
    std::vector<drive::endOfWrapPosition> eowPositions = RAOTestEnvironment::getEndOfWrapPositions();
    cta::catalogue::MediaType mediaType = RAOTestEnvironment::getLTO7MMediaType();
  
    rao::InterpolationFilePositionEstimator estimator(eowPositions,mediaType);
    //TODO : GENERATE vector of cta::RetrieveJob
    
    ASSERT_TRUE(true);
  }
  
}
