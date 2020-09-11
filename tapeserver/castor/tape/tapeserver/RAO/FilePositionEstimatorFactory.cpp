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

#include "FilePositionEstimatorFactory.hpp"
#include "common/Timer.hpp"
#include "RAOHelpers.hpp"
#include "InterpolationFilePositionEstimator.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

  std::unique_ptr<FilePositionEstimator> FilePositionEstimatorFactory::createInterpolationFilePositionEstimator(const std::string & vid, cta::catalogue::Catalogue *catalogue, drive::DriveInterface *drive, cta::log::TimingList &tl){
    std::unique_ptr<FilePositionEstimator> ret;
    cta::utils::Timer t;
    cta::catalogue::MediaType tapeMediaType = catalogue->getMediaTypeByVid(vid);
    tl.insertAndReset("catalogueGetMediaTypeByVidTime",t);
    std::vector<drive::endOfWrapPosition> endOfWrapPositions = drive->getEndOfWrapPositions();
    tl.insertAndReset("getEndOfWrapPositionsTime",t);
    RAOHelpers::improveEndOfLastWrapPositionIfPossible(endOfWrapPositions);
    tl.insertAndReset("improveEndOfWrapPositionsIfPossibleTime",t);
    ret.reset(new InterpolationFilePositionEstimator(endOfWrapPositions,tapeMediaType));
    return ret;
  }
  
  std::unique_ptr<FilePositionEstimator> FilePositionEstimatorFactory::createInterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition> & endOfWrapPositions, cta::catalogue::MediaType & mediaType){
    std::unique_ptr<FilePositionEstimator> ret;
    ret.reset(new InterpolationFilePositionEstimator(endOfWrapPositions,mediaType));
    return ret;
  }

}}}}