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

#include "catalogue/Catalogue.hpp"
#include "common/Timer.hpp"
#include "FilePositionEstimatorFactory.hpp"
#include "InterpolationFilePositionEstimator.hpp"
#include "RAOHelpers.hpp"

namespace castor::tape::tapeserver::rao {

  std::unique_ptr<FilePositionEstimator> FilePositionEstimatorFactory::createInterpolationFilePositionEstimator(const std::string & vid, cta::catalogue::Catalogue *catalogue, drive::DriveInterface *drive, cta::log::TimingList &tl){
    std::unique_ptr<FilePositionEstimator> ret;
    cta::utils::Timer t;
    cta::catalogue::MediaType tapeMediaType = catalogue->MediaType()->getMediaTypeByVid(vid);
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

} // namespace castor::tape::tapeserver::rao
