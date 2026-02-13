/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "FilePositionEstimatorFactory.hpp"

#include "InterpolationFilePositionEstimator.hpp"
#include "RAOHelpers.hpp"
#include "catalogue/Catalogue.hpp"
#include "common/utils/Timer.hpp"

namespace castor::tape::tapeserver::rao {

std::unique_ptr<FilePositionEstimator>
FilePositionEstimatorFactory::createInterpolationFilePositionEstimator(const std::string& vid,
                                                                       cta::catalogue::Catalogue* catalogue,
                                                                       drive::DriveInterface* drive,
                                                                       cta::log::TimingList& tl) {
  std::unique_ptr<FilePositionEstimator> ret;
  cta::utils::Timer t;
  cta::catalogue::MediaType tapeMediaType = catalogue->MediaType()->getMediaTypeByVid(vid);
  tl.insertAndReset("catalogueGetMediaTypeByVidTime", t);
  std::vector<drive::endOfWrapPosition> endOfWrapPositions = drive->getEndOfWrapPositions();
  tl.insertAndReset("getEndOfWrapPositionsTime", t);
  RAOHelpers::improveEndOfLastWrapPositionIfPossible(endOfWrapPositions);
  tl.insertAndReset("improveEndOfWrapPositionsIfPossibleTime", t);
  ret.reset(new InterpolationFilePositionEstimator(endOfWrapPositions, tapeMediaType));
  return ret;
}

std::unique_ptr<FilePositionEstimator> FilePositionEstimatorFactory::createInterpolationFilePositionEstimator(
  const std::vector<drive::endOfWrapPosition>& endOfWrapPositions,
  cta::catalogue::MediaType& mediaType) {
  std::unique_ptr<FilePositionEstimator> ret;
  ret.reset(new InterpolationFilePositionEstimator(endOfWrapPositions, mediaType));
  return ret;
}

}  // namespace castor::tape::tapeserver::rao
