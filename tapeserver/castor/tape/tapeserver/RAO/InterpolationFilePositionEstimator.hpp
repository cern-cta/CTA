/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "FilePositionEstimator.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "catalogue/MediaType.hpp"
#include <vector>

namespace castor::tape::tapeserver::rao {

/**
 * This class implements an interpolation file position estimator.
 * The file position is determined by interpolating the position according to the
 * tape end of wrap positions and its mediatype.
 * @param endOfWrapPositions the end of wrap positions of the mounted tape
 * @param mediaType the media type of the mounted tape
 */
class InterpolationFilePositionEstimator : public FilePositionEstimator{
public:
  InterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition> & endOfWrapPositions, const cta::catalogue::MediaType & mediaType);
  FilePositionInfos getFilePosition(const cta::RetrieveJob& job) const override;
  virtual ~InterpolationFilePositionEstimator() = default;

  static const uint64_t c_blockSize = 256 * 1024;

private:
  std::vector<drive::endOfWrapPosition> m_endOfWrapPositions;
  cta::catalogue::MediaType m_mediaType;

  /**
   * In order for this position estimator to work, we need to check that the mediatype given in the constructor contains
   * the information we need.
   */
  void checkMediaTypeConsistency();

  /**
   * Determine the physical position of the blockId passed in parameter
   * @param blockId the blockId to determine its physical position
   */
  Position getPhysicalPosition(const uint64_t blockId) const;
  /**
   * Determine the wrap number in which the blockId passed in parameter is located
   * @param blockId the blockId to determine its wrap number
   */
  uint64_t determineWrapNb(const uint64_t blockId) const;
  /**
   * This method determine the longitudinal position (LPos) of the blockId
   * passed in parameter
   * @param blockId the blockId to determine its longitudinal position
   * @param wrapNumber the wrapNumber where the blockId is located
   * @return the longitudinal position that has a value comprised between mediaType.minLPos and mediaType.maxLPos
   */
  uint64_t determineLPos(const uint64_t blockId, const uint64_t wrapNumber) const;
  /**
   * Determine the blockId of the end of the file passed in parameter
   * @param file the file to determine its end blockId
   * @return the blockId of the end of the file passed in parameter
   */
  uint64_t determineEndBlockId(const cta::common::dataStructures::TapeFile & file) const;
};

} // namespace castor::tape::tapeserver::rao
