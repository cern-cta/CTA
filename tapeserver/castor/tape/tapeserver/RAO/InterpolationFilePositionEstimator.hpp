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

#pragma once

#include "FilePositionEstimator.hpp"
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "catalogue/MediaType.hpp"
#include <vector>

namespace castor { namespace tape { namespace tapeserver { namespace rao {

class InterpolationFilePositionEstimator : public FilePositionEstimator{
public:
  InterpolationFilePositionEstimator(const std::vector<drive::endOfWrapPosition> & endOfWrapPositions, const cta::catalogue::MediaType & mediaType);
  FilePosition getFilePosition(const cta::RetrieveJob& job) const override;
  virtual ~InterpolationFilePositionEstimator();
  
  static const uint64_t c_blockSize = 256000;
private:
  Position getPhysicalPosition(const uint64_t blockId) const;
  uint64_t determineWrapNb(const uint64_t blockId) const;
  uint64_t determineLPos(const uint64_t blockId, const uint64_t wrapNumber) const;
  uint64_t determineEndBlock(const cta::common::dataStructures::TapeFile & file) const;
  std::vector<drive::endOfWrapPosition> m_endOfWrapPositions;
  cta::catalogue::MediaType m_mediaType;
};

}}}}