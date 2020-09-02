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

#include <vector>
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {
  
class RAOHelpers {
public:
  /**
   * In the LTO documentation, the Read End Of Wrap Position (REOWP) command will not give the last
   * wrap correct EOWP. It will give the last blockId written by the drive
   * on the tape
   * This method will modify the last wrap EOWP (blockId) from the vector of EOWP passed in parameter
   * to set it to the penultimate wrap EOWP + the mean of the number of blocks each wrap contains 
   */
  static void improveEndOfLastWrapPositionIfPossible(std::vector<drive::endOfWrapPosition> & endOfWrapPositions);

  static uint8_t determineBand(uint32_t nbWrapsOnTape, uint32_t wrapNumber);
  
  static uint8_t determineLandingZone(uint64_t minTapeLPos, uint64_t maxTapeLpos, uint64_t fileLPos);
  
};

}}}}