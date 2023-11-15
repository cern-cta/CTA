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

#pragma once

#include <vector>
#include "castor/tape/tapeserver/drive/DriveInterface.hpp"
#include "FilePositionInfos.hpp"

namespace castor::tape::tapeserver::rao {

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

  /**
   * Determine the band on which the block located in the wrapNumber belongs to
   * @param nbWrapsOnTape the total number of wraps the tape contains
   * @param wrapNumber the wrapNumber where the block is located
   * @return the band number corresponding to the wrap number of the block passed in parameter
   */
  static uint8_t determineBand(uint32_t nbWrapsOnTape, uint32_t wrapNumber);
  
  /**
   * Determine the landing zone (0 or 1) on which the blockLpos is located
   * @param minTapeLpos the minimum longitudinal position of the tape
   * @param maxTapeLpos the maximum longitudinal position of the tape
   * @param blockLpos the logical longitudinal position where the block is located 
   * @return the landing zone on which the blockLpos is located
   */
  static uint8_t determineLandingZone(uint64_t minTapeLpos, uint64_t maxTapeLpos, uint64_t blockLpos);
  
  /**
   * Returns true if there is a wrap change when going from file1 to file2
   * @param file1 the source file
   * @param file2 the destination file
   * @return true if there is a wrap change when going from file1 to file2
   */
  static bool doesWrapChange(const FilePositionInfos & file1, const FilePositionInfos & file2);
  
  /**
   * Returns true if there is a band change when going from file1 to file2
   * @param file1 the source file
   * @param file2 the destination file
   * @return true if there is a band change when going from file1 to file2 
   */
  static bool doesBandChange(const FilePositionInfos & file1, const FilePositionInfos & file2);
  
  /**
   * Returns true if there is a landing zone change when going from file1 to file2
   * @param file1 the source file
   * @param file2 the destination file
   * @return true if there is a landing zone change when going from file1 to file2 
   */
  static bool doesLandingZoneChange(const FilePositionInfos & file1, const FilePositionInfos & file2);
  
  /**
   * Returns true if there is a direction change when going from file1 to file2
   * @param file1 the source file
   * @param file2 the destination file
   * @return true if there is a direction change when going from file1 to file2 
   */
  static bool doesDirectionChange(const FilePositionInfos & file1, const FilePositionInfos & file2);
  
  /**
   * Returns true if there is a step back when going from file1 to file2
   * @param file1 the source file
   * @param file2 the destination file
   * @return true if there is a step back when going from file1 to file2 
   */
  static bool doesStepBack(const FilePositionInfos & file1, const FilePositionInfos & file2);
  
  /**
   * Compute the longitudinal distance to go from the file1 to the file2
   * @param file1 the source file
   * @param file2 the destination file
   * @return the longitudinal distance to go from the file1 to the file2 
   */
  static uint64_t computeLongitudinalDistance(const FilePositionInfos & file1, const FilePositionInfos & file2);
  
};

} // namespace castor::tape::tapeserver::rao
