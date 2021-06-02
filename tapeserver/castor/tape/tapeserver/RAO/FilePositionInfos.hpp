/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
 * @license        This program is free software: you can redistribute it and/or modify
 *                 it under the terms of the GNU General Public License as published by
 *                 the Free Software Foundation, either version 3 of the License, or
 *                 (at your option) any later version.
 *
 *                 This program is distributed in the hope that it will be useful,
 *                 but WITHOUT ANY WARRANTY; without even the implied warranty of
 *                 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *                 GNU General Public License for more details.
 *
 *                 You should have received a copy of the GNU General Public License
 *                 along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include "Position.hpp"


namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * This class holds the position informations about a file : its physical position (start and end) and
 * informations about the band and the landing zone of the beginning and the end of the file.
 */
class FilePositionInfos {
public:
  /**
   * Default constructor
   */
  FilePositionInfos();
  FilePositionInfos(const FilePositionInfos & other);
  FilePositionInfos & operator=(const FilePositionInfos & other);
  /**
   * Set the beginning of the file position
   * @param beginningPosition the beginning of the file position
   */
  void setBeginningPosition(const Position & beginningPosition);
  /**
   * Set the end of the file position
   * @param endPosition the end of the file position
   */
  void setEndPosition(const Position & endPosition);
  /**
   * Set the beginning of the file band
   * @param beginningBand the beginning of the file band
   */
  void setBeginningBand(const uint8_t beginningBand);
  /**
   * Set the end of the file band
   * @param endBand the end of the file band
   */
  void setEndBand(const uint8_t endBand);
  /**
   * Set the beginning of the file landing zone
   * @param startLandingZone
   */
  void setBeginningLandingZone(const uint8_t beginningLandingZone);
  /**
   * Set the end of the file landing zone
   * @param endLandingZone
   */
  void setEndLandingZone(const uint8_t endLandingZone);
  /**
   * Get the physical position of the beginning of the file
   */
  Position getBeginningPosition() const;
  /**
   * Get the physical position of the end of the file
   */
  Position getEndPosition() const;
  /**
   * Get the beginning of the file band
   */
  uint8_t getBeginningBand() const;
  /**
   * Get the end of the file band
   */
  uint8_t getEndBand() const;
  /**
   * Get the beginning of the file landing zone
   */
  uint8_t getBeginningLandingZone() const;
  /**
   * Get the end of the file landing zone
   */
  uint8_t getEndLandingZone() const;
  virtual ~FilePositionInfos();
private:
  Position m_beginningPosition;
  Position m_endPosition;
  uint8_t m_beginningBand = 0;
  uint8_t m_endBand = 0;
  uint8_t m_beginningLandingZone = 0;
  uint8_t m_endLandingZone = 0;
};

}}}}