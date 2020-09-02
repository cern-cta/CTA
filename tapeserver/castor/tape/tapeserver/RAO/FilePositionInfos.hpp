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

#include "Position.hpp"


namespace castor { namespace tape { namespace tapeserver { namespace rao {

class FilePositionInfos {
public:
  FilePositionInfos();
  FilePositionInfos(const FilePositionInfos & other);
  FilePositionInfos & operator=(const FilePositionInfos & other);
  void setStartPosition(const Position & startPosition);
  void setEndPosition(const Position & endPosition);
  void setStartBand(const uint8_t startBand);
  void setEndBand(const uint8_t endBand);
  void setStartLandingZone(const uint8_t startLandingZone);
  void setEndLandingZone(const uint8_t endLandingZone);
  Position getStartPosition() const;
  Position getEndPosition() const;
  uint8_t getStartBand() const;
  uint8_t getEndBand() const;
  uint8_t getStartLandingZone() const;
  uint8_t getEndLandingZone() const;
  virtual ~FilePositionInfos();
private:
  Position m_startPosition;
  Position m_endPosition;
  uint8_t m_startBand;
  uint8_t m_endBand;
  uint8_t m_startLandingZone;
  uint8_t m_endLandingZone;
};

}}}}