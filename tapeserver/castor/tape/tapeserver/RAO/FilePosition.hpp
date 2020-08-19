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

class FilePosition {
public:
  FilePosition();
  void setStartPosition(const Position & startPosition);
  void setEndPosition(const Position & endPosition);
  Position getStartPosition() const;
  Position getEndPosition() const;
  virtual ~FilePosition();
private:
  Position m_startPosition;
  Position m_endPosition;
};

}}}}