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

#include "FilePosition.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

FilePosition::FilePosition() {
}

FilePosition::~FilePosition() {
}

void FilePosition::setStartPosition(const Position & startPosition) {
  m_startPosition = startPosition;
}

void FilePosition::setEndPosition(const Position& endPosition) {
  m_endPosition = endPosition;
}

Position FilePosition::getStartPosition() const {
  return m_startPosition;
}

Position FilePosition::getEndPosition() const {
  return m_endPosition;
}


}}}}