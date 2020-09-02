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

#include "FilePositionInfos.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

FilePositionInfos::FilePositionInfos() {
}

FilePositionInfos::FilePositionInfos(const FilePositionInfos& other){
  if(this != &other){
    m_startPosition = other.m_startPosition;
    m_endPosition = other.m_endPosition;
    m_startBand = other.m_startBand;
    m_endBand = other.m_endBand;
    m_startLandingZone = other.m_startLandingZone;
    m_endLandingZone = other.m_endLandingZone;
  }
}

FilePositionInfos & FilePositionInfos::operator =(const FilePositionInfos& other){
  if(this != &other){
    m_startPosition = other.m_startPosition;
    m_endPosition = other.m_endPosition;
    m_startBand = other.m_startBand;
    m_endBand = other.m_endBand;
    m_startLandingZone = other.m_startLandingZone;
    m_endLandingZone = other.m_endLandingZone;
  }
  return *this;
}

FilePositionInfos::~FilePositionInfos() {
}

void FilePositionInfos::setStartPosition(const Position & startPosition) {
  m_startPosition = startPosition;
}

void FilePositionInfos::setEndPosition(const Position& endPosition) {
  m_endPosition = endPosition;
}

Position FilePositionInfos::getStartPosition() const {
  return m_startPosition;
}

Position FilePositionInfos::getEndPosition() const {
  return m_endPosition;
}

void FilePositionInfos::setStartBand(const uint8_t startBand){
  m_startBand = startBand;
}

void FilePositionInfos::setEndBand(const uint8_t endBand){
  m_endBand = endBand;
}

void FilePositionInfos::setStartLandingZone(const uint8_t startLandingZone){
  m_startLandingZone = startLandingZone;
}

void FilePositionInfos::setEndLandingZone(const uint8_t endLandingZone){
  m_endLandingZone = endLandingZone;
}

uint8_t FilePositionInfos::getStartBand() const {
  return m_startBand;
}

uint8_t FilePositionInfos::getEndBand() const {
  return m_endBand;
}

uint8_t FilePositionInfos::getStartLandingZone() const {
  return m_startLandingZone;
}

uint8_t FilePositionInfos::getEndLandingZone() const {
  return m_endLandingZone;
}



}}}}