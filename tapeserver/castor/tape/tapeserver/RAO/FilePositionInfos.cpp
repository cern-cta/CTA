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

#include "FilePositionInfos.hpp"

namespace castor::tape::tapeserver::rao {

FilePositionInfos::FilePositionInfos(const FilePositionInfos& other){
  if(this != &other){
    m_beginningPosition = other.m_beginningPosition;
    m_endPosition = other.m_endPosition;
    m_beginningBand = other.m_beginningBand;
    m_endBand = other.m_endBand;
    m_beginningLandingZone = other.m_beginningLandingZone;
    m_endLandingZone = other.m_endLandingZone;
  }
}

FilePositionInfos & FilePositionInfos::operator =(const FilePositionInfos& other){
  if(this != &other){
    m_beginningPosition = other.m_beginningPosition;
    m_endPosition = other.m_endPosition;
    m_beginningBand = other.m_beginningBand;
    m_endBand = other.m_endBand;
    m_beginningLandingZone = other.m_beginningLandingZone;
    m_endLandingZone = other.m_endLandingZone;
  }
  return *this;
}

void FilePositionInfos::setBeginningPosition(const Position & beginningPosition) {
  m_beginningPosition = beginningPosition;
}

void FilePositionInfos::setEndPosition(const Position& endPosition) {
  m_endPosition = endPosition;
}

Position FilePositionInfos::getBeginningPosition() const {
  return m_beginningPosition;
}

Position FilePositionInfos::getEndPosition() const {
  return m_endPosition;
}

void FilePositionInfos::setBeginningBand(const uint8_t beginningBand){
  m_beginningBand = beginningBand;
}

void FilePositionInfos::setEndBand(const uint8_t endBand){
  m_endBand = endBand;
}

void FilePositionInfos::setBeginningLandingZone(const uint8_t beginningLandingZone){
  m_beginningLandingZone = beginningLandingZone;
}

void FilePositionInfos::setEndLandingZone(const uint8_t endLandingZone){
  m_endLandingZone = endLandingZone;
}

uint8_t FilePositionInfos::getBeginningBand() const {
  return m_beginningBand;
}

uint8_t FilePositionInfos::getEndBand() const {
  return m_endBand;
}

uint8_t FilePositionInfos::getBeginningLandingZone() const {
  return m_beginningLandingZone;
}

uint8_t FilePositionInfos::getEndLandingZone() const {
  return m_endLandingZone;
}



} // namespace castor::tape::tapeserver::rao
