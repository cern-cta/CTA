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

#include "RAOFile.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

RAOFile::RAOFile(const uint64_t index, const FilePositionInfos & filePositionInfos):m_index(index),m_filePositionInfos(filePositionInfos) {
}

RAOFile::RAOFile(const RAOFile & other) {
  if(this != &other){
    m_index = other.m_index;
    m_filePositionInfos = other.m_filePositionInfos;
  }
}

RAOFile & RAOFile::operator=(const RAOFile & other){
  if(this != &other){
    m_index = other.m_index;
    m_filePositionInfos = other.m_filePositionInfos;
  }
  return *this;
}

uint64_t RAOFile::getIndex() const{
  return m_index;
}

FilePositionInfos RAOFile::getFilePositionInfos() const{
  return m_filePositionInfos;
}

void RAOFile::addDistanceToFile(const double distance, const RAOFile& file){
  m_distancesWithOtherFiles.push_back(std::make_pair(distance,file.getIndex()));
}

RAOFile::~RAOFile() {
}

}}}}