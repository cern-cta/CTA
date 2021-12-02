/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2003-2021 CERN
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

#include "RAOFile.hpp"

#include <algorithm>

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
  m_distancesWithOtherFiles.push_back(DistanceToFile(distance,file.getIndex()));
}

uint64_t RAOFile::getClosestFileIndex() const {
  //The closest file is the one that has the lower cost
  auto minElementItor = std::min_element(m_distancesWithOtherFiles.begin(), m_distancesWithOtherFiles.end());
  //This method should never throw as there is always at least two files in a RAO batch
  return minElementItor->getDestinationFileIndex();
}

bool RAOFile::operator <(const RAOFile& other) const {
  return m_index < other.m_index;
}

bool RAOFile::operator ==(const RAOFile& other) const {
  return m_index == other.getIndex();
}

RAOFile::~RAOFile() {
}

RAOFile::DistanceToFile::DistanceToFile(const double cost, const uint64_t destinationFileIndex):m_cost(cost),m_destinationFileIndex(destinationFileIndex){
}

bool RAOFile::DistanceToFile::operator <(const DistanceToFile& other) const {
  return m_cost < other.m_cost;
}

double RAOFile::DistanceToFile::getCost() const{
  return m_cost;
}

uint64_t RAOFile::DistanceToFile::getDestinationFileIndex() const {
  return m_destinationFileIndex;
}
}}}}
