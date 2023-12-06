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

#include <utility>
#include <list>

#include "FilePositionInfos.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * This class represents an RAO file. It contains the index of the file in the vector
 * of jobs passed in the RAOAlgorithm::performRAO() method and the file position informations.
 * It also stores the distance this file has with other files.
 */  
class RAOFile {
public:
  RAOFile(const uint64_t index, const FilePositionInfos& filePositionInfos) :
    m_index(index), m_filePositionInfos(filePositionInfos) { }

  RAOFile(const RAOFile& other);
  virtual ~RAOFile() = default;
  RAOFile& operator=(const RAOFile& other);
  uint64_t getIndex() const;

  /**
   * Get the position informations about this file
   * @return the position informations about this file
   */
  FilePositionInfos getFilePositionInfos() const;
  /**
   * Add a distance between this file and another RAOFile
   * @param distance the distance to go from this file to another RAOFile
   * @param file the destination file
   */
  void addDistanceToFile(const double distance, const RAOFile & file);
  /**
   * Get the closest file index i.e the file to which the cost to go to is the lowest
   * @return the closest file index.
   */
  uint64_t getClosestFileIndex() const;
  bool operator<(const RAOFile &other) const;
  bool operator==(const RAOFile & other) const;
  
private:
  uint64_t m_index;
  FilePositionInfos m_filePositionInfos;
  
  /**
   * This class holds information about the 
   * cost to go to the destination file
   * @param cost the cost to go to the file located at the destinationFileIndex
   * @param destinationFileIndex the file to which we store the cost to go to
   */
  class DistanceToFile {
  public:
    DistanceToFile(const double cost, const uint64_t destinationFileIndex) :
      m_cost(cost),m_destinationFileIndex(destinationFileIndex) { }

    bool operator<(const DistanceToFile &other) const;
    /**
     * Returns the cost to go to the destination file located at the destinationFileIndex
     * @return the cost to go to the destination file
     */
    double getCost() const;
    /**
     * Get the destination file index
     * @return the destination file index
     */
    uint64_t getDestinationFileIndex() const;
  private:
    double m_cost;
    uint64_t m_destinationFileIndex;
  };
  
  std::list<DistanceToFile> m_distancesWithOtherFiles;
};

} // namespace castor::tape::tapeserver::rao
