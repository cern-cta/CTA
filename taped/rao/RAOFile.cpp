/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "RAOFile.hpp"

#include <algorithm>

namespace castor::tape::tapeserver::rao {

void RAOFile::addDistanceToFile(const double distance, const RAOFile& file) {
  m_distancesWithOtherFiles.push_back(DistanceToFile(distance, file.getIndex()));
}

uint64_t RAOFile::getClosestFileIndex() const {
  //The closest file is the one that has the lower cost
  auto minElementItor = std::min_element(m_distancesWithOtherFiles.begin(), m_distancesWithOtherFiles.end());
  //This method should never throw as there is always at least two files in a RAO batch
  return minElementItor->getDestinationFileIndex();
}

}  // namespace castor::tape::tapeserver::rao
