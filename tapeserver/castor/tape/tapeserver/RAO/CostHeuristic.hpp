/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include "FilePositionInfos.hpp"

namespace castor::tape::tapeserver::rao {

class CostHeuristic {
public:
  /**
   * Compute the cost for going from the end of file1 to the beginning of file2
   * @param file1 the file from which we will go from it's end position
   * @param file2 the file to which we will arrive (beginning position)
   * @return the value it costs for going from the end of file1 to the beginning of file2
   */
  virtual double getCost(const FilePositionInfos & file1, const FilePositionInfos & file2) const = 0;
  virtual ~CostHeuristic() = default;
};

} // namespace castor::tape::tapeserver::rao
