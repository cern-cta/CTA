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

#pragma once

#include "FilePositionInfos.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

class CostHeuristic {
public:
  /**
   * Compute the cost for going from the end of file1 to the beginning of file2
   * @param file1 the file from which we will go from it's end position
   * @param file2 the file to which we will arrive (beginning position)
   * @return the value it costs for going from the end of file1 to the beginning of file2
   */
  virtual double getCost(const FilePositionInfos & file1, const FilePositionInfos & file2) const = 0;
  virtual ~CostHeuristic();
private:

};

}}}}