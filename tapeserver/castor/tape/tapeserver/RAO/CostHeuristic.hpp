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

#include "tapeserver/castor/tape/tapeserver/RAO/FilePositionInfos.hpp"

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
