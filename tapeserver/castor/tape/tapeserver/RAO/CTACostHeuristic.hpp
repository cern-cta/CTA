/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2021 CERN
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

#include "CostHeuristic.hpp"

namespace castor { namespace tape { namespace tapeserver { namespace rao {

/**
 * This class implements the CTA CostHeuristic that is documented here:
 * https://codimd.web.cern.ch/3adcp34cTqiv7tZJHpdxgA#
 */
class CTACostHeuristic : public CostHeuristic{
public:
  CTACostHeuristic();
  /**
   * 
   * The coefficients used by this method are 
   * documented here : https://codimd.web.cern.ch/3adcp34cTqiv7tZJHpdxgA#Cost-Coefficients-calculated-by-Germ%C3%A1n-for-LTO-7M-media
   */
  double getCost(const FilePositionInfos & file1, const FilePositionInfos & file2) const override;
  virtual ~CTACostHeuristic();

};

}}}}
