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

#include "CostHeuristic.hpp"

namespace castor::tape::tapeserver::rao {

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

} // namespace castor::tape::tapeserver::rao
