/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include "CostHeuristic.hpp"

namespace castor::tape::tapeserver::rao {

/**
 * This class implements the CTA CostHeuristic that is documented here:
 * https://codimd.web.cern.ch/3adcp34cTqiv7tZJHpdxgA#
 */
class CTACostHeuristic : public CostHeuristic {
public:
  CTACostHeuristic() = default;
  ~CTACostHeuristic() final = default;

  /**
   *
   * The coefficients used by this method are documented here :
   * https://codimd.web.cern.ch/3adcp34cTqiv7tZJHpdxgA#Cost-Coefficients-calculated-by-Germ%C3%A1n-for-LTO-7M-media
   */
  double getCost(const FilePositionInfos& file1, const FilePositionInfos& file2) const override;
};

}  // namespace castor::tape::tapeserver::rao
