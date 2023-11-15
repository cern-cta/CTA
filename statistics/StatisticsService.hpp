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

#include <memory>

#include "Statistics.hpp"

namespace cta::statistics {

/**
 * This service class allows to access CTA statistics and do operations with it
 */
class StatisticsService {
 public:
  StatisticsService();
  StatisticsService(const StatisticsService& orig) = delete;
  StatisticsService & operator=(const StatisticsService &) = delete;
  virtual ~StatisticsService();

  /**
   * Update the TAPE statistics of CTA
   */
  virtual void updateStatisticsPerTape() = 0;
  /**
   * Save the statistics
   * @param statistics the statistics to save
   */
  virtual void saveStatistics(const cta::statistics::Statistics &statistics) = 0;
  /**
   * Get the statistics
   * @return the statistics
   */
  virtual std::unique_ptr<cta::statistics::Statistics> getStatistics() = 0;

  /**
   * Returns the number of TAPE updated by the updateStatistics() method
   * @return the number of TAPE updated by the updateStatistics() method
   */
  uint64_t getNbUpdatedTapes();

 protected:
  uint64_t m_nbUpdatedTapes = 0;
};

} // namespace cta::statistics
