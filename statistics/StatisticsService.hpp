/*
 * @project        The CERN Tape Archive (CTA)
 * @copyright      Copyright(C) 2015-2021 CERN
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

#include <memory>

#include "Statistics.hpp"

namespace cta { namespace statistics {

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

}  // namespace statistics
}  // namespace cta
