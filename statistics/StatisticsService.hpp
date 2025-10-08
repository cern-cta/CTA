/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
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
  StatisticsService() = default;
  virtual ~StatisticsService() = default;
  StatisticsService(const StatisticsService& orig) = delete;
  StatisticsService& operator=(const StatisticsService&) = delete;

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
  uint64_t getNbUpdatedTapes() const { return m_nbUpdatedTapes; }

protected:
  uint64_t m_nbUpdatedTapes = 0;
};

} // namespace cta::statistics
