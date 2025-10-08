/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
#pragma once

#include <memory>

#include "StatisticsService.hpp"
#include "Statistics.hpp"

namespace cta::statistics {

/**
 * This class performs statistics operations to and from a JSON file
 */
class JsonStatisticsService : public StatisticsService {
public:
  typedef std::ostream OutputStream;
  typedef std::istream InputStream;

  /**
   * Constructor of the service with a OutputStream object
   */
  explicit JsonStatisticsService(OutputStream * output);

  /**
   * Constructor of the service with the OutputStream and InputStream objects
   * @param output the OutputStream object
   * @param input the InputStream object
   */
  JsonStatisticsService(OutputStream * output, InputStream * input);

  JsonStatisticsService(const JsonStatisticsService& orig) = delete;

  ~JsonStatisticsService() final = default;

  void saveStatistics(const cta::statistics::Statistics& statistics) override;
  std::unique_ptr<cta::statistics::Statistics> getStatistics() override;
  void updateStatisticsPerTape() override;

private:
  OutputStream* m_output;
  InputStream* m_input;
};

} // namespace cta::statistics
