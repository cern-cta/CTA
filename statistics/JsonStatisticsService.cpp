/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#include "JsonStatisticsService.hpp"

namespace cta::statistics {

JsonStatisticsService::JsonStatisticsService(OutputStream * output)
  : m_output(output), m_input(nullptr) {
}

JsonStatisticsService::JsonStatisticsService(OutputStream * output, InputStream * input)
  : m_output(output), m_input(input) {
}

void JsonStatisticsService::saveStatistics(const cta::statistics::Statistics& statistics) {
  *m_output << statistics;
}

std::unique_ptr<cta::statistics::Statistics> JsonStatisticsService::getStatistics() {
  throw cta::exception::Exception("In JsonStatisticsService::getStatistics(), method not implemented.");
}

void JsonStatisticsService::updateStatisticsPerTape() {
  throw cta::exception::Exception("In JsonStatistics::updateStatisticsPerTape(), method not implemented.");
}

} // namespace cta::statistics
