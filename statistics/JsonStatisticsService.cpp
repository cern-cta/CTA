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


JsonStatisticsService::~JsonStatisticsService() {
}

} // namespace cta::statistics
