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

#include "StatisticsService.hpp"
#include "Statistics.hpp"

namespace cta {
namespace statistics {

  /**
   * This class is a JSON statistics service
   * allowing to perform statistics operation
   * to and from a JSON file
   */
class JsonStatisticsService: public StatisticsService {
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

  void saveStatistics(const cta::statistics::Statistics& statistics) override;
  std::unique_ptr<cta::statistics::Statistics> getStatistics() override;
  void updateStatisticsPerTape() override;

  virtual ~JsonStatisticsService();

 private:
  OutputStream * m_output;
  InputStream * m_input;
};

}  // namespace statistics
}  // namespace cta
