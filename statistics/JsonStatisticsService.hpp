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
