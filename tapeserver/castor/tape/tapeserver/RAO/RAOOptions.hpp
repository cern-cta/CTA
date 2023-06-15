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

#include <string>
#include <map>
#include <vector>

namespace castor {
namespace tape {
namespace tapeserver {
namespace rao {

/**
 * This class allows to manage the RAO options given
 * by the tapeserver configuration RAOLTOAlgorithmOptions
 */
class RAOOptions {
public:
  /**
   * Existing CostHeuristic type
   */
  enum CostHeuristicType { cta };

  /**
   * Existing FilePositionEstimator type
   */
  enum FilePositionEstimatorType { interpolation };

  /**
   * Default constructor
   */
  RAOOptions();

  /**
   * Constructor of a RAOOptions instance
   * @param options the option string that should be under the format option1Name:value1,option2Name:value2,[...],optionNName:valueN
   */
  RAOOptions(const std::string& options);

  /**
   * Returns the cost heuristic type from the RAO option string passed in the constructor
   * @return the cost heuristic type
   * @throws cta::exception::Exception if the CostHeuristicType is unknown or if it cannot be fetched
   */
  CostHeuristicType getCostHeuristicType();

  /**
   * Returns the file position estimator type from the RAO option string passed in the constructor
   * !!! WARNING !!! For now it only returns the interpolation FilePositionEstimatorType, this method
   * should be modified if we want to get the estimator from the options
   * @return the file position estimator type
   * @throws cta::exception::Exception if the FilePositionEstimatorType is unknown or if it cannot be fetched
   */
  FilePositionEstimatorType getFilePositionEstimatorType();

  /**
   * Returns the RAOLTOAlgorithmOptions
   * @return 
   */
  std::string getOptionsString();

  virtual ~RAOOptions();

private:
  std::string m_options;
  std::vector<std::string> m_allOptions;

  /**
   * Returns the boolean value of the option
   * whose name is passed in parameter
   * @param name the name of the option to get its boolean value
   * @return the boolean value of the option.
   */
  bool getBooleanValue(const std::string& name) const;
  /**
   * Returns the string value of the option
   * whose name is passed in parameter
   * @param name the name of the option to get its string value
   * @return the string value of the option
   */
  std::string getStringValue(const std::string& name) const;

  static std::map<std::string, CostHeuristicType> c_mapStringCostHeuristicType;
};

}  // namespace rao
}  // namespace tapeserver
}  // namespace tape
}  // namespace castor