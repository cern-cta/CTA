/*
 * SPDX-FileCopyrightText: 2021 CERN
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

#pragma once

#include <string>
#include <map>
#include <vector>

namespace castor::tape::tapeserver::rao {

/**
 * This class allows to manage the RAO options given
 * by the tapeserver configuration RAOLTOAlgorithmOptions
 */
class RAOOptions {
public:
  /**
   * Existing CostHeuristic type
   */
  enum class CostHeuristicType{ cta };

  /**
   * Existing FilePositionEstimator type
   */
  enum class FilePositionEstimatorType { interpolation };

  /**
   * Default constructor
   */
  RAOOptions() = default;

  /**
   * Constructor
   *
   * @param options    string in the format option1Name:value1,option2Name:value2,[...],optionNName:valueN
   */
  explicit RAOOptions(std::string_view options);

  /**
   * Destructor
   */
  virtual ~RAOOptions() = default;

  /**
   * Returns the cost heuristic type from the RAO option string passed in the constructor
   * @return the cost heuristic type
   * @throws cta::exception::Exception if the CostHeuristicType is unknown or if it cannot be fetched
   */
  CostHeuristicType getCostHeuristicType() const;

  /**
   * Returns the file position estimator type from the RAO option string passed in the constructor
   * !!! WARNING !!! For now it only returns the interpolation FilePositionEstimatorType, this method
   * should be modified if we want to get the estimator from the options
   * @return the file position estimator type
   * @throws cta::exception::Exception if the FilePositionEstimatorType is unknown or if it cannot be fetched
   */
  FilePositionEstimatorType getFilePositionEstimatorType() const;

  /**
   * Returns the RAOLTOAlgorithmOptions
   * @return
   */
  const std::string& getOptionsString() const { return m_options; };

private:
  std::string m_options;
  std::vector<std::string> m_allOptions;

  /**
   * Returns the boolean value of the option
   * whose name is passed in parameter
   * @param name the name of the option to get its boolean value
   * @return the boolean value of the option.
   */
  bool getBooleanValue(const std::string & name) const;
  /**
   * Returns the string value of the option
   * whose name is passed in parameter
   * @param name the name of the option to get its string value
   * @return the string value of the option
   */
  std::string getStringValue(const std::string & name) const;

  static std::map<std::string, CostHeuristicType> c_mapStringCostHeuristicType;
};

} // namespace castor::tape::tapeserver::rao
