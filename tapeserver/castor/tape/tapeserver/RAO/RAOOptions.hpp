/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2019  CERN
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

#pragma once

#include <string>
#include <map>
#include <vector>

namespace castor { namespace tape { namespace tapeserver { namespace rao {

class RAOOptions {
public:
  
  enum CostHeuristicType{
    cta
  };
  
  enum FilePositionEstimatorType {
    interpolation
  };
  
  RAOOptions();
  RAOOptions(const std::string & options);
  
  CostHeuristicType getCostHeuristicType();
  
  FilePositionEstimatorType getFilePositionEstimatorType();
  
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

}}}}