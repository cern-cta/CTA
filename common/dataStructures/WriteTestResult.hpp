/*
 * The CERN Tape Archive (CTA) project
 * Copyright (C) 2015  CERN
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

#include <list>
#include <map>
#include <stdint.h>
#include <string>


namespace cta {
namespace common {
namespace dataStructures {

class WriteTestResult {

public:

  /**
   * Constructor
   */
  WriteTestResult();

  /**
   * Destructor
   */
  ~WriteTestResult() throw();

  void setChecksums(const std::map<int,std::pair<std::string,std::string>> &checksums);
  std::map<int,std::pair<std::string,std::string>> getChecksums() const;

  void setDriveName(const std::string &driveName);
  std::string getDriveName() const;

  void setErrors(const std::map<int,std::string> &errors);
  std::map<int,std::string> getErrors() const;

  void setNoOfFilesWritten(const uint64_t noOfFilesWritten);
  uint64_t getNoOfFilesWritten() const;

  void setTotalBytesWritten(const uint64_t totalBytesWritten);
  uint64_t getTotalBytesWritten() const;

  void setTotalFilesWritten(const uint64_t totalFilesWritten);
  uint64_t getTotalFilesWritten() const;

  void setTotalTimeInSeconds(const uint64_t totalTimeInSeconds);
  uint64_t getTotalTimeInSeconds() const;

  void setVid(const std::string &vid);
  std::string getVid() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::map<int,std::pair<std::string,std::string>> m_checksums;
  bool m_checksumsSet;

  std::string m_driveName;
  bool m_driveNameSet;

  std::map<int,std::string> m_errors;
  bool m_errorsSet;

  uint64_t m_noOfFilesWritten;
  bool m_noOfFilesWrittenSet;

  uint64_t m_totalBytesWritten;
  bool m_totalBytesWrittenSet;

  uint64_t m_totalFilesWritten;
  bool m_totalFilesWrittenSet;

  uint64_t m_totalTimeInSeconds;
  bool m_totalTimeInSecondsSet;

  std::string m_vid;
  bool m_vidSet;

}; // class WriteTestResult

} // namespace dataStructures
} // namespace common
} // namespace cta
