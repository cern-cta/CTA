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

#include "common/dataStructures/DRData.hpp"
#include "common/dataStructures/TapeFileLocation.hpp"

namespace cta {
namespace common {
namespace dataStructures {

class ArchiveFile {

public:

  /**
   * Constructor
   */
  ArchiveFile();

  /**
   * Destructor
   */
  ~ArchiveFile() throw();

  void setArchiveFileID(const std::string &archiveFileID);
  std::string getArchiveFileID() const;

  void setChecksumType(const std::string &checksumType);
  std::string getChecksumType() const;

  void setChecksumValue(const std::string &checksumValue);
  std::string getChecksumValue() const;

  void setDrData(const cta::common::dataStructures::DRData &drData);
  cta::common::dataStructures::DRData getDrData() const;

  void setEosFileID(const std::string &eosFileID);
  std::string getEosFileID() const;

  void setFileSize(const uint64_t fileSize);
  uint64_t getFileSize() const;

  void setStorageClass(const std::string &storageClass);
  std::string getStorageClass() const;

  void setTapeCopies(const std::map<int,cta::common::dataStructures::TapeFileLocation> &tapeCopies);
  std::map<int,cta::common::dataStructures::TapeFileLocation> getTapeCopies() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_archiveFileID;
  bool m_archiveFileIDSet;

  std::string m_checksumType;
  bool m_checksumTypeSet;

  std::string m_checksumValue;
  bool m_checksumValueSet;

  cta::common::dataStructures::DRData m_drData;
  bool m_drDataSet;

  std::string m_eosFileID;
  bool m_eosFileIDSet;

  uint64_t m_fileSize;
  bool m_fileSizeSet;

  std::string m_storageClass;
  bool m_storageClassSet;

  std::map<int,cta::common::dataStructures::TapeFileLocation> m_tapeCopies;
  bool m_tapeCopiesSet;

}; // class ArchiveFile

} // namespace dataStructures
} // namespace common
} // namespace cta
