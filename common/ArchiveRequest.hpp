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

#include <stdint.h>
#include <string>

namespace cta {

/**
 * Structure to store an archive request.
 */
struct ArchiveRequest {

public:
  
  /**
   * Constructor
   */
  ArchiveRequest();
  
  /**
   * Destructor
   */
  ~ArchiveRequest() throw();
  
  void setDrBlob(std::string drBlob);
  std::string getDrBlob() const;
  void setDrGroup(std::string drGroup);
  std::string getDrGroup() const;
  void setDrOwner(std::string drOwner);
  std::string getDrOwner() const;
  void setDrPath(std::string drPath);
  std::string getDrPath() const;
  void setDrInstance(std::string drInstance);
  std::string getDrInstance() const;
  void setStorageClass(std::string storageClass);
  std::string getStorageClass() const;
  void setChecksumValue(std::string checksumValue);
  std::string getChecksumValue() const;
  void setChecksumType(std::string checksumType);
  std::string getChecksumType() const;
  void setFileSize(uint64_t fileSize);
  uint64_t getFileSize() const;
  void setSrcURL(std::string srcURL);
  std::string getSrcURL() const;
  
private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;
  
  /**
   * The EOS src URL.
   */
  std::string m_srcURL;
  bool m_srcURLSet;

  /**
   * The size of the file to be archived in bytes.
   */
  uint64_t m_fileSize;
  bool m_fileSizeSet;

  /**
   * The checksum type.
   */
  std::string m_checksumType;
  bool m_checksumTypeSet;

  /**
   * The checksum value.
   */
  std::string m_checksumValue;
  bool m_checksumValueSet;
  
  /**
   * The storage class name.
   */
  std::string m_storageClass;
  bool m_storageClassSet;
  
  /**
   * The disaster recovery EOS instance.
   */
  std::string m_drInstance;
  bool m_drInstanceSet;

  /**
   * The disaster recovery EOS path.
   */
  std::string m_drPath;
  bool m_drPathSet;

  /**
   * The disaster recovery EOS owner.
   */
  std::string m_drOwner;
  bool m_drOwnerSet;

  /**
   * The disaster recovery EOS group.
   */
  std::string m_drGroup;
  bool m_drGroupSet;
  
  /**
   * The disaster recovery EOS key-value string containing everything above and more (no parsing by CTA).
   */
  std::string m_drBlob;
  bool m_drBlobSet;

}; // struct ArchiveRequest

} // namespace cta
