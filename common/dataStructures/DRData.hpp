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

class DRData {

public:

  /**
   * Constructor
   */
  DRData();

  /**
   * Destructor
   */
  ~DRData() throw();

  void setDrBlob(const std::string &drBlob);
  std::string getDrBlob() const;

  void setDrGroup(const std::string &drGroup);
  std::string getDrGroup() const;

  void setDrInstance(const std::string &drInstance);
  std::string getDrInstance() const;

  void setDrOwner(const std::string &drOwner);
  std::string getDrOwner() const;

  void setDrPath(const std::string &drPath);
  std::string getDrPath() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_drBlob;
  bool m_drBlobSet;

  std::string m_drGroup;
  bool m_drGroupSet;

  std::string m_drInstance;
  bool m_drInstanceSet;

  std::string m_drOwner;
  bool m_drOwnerSet;

  std::string m_drPath;
  bool m_drPathSet;

}; // class DRData

} // namespace dataStructures
} // namespace common
} // namespace cta
