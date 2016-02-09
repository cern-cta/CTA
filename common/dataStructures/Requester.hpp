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
namespace dataStructures {

class Requester {

public:

  /**
   * Constructor
   */
  Requester();

  /**
   * Destructor
   */
  ~Requester() throw();

  void setGroupName(const std::string &groupName);
  std::string getGroupName() const;

  void setUserName(const std::string &userName);
  std::string getUserName() const;
  

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_groupName;
  bool m_groupNameSet;

  std::string m_userName;
  bool m_userNameSet;

}; // class Requester

} // namespace dataStructures
} // namespace cta
