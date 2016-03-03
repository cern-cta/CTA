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

#include "common/dataStructures/UserIdentity.hpp"

#include <list>
#include <map>
#include <ostream>
#include <stdint.h>
#include <string>

namespace cta {
namespace common {
namespace dataStructures {

class EntryLog {

public:

  /**
   * Constructor
   */
  EntryLog();

  /**
   * Destructor
   */
  ~EntryLog() throw();

  /**
   * Equality operator.
   *
   * @param rhs The right hand side of the operator.
   */
  bool operator==(const EntryLog &rhs) const;

  void setHost(const std::string &host);
  std::string getHost() const;

  void setTime(const time_t &time);
  time_t getTime() const;

  void setUser(const cta::common::dataStructures::UserIdentity &user);
  cta::common::dataStructures::UserIdentity getUser() const;

private:
  
  /**
   * @return true if all fields have been set, false otherwise
   */
  bool allFieldsSet() const;

  std::string m_host;
  bool m_hostSet;

  time_t m_time;
  bool m_timeSet;

  cta::common::dataStructures::UserIdentity m_user;
  bool m_userSet;

  friend std::ostream &operator<<(std::ostream &, const EntryLog &);

}; // class EntryLog

/**
 * Output stream operator for EntryLog.
 *
 * @param os The output stream.
 * @param entryLog The entry log.
 * @return The output stream.
 */
std::ostream &operator<<(std::ostream &os, const EntryLog &entryLog);

} // namespace dataStructures
} // namespace common
} // namespace cta
