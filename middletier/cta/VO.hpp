/**
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

#include "cta/ConfigurationItem.hpp"

#include <string>

namespace cta {

/**
 * Class representing a virtual organisation.
 */
class VO: public ConfigurationItem {
public:

  /**
   * Constructor.
   */
  VO();

  /**
   * Destructor.
   */
  ~VO() throw();

  /**
   * Constructor.
   *
   * @param name The name of the virtual organisation.
   * @param creator The identity of the user that created this configuration
   * item.
   * @param comment The comment describing this configuration item.
   * @param creationTime Optionally the absolute time at which this
   * configuration item was created.  If no value is given then the current
   * time is used.
   */
  VO(
    const std::string &name,
    const UserIdentity &creator,
    const std::string &comment,
    const time_t creationTime = time(NULL));

  /**
   * Returns the name of the tape pool.
   *
   * @return The name of the tape pool.
   */
  const std::string &getName() const throw();

private:

  /**
   * The name of the tape pool.
   */
  std::string m_name;

}; // class VO

} // namespace cta
