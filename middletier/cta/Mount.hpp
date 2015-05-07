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

#include <string>

namespace cta {

/**
 * Abstract class representing a tape mount.
 */
class Mount {
public:

  /**
   * Constructor.
   *
   * @param id The identifier of the mount.
   * @param vid The volume idenitifier of the tape to be mounted.
   */
  Mount(const std::string &id, const std::string &vid);

  /**
   * Destructor.
   */
  virtual ~Mount() throw() = 0;

  /**
   * Returns the identiifer of the mount.
   */
  const std::string &getId() const throw();

  /**
   * Returns the volume identiifer of the mount.
   */
  const std::string &getVid() const throw();

private:

  /**
   * The identiifer of the mount.
   */
  std::string m_id;

  /**
   * The volume identiifer of the tape to be mounted.
   */
  std::string m_vid;

}; // class Mount

} // namespace cta
