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

#include <string>

namespace cta {

/**
 * The path of a file in a remote storage system of the form
 * "scheme://path".
 */
class RemotePath {
public:

  /**
   * Constructor.
   */
  RemotePath();

  /**
   * Constructor.
   *
   * @param raw The raw path in the form "scheme:hierarchical_part".
   */
  RemotePath(const std::string &raw);

  /**
   * Equals operator.
   */
  bool operator==(const RemotePath &rhs) const;

  /**
   * Returns the raw path in the form "scheme:hierarchical_part".
   */
  const std::string &getRaw() const throw();

  /**
   * Returns the scheme part of the remote path.
   */
  const std::string &getScheme() const throw();

private:

  /**
   * The raw path in the form "scheme:hierarchical_part".
   */
  std::string m_raw;

  /**
   * The scheme part of the remote path.
   */
  std::string m_scheme;

}; // class RemotePath

} // namespace cta
