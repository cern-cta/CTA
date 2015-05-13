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

#include "middletier/interface/Mount.hpp"

#include <stdint.h>

namespace cta {

/**
 * Class representing an archival mount.
 */
class ArchivalMount: public Mount {
public:

  /**
   * Constructor.
   *
   * @param id The identifier of the mount.
   * @param vid The volume idenitifier of the tape to be mounted.
   * @param nbFilesOnTapeBeforeMount The number of files on the tape before the
   * mount.
   */
  ArchivalMount(const std::string &id, const std::string &vid,
    const uint64_t nbFilesOnTapeBeforeMount);

  /**
   * Destructor.
   */
  ~ArchivalMount() throw();

  /**
   * Returns the number of files on the tape before the mount.
   */
  uint64_t getNbFilesOnTapeBeforeMount() const throw();

private:

  /**
   * The number of files on the tape before the mount.
   */
  uint64_t m_nbFilesOnTapeBeforeMount;

}; // class ArchivalMount

} // namespace cta
