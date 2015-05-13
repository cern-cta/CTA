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
 * Class repsenting the location of a file on a tape.
 */
class TapeFileLocation {
public:

  /**
   * Constructor.
   */
  TapeFileLocation();

  /**
   * Constructor.
   *
   * @param vid The volume identifier of the tape.
   * @param fseq The sequence number of the file.
   * @param blockId The block identifier of the file.
   */
  TapeFileLocation(const std::string &vid, const uint64_t fseq,
    const uint64_t blockId);

  /**
   * Returns the volume identifier of the tape.
   *
   * @return The volume identifier of the tape.
   */
  const std::string &getVid() const throw();

  /**
   * Returns the sequence number of the file.
   *
   * @return The sequence number of the file.
   */
  uint64_t getFseq() const throw();

  /**
   * Returns the block identifier of the file.
   *
   * @return The block identifier of the file.
   */
  uint64_t getBlockId() const throw();

private:

  /**
   * The volume identifier of the tape.
   */
  std::string m_vid;

  /**
   * The sequence number of the file.
   */
  uint64_t m_fseq;

  /**
   * The block identifier of the file.
   */
  uint64_t m_blockId;

}; // class TapeFileLocation

} // namepsace cta
