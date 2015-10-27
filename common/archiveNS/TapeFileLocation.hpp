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
 * The location of a tape copy.
 */
struct TapeFileLocation {

  /**
   * Constructor.
   */
  TapeFileLocation();

  /**
   * Constructor.
   *
   * @param fSeq The sequence number of the file.
   * @param blockId The block identifier of the file.
   * @param vid The vid of the tape containing the file. TODO: to be put in the mount object in the future
   * @param copyNb The copy number of the tape file. TODO: to be put in the mount object in the future
   */
  TapeFileLocation(const uint64_t fSeq, const uint64_t blockId, const std::string &vid, const uint8_t copyNb);

  /**
   * Equality operator.
   *
   * @param ths The right hand side of the operator.
   */
  bool operator==(const TapeFileLocation &rhs) const;

  /**
   * The sequence number of the file.
   */
  uint64_t fSeq;

  /**
   * The block identifier of the file.
   */
  uint64_t blockId;
  
  /**
   * The vid of the tape containing the file. TODO: to be put in the mount object in the future
   */
  std::string vid;
  
  /**
   * The copy number of the tape file. TODO: to be put in the mount object in the future
   */
  uint16_t copyNb;

}; // struct TapeFileLocation

} // namepsace cta
