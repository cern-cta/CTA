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
struct TapeCopyInfo {

  /**
   * Constructor.
   */
  TapeCopyInfo();

  /**
   * Constructor.
   *
   * @param vid The volume identifier of the tape.
   * @param fseq The sequence number of the file.
   * @param blockId The block identifier of the file.
   */
  TapeCopyInfo(const std::string &vid, const uint64_t fseq,
    const uint64_t blockId);
  
  /**
   * The path of the archive file.
   */
  std::string archiveFilePath;
  
  /**
   * The ID of the file
   */
  uint64_t fileId;

  /**
   * The volume identifier of the tape.
   */
  std::string vid;

  /**
   * The sequence number of the file.
   */
  uint64_t fseq;

  /**
   * The block identifier of the file.
   */
  uint64_t blockId;
  
  /**
   * The hostname of the nameserver holding the file
   */
  std::string nsHostName;
  
  /**
   * The copy number for this tape copy
   */
  uint16_t copyNumber;

}; // struct TapeCopyLocation

} // namepsace cta
